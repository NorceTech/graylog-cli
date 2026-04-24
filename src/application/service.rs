use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use exn::ResultExt;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::Url;

use crate::application::ports::{CacheStore, ConfigStore, GraylogGateway, GraylogGatewayFactory};
use crate::domain::config::{Config, GraylogConfig};
use crate::domain::error::{CliError, HttpError, ValidationError};
use crate::domain::models::{
    AggregateCommandInput, AggregateSearchRequest, AggregateStatus, AuthStatus, FieldsStatus,
    MessageSearchRequest, MessageSearchStatus, NormalizedRow, PingStatus, SearchCommandInput,
    SearchGroup, SortDirection, StreamFindStatus, StreamStatus, StreamsStatus, SystemInfoStatus,
};

const DEFAULT_SEARCH_LIMIT: u64 = 50;
const MAX_STREAM_SEARCH_LIMIT: u64 = 100;
const DEFAULT_SEARCH_OFFSET: u64 = 0;
const DEFAULT_SEARCH_SORT: &str = "timestamp";

#[derive(Serialize, Deserialize)]
struct CachedFields {
    fields: Vec<String>,
    fetched_at: u64,
}

#[derive(Clone)]
pub struct ApplicationService {
    config_store: Arc<dyn ConfigStore>,
    gateway_factory: Arc<dyn GraylogGatewayFactory>,
    fields_cache_store: Arc<dyn CacheStore>,
}

impl ApplicationService {
    pub fn new(
        config_store: Arc<dyn ConfigStore>,
        gateway_factory: Arc<dyn GraylogGatewayFactory>,
        fields_cache_store: Arc<dyn CacheStore>,
    ) -> Self {
        Self {
            config_store,
            gateway_factory,
            fields_cache_store,
        }
    }

    pub async fn authenticate(
        &self,
        base_url: Url,
        token: secrecy::SecretString,
    ) -> exn::Result<AuthStatus, CliError> {
        let trimmed_token = token.expose_secret().trim().to_owned();
        if trimmed_token.is_empty() {
            return Err(CliError::Validation(ValidationError::EmptyField {
                field: "graylog.token",
            })
            .into());
        }

        let graylog_config = GraylogConfig::new(base_url.clone(), token);
        let config = Config {
            graylog: graylog_config,
        };

        self.config_store
            .save(config)
            .await
            .or_raise(|| CliError::Config("failed to persist config".to_string()))?;

        Ok(AuthStatus::ok(base_url.to_string()))
    }

    pub async fn search(
        &self,
        input: SearchCommandInput,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let mut input = input;

        if input.all_fields && input.fields.is_empty() {
            let config = self
                .config_store
                .load()
                .await
                .or_raise(|| CliError::Config("failed to load runtime config".to_string()))?
                .ok_or_else(|| {
                    CliError::Config(
                        "graylog is not configured, run `graylog-cli auth` first".to_string(),
                    )
                })?;
            let ttl = config.graylog.fields_cache_ttl_seconds;
            let cache_key = "fields".to_string();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let fields = match self
                .fields_cache_store
                .get_serialized(&cache_key)
                .await
                .ok()
                .flatten()
                .and_then(|serialized| serde_json::from_str::<CachedFields>(&serialized).ok())
            {
                Some(cached) if now.saturating_sub(cached.fetched_at) < ttl => cached.fields,
                _ => {
                    let client = self.graylog_gateway_with_config(config.graylog)?;
                    let result = client.list_fields().await.or_raise(|| {
                        CliError::Http(HttpError::Unavailable {
                            message: "failed to list fields".to_string(),
                        })
                    })?;
                    let cache_data = CachedFields {
                        fields: result.fields.clone(),
                        fetched_at: now,
                    };
                    if let Ok(serialized) = serde_json::to_string(&cache_data) {
                        let _ = self
                            .fields_cache_store
                            .save_serialized(cache_key, serialized)
                            .await;
                    }
                    result.fields
                }
            };

            input.fields = fields;
        }

        if let Some(ref group_by) = input.group_by
            && !input.fields.contains(group_by)
        {
            input.fields.push(group_by.clone());
        }

        let group_by = input.group_by.clone();
        let mut status = if input.all_pages {
            self.execute_paginated_search(&input).await?
        } else {
            self.execute_message_search(
                "search",
                self.build_search_request(input, DEFAULT_SEARCH_LIMIT),
            )
            .await?
        };

        if let Some(group_by) = group_by.as_deref() {
            status = apply_grouping(status, group_by);
        }

        Ok(status)
    }

    pub async fn aggregate(
        &self,
        input: AggregateCommandInput,
    ) -> exn::Result<AggregateStatus, CliError> {
        self.execute_aggregate("aggregate", self.build_aggregate_request(input))
            .await
    }

    pub async fn count_by_level(
        &self,
        input: AggregateCommandInput,
    ) -> exn::Result<AggregateStatus, CliError> {
        self.execute_aggregate("count-by-level", self.build_aggregate_request(input))
            .await
    }

    pub async fn streams_list(&self) -> exn::Result<StreamsStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.list_streams().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "failed to list streams".to_string(),
            })
        })?;

        Ok(StreamsStatus {
            ok: true,
            command: "streams.list",
            streams: result.streams,
        })
    }

    pub async fn streams_show(&self, stream_id: &str) -> exn::Result<StreamStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.get_stream(stream_id).await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: format!("failed to get stream `{stream_id}`"),
            })
        })?;

        Ok(StreamStatus {
            ok: true,
            command: "streams.show",
            stream: result.stream,
        })
    }

    pub async fn streams_find(&self, name: &str) -> exn::Result<StreamFindStatus, CliError> {
        let name = name.trim();

        if name.is_empty() {
            return Err(CliError::Validation(ValidationError::EmptyField { field: "name" }).into());
        }

        let client = self.graylog_gateway().await?;
        let result = client.list_streams().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "failed to list streams".to_string(),
            })
        })?;
        let needle = name.to_lowercase();
        let streams = result
            .streams
            .into_iter()
            .filter(|stream| {
                stream
                    .get("title")
                    .or_else(|| stream.get("name"))
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_lowercase().contains(&needle))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        Ok(StreamFindStatus {
            ok: true,
            command: "streams.find",
            name: name.to_string(),
            returned: streams.len(),
            streams,
        })
    }

    pub async fn streams_search(
        &self,
        input: SearchCommandInput,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let request = self.build_stream_search_request(input)?;
        self.execute_stream_message_search("streams.search", request)
            .await
    }

    pub async fn streams_last_event(
        &self,
        stream_id: String,
        timerange: Option<crate::domain::timerange::CommandTimerange>,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let request = self.build_stream_search_request(SearchCommandInput {
            query: "*".to_string(),
            timerange,
            fields: Vec::new(),
            limit: Some(1),
            offset: Some(DEFAULT_SEARCH_OFFSET),
            sort: Some(DEFAULT_SEARCH_SORT.to_string()),
            sort_direction: Some(SortDirection::Desc),
            group_by: None,
            all_pages: false,
            all_fields: false,
            streams: vec![stream_id],
        })?;

        self.execute_stream_message_search("streams.last-event", request)
            .await
    }

    pub async fn system_info(&self) -> exn::Result<SystemInfoStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.system_info().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "failed to get system info".to_string(),
            })
        })?;

        Ok(SystemInfoStatus {
            ok: true,
            command: "system.info",
            system: result.system,
        })
    }

    pub async fn fields(&self) -> exn::Result<FieldsStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.list_fields().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "failed to list fields".to_string(),
            })
        })?;
        Ok(FieldsStatus {
            ok: true,
            command: "fields",
            fields: result.fields.clone(),
            total: result.fields.len(),
        })
    }

    pub async fn ping(&self) -> exn::Result<PingStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let graylog_url = client.base_url().to_string();

        client.ping().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "Graylog is unreachable".to_string(),
            })
        })?;

        Ok(PingStatus {
            ok: true,
            command: "ping",
            reachable: true,
            graylog_url,
        })
    }

    fn build_search_request(
        &self,
        input: SearchCommandInput,
        default_limit: u64,
    ) -> MessageSearchRequest {
        MessageSearchRequest {
            query: input.query,
            timerange: input.timerange,
            fields: input.fields,
            limit: input.limit.unwrap_or(default_limit),
            offset: input.offset.unwrap_or(DEFAULT_SEARCH_OFFSET),
            sort: input
                .sort
                .unwrap_or_else(|| DEFAULT_SEARCH_SORT.to_string()),
            sort_direction: input.sort_direction.unwrap_or(SortDirection::Desc),
            streams: input.streams,
        }
    }

    fn build_aggregate_request(&self, input: AggregateCommandInput) -> AggregateSearchRequest {
        AggregateSearchRequest {
            query: input.query,
            timerange: input.timerange,
            aggregation_type: input.aggregation_type,
            field: input.field,
            size: input.size,
            interval: input.interval,
            streams: input.streams,
        }
    }

    fn build_stream_search_request(
        &self,
        input: SearchCommandInput,
    ) -> exn::Result<MessageSearchRequest, CliError> {
        if input.streams.len() != 1 {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "stream_id",
                message: "exactly one stream id is required".to_string(),
            })
            .into());
        }

        let mut request = self.build_search_request(input, DEFAULT_SEARCH_LIMIT);
        request.limit = request.limit.min(MAX_STREAM_SEARCH_LIMIT);
        request.sort = DEFAULT_SEARCH_SORT.to_string();
        request.sort_direction = SortDirection::Desc;

        Ok(request)
    }

    async fn execute_message_search(
        &self,
        command: &'static str,
        request: MessageSearchRequest,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let config = self
            .config_store
            .load()
            .await
            .or_raise(|| CliError::Config("failed to load runtime config".to_string()))?
            .ok_or_else(|| {
                CliError::Config(
                    "graylog is not configured, run `graylog-cli auth` first".to_string(),
                )
            })?;
        let client = self.graylog_gateway_with_config(config.graylog)?;
        let result = client.search_messages(request.clone()).await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "message search failed".to_string(),
            })
        })?;
        let mut metadata = result.metadata;

        if let Some(total_results) = result.total_results {
            metadata.insert("total_results".to_string(), json!(total_results));
        }

        Ok(MessageSearchStatus {
            ok: true,
            command,
            query: request.query,
            returned: result.messages.len(),
            messages: result.messages,
            grouped_by: None,
            groups: None,
            metadata,
        })
    }

    async fn execute_paginated_search(
        &self,
        input: &SearchCommandInput,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let config = self
            .config_store
            .load()
            .await
            .or_raise(|| CliError::Config("failed to load runtime config".to_string()))?
            .ok_or_else(|| {
                CliError::Config(
                    "graylog is not configured, run `graylog-cli auth` first".to_string(),
                )
            })?;
        let client = self.graylog_gateway_with_config(config.graylog)?;
        let mut request = self.build_search_request(input.clone(), DEFAULT_SEARCH_LIMIT);
        let mut all_messages = Vec::new();
        let mut metadata = serde_json::Map::new();
        let mut total_results = None;

        request.limit = 500;
        request.offset = 0;

        loop {
            let result = client.search_messages(request.clone()).await.or_raise(|| {
                CliError::Http(HttpError::Unavailable {
                    message: "message search failed".to_string(),
                })
            })?;

            if metadata.is_empty() {
                metadata = result.metadata.clone();
            }

            if total_results.is_none() {
                total_results = result.total_results;
            }

            let fetched = result.messages.len();
            all_messages.extend(result.messages);

            request.offset += fetched as u64;

            if fetched == 0
                || total_results.is_some_and(|total| request.offset >= total)
                || fetched < request.limit as usize
            {
                break;
            }
        }

        if let Some(total_results) = total_results {
            metadata.insert("total_results".to_string(), json!(total_results));
        }

        Ok(MessageSearchStatus {
            ok: true,
            command: "search",
            query: request.query,
            returned: all_messages.len(),
            messages: all_messages,
            grouped_by: None,
            groups: None,
            metadata,
        })
    }

    async fn execute_stream_message_search(
        &self,
        command: &'static str,
        request: MessageSearchRequest,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let client = self.graylog_gateway().await?;

        if let Some(stream_id) = request.streams.first() {
            client.get_stream(stream_id).await.or_raise(|| {
                CliError::Http(HttpError::Unavailable {
                    message: format!("failed to get stream `{stream_id}`"),
                })
            })?;
        }

        let result = client.search_messages(request.clone()).await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "message search failed".to_string(),
            })
        })?;
        let mut metadata = result.metadata;

        if let Some(total_results) = result.total_results {
            metadata.insert("total_results".to_string(), json!(total_results));
        }

        Ok(MessageSearchStatus {
            ok: true,
            command,
            query: request.query,
            returned: result.messages.len(),
            messages: result.messages,
            grouped_by: None,
            groups: None,
            metadata,
        })
    }

    async fn execute_aggregate(
        &self,
        command: &'static str,
        request: AggregateSearchRequest,
    ) -> exn::Result<AggregateStatus, CliError> {
        let config = self
            .config_store
            .load()
            .await
            .or_raise(|| CliError::Config("failed to load runtime config".to_string()))?
            .ok_or_else(|| {
                CliError::Config(
                    "graylog is not configured, run `graylog-cli auth` first".to_string(),
                )
            })?;
        let client = self.graylog_gateway_with_config(config.graylog)?;
        let aggregation_type = request.aggregation_type.as_cli_value();
        let result = client.search_aggregate(request).await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "aggregate search failed".to_string(),
            })
        })?;

        Ok(AggregateStatus {
            ok: true,
            command,
            aggregation_type,
            rows: result.rows,
            metadata: result.metadata,
        })
    }

    async fn graylog_gateway(&self) -> exn::Result<Arc<dyn GraylogGateway>, CliError> {
        let config = self
            .config_store
            .load()
            .await
            .or_raise(|| CliError::Config("failed to load runtime config".to_string()))?
            .ok_or_else(|| {
                CliError::Config(
                    "graylog is not configured, run `graylog-cli auth` first".to_string(),
                )
            })?;

        self.graylog_gateway_with_config(config.graylog)
    }

    fn graylog_gateway_with_config(
        &self,
        config: GraylogConfig,
    ) -> exn::Result<Arc<dyn GraylogGateway>, CliError> {
        self.gateway_factory.build_from_config(config).or_raise(|| {
            CliError::Http(HttpError::RequestBuild {
                message: "failed to build Graylog client from config".to_string(),
            })
        })
    }
}

fn apply_grouping(mut status: MessageSearchStatus, group_by: &str) -> MessageSearchStatus {
    status.grouped_by = Some(group_by.to_string());
    status.groups = Some(build_search_groups(&status.messages, group_by));
    status
}

fn build_search_groups(messages: &[NormalizedRow], group_by: &str) -> Vec<SearchGroup> {
    let mut groups: BTreeMap<String, Vec<&NormalizedRow>> = BTreeMap::new();

    for row in messages {
        let key = row
            .get(group_by)
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        groups.entry(key).or_default().push(row);
    }

    groups
        .into_iter()
        .map(|(key, rows)| SearchGroup {
            key,
            count: rows.len(),
            duration_ms: compute_group_duration(&rows),
        })
        .collect()
}

fn compute_group_duration(rows: &[&NormalizedRow]) -> Option<u64> {
    let first_ts = rows
        .first()
        .and_then(|row| row.get("timestamp").and_then(|value| value.as_str()))?;
    let last_ts = rows
        .last()
        .and_then(|row| row.get("timestamp").and_then(|value| value.as_str()))?;
    let first_millis = parse_timestamp_to_millis(first_ts)?;
    let last_millis = parse_timestamp_to_millis(last_ts)?;
    Some((last_millis - first_millis).max(0) as u64)
}

fn parse_timestamp_to_millis(ts: &str) -> Option<i64> {
    use time::format_description::well_known::Rfc3339;
    let dt = time::OffsetDateTime::parse(ts, &Rfc3339).ok()?;
    Some(dt.unix_timestamp() * 1000 + dt.millisecond() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::{Map, Value};

    use crate::application::ports::cache_store::CacheError;
    use crate::application::ports::config_store::ConfigError;
    use crate::domain::config::{DEFAULT_FIELDS_CACHE_TTL_SECONDS, DEFAULT_TIMEOUT_SECONDS};
    use crate::domain::models::{
        AggregateSearchResult, AggregationType, FieldsResult, JsonObject, MessageSearchResult,
        StreamResult, StreamsResult, SystemResult,
    };
    use crate::domain::timerange::CommandTimerange;

    #[derive(Clone)]
    struct FakeConfigStore {
        state: Arc<Mutex<Option<Config>>>,
    }

    impl FakeConfigStore {
        fn new(config: Config) -> Self {
            Self {
                state: Arc::new(Mutex::new(Some(config))),
            }
        }

        fn empty() -> Self {
            Self {
                state: Arc::new(Mutex::new(None)),
            }
        }

        fn saved_config(&self) -> Option<Config> {
            self.state
                .lock()
                .expect("config mutex should not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl ConfigStore for FakeConfigStore {
        async fn load(&self) -> exn::Result<Option<Config>, ConfigError> {
            Ok(self
                .state
                .lock()
                .expect("config mutex should not be poisoned")
                .clone())
        }

        async fn save(&self, config: Config) -> exn::Result<(), ConfigError> {
            *self
                .state
                .lock()
                .expect("config mutex should not be poisoned") = Some(config);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct FakeCacheStore {
        storage: Arc<Mutex<HashMap<String, String>>>,
    }

    impl FakeCacheStore {
        fn get(&self, key: &str) -> Option<String> {
            self.storage
                .lock()
                .expect("cache mutex should not be poisoned")
                .get(key)
                .cloned()
        }

        fn insert(&self, key: &str, value: String) {
            self.storage
                .lock()
                .expect("cache mutex should not be poisoned")
                .insert(key.to_string(), value);
        }
    }

    #[async_trait]
    impl CacheStore for FakeCacheStore {
        async fn get_serialized(&self, key: &str) -> exn::Result<Option<String>, CacheError> {
            Ok(self
                .storage
                .lock()
                .expect("cache mutex should not be poisoned")
                .get(key)
                .cloned())
        }

        async fn save_serialized(&self, key: String, data: String) -> exn::Result<(), CacheError> {
            self.storage
                .lock()
                .expect("cache mutex should not be poisoned")
                .insert(key, data);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FakeGraylogGateway {
        search_results: Arc<Mutex<Vec<MessageSearchResult>>>,
        fields_result: Arc<Mutex<FieldsResult>>,
        streams_result: Arc<Mutex<StreamsResult>>,
        stream_result: Arc<Mutex<StreamResult>>,
        ping_result: Arc<Mutex<Result<(), HttpError>>>,
        system_result: Arc<Mutex<SystemResult>>,
        aggregate_result: Arc<Mutex<AggregateSearchResult>>,
        search_requests: Arc<Mutex<Vec<MessageSearchRequest>>>,
        aggregate_requests: Arc<Mutex<Vec<AggregateSearchRequest>>>,
        list_fields_calls: Arc<Mutex<usize>>,
    }

    impl FakeGraylogGateway {
        fn new() -> Self {
            Self {
                search_results: Arc::new(Mutex::new(vec![make_search_messages_result(
                    Vec::new(),
                    Some(0),
                    Map::new(),
                )])),
                fields_result: Arc::new(Mutex::new(FieldsResult { fields: Vec::new() })),
                streams_result: Arc::new(Mutex::new(StreamsResult {
                    streams: Vec::new(),
                    total: Some(0),
                    metadata: Map::new(),
                })),
                stream_result: Arc::new(Mutex::new(StreamResult { stream: Map::new() })),
                ping_result: Arc::new(Mutex::new(Ok(()))),
                system_result: Arc::new(Mutex::new(SystemResult { system: Map::new() })),
                aggregate_result: Arc::new(Mutex::new(AggregateSearchResult {
                    rows: Vec::new(),
                    metadata: Map::new(),
                })),
                search_requests: Arc::new(Mutex::new(Vec::new())),
                aggregate_requests: Arc::new(Mutex::new(Vec::new())),
                list_fields_calls: Arc::new(Mutex::new(0)),
            }
        }

        fn with_search_results(results: Vec<MessageSearchResult>) -> Self {
            let gateway = Self::new();
            *gateway
                .search_results
                .lock()
                .expect("search result mutex should not be poisoned") = results;
            gateway
        }

        fn set_fields(&self, fields: Vec<String>) {
            *self
                .fields_result
                .lock()
                .expect("fields mutex should not be poisoned") = FieldsResult { fields };
        }

        fn set_streams(&self, streams: Vec<JsonObject>) {
            *self
                .streams_result
                .lock()
                .expect("streams mutex should not be poisoned") = StreamsResult {
                total: Some(streams.len() as u64),
                streams,
                metadata: Map::new(),
            };
        }

        fn set_stream(&self, stream: JsonObject) {
            *self
                .stream_result
                .lock()
                .expect("stream mutex should not be poisoned") = StreamResult { stream };
        }

        fn set_system(&self, system: JsonObject) {
            *self
                .system_result
                .lock()
                .expect("system mutex should not be poisoned") = SystemResult { system };
        }

        fn set_aggregate(&self, rows: Vec<NormalizedRow>, metadata: JsonObject) {
            *self
                .aggregate_result
                .lock()
                .expect("aggregate mutex should not be poisoned") =
                AggregateSearchResult { rows, metadata };
        }

        fn search_requests(&self) -> Vec<MessageSearchRequest> {
            self.search_requests
                .lock()
                .expect("search request mutex should not be poisoned")
                .clone()
        }

        fn aggregate_requests(&self) -> Vec<AggregateSearchRequest> {
            self.aggregate_requests
                .lock()
                .expect("aggregate request mutex should not be poisoned")
                .clone()
        }

        fn list_fields_call_count(&self) -> usize {
            *self
                .list_fields_calls
                .lock()
                .expect("field call mutex should not be poisoned")
        }
    }

    #[async_trait]
    impl GraylogGateway for FakeGraylogGateway {
        fn base_url(&self) -> &str {
            "http://localhost:9000"
        }

        async fn ping(&self) -> Result<(), HttpError> {
            self.ping_result
                .lock()
                .expect("ping mutex should not be poisoned")
                .as_ref()
                .map(|_| ())
                .map_err(|error| error.clone())
        }

        async fn search_messages(
            &self,
            request: MessageSearchRequest,
        ) -> Result<MessageSearchResult, HttpError> {
            self.search_requests
                .lock()
                .expect("search request mutex should not be poisoned")
                .push(request);
            let mut results = self
                .search_results
                .lock()
                .expect("search result mutex should not be poisoned");
            if results.is_empty() {
                Ok(make_search_messages_result(Vec::new(), Some(0), Map::new()))
            } else {
                Ok(results.remove(0))
            }
        }

        async fn search_aggregate(
            &self,
            request: AggregateSearchRequest,
        ) -> Result<AggregateSearchResult, HttpError> {
            self.aggregate_requests
                .lock()
                .expect("aggregate request mutex should not be poisoned")
                .push(request);
            Ok(self
                .aggregate_result
                .lock()
                .expect("aggregate mutex should not be poisoned")
                .clone())
        }

        async fn list_streams(&self) -> Result<StreamsResult, HttpError> {
            Ok(self
                .streams_result
                .lock()
                .expect("streams mutex should not be poisoned")
                .clone())
        }

        async fn get_stream(&self, _stream_id: &str) -> Result<StreamResult, HttpError> {
            Ok(self
                .stream_result
                .lock()
                .expect("stream mutex should not be poisoned")
                .clone())
        }

        async fn system_info(&self) -> Result<SystemResult, HttpError> {
            Ok(self
                .system_result
                .lock()
                .expect("system mutex should not be poisoned")
                .clone())
        }

        async fn list_fields(&self) -> Result<FieldsResult, HttpError> {
            *self
                .list_fields_calls
                .lock()
                .expect("field call mutex should not be poisoned") += 1;
            Ok(self
                .fields_result
                .lock()
                .expect("fields mutex should not be poisoned")
                .clone())
        }
    }

    struct FakeGraylogGatewayFactory {
        gateway: Arc<dyn GraylogGateway>,
        failure: Option<String>,
    }

    impl FakeGraylogGatewayFactory {
        fn new(gateway: Arc<dyn GraylogGateway>) -> Self {
            Self {
                gateway,
                failure: None,
            }
        }
    }

    impl GraylogGatewayFactory for FakeGraylogGatewayFactory {
        fn build_from_config(
            &self,
            _config: GraylogConfig,
        ) -> Result<Arc<dyn GraylogGateway>, HttpError> {
            if let Some(message) = &self.failure {
                Err(HttpError::RequestBuild {
                    message: message.clone(),
                })
            } else {
                Ok(Arc::clone(&self.gateway))
            }
        }
    }

    fn test_config() -> Config {
        Config {
            graylog: GraylogConfig::new(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("test-token".to_owned().into()),
            ),
        }
    }

    fn test_service(
        config_store: Arc<dyn ConfigStore>,
        cache_store: Arc<dyn CacheStore>,
        gateway_factory: Arc<dyn GraylogGatewayFactory>,
    ) -> ApplicationService {
        ApplicationService::new(config_store, gateway_factory, cache_store)
    }

    fn service_with_gateway(
        config_store: FakeConfigStore,
        cache_store: FakeCacheStore,
        gateway: FakeGraylogGateway,
    ) -> (ApplicationService, FakeGraylogGateway, FakeCacheStore) {
        let service = test_service(
            Arc::new(config_store),
            Arc::new(cache_store.clone()),
            Arc::new(FakeGraylogGatewayFactory::new(Arc::new(gateway.clone()))),
        );
        (service, gateway, cache_store)
    }

    fn make_search_messages_result(
        messages: Vec<NormalizedRow>,
        total_results: Option<u64>,
        metadata: JsonObject,
    ) -> MessageSearchResult {
        MessageSearchResult {
            messages,
            total_results,
            metadata,
        }
    }

    fn make_search_input() -> SearchCommandInput {
        SearchCommandInput {
            query: "source:app".to_string(),
            timerange: None,
            fields: Vec::new(),
            limit: None,
            offset: None,
            sort: None,
            sort_direction: None,
            group_by: None,
            all_pages: false,
            all_fields: false,
            streams: Vec::new(),
        }
    }

    fn make_aggregate_input() -> AggregateCommandInput {
        AggregateCommandInput {
            query: "source:app".to_string(),
            timerange: None,
            aggregation_type: AggregationType::Terms,
            field: "level".to_string(),
            size: None,
            interval: None,
            streams: Vec::new(),
        }
    }

    fn object(entries: Vec<(&str, Value)>) -> JsonObject {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    fn rows(count: usize) -> Vec<NormalizedRow> {
        (0..count)
            .map(|index| object(vec![("message", json!(format!("message-{index}")))]))
            .collect()
    }

    fn assert_empty_field(error: exn::Exn<CliError>, expected_field: &'static str) {
        assert!(
            matches!(&*error, CliError::Validation(ValidationError::EmptyField { field }) if *field == expected_field),
            "expected EmptyField for {expected_field}, got {error:?}"
        );
    }

    fn assert_config_error_contains(error: exn::Exn<CliError>, expected: &str) {
        assert!(
            matches!(&*error, CliError::Config(message) if message.contains(expected)),
            "expected config error containing {expected}, got {error:?}"
        );
    }

    #[tokio::test]
    async fn authenticate_rejects_empty_token() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .authenticate(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("".to_owned().into()),
            )
            .await
            .expect_err("empty token should be rejected");
        assert_empty_field(error, "graylog.token");
    }

    #[tokio::test]
    async fn authenticate_rejects_whitespace_only_token() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .authenticate(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("  ".to_owned().into()),
            )
            .await
            .expect_err("whitespace token should be rejected");
        assert_empty_field(error, "graylog.token");
    }

    #[tokio::test]
    async fn authenticate_persists_config_with_defaults() {
        let config_store = FakeConfigStore::empty();
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let url = Url::parse("http://localhost:9000").expect("test URL should parse");
        service
            .authenticate(
                url.clone(),
                secrecy::SecretString::new("test-token".to_owned().into()),
            )
            .await
            .expect("authentication should persist config");
        let saved = config_store
            .saved_config()
            .expect("config should be saved after authentication");
        assert_eq!(saved.graylog.url, url);
        assert_eq!(saved.graylog.token.expose_secret(), "test-token");
        assert_eq!(saved.graylog.timeout_seconds, DEFAULT_TIMEOUT_SECONDS);
        assert!(saved.graylog.verify_tls);
        assert_eq!(
            saved.graylog.fields_cache_ttl_seconds,
            DEFAULT_FIELDS_CACHE_TTL_SECONDS
        );
    }

    #[tokio::test]
    async fn authenticate_returns_ok_status_with_url() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let url = Url::parse("http://localhost:9000").expect("test URL should parse");
        let status = service
            .authenticate(
                url.clone(),
                secrecy::SecretString::new("test-token".to_owned().into()),
            )
            .await
            .expect("authentication should succeed");
        assert!(status.ok);
        assert_eq!(status.graylog_url, url.to_string());
    }

    #[tokio::test]
    async fn search_returns_config_error_when_not_authenticated() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .search(make_search_input())
            .await
            .expect_err("search should require config");
        assert_config_error_contains(error, "graylog is not configured");
    }

    #[tokio::test]
    async fn search_builds_default_request() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        service
            .search(make_search_input())
            .await
            .expect("search should succeed");
        let requests = gateway.search_requests();
        let request = requests
            .first()
            .expect("one search request should be recorded");
        assert_eq!(request.limit, 50);
        assert_eq!(request.offset, 0);
        assert_eq!(request.sort, "timestamp");
        assert_eq!(request.sort_direction, SortDirection::Desc);
    }

    #[tokio::test]
    async fn search_preserves_explicit_values() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let timerange = CommandTimerange::relative("15m").expect("relative timerange should parse");
        let mut input = make_search_input();
        input.query = "level:ERROR".to_string();
        input.fields = vec!["message".to_string(), "source".to_string()];
        input.limit = Some(10);
        input.offset = Some(5);
        input.sort = Some("source".to_string());
        input.sort_direction = Some(SortDirection::Asc);
        input.timerange = Some(timerange.clone());
        input.streams = vec!["stream-1".to_string(), "stream-2".to_string()];
        service.search(input).await.expect("search should succeed");
        let requests = gateway.search_requests();
        let request = requests
            .first()
            .expect("one search request should be recorded");
        assert_eq!(request.query, "level:ERROR");
        assert_eq!(request.fields, vec!["message", "source"]);
        assert_eq!(request.limit, 10);
        assert_eq!(request.offset, 5);
        assert_eq!(request.sort, "source");
        assert_eq!(request.sort_direction, SortDirection::Asc);
        assert_eq!(request.timerange, Some(timerange));
        assert_eq!(request.streams, vec!["stream-1", "stream-2"]);
    }

    #[tokio::test]
    async fn search_includes_total_results_in_metadata() {
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            Vec::new(),
            Some(100),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .search(make_search_input())
            .await
            .expect("search should succeed");
        assert_eq!(status.metadata.get("total_results"), Some(&json!(100)));
    }

    #[tokio::test]
    async fn search_with_group_by_injects_field_into_fields() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.fields = vec!["message".to_string()];
        input.group_by = Some("level".to_string());
        service.search(input).await.expect("search should succeed");
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["message", "level"]
        );
    }

    #[tokio::test]
    async fn search_with_group_by_does_not_duplicate_field() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.fields = vec!["message".to_string(), "level".to_string()];
        input.group_by = Some("level".to_string());
        service.search(input).await.expect("search should succeed");
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["message", "level"]
        );
    }

    #[tokio::test]
    async fn search_with_group_by_returns_grouped_output() {
        let messages = vec![
            object(vec![("level", json!("ERROR"))]),
            object(vec![("level", json!("ERROR"))]),
            object(vec![("level", json!("ERROR"))]),
            object(vec![("level", json!("WARN"))]),
            object(vec![("level", json!("WARN"))]),
        ];
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            messages,
            Some(5),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.group_by = Some("level".to_string());
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(status.grouped_by, Some("level".to_string()));
        let groups = status.groups.expect("groups should be present");
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].key, "ERROR");
        assert_eq!(groups[0].count, 3);
        assert_eq!(groups[1].key, "WARN");
        assert_eq!(groups[1].count, 2);
    }

    #[tokio::test]
    async fn search_groups_compute_duration_from_timestamps() {
        let messages = vec![
            object(vec![
                ("level", json!("ERROR")),
                ("timestamp", json!("2026-01-01T00:00:00Z")),
            ]),
            object(vec![
                ("level", json!("ERROR")),
                ("timestamp", json!("2026-01-01T00:00:02.500Z")),
            ]),
        ];
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            messages,
            Some(2),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.group_by = Some("level".to_string());
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(
            status.groups.expect("groups should be present")[0].duration_ms,
            Some(2_500)
        );
    }

    #[tokio::test]
    async fn search_groups_unknown_bucket_for_missing_field() {
        let messages = vec![
            object(vec![("message", json!("first"))]),
            object(vec![("message", json!("second"))]),
        ];
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            messages,
            Some(2),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.group_by = Some("level".to_string());
        let status = service.search(input).await.expect("search should succeed");
        let groups = status.groups.expect("groups should be present");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].key, "unknown");
        assert_eq!(groups[0].count, 2);
    }

    #[tokio::test]
    async fn search_all_pages_fetches_all_pages() {
        let gateway = FakeGraylogGateway::with_search_results(vec![
            make_search_messages_result(rows(500), Some(1250), Map::new()),
            make_search_messages_result(rows(500), Some(1250), Map::new()),
            make_search_messages_result(rows(250), Some(1250), Map::new()),
        ]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        let requests = gateway.search_requests();
        assert_eq!(status.messages.len(), 1_250);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].offset, 0);
        assert_eq!(requests[1].offset, 500);
        assert_eq!(requests[2].offset, 1_000);
        assert!(requests.iter().all(|request| request.limit == 500));
    }

    #[tokio::test]
    async fn search_all_pages_stops_on_empty_page() {
        let gateway = FakeGraylogGateway::with_search_results(vec![
            make_search_messages_result(rows(500), None, Map::new()),
            make_search_messages_result(Vec::new(), None, Map::new()),
        ]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(status.messages.len(), 500);
        assert_eq!(gateway.search_requests().len(), 2);
    }

    #[tokio::test]
    async fn search_all_pages_stops_on_short_page() {
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            rows(300),
            None,
            Map::new(),
        )]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(status.messages.len(), 300);
        assert_eq!(gateway.search_requests().len(), 1);
    }

    #[tokio::test]
    async fn search_all_pages_stops_on_total_reached() {
        let gateway = FakeGraylogGateway::with_search_results(vec![
            make_search_messages_result(rows(500), Some(600), Map::new()),
            make_search_messages_result(rows(100), Some(600), Map::new()),
        ]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(status.messages.len(), 600);
        assert_eq!(gateway.search_requests().len(), 2);
    }

    #[tokio::test]
    async fn search_all_fields_uses_cached_fields_on_hit() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec!["uncached".to_string()]);
        let cache_store = FakeCacheStore::default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_secs();
        cache_store.insert(
            "fields",
            serde_json::to_string(&CachedFields {
                fields: vec!["message".to_string(), "source".to_string()],
                fetched_at: now,
            })
            .expect("cached fields should serialize"),
        );
        let (service, gateway, _) =
            service_with_gateway(FakeConfigStore::new(test_config()), cache_store, gateway);
        let mut input = make_search_input();
        input.all_fields = true;
        service.search(input).await.expect("search should succeed");
        assert_eq!(gateway.list_fields_call_count(), 0);
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["message", "source"]
        );
    }

    #[tokio::test]
    async fn search_all_fields_fetches_and_caches_on_miss() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec!["message".to_string(), "level".to_string()]);
        let cache_store = FakeCacheStore::default();
        let (service, gateway, cache_store) =
            service_with_gateway(FakeConfigStore::new(test_config()), cache_store, gateway);
        let mut input = make_search_input();
        input.all_fields = true;
        service.search(input).await.expect("search should succeed");
        assert_eq!(gateway.list_fields_call_count(), 1);
        assert!(cache_store.get("fields").is_some());
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["message", "level"]
        );
    }

    #[tokio::test]
    async fn search_all_fields_refetches_on_expired_cache() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec!["fresh".to_string()]);
        let cache_store = FakeCacheStore::default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_secs();
        cache_store.insert(
            "fields",
            serde_json::to_string(&CachedFields {
                fields: vec!["stale".to_string()],
                fetched_at: now - DEFAULT_FIELDS_CACHE_TTL_SECONDS - 1,
            })
            .expect("cached fields should serialize"),
        );
        let (service, gateway, _) =
            service_with_gateway(FakeConfigStore::new(test_config()), cache_store, gateway);
        let mut input = make_search_input();
        input.all_fields = true;
        service.search(input).await.expect("search should succeed");
        assert_eq!(gateway.list_fields_call_count(), 1);
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["fresh"]
        );
    }

    #[tokio::test]
    async fn aggregate_builds_request_from_input() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let timerange = CommandTimerange::relative("1h").expect("relative timerange should parse");
        let mut input = make_aggregate_input();
        input.query = "level:ERROR".to_string();
        input.timerange = Some(timerange.clone());
        input.aggregation_type = AggregationType::DateHistogram;
        input.field = "timestamp".to_string();
        input.size = Some(25);
        input.interval = Some("minute".to_string());
        input.streams = vec!["stream-1".to_string()];
        service
            .aggregate(input)
            .await
            .expect("aggregate should succeed");
        let requests = gateway.aggregate_requests();
        let request = requests
            .first()
            .expect("one aggregate request should be recorded");
        assert_eq!(request.query, "level:ERROR");
        assert_eq!(request.timerange, Some(timerange));
        assert_eq!(request.aggregation_type, AggregationType::DateHistogram);
        assert_eq!(request.field, "timestamp");
        assert_eq!(request.size, Some(25));
        assert_eq!(request.interval, Some("minute".to_string()));
        assert_eq!(request.streams, vec!["stream-1"]);
    }

    #[tokio::test]
    async fn aggregate_returns_aggregate_status() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_aggregate(
            vec![object(vec![("level", json!("ERROR")), ("count", json!(2))])],
            object(vec![("source", json!("aggregate"))]),
        );
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .aggregate(make_aggregate_input())
            .await
            .expect("aggregate should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "aggregate");
        assert_eq!(status.aggregation_type, "terms");
        assert_eq!(status.rows.len(), 1);
    }

    #[tokio::test]
    async fn count_by_level_returns_correct_command() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .count_by_level(make_aggregate_input())
            .await
            .expect("count-by-level should succeed");
        assert_eq!(status.command, "count-by-level");
    }

    #[tokio::test]
    async fn aggregate_returns_config_error_when_not_authenticated() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .aggregate(make_aggregate_input())
            .await
            .expect_err("aggregate should require config");
        assert_config_error_contains(error, "graylog is not configured");
    }

    #[tokio::test]
    async fn streams_find_rejects_empty_name() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .streams_find("")
            .await
            .expect_err("empty name should fail");
        assert_empty_field(error, "name");
    }

    #[tokio::test]
    async fn streams_find_rejects_whitespace_name() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .streams_find("  ")
            .await
            .expect_err("whitespace name should fail");
        assert_empty_field(error, "name");
    }

    #[tokio::test]
    async fn streams_find_filters_case_insensitively_by_title() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_streams(vec![
            object(vec![("title", json!("All Errors"))]),
            object(vec![("title", json!("error logs"))]),
            object(vec![("title", json!("Info"))]),
        ]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_find("error")
            .await
            .expect("find should succeed");
        assert_eq!(status.returned, 2);
        assert_eq!(status.streams.len(), 2);
    }

    #[tokio::test]
    async fn streams_find_matches_by_name_when_title_absent() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_streams(vec![
            object(vec![("name", json!("error logs"))]),
            object(vec![("name", json!("info logs"))]),
        ]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_find("error")
            .await
            .expect("find should succeed");
        assert_eq!(status.returned, 1);
        assert_eq!(status.streams[0].get("name"), Some(&json!("error logs")));
    }

    #[tokio::test]
    async fn streams_find_trims_input() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_streams(vec![object(vec![("title", json!("error logs"))])]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_find("  error  ")
            .await
            .expect("find should succeed");
        assert_eq!(status.name, "error");
        assert_eq!(status.returned, 1);
    }

    #[tokio::test]
    async fn streams_search_requires_exactly_one_stream_id() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let no_stream_error = service
            .streams_search(make_search_input())
            .await
            .expect_err("missing stream id should fail");
        assert!(matches!(
            &*no_stream_error,
            CliError::Validation(ValidationError::InvalidValue {
                field: "stream_id",
                ..
            })
        ));
        let mut input = make_search_input();
        input.streams = vec!["stream-1".to_string(), "stream-2".to_string()];
        let two_stream_error = service
            .streams_search(input)
            .await
            .expect_err("multiple stream ids should fail");
        assert!(matches!(
            &*two_stream_error,
            CliError::Validation(ValidationError::InvalidValue {
                field: "stream_id",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn streams_search_caps_limit_at_100() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.streams = vec!["stream-1".to_string()];
        input.limit = Some(200);
        service
            .streams_search(input)
            .await
            .expect("stream search should succeed");
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one stream search request should be recorded")
                .limit,
            100
        );
    }

    #[tokio::test]
    async fn streams_search_forces_sort_timestamp_desc() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.streams = vec!["stream-1".to_string()];
        input.sort = Some("source".to_string());
        input.sort_direction = Some(SortDirection::Asc);
        service
            .streams_search(input)
            .await
            .expect("stream search should succeed");
        let requests = gateway.search_requests();
        let request = requests
            .first()
            .expect("one stream search request should be recorded");
        assert_eq!(request.sort, "timestamp");
        assert_eq!(request.sort_direction, SortDirection::Desc);
    }

    #[tokio::test]
    async fn streams_last_event_builds_default_request() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        service
            .streams_last_event("stream-1".to_string(), None)
            .await
            .expect("last event should succeed");
        let requests = gateway.search_requests();
        let request = requests
            .first()
            .expect("one stream search request should be recorded");
        assert_eq!(request.query, "*");
        assert_eq!(request.limit, 1);
        assert_eq!(request.offset, 0);
        assert_eq!(request.sort, "timestamp");
        assert_eq!(request.sort_direction, SortDirection::Desc);
        assert_eq!(request.streams, vec!["stream-1"]);
    }

    #[tokio::test]
    async fn streams_list_returns_all_streams() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_streams(vec![
            object(vec![("id", json!("1"))]),
            object(vec![("id", json!("2"))]),
            object(vec![("id", json!("3"))]),
        ]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_list()
            .await
            .expect("streams list should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "streams.list");
        assert_eq!(status.streams.len(), 3);
    }

    #[tokio::test]
    async fn streams_show_returns_single_stream() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_stream(object(vec![("id", json!("stream-1"))]));
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_show("stream-1")
            .await
            .expect("streams show should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "streams.show");
        assert_eq!(status.stream.get("id"), Some(&json!("stream-1")));
    }

    #[tokio::test]
    async fn system_info_returns_gateway_payload() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_system(object(vec![("version", json!("6.0.0"))]));
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .system_info()
            .await
            .expect("system info should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "system.info");
        assert_eq!(status.system.get("version"), Some(&json!("6.0.0")));
    }

    #[tokio::test]
    async fn fields_returns_list_and_count() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec![
            "message".to_string(),
            "source".to_string(),
            "level".to_string(),
        ]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service.fields().await.expect("fields should succeed");
        assert!(status.ok);
        assert_eq!(status.fields, vec!["message", "source", "level"]);
        assert_eq!(status.total, 3);
    }

    #[tokio::test]
    async fn ping_returns_reachable_status() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service.ping().await.expect("ping should succeed");
        assert!(status.ok);
        assert!(status.reachable);
        assert_eq!(status.graylog_url, "http://localhost:9000");
    }
}
