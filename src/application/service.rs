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
        let result = client
            .get_stream(stream_id.to_string())
            .await
            .or_raise(|| {
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
            client.get_stream(stream_id.clone()).await.or_raise(|| {
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
