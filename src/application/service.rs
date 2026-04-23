use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use exn::ResultExt;
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;

use crate::application::ports::{
    ConfigStore, FieldsCacheStore, GraylogGateway, GraylogGatewayFactory,
};
use crate::domain::config::{
    DEFAULT_FIELDS_CACHE_TTL_SECONDS, DEFAULT_TIMEOUT_SECONDS, GraylogConfig, StoredConfig,
    normalize_url,
};
use crate::domain::error::CliError;
use crate::domain::error::ConfigError;
use crate::domain::error::HttpError;
use crate::domain::error::ValidationError;
use crate::domain::models::{
    AggregateCommandInput, AggregateSearchRequest, AggregateStatus, AuthStatus, CommandMetadata,
    CommandStatus, FieldsStatus, MessageSearchRequest, MessageSearchStatus, NormalizedRow,
    PingStatus, SearchCommandInput, SearchGroup, SortDirection, StreamFindStatus, StreamStatus,
    StreamsStatus, SystemInfoStatus,
};

const DEFAULT_SEARCH_LIMIT: u64 = 50;
const MAX_STREAM_SEARCH_LIMIT: u64 = 100;
const DEFAULT_SEARCH_OFFSET: u64 = 0;
const DEFAULT_SEARCH_SORT: &str = "timestamp";

#[derive(Clone)]
pub struct ApplicationService {
    config_store: Arc<dyn ConfigStore>,
    gateway_factory: Arc<dyn GraylogGatewayFactory>,
    fields_cache_store: Arc<dyn FieldsCacheStore>,
}

impl Default for ApplicationService {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplicationService {
    pub fn new() -> Self {
        Self::with_dependencies(
            Arc::new(UnconfiguredConfigStore),
            Arc::new(UnconfiguredGraylogGatewayFactory),
            Arc::new(UnconfiguredFieldsCacheStore),
        )
    }

    pub fn with_config_store<T>(config_store: Arc<T>) -> Self
    where
        T: ConfigStore + FieldsCacheStore + 'static,
    {
        let fields_cache_store: Arc<dyn FieldsCacheStore> = config_store.clone();
        let config_store: Arc<dyn ConfigStore> = config_store;

        Self::with_dependencies(
            config_store,
            Arc::new(UnconfiguredGraylogGatewayFactory),
            fields_cache_store,
        )
    }

    pub fn with_dependencies(
        config_store: Arc<dyn ConfigStore>,
        gateway_factory: Arc<dyn GraylogGatewayFactory>,
        fields_cache_store: Arc<dyn FieldsCacheStore>,
    ) -> Self {
        Self {
            config_store,
            gateway_factory,
            fields_cache_store,
        }
    }

    pub async fn placeholder_success(
        &self,
        command: &'static str,
    ) -> Result<CommandStatus, CliError> {
        Self::validate_command_name(command).map_err(|_| CliError::CommandFailed {
            command: command.to_string(),
        })?;

        let configured = self
            .config_store
            .load()
            .await
            .map_err(|_| {
                CliError::Config(ConfigError::StoreUnavailable {
                    backend: "config-store",
                    message: "failed to load runtime config".to_string(),
                })
            })?
            .is_some();

        Ok(CommandStatus::with_metadata(CommandMetadata {
            command,
            configured,
        }))
    }

    pub async fn load_config(&self) -> exn::Result<Option<GraylogConfig>, CliError> {
        self.config_store.load().await.or_raise(|| {
            CliError::Config(ConfigError::StoreUnavailable {
                backend: "config-store",
                message: "failed to load runtime config".to_string(),
            })
        })
    }

    pub async fn require_config(&self) -> exn::Result<GraylogConfig, CliError> {
        self.load_config()
            .await?
            .ok_or_else(|| CliError::Config(ConfigError::NotConfigured).into())
    }

    pub async fn save_config(&self, config: StoredConfig) -> exn::Result<(), CliError> {
        self.config_store.save(config).await.or_raise(|| {
            CliError::Config(ConfigError::StoreUnavailable {
                backend: "config-store",
                message: "failed to persist config".to_string(),
            })
        })
    }

    pub async fn authenticate(
        &self,
        base_url: String,
        token: SecretString,
    ) -> Result<AuthStatus, CliError> {
        let normalized_url = normalize_url(base_url)?;
        let trimmed_token = token.expose_secret().trim().to_owned();

        if trimmed_token.is_empty() {
            return Err(CliError::Validation(ValidationError::EmptyField {
                field: "graylog.token",
            }));
        }

        let runtime_config = GraylogConfig::new(
            normalized_url.clone(),
            SecretString::new(trimmed_token.into()),
            DEFAULT_TIMEOUT_SECONDS,
            true,
            DEFAULT_FIELDS_CACHE_TTL_SECONDS,
        )?;

        let config_path = self.config_store.config_path().map_err(CliError::from)?;

        self.config_store
            .save(runtime_config.to_stored())
            .await
            .map_err(CliError::from)?;

        Ok(AuthStatus::ok(
            config_path.display().to_string(),
            normalized_url,
        ))
    }

    pub async fn search(&self, input: SearchCommandInput) -> Result<MessageSearchStatus, CliError> {
        let mut input = input;

        if input.all_fields && input.fields.is_empty() {
            let config = self
                .config_store
                .load()
                .await?
                .ok_or(CliError::Config(ConfigError::NotConfigured))?;
            let config_path = self.config_store.config_path()?;
            let ttl = config.fields_cache_ttl_seconds;

            let fields = match self
                .fields_cache_store
                .load_fields(&config_path, ttl)
                .await?
            {
                Some(fields) => fields,
                None => {
                    let client = self.graylog_gateway_with_config(config)?;
                    let result = client.list_fields().await?;
                    self.fields_cache_store
                        .save_fields(&config_path, &result.fields)
                        .await?;
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
    ) -> Result<AggregateStatus, CliError> {
        self.execute_aggregate("aggregate", self.build_aggregate_request(input))
            .await
    }

    pub async fn count_by_level(
        &self,
        input: AggregateCommandInput,
    ) -> Result<AggregateStatus, CliError> {
        self.execute_aggregate("count-by-level", self.build_aggregate_request(input))
            .await
    }

    pub async fn streams_list(&self) -> Result<StreamsStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.list_streams().await?;

        Ok(StreamsStatus {
            ok: true,
            command: "streams.list",
            streams: result.streams,
        })
    }

    pub async fn streams_show(&self, stream_id: &str) -> Result<StreamStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.get_stream(stream_id.to_string()).await?;

        Ok(StreamStatus {
            ok: true,
            command: "streams.show",
            stream: result.stream,
        })
    }

    pub async fn streams_find(&self, name: &str) -> Result<StreamFindStatus, CliError> {
        let name = name.trim();

        if name.is_empty() {
            return Err(CliError::Validation(ValidationError::EmptyField {
                field: "name",
            }));
        }

        let client = self.graylog_gateway().await?;
        let result = client.list_streams().await?;
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
    ) -> Result<MessageSearchStatus, CliError> {
        let request = self.build_stream_search_request(input)?;
        self.execute_stream_message_search("streams.search", request)
            .await
    }

    pub async fn streams_last_event(
        &self,
        stream_id: String,
        timerange: Option<crate::domain::timerange::CommandTimerange>,
    ) -> Result<MessageSearchStatus, CliError> {
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

    pub async fn system_info(&self) -> Result<SystemInfoStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.system_info().await?;

        Ok(SystemInfoStatus {
            ok: true,
            command: "system.info",
            system: result.system,
        })
    }

    pub async fn fields(&self) -> Result<FieldsStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.list_fields().await?;
        Ok(FieldsStatus {
            ok: true,
            command: "fields",
            fields: result.fields.clone(),
            total: result.fields.len(),
        })
    }

    pub async fn ping(&self) -> Result<PingStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let graylog_url = client.base_url().to_string();

        client.ping().await?;

        Ok(PingStatus {
            ok: true,
            command: "ping",
            reachable: true,
            graylog_url,
        })
    }

    fn validate_command_name(command: &'static str) -> Result<(), ValidationError> {
        if command.trim().is_empty() {
            return Err(ValidationError::EmptyField { field: "command" });
        }

        Ok(())
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
    ) -> Result<MessageSearchRequest, CliError> {
        if input.streams.len() != 1 {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "stream_id",
                message: "exactly one stream id is required".to_string(),
            }));
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
    ) -> Result<MessageSearchStatus, CliError> {
        let config = self
            .config_store
            .load()
            .await?
            .ok_or(CliError::Config(ConfigError::NotConfigured))?;
        let client = self.graylog_gateway_with_config(config)?;
        let result = client.search_messages(request.clone()).await?;
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
    ) -> Result<MessageSearchStatus, CliError> {
        let config = self
            .config_store
            .load()
            .await?
            .ok_or(CliError::Config(ConfigError::NotConfigured))?;
        let client = self.graylog_gateway_with_config(config)?;
        let mut request = self.build_search_request(input.clone(), DEFAULT_SEARCH_LIMIT);
        let mut all_messages = Vec::new();
        let mut metadata = serde_json::Map::new();
        let mut total_results = None;

        request.limit = 500;
        request.offset = 0;

        loop {
            let result = client.search_messages(request.clone()).await?;

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
    ) -> Result<MessageSearchStatus, CliError> {
        let client = self.graylog_gateway().await?;

        if let Some(stream_id) = request.streams.first() {
            client.get_stream(stream_id.clone()).await?;
        }

        let result = client.search_messages(request.clone()).await?;
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
    ) -> Result<AggregateStatus, CliError> {
        let config = self
            .config_store
            .load()
            .await?
            .ok_or(CliError::Config(ConfigError::NotConfigured))?;
        let client = self.graylog_gateway_with_config(config)?;
        let aggregation_type = request.aggregation_type.as_cli_value();
        let result = client.search_aggregate(request).await?;

        Ok(AggregateStatus {
            ok: true,
            command,
            aggregation_type,
            rows: result.rows,
            metadata: result.metadata,
        })
    }

    async fn graylog_gateway(&self) -> Result<Arc<dyn GraylogGateway>, CliError> {
        let config = self
            .config_store
            .load()
            .await?
            .ok_or(CliError::Config(ConfigError::NotConfigured))?;

        self.graylog_gateway_with_config(config)
    }

    fn graylog_gateway_with_config(
        &self,
        config: GraylogConfig,
    ) -> Result<Arc<dyn GraylogGateway>, CliError> {
        self.gateway_factory
            .build_from_config(config)
            .map_err(CliError::from)
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
    Some(last_millis.saturating_sub(first_millis))
}

fn parse_timestamp_to_millis(ts: &str) -> Option<u64> {
    let year: u64 = ts.get(0..4)?.parse().ok()?;
    let month: u64 = ts.get(5..7)?.parse().ok()?;
    let day: u64 = ts.get(8..10)?.parse().ok()?;
    let hour: u64 = ts.get(11..13)?.parse().ok()?;
    let minute: u64 = ts.get(14..16)?.parse().ok()?;
    let second: u64 = ts.get(17..19)?.parse().ok()?;
    let millis: u64 = ts
        .get(20..23)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);

    let days = (year - 1970) * 365 + (month.saturating_sub(1)) * 31 + day.saturating_sub(1);
    Some(days * 86_400_000 + hour * 3_600_000 + minute * 60_000 + second * 1_000 + millis)
}

struct UnconfiguredConfigStore;
struct UnconfiguredGraylogGatewayFactory;
struct UnconfiguredFieldsCacheStore;

#[async_trait]
impl ConfigStore for UnconfiguredConfigStore {
    fn config_path(&self) -> Result<PathBuf, ConfigError> {
        Err(ConfigError::StoreUnavailable {
            backend: "unconfigured",
            message: "no config store has been attached to ApplicationService".to_string(),
        })
    }

    async fn load(&self) -> Result<Option<GraylogConfig>, ConfigError> {
        Err(ConfigError::StoreUnavailable {
            backend: "unconfigured",
            message: "no config store has been attached to ApplicationService".to_string(),
        })
    }

    async fn save(&self, _config: StoredConfig) -> Result<(), ConfigError> {
        Err(ConfigError::StoreUnavailable {
            backend: "unconfigured",
            message: "no config store has been attached to ApplicationService".to_string(),
        })
    }
}

impl GraylogGatewayFactory for UnconfiguredGraylogGatewayFactory {
    fn build_from_config(
        &self,
        _config: GraylogConfig,
    ) -> Result<Arc<dyn GraylogGateway>, HttpError> {
        Err(HttpError::RequestBuild {
            message: "no Graylog gateway factory has been attached to ApplicationService"
                .to_string(),
        })
    }
}

#[async_trait]
impl FieldsCacheStore for UnconfiguredFieldsCacheStore {
    async fn load_fields(
        &self,
        _config_path: &std::path::Path,
        _ttl_seconds: u64,
    ) -> Result<Option<Vec<String>>, ConfigError> {
        Err(ConfigError::StoreUnavailable {
            backend: "unconfigured",
            message: "no fields cache store has been attached to ApplicationService".to_string(),
        })
    }

    async fn save_fields(
        &self,
        _config_path: &std::path::Path,
        _fields: &[String],
    ) -> Result<(), ConfigError> {
        Err(ConfigError::StoreUnavailable {
            backend: "unconfigured",
            message: "no fields cache store has been attached to ApplicationService".to_string(),
        })
    }
}
