use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use exn::ResultExt;
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;

use crate::domain::config::{DEFAULT_TIMEOUT_SECONDS, GraylogConfig, StoredConfig, normalize_url};
use crate::domain::error::CliError;
use crate::domain::error::ConfigError;
use crate::domain::error::HttpError;
use crate::domain::error::ValidationError;
use crate::domain::models::{
    AggregateCommandInput, AggregateSearchRequest, AggregateSearchResult, AggregateStatus,
    AuthStatus, CommandMetadata, CommandStatus, ErrorsCommandInput, FieldsResult, FieldsStatus,
    MessageSearchRequest, MessageSearchResult, MessageSearchStatus, NormalizedRow, PingStatus,
    SearchCommandInput, SortDirection, StreamFindStatus, StreamResult, StreamStatus, StreamsResult,
    StreamsStatus, SystemInfoStatus, SystemResult, TraceCommandInput, TraceEvent, TraceGroup,
    TraceStatus, TraceSummary,
};

const DEFAULT_SEARCH_LIMIT: u64 = 50;
const DEFAULT_ERRORS_LIMIT: u64 = 100;
const MAX_STREAM_SEARCH_LIMIT: u64 = 100;
const DEFAULT_SEARCH_OFFSET: u64 = 0;
const DEFAULT_SEARCH_SORT: &str = "timestamp";
const ERRORS_QUERY: &str = "level:<=3";
const TRACE_DEFAULT_TIME_RANGE: &str = "1h";
const TRACE_MAX_LIMIT: u64 = 500;

pub trait ConfigStore: Send + Sync {
    fn config_path(&self) -> Result<PathBuf, ConfigError>;

    fn load(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<GraylogConfig>, ConfigError>> + Send + '_>>;

    fn save(
        &self,
        config: StoredConfig,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConfigError>> + Send + '_>>;
}

pub trait GraylogGateway: Send + Sync {
    fn base_url(&self) -> &str;

    fn ping(&self) -> Pin<Box<dyn Future<Output = Result<(), HttpError>> + Send + '_>>;

    fn search_messages(
        &self,
        request: MessageSearchRequest,
    ) -> Pin<Box<dyn Future<Output = Result<MessageSearchResult, HttpError>> + Send + '_>>;

    fn search_aggregate(
        &self,
        request: AggregateSearchRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AggregateSearchResult, HttpError>> + Send + '_>>;

    fn list_streams(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<StreamsResult, HttpError>> + Send + '_>>;

    fn get_stream(
        &self,
        stream_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<StreamResult, HttpError>> + Send + '_>>;

    fn system_info(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<SystemResult, HttpError>> + Send + '_>>;

    fn list_fields(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<FieldsResult, HttpError>> + Send + '_>>;
}

pub trait GraylogGatewayFactory: Send + Sync {
    fn build_from_config(
        &self,
        config: GraylogConfig,
    ) -> Result<Arc<dyn GraylogGateway>, HttpError>;
}

#[derive(Clone)]
pub struct ApplicationService {
    config_store: Arc<dyn ConfigStore>,
    gateway_factory: Arc<dyn GraylogGatewayFactory>,
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
        )
    }

    pub fn with_config_store(config_store: Arc<dyn ConfigStore>) -> Self {
        Self::with_dependencies(config_store, Arc::new(UnconfiguredGraylogGatewayFactory))
    }

    pub fn with_dependencies(
        config_store: Arc<dyn ConfigStore>,
        gateway_factory: Arc<dyn GraylogGatewayFactory>,
    ) -> Self {
        Self {
            config_store,
            gateway_factory,
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
            SecretString::new(trimmed_token),
            DEFAULT_TIMEOUT_SECONDS,
            true,
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
        self.execute_message_search(
            "search",
            self.build_search_request(input, DEFAULT_SEARCH_LIMIT),
        )
        .await
    }

    pub async fn errors(&self, input: ErrorsCommandInput) -> Result<MessageSearchStatus, CliError> {
        self.execute_message_search(
            "errors",
            self.build_search_request(
                SearchCommandInput {
                    query: ERRORS_QUERY.to_string(),
                    timerange: input.timerange,
                    fields: Vec::new(),
                    limit: input.limit,
                    offset: None,
                    sort: None,
                    sort_direction: None,
                    streams: Vec::new(),
                },
                DEFAULT_ERRORS_LIMIT,
            ),
        )
        .await
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

    pub async fn trace(&self, input: TraceCommandInput) -> Result<TraceStatus, CliError> {
        let query = input.query.trim().to_string();
        if query.is_empty() {
            return Err(CliError::Validation(ValidationError::EmptyField {
                field: "query",
            }));
        }

        let group_by = input.group_by.trim().to_string();
        if group_by.is_empty() {
            return Err(CliError::Validation(ValidationError::EmptyField {
                field: "group_by",
            }));
        }

        let timerange = match input.timerange {
            Some(timerange) => timerange,
            None => crate::domain::timerange::CommandTimerange::from_input(
                crate::domain::timerange::TimerangeInput {
                    relative: Some(TRACE_DEFAULT_TIME_RANGE.to_string()),
                    from: None,
                    to: None,
                },
            )
            .map_err(CliError::from)?,
        };

        let request = MessageSearchRequest {
            query: query.clone(),
            timerange: Some(timerange),
            fields: vec![
                "message".to_string(),
                "source".to_string(),
                "timestamp".to_string(),
                "facility".to_string(),
                "correlationId".to_string(),
                "level".to_string(),
            ],
            limit: TRACE_MAX_LIMIT,
            offset: 0,
            sort: "timestamp".to_string(),
            sort_direction: SortDirection::Asc,
            streams: vec![],
        };

        let config = self
            .config_store
            .load()
            .await?
            .ok_or(CliError::Config(ConfigError::NotConfigured))?;
        let client = self.graylog_gateway_with_config(config)?;
        let result = client.search_messages(request).await?;

        let trace_groups = build_trace_groups(&result.messages, &group_by);
        let summary = build_trace_summary(&trace_groups);

        Ok(TraceStatus {
            ok: true,
            command: "trace",
            query,
            grouped_by: group_by,
            total_events: result.messages.len(),
            trace_groups,
            summary,
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

fn build_trace_groups(messages: &[NormalizedRow], group_by: &str) -> Vec<TraceGroup> {
    let field_key = format!("field: {group_by}");
    let mut groups: BTreeMap<String, Vec<&NormalizedRow>> = BTreeMap::new();

    for row in messages {
        let group_key = row
            .get(&field_key)
            .or_else(|| row.get(group_by))
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        groups.entry(group_key).or_default().push(row);
    }

    groups
        .into_iter()
        .map(|(correlation_id, rows)| {
            let trigger = rows
                .first()
                .and_then(|row| row.get("field: message").or_else(|| row.get("message")))
                .and_then(|value| value.as_str())
                .map(extract_trigger)
                .unwrap_or_else(|| "unknown".to_string());

            let duration_ms = compute_group_duration(&rows);
            let events = categorize_events(&rows);

            TraceGroup {
                correlation_id,
                trigger,
                duration_ms,
                events,
            }
        })
        .collect()
}

fn extract_trigger(message: &str) -> String {
    if message.starts_with("Request started") {
        let parts: Vec<&str> = message.splitn(4, '"').collect();
        if parts.len() >= 4 {
            let method_path = parts[3].trim().trim_matches('"');
            return method_path.to_string();
        }
    }

    message.chars().take(120).collect()
}

fn compute_group_duration(rows: &[&NormalizedRow]) -> Option<u64> {
    let first_ts = rows.first().and_then(|row| {
        row.get("field: timestamp")
            .or_else(|| row.get("timestamp"))
            .and_then(|value| value.as_str())
    })?;
    let last_ts = rows.last().and_then(|row| {
        row.get("field: timestamp")
            .or_else(|| row.get("timestamp"))
            .and_then(|value| value.as_str())
    })?;
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

fn categorize_events(rows: &[&NormalizedRow]) -> Vec<TraceEvent> {
    let mut events = Vec::new();
    let mut db_count = 0_usize;
    let mut internal_count = 0_usize;

    for row in rows {
        let message = row
            .get("field: message")
            .or_else(|| row.get("message"))
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let level = row
            .get("field: level")
            .or_else(|| row.get("level"))
            .and_then(|value| value.as_u64())
            .map(|value| value as u8);
        let source = row
            .get("field: source")
            .or_else(|| row.get("source"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());
        let timestamp = row
            .get("field: timestamp")
            .or_else(|| row.get("timestamp"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());

        if level.is_some_and(|current_level| current_level <= 3) {
            events.push(TraceEvent {
                event_type: "error".to_string(),
                timestamp,
                source,
                level,
                message: Some(message.to_string()),
                method: None,
                target: None,
                status: None,
                duration_ms: None,
                field: None,
                old: None,
                new: None,
                target_adapter: None,
                path: None,
                count: None,
            });
            continue;
        }

        if level == Some(4) {
            events.push(TraceEvent {
                event_type: "warning".to_string(),
                timestamp,
                source,
                level,
                message: Some(message.to_string()),
                method: None,
                target: None,
                status: None,
                duration_ms: None,
                field: None,
                old: None,
                new: None,
                target_adapter: None,
                path: None,
                count: None,
            });
            continue;
        }

        if message.contains("Differences found") || message.contains("Object comparison found") {
            let diffs = if message.contains("Differences found:") {
                message
                    .split_once("Differences found: ")
                    .map(|(_, diff)| diff)
                    .unwrap_or("")
            } else {
                ""
            };
            events.push(TraceEvent {
                event_type: "state_change".to_string(),
                timestamp,
                source,
                level,
                message: None,
                method: None,
                target: None,
                status: None,
                duration_ms: None,
                field: Some(diffs.chars().take(200).collect()),
                old: None,
                new: None,
                target_adapter: None,
                path: None,
                count: None,
            });
            continue;
        }

        if message.contains("HooksHttpClient")
            && message.contains("Sending HTTP request")
            && let Some(url) = extract_url_from_message(message)
        {
            let adapter = extract_adapter_from_url(&url);
            let path = extract_path_from_url(&url);
            events.push(TraceEvent {
                event_type: "callback".to_string(),
                timestamp,
                source,
                level,
                message: None,
                method: None,
                target: None,
                status: None,
                duration_ms: None,
                field: None,
                old: None,
                new: None,
                target_adapter: adapter,
                path,
                count: None,
            });
            continue;
        }

        if message.contains("Sending HTTP request")
            && let Some(url) = extract_url_from_message(message)
        {
            let method = extract_method_from_message(message);
            events.push(TraceEvent {
                event_type: "external_call".to_string(),
                timestamp,
                source,
                level,
                message: None,
                method,
                target: Some(url),
                status: None,
                duration_ms: None,
                field: None,
                old: None,
                new: None,
                target_adapter: None,
                path: None,
                count: None,
            });
            continue;
        }

        if (message.contains("Received HTTP response after")
            || message.contains("Received Cosmos DB response after"))
            && let Some(duration) = extract_duration_from_message(message)
        {
            let status = extract_status_from_response(message);
            if message.contains("Cosmos DB") {
                db_count += 1;
            } else {
                events.push(TraceEvent {
                    event_type: "external_call_response".to_string(),
                    timestamp,
                    source,
                    level,
                    message: None,
                    method: None,
                    target: None,
                    status,
                    duration_ms: Some(duration),
                    field: None,
                    old: None,
                    new: None,
                    target_adapter: None,
                    path: None,
                    count: None,
                });
            }
            continue;
        }

        if message.starts_with("Request started") || message.starts_with("Request processed") {
            internal_count += 1;
            continue;
        }

        if message.contains("Cosmos DB request") || message.contains("Cosmos DB response") {
            db_count += 1;
            continue;
        }

        internal_count += 1;
    }

    if db_count > 0 {
        events.push(TraceEvent {
            event_type: "db_op".to_string(),
            timestamp: None,
            source: None,
            level: None,
            message: None,
            method: None,
            target: None,
            status: None,
            duration_ms: None,
            field: None,
            old: None,
            new: None,
            target_adapter: None,
            path: None,
            count: Some(db_count),
        });
    }

    if internal_count > 0 {
        events.push(TraceEvent {
            event_type: "internal".to_string(),
            timestamp: None,
            source: None,
            level: None,
            message: None,
            method: None,
            target: None,
            status: None,
            duration_ms: None,
            field: None,
            old: None,
            new: None,
            target_adapter: None,
            path: None,
            count: Some(internal_count),
        });
    }

    events
}

fn extract_url_from_message(message: &str) -> Option<String> {
    for part in message.split_whitespace() {
        if part.starts_with("http://") || part.starts_with("https://") {
            return Some(part.trim_matches('"').to_string());
        }
    }

    None
}

fn extract_method_from_message(message: &str) -> Option<String> {
    for part in message.split('"') {
        let trimmed = part.trim();
        if matches!(trimmed, "GET" | "POST" | "PUT" | "PATCH" | "DELETE") {
            return Some(trimmed.to_string());
        }
    }

    None
}

fn extract_duration_from_message(message: &str) -> Option<u64> {
    if let Some(after) = message.split("after ").nth(1) {
        let num_str: String = after
            .chars()
            .take_while(|value| value.is_ascii_digit() || *value == '.')
            .collect();
        let duration: f64 = num_str.parse().ok()?;
        Some(duration as u64)
    } else {
        None
    }
}

fn extract_status_from_response(message: &str) -> Option<u64> {
    if message.contains(" BadRequest") {
        return Some(400);
    }
    if message.contains(" NotFound") {
        return Some(404);
    }
    if message.contains(" Conflict") {
        return Some(409);
    }
    if message.contains(" InternalServerError") {
        return Some(500);
    }
    if message.contains(" Created") {
        return Some(201);
    }
    if message.contains(" OK") {
        return Some(200);
    }
    if message.contains(" NoContent") {
        return Some(204);
    }

    None
}

fn extract_adapter_from_url(url: &str) -> Option<String> {
    let host = url.split("//").nth(1)?.split('.').next()?;
    Some(host.to_string())
}

fn extract_path_from_url(url: &str) -> Option<String> {
    let after_scheme = url.split("//").nth(1)?;
    let path_start = after_scheme.find('/')?;
    Some(after_scheme[path_start..].to_string())
}

fn build_trace_summary(groups: &[TraceGroup]) -> TraceSummary {
    let mut total_errors = 0_usize;
    let mut total_external_calls = 0_usize;
    let mut services = BTreeSet::new();

    for group in groups {
        for event in &group.events {
            match event.event_type.as_str() {
                "error" => total_errors += 1,
                "external_call" | "external_call_response" => total_external_calls += 1,
                _ => {}
            }
            if let Some(source) = &event.source {
                let service = source.split('-').next().unwrap_or(source);
                services.insert(service.to_string());
            }
        }
    }

    TraceSummary {
        total_errors,
        total_external_calls,
        services_involved: services.into_iter().collect(),
    }
}

struct UnconfiguredConfigStore;
struct UnconfiguredGraylogGatewayFactory;

impl ConfigStore for UnconfiguredConfigStore {
    fn config_path(&self) -> Result<PathBuf, ConfigError> {
        Err(ConfigError::StoreUnavailable {
            backend: "unconfigured",
            message: "no config store has been attached to ApplicationService".to_string(),
        })
    }

    fn load(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<GraylogConfig>, ConfigError>> + Send + '_>> {
        Box::pin(async {
            Err(ConfigError::StoreUnavailable {
                backend: "unconfigured",
                message: "no config store has been attached to ApplicationService".to_string(),
            })
        })
    }

    fn save(
        &self,
        _config: StoredConfig,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConfigError>> + Send + '_>> {
        Box::pin(async {
            Err(ConfigError::StoreUnavailable {
                backend: "unconfigured",
                message: "no config store has been attached to ApplicationService".to_string(),
            })
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
