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
    AuthStatus, CommandMetadata, CommandStatus, ErrorsCommandInput, MessageSearchRequest,
    MessageSearchResult, MessageSearchStatus, PingStatus, SearchCommandInput, SortDirection,
    StreamFindStatus, StreamResult, StreamStatus, StreamsResult, StreamsStatus, SystemInfoStatus,
    SystemResult,
};

const DEFAULT_SEARCH_LIMIT: u64 = 50;
const DEFAULT_ERRORS_LIMIT: u64 = 100;
const MAX_STREAM_SEARCH_LIMIT: u64 = 100;
const DEFAULT_SEARCH_OFFSET: u64 = 0;
const DEFAULT_SEARCH_SORT: &str = "timestamp";
const ERRORS_QUERY: &str = "level:<=3";

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
