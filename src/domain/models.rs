use secrecy::SecretString;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::domain::config::StoredConfig;
use crate::domain::timerange::CommandTimerange;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationType {
    Terms,
    DateHistogram,
    Cardinality,
    Stats,
    Min,
    Max,
    Avg,
    Sum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

pub type PersistedConfig = StoredConfig;
pub type RuntimeToken = SecretString;
pub type JsonObject = Map<String, Value>;
pub type NormalizedRow = JsonObject;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCommandInput {
    pub query: String,
    pub timerange: Option<CommandTimerange>,
    pub fields: Vec<String>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub sort: Option<String>,
    pub sort_direction: Option<SortDirection>,
    pub group_by: Option<String>,
    pub all_pages: bool,
    pub all_fields: bool,
    pub streams: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorsCommandInput {
    pub timerange: Option<CommandTimerange>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageSearchStatus {
    pub ok: bool,
    pub command: &'static str,
    pub query: String,
    pub returned: usize,
    pub messages: Vec<NormalizedRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouped_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<SearchGroup>>,
    pub metadata: JsonObject,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchGroup {
    pub key: String,
    pub count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSearchRequest {
    pub query: String,
    pub timerange: Option<CommandTimerange>,
    pub fields: Vec<String>,
    pub limit: u64,
    pub offset: u64,
    pub sort: String,
    pub sort_direction: SortDirection,
    pub streams: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageSearchResult {
    pub messages: Vec<NormalizedRow>,
    pub total_results: Option<u64>,
    pub metadata: JsonObject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSearchRequest {
    pub query: String,
    pub timerange: Option<CommandTimerange>,
    pub aggregation_type: AggregationType,
    pub field: String,
    pub size: Option<u64>,
    pub interval: Option<String>,
    pub streams: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateCommandInput {
    pub query: String,
    pub timerange: Option<CommandTimerange>,
    pub aggregation_type: AggregationType,
    pub field: String,
    pub size: Option<u64>,
    pub interval: Option<String>,
    pub streams: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AggregateSearchResult {
    pub rows: Vec<NormalizedRow>,
    pub metadata: JsonObject,
}

#[derive(Debug, Clone, Serialize)]
pub struct AggregateStatus {
    pub ok: bool,
    pub command: &'static str,
    pub aggregation_type: &'static str,
    pub rows: Vec<NormalizedRow>,
    pub metadata: JsonObject,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamsResult {
    pub streams: Vec<JsonObject>,
    pub total: Option<u64>,
    pub metadata: JsonObject,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamResult {
    pub stream: JsonObject,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamsStatus {
    pub ok: bool,
    pub command: &'static str,
    pub streams: Vec<JsonObject>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamStatus {
    pub ok: bool,
    pub command: &'static str,
    pub stream: JsonObject,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamFindStatus {
    pub ok: bool,
    pub command: &'static str,
    pub name: String,
    pub returned: usize,
    pub streams: Vec<JsonObject>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemResult {
    pub system: JsonObject,
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemInfoStatus {
    pub ok: bool,
    pub command: &'static str,
    pub system: JsonObject,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PingStatus {
    pub ok: bool,
    pub command: &'static str,
    pub reachable: bool,
    pub graylog_url: String,
}

impl AggregationType {
    pub fn as_cli_value(self) -> &'static str {
        match self {
            Self::Terms => "terms",
            Self::DateHistogram => "date_histogram",
            Self::Cardinality => "cardinality",
            Self::Stats => "stats",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::Sum => "sum",
        }
    }

    pub fn graylog_metric_name(self) -> Option<&'static str> {
        match self {
            Self::Terms | Self::DateHistogram => None,
            Self::Cardinality => Some("card"),
            Self::Stats => None,
            Self::Min => Some("min"),
            Self::Max => Some("max"),
            Self::Avg => Some("avg"),
            Self::Sum => Some("sum"),
        }
    }
}

impl SortDirection {
    pub fn as_api_value(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommandMetadata {
    pub command: &'static str,
    pub configured: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CommandStatus {
    pub ok: bool,
    pub command: &'static str,
    pub configured: bool,
}

impl CommandStatus {
    pub fn ok(command: &'static str) -> Self {
        Self {
            ok: true,
            command,
            configured: false,
        }
    }

    pub fn with_metadata(metadata: CommandMetadata) -> Self {
        Self {
            ok: true,
            command: metadata.command,
            configured: metadata.configured,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuthStatus {
    pub ok: bool,
    pub command: &'static str,
    pub config_path: String,
    pub graylog_url: String,
}

impl AuthStatus {
    pub fn ok(config_path: String, graylog_url: String) -> Self {
        Self {
            ok: true,
            command: "auth",
            config_path,
            graylog_url,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldsResult {
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldsStatus {
    pub ok: bool,
    pub command: &'static str,
    pub fields: Vec<String>,
    pub total: usize,
}
