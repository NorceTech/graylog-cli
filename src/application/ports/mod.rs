use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::config::{GraylogConfig, StoredConfig};
use crate::domain::error::{ConfigError, HttpError};
use crate::domain::models::{
    AggregateSearchRequest, AggregateSearchResult, FieldsResult, MessageSearchRequest,
    MessageSearchResult, StreamResult, StreamsResult, SystemResult,
};

#[async_trait]
pub trait ConfigStore: Send + Sync {
    fn config_path(&self) -> Result<PathBuf, ConfigError>;

    async fn load(&self) -> Result<Option<GraylogConfig>, ConfigError>;

    async fn save(&self, config: StoredConfig) -> Result<(), ConfigError>;
}

#[async_trait]
pub trait FieldsCacheStore: Send + Sync {
    async fn load_fields(
        &self,
        config_path: &Path,
        ttl_seconds: u64,
    ) -> Result<Option<Vec<String>>, ConfigError>;

    async fn save_fields(&self, config_path: &Path, fields: &[String]) -> Result<(), ConfigError>;
}

#[async_trait]
pub trait GraylogGateway: Send + Sync {
    fn base_url(&self) -> &str;

    async fn ping(&self) -> Result<(), HttpError>;

    async fn search_messages(
        &self,
        request: MessageSearchRequest,
    ) -> Result<MessageSearchResult, HttpError>;

    async fn search_aggregate(
        &self,
        request: AggregateSearchRequest,
    ) -> Result<AggregateSearchResult, HttpError>;

    async fn list_streams(&self) -> Result<StreamsResult, HttpError>;

    async fn get_stream(&self, stream_id: String) -> Result<StreamResult, HttpError>;

    async fn system_info(&self) -> Result<SystemResult, HttpError>;

    async fn list_fields(&self) -> Result<FieldsResult, HttpError>;
}

pub trait GraylogGatewayFactory: Send + Sync {
    fn build_from_config(
        &self,
        config: GraylogConfig,
    ) -> Result<Arc<dyn GraylogGateway>, HttpError>;
}
