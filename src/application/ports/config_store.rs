use crate::domain::config::Config;
use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config store unavailable: {0}")]
    StoreUnavailable(String),
    #[error("invalid config format: {0}")]
    InvalidFormat(String),
    #[error("operation failed: {0}")]
    OperationFailure(String),
}

#[async_trait]
pub trait ConfigStore: Send + Sync {
    async fn load(&self) -> exn::Result<Option<Config>, ConfigError>;

    async fn save(&self, config: Config) -> exn::Result<(), ConfigError>;
}
