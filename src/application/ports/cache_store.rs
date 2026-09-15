use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache store unavailable: {0}")]
    StoreUnavailable(String),
    #[error("invalid config format: {0}")]
    InvalidFormat(String),
    #[error("operation failed: {0}")]
    OperationFailure(String),
}

#[async_trait]
pub trait CacheStore: Send + Sync {
    async fn get_serialized(&self, key: &str) -> exn::Result<Option<String>, CacheError>;

    async fn save_serialized(&self, key: String, data: String) -> exn::Result<(), CacheError>;

    /// Drops a cached entry; missing keys are not an error. Used to
    /// invalidate per-profile caches when a profile is replaced.
    async fn remove_serialized(&self, key: &str) -> exn::Result<(), CacheError>;
}
