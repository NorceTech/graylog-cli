use async_trait::async_trait;
use exn::ResultExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("config store unavailable: {0}")]
    StoreUnavailable(String),
    #[error("invalid config format: {0}")]
    InvalidFormat(String),
    #[error("operation failed: {0}")]
    OperationFailure(String),
}

#[async_trait]
pub trait CacheStore: Send + Sync {
    async fn get_serialized(&self, key: &String) -> exn::Result<Option<String>, CacheError>;

    async fn save_serialized(&self, key: String, data: String) -> exn::Result<(), CacheError>;
}

#[async_trait]
pub trait CacheStoreExt: CacheStore {
    async fn get<T>(&self, key: &String) -> exn::Result<Option<T>, CacheError>
    where
        T: DeserializeOwned + Send,
    {
        let Some(serialized) = self.get_serialized(key).await? else {
            return Ok(None);
        };
        let deserialized = serde_json::from_str::<T>(&serialized).or_raise(|| {
            CacheError::InvalidFormat(format!("failed to deserialize cache data for key '{key}'"))
        })?;
        Ok(Some(deserialized))
    }

    async fn save<T>(&self, key: String, data: String) -> exn::Result<(), CacheError>
    where
        T: Serialize + Send,
    {
        let serialized = serde_json::to_string(&data).or_raise(|| {
            CacheError::InvalidFormat(format!("failed to serialize cache data for key '{key}'"))
        })?;
        self.save_serialized(key, serialized).await
    }
}
