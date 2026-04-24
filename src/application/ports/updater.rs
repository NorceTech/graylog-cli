use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UpdaterError {
    #[error("updater unavailable: {0}")]
    Unavailable(String),
    #[error("network error while contacting release server: {0}")]
    Network(String),
    #[error("invalid release metadata: {0}")]
    InvalidMetadata(String),
    #[error("unsupported platform: {0}")]
    UnsupportedPlatform(String),
    #[error("no release asset `{0}` available for this platform")]
    AssetNotFound(String),
    #[error("download failed: {0}")]
    Download(String),
    #[error("apply failed: {0}")]
    Apply(String),
    #[error("invalid version `{value}`: {message}")]
    InvalidVersion { value: String, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseInfo {
    pub version: String,
    pub asset_url: String,
    pub asset_name: String,
}

#[async_trait]
pub trait UpdaterGateway: Send + Sync {
    async fn latest_release(&self, asset_name: &str) -> Result<ReleaseInfo, UpdaterError>;

    async fn download_asset(&self, url: &str) -> Result<Vec<u8>, UpdaterError>;
}
