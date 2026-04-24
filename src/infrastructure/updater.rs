use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode, header};
use serde::Deserialize;

use crate::application::ports::updater::{ReleaseInfo, UpdaterError, UpdaterGateway};

const DEFAULT_RELEASES_URL: &str =
    "https://api.github.com/repos/NorceTech/graylog-cli/releases/latest";
const USER_AGENT: &str = concat!("graylog-cli/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone)]
pub struct GitHubUpdaterGateway {
    client: Client,
    releases_url: String,
}

impl GitHubUpdaterGateway {
    pub fn new() -> Result<Self, UpdaterError> {
        Self::with_url(DEFAULT_RELEASES_URL.to_string())
    }

    pub fn with_url(releases_url: String) -> Result<Self, UpdaterError> {
        let client = Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| {
                UpdaterError::Unavailable(format!("failed to build HTTP client: {error}"))
            })?;
        Ok(Self {
            client,
            releases_url,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    #[serde(default)]
    assets: Vec<GitHubAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubAsset {
    name: String,
    browser_download_url: String,
}

#[async_trait]
impl UpdaterGateway for GitHubUpdaterGateway {
    async fn latest_release(&self, asset_name: &str) -> Result<ReleaseInfo, UpdaterError> {
        let response = self
            .client
            .get(&self.releases_url)
            .header(header::ACCEPT, "application/vnd.github+json")
            .send()
            .await
            .map_err(|error| UpdaterError::Network(error.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            return Err(UpdaterError::Network(format!(
                "github releases API returned HTTP {status}"
            )));
        }

        let release = response
            .json::<GitHubRelease>()
            .await
            .map_err(|error| UpdaterError::InvalidMetadata(error.to_string()))?;

        let asset = release
            .assets
            .into_iter()
            .find(|candidate| candidate.name == asset_name)
            .ok_or_else(|| UpdaterError::AssetNotFound(asset_name.to_string()))?;

        Ok(ReleaseInfo {
            version: release.tag_name,
            asset_url: asset.browser_download_url,
            asset_name: asset.name,
        })
    }

    async fn download_asset(&self, url: &str) -> Result<Vec<u8>, UpdaterError> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|error| UpdaterError::Download(error.to_string()))?;

        let status = response.status();
        if status == StatusCode::FOUND || status == StatusCode::MOVED_PERMANENTLY {
            return Err(UpdaterError::Download(format!(
                "unexpected redirect {status} while downloading asset"
            )));
        }
        if !status.is_success() {
            return Err(UpdaterError::Download(format!(
                "asset download returned HTTP {status}"
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|error| UpdaterError::Download(error.to_string()))?;

        Ok(bytes.to_vec())
    }
}
