use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use config::{Config, File, FileFormat};
use serde::{Deserialize, Serialize};
use tokio::task;

use crate::application::ports::{ConfigStore, FieldsCacheStore};
use crate::domain::config::{GraylogConfig, StoredConfig};
use crate::domain::error::ConfigError;

#[derive(Debug, Default, Clone, Copy)]
pub struct FileConfigStore;

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldsCache {
    pub fields: Vec<String>,
    pub fetched_at: u64,
}

impl FileConfigStore {
    pub fn new() -> Self {
        Self
    }

    fn config_path_impl() -> Result<PathBuf, ConfigError> {
        dirs::config_dir()
            .ok_or_else(|| ConfigError::StoreUnavailable {
                backend: "filesystem",
                message: "could not determine config directory".to_string(),
            })
            .map(|dir| dir.join("graylog-cli").join("config.toml"))
    }
}

#[async_trait]
impl ConfigStore for FileConfigStore {
    fn config_path(&self) -> Result<PathBuf, ConfigError> {
        Self::config_path_impl()
    }

    async fn load(&self) -> Result<Option<GraylogConfig>, ConfigError> {
        let config_path = Self::config_path_impl()?;

        if !config_path.exists() {
            return Ok(None);
        }

        let config_path_clone = config_path.clone();
        let stored = task::spawn_blocking(move || {
            Config::builder()
                .add_source(File::from(config_path_clone).format(FileFormat::Toml))
                .build()
                .and_then(|c| c.try_deserialize::<StoredConfig>())
                .map_err(|error| ConfigError::Deserialization {
                    message: error.to_string(),
                })
        })
        .await
        .map_err(|error| ConfigError::StoreUnavailable {
            backend: "filesystem",
            message: format!("failed to join config read task: {error}"),
        })??;

        stored
            .into_runtime()
            .map(Some)
            .map_err(|error| ConfigError::InvalidFormat {
                message: error.to_string(),
            })
    }

    async fn save(&self, config: StoredConfig) -> Result<(), ConfigError> {
        let config_path = Self::config_path_impl()?;
        let serialized = toml::to_string(&config).map_err(|error| ConfigError::Serialization {
            message: error.to_string(),
        })?;

        task::spawn_blocking(move || write_config_atomically(&config_path, &serialized))
            .await
            .map_err(|error| ConfigError::StoreUnavailable {
                backend: "filesystem",
                message: format!("failed to join config write task: {error}"),
            })?
    }
}

#[async_trait]
impl FieldsCacheStore for FileConfigStore {
    async fn load_fields(
        &self,
        config_path: &Path,
        ttl_seconds: u64,
    ) -> Result<Option<Vec<String>>, ConfigError> {
        let config_path = config_path.to_path_buf();

        task::spawn_blocking(move || Ok(read_fields_cache(&config_path, ttl_seconds)))
            .await
            .map_err(|error| ConfigError::StoreUnavailable {
                backend: "filesystem",
                message: format!("failed to join fields cache read task: {error}"),
            })?
    }

    async fn save_fields(&self, config_path: &Path, fields: &[String]) -> Result<(), ConfigError> {
        let config_path = config_path.to_path_buf();
        let fields = fields.to_vec();

        task::spawn_blocking(move || write_fields_cache(&config_path, &fields))
            .await
            .map_err(|error| ConfigError::StoreUnavailable {
                backend: "filesystem",
                message: format!("failed to join fields cache write task: {error}"),
            })?
    }
}

fn fields_cache_path(config_path: &Path) -> PathBuf {
    config_path.with_file_name("fields_cache.json")
}

fn read_fields_cache(config_path: &Path, ttl_seconds: u64) -> Option<Vec<String>> {
    let cache_path = fields_cache_path(config_path);
    let contents = std::fs::read_to_string(&cache_path).ok()?;
    let cache: FieldsCache = serde_json::from_str(&contents).ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();

    if now.saturating_sub(cache.fetched_at) < ttl_seconds {
        Some(cache.fields)
    } else {
        None
    }
}

fn write_fields_cache(config_path: &Path, fields: &[String]) -> Result<(), ConfigError> {
    let cache_path = fields_cache_path(config_path);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let cache = FieldsCache {
        fields: fields.to_vec(),
        fetched_at: now,
    };
    let json = serde_json::to_string(&cache).map_err(|error| ConfigError::Serialization {
        message: error.to_string(),
    })?;

    std::fs::write(&cache_path, json).map_err(|error| ConfigError::Filesystem {
        operation: "writing fields cache",
        path: cache_path.display().to_string(),
        message: error.to_string(),
    })
}

fn write_config_atomically(config_path: &Path, serialized: &str) -> Result<(), ConfigError> {
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    let config_dir = config_path
        .parent()
        .ok_or_else(|| ConfigError::StoreUnavailable {
            backend: "filesystem",
            message: format!(
                "config path `{}` has no parent directory",
                config_path.display()
            ),
        })?;

    std::fs::create_dir_all(config_dir).map_err(|error| ConfigError::Filesystem {
        operation: "creating directory",
        path: config_dir.display().to_string(),
        message: error.to_string(),
    })?;

    set_directory_permissions(config_dir)?;

    // tempfile::NamedTempFile::new_in creates files with mode 0o600 on Unix by default.
    let mut temp_file =
        NamedTempFile::new_in(config_dir).map_err(|error| ConfigError::Filesystem {
            operation: "creating temporary file",
            path: config_dir.display().to_string(),
            message: error.to_string(),
        })?;

    temp_file
        .write_all(serialized.as_bytes())
        .and_then(|_| temp_file.as_file().sync_all())
        .map_err(|error| ConfigError::Filesystem {
            operation: "writing temporary file",
            path: temp_file.path().display().to_string(),
            message: error.to_string(),
        })?;

    temp_file
        .persist(config_path)
        .map_err(|error| ConfigError::Filesystem {
            operation: "replacing config file",
            path: config_path.display().to_string(),
            message: error.error.to_string(),
        })?;

    Ok(())
}

fn set_directory_permissions(config_dir: &Path) -> Result<(), ConfigError> {
    #[cfg(unix)]
    {
        std::fs::set_permissions(config_dir, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| ConfigError::Filesystem {
                operation: "setting directory permissions",
                path: config_dir.display().to_string(),
                message: error.to_string(),
            },
        )?;
    }

    Ok(())
}
