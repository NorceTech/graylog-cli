use std::env;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use std::time::{SystemTime, UNIX_EPOCH};

use tokio::task;

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::application::service::ConfigStore;
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
        if let Some(path) = non_empty_env_path("XDG_CONFIG_HOME")? {
            return Ok(path.join("graylog-cli").join("config.toml"));
        }

        let home = non_empty_env_path("HOME")?.ok_or_else(|| ConfigError::StoreUnavailable {
            backend: "filesystem",
            message: "HOME is not set and XDG_CONFIG_HOME is unavailable".to_string(),
        })?;

        Ok(home.join(".config").join("graylog-cli").join("config.toml"))
    }

    async fn load_impl(&self) -> Result<Option<GraylogConfig>, ConfigError> {
        let config_path = Self::config_path_impl()?;

        let contents = match task::spawn_blocking({
            let config_path = config_path.clone();
            move || std::fs::read_to_string(&config_path)
        })
        .await
        .map_err(|error| ConfigError::StoreUnavailable {
            backend: "filesystem",
            message: format!("failed to join config read task: {error}"),
        })? {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ConfigError::Filesystem {
                    operation: "reading",
                    path: config_path.display().to_string(),
                    message: error.to_string(),
                });
            }
        };

        let stored = toml::from_str::<StoredConfig>(&contents).map_err(|error| {
            ConfigError::Deserialization {
                message: error.to_string(),
            }
        })?;

        stored
            .into_runtime()
            .map(Some)
            .map_err(|error| ConfigError::InvalidFormat {
                message: error.to_string(),
            })
    }

    async fn save_impl(&self, config: StoredConfig) -> Result<(), ConfigError> {
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

fn non_empty_env_path(name: &'static str) -> Result<Option<PathBuf>, ConfigError> {
    match env::var_os(name) {
        Some(value) if value.is_empty() => Err(ConfigError::StoreUnavailable {
            backend: "filesystem",
            message: format!("{name} is set but empty"),
        }),
        Some(value) => Ok(Some(PathBuf::from(value))),
        None => Ok(None),
    }
}

impl ConfigStore for FileConfigStore {
    fn config_path(&self) -> Result<PathBuf, ConfigError> {
        Self::config_path_impl()
    }

    fn load(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<GraylogConfig>, ConfigError>> + Send + '_>> {
        Box::pin(async move { self.load_impl().await })
    }

    fn save(
        &self,
        config: StoredConfig,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConfigError>> + Send + '_>> {
        Box::pin(async move { self.save_impl(config).await })
    }
}

fn fields_cache_path(config_path: &Path) -> PathBuf {
    config_path.with_file_name("fields_cache.json")
}

pub fn read_fields_cache(config_path: &Path, ttl_seconds: u64) -> Option<Vec<String>> {
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

pub fn write_fields_cache(config_path: &Path, fields: &[String]) -> Result<(), ConfigError> {
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

    let temp_path = temporary_config_path(config_path);
    let temp_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)
        .map_err(|error| ConfigError::Filesystem {
            operation: "creating temporary file",
            path: temp_path.display().to_string(),
            message: error.to_string(),
        })?;

    set_file_permissions(&temp_path, &temp_file)?;

    let mut temp_file = temp_file;
    use std::io::Write as _;
    temp_file
        .write_all(serialized.as_bytes())
        .and_then(|_| temp_file.sync_all())
        .map_err(|error| ConfigError::Filesystem {
            operation: "writing temporary file",
            path: temp_path.display().to_string(),
            message: error.to_string(),
        })?;

    std::fs::rename(&temp_path, config_path).map_err(|error| ConfigError::Filesystem {
        operation: "replacing config file",
        path: config_path.display().to_string(),
        message: error.to_string(),
    })?;

    Ok(())
}

fn temporary_config_path(config_path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();

    config_path.with_extension(format!("tmp-{}-{nanos}", std::process::id()))
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

fn set_file_permissions(config_path: &Path, file: &std::fs::File) -> Result<(), ConfigError> {
    #[cfg(unix)]
    {
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|error| ConfigError::Filesystem {
                operation: "setting file permissions",
                path: config_path.display().to_string(),
                message: error.to_string(),
            })?;
    }

    Ok(())
}
