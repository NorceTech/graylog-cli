use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use async_trait::async_trait;
use tokio::task;

use crate::application::ports::cache_store::{CacheError, CacheStore};
use crate::application::ports::config_store::{ConfigError, ConfigStore};
use crate::domain::config::Config;

#[derive(Debug, Default, Clone, Copy)]
pub struct FileConfigStore;

impl FileConfigStore {
    pub fn new() -> Self {
        Self
    }

    fn config_path_impl() -> Result<PathBuf, ConfigError> {
        dirs::config_dir()
            .ok_or_else(|| {
                ConfigError::StoreUnavailable("could not determine config directory".to_string())
            })
            .map(|dir| dir.join("graylog-cli").join("config.toml"))
    }

    fn cache_path_for_key(key: &str) -> Result<PathBuf, CacheError> {
        let config_dir = dirs::config_dir().ok_or_else(|| {
            CacheError::StoreUnavailable("could not determine config directory".to_string())
        })?;

        Ok(config_dir.join("graylog-cli").join(format!("{key}.json")))
    }
}

#[async_trait]
impl ConfigStore for FileConfigStore {
    async fn load(&self) -> exn::Result<Option<Config>, ConfigError> {
        let config_path = Self::config_path_impl()
            .map_err(|error| ConfigError::OperationFailure(error.to_string()))?;

        if !config_path.exists() {
            return Ok(None);
        }

        let config_path_clone = config_path.clone();
        task::spawn_blocking(move || {
            let contents = std::fs::read_to_string(&config_path_clone).map_err(|error| {
                ConfigError::OperationFailure(format!("failed to read config: {error}"))
            })?;

            toml::from_str::<Config>(&contents).map_err(|error| {
                ConfigError::InvalidFormat(format!("failed to parse config: {error}"))
            })
        })
        .await
        .map_err(|error| {
            ConfigError::StoreUnavailable(format!("failed to join config read task: {error}"))
        })?
        .map_err(Into::into)
        .map(Some)
    }

    async fn save(&self, config: Config) -> exn::Result<(), ConfigError> {
        let config_path = Self::config_path_impl()
            .map_err(|error| ConfigError::OperationFailure(error.to_string()))?;
        let serialized = toml::to_string(&config).map_err(|error| {
            ConfigError::InvalidFormat(format!("failed to serialize config: {error}"))
        })?;

        task::spawn_blocking(move || write_config_atomically(&config_path, &serialized))
            .await
            .map_err(|error| {
                ConfigError::StoreUnavailable(format!("failed to join config write task: {error}"))
            })?
            .map_err(Into::into)
    }
}

#[async_trait]
impl CacheStore for FileConfigStore {
    async fn get_serialized(&self, key: &str) -> exn::Result<Option<String>, CacheError> {
        let cache_path = Self::cache_path_for_key(key)?;

        let contents = task::spawn_blocking(move || std::fs::read_to_string(&cache_path).ok())
            .await
            .map_err(|error| {
                CacheError::StoreUnavailable(format!("failed to read cache: {error}"))
            })?;

        Ok(contents)
    }

    async fn save_serialized(&self, key: String, data: String) -> exn::Result<(), CacheError> {
        let cache_path = Self::cache_path_for_key(&key)?;

        task::spawn_blocking(move || {
            let parent = cache_path.parent().ok_or_else(|| {
                CacheError::OperationFailure("cache path has no parent directory".to_string())
            })?;
            std::fs::create_dir_all(parent)
                .map_err(|error| CacheError::OperationFailure(error.to_string()))?;
            std::fs::write(&cache_path, data)
                .map_err(|error| CacheError::OperationFailure(error.to_string()))?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(
                    &cache_path,
                    std::fs::Permissions::from_mode(0o600),
                );
            }

            Ok::<(), CacheError>(())
        })
        .await
        .map_err(|error| CacheError::StoreUnavailable(format!("failed to write cache: {error}")))?
        .map_err(Into::into)
    }
}

fn write_config_atomically(config_path: &Path, serialized: &str) -> Result<(), ConfigError> {
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    let config_dir = config_path.parent().ok_or_else(|| {
        ConfigError::StoreUnavailable(format!(
            "config path `{}` has no parent directory",
            config_path.display()
        ))
    })?;

    std::fs::create_dir_all(config_dir)
        .map_err(|error| ConfigError::OperationFailure(error.to_string()))?;

    set_directory_permissions(config_dir)?;

    let mut temp_file = NamedTempFile::new_in(config_dir)
        .map_err(|error| ConfigError::OperationFailure(error.to_string()))?;

    temp_file
        .write_all(serialized.as_bytes())
        .and_then(|_| temp_file.as_file().sync_all())
        .map_err(|error| ConfigError::OperationFailure(error.to_string()))?;

    temp_file
        .persist(config_path)
        .map_err(|error| ConfigError::OperationFailure(error.error.to_string()))?;

    Ok(())
}

fn set_directory_permissions(config_dir: &Path) -> Result<(), ConfigError> {
    #[cfg(unix)]
    {
        std::fs::set_permissions(config_dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| ConfigError::OperationFailure(error.to_string()))?;
    }

    Ok(())
}
