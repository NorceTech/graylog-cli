use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::task;

use crate::application::ports::cache_store::{CacheError, CacheStore};
use crate::application::ports::config_store::{ConfigError, ConfigStore};
use crate::domain::config::{Config, DEFAULT_PROFILE_NAME, GraylogConfig, UpdaterConfig};

/// On-disk shape of `config.toml`, supporting both the current profile-based
/// format and the legacy single-instance `[graylog]` format.
///
/// This type is private to the infrastructure layer: legacy files are migrated
/// in memory while loading, and `ConfigStore::save` only ever writes the new
/// format. Loading a legacy file never rewrites it.
#[derive(Debug, Deserialize)]
struct RawConfigFile {
    #[serde(default)]
    graylog: Option<GraylogConfig>,
    #[serde(default)]
    profiles: BTreeMap<String, GraylogConfig>,
    #[serde(default)]
    active_profile: Option<String>,
    #[serde(default)]
    updater: UpdaterConfig,
}

/// Parses raw file contents into a domain `Config`, migrating a legacy
/// `[graylog]` table into `profiles.default` and selecting an active profile
/// when the file does not name one.
fn parse_config_file(contents: &str) -> Result<Config, ConfigError> {
    let raw: RawConfigFile = toml::from_str(contents)
        .map_err(|error| ConfigError::InvalidFormat(format!("failed to parse config: {error}")))?;

    let mut profiles = raw.profiles;
    if let Some(legacy) = raw.graylog {
        if !profiles.contains_key(DEFAULT_PROFILE_NAME) {
            profiles.insert(DEFAULT_PROFILE_NAME.to_string(), legacy);
        } else {
            tracing::warn!("legacy [graylog] table ignored: profiles.default already exists");
        }
    }

    let active_profile = raw.active_profile.or_else(|| {
        profiles
            .contains_key(DEFAULT_PROFILE_NAME)
            .then(|| DEFAULT_PROFILE_NAME.to_string())
            .or_else(|| profiles.keys().next().cloned())
    });

    Ok(Config {
        profiles,
        active_profile,
        updater: raw.updater,
    })
}

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

            parse_config_file(&contents)
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
                let _ =
                    std::fs::set_permissions(&cache_path, std::fs::Permissions::from_mode(0o600));
            }

            Ok::<(), CacheError>(())
        })
        .await
        .map_err(|error| CacheError::StoreUnavailable(format!("failed to write cache: {error}")))?
        .map_err(Into::into)
    }

    async fn remove_serialized(&self, key: &str) -> exn::Result<(), CacheError> {
        let cache_path = Self::cache_path_for_key(key)?;

        task::spawn_blocking(move || match std::fs::remove_file(&cache_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CacheError::OperationFailure(error.to_string())),
        })
        .await
        .map_err(|error| CacheError::StoreUnavailable(format!("failed to clear cache: {error}")))?
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

#[cfg(test)]
mod tests {
    use super::parse_config_file;
    use crate::domain::config::DEFAULT_PROFILE_NAME;

    #[test]
    fn legacy_graylog_table_migrates_into_default_profile() {
        let contents = r#"
            [graylog]
            url = "https://graylog.example.com"
            token = "legacy-token"
            timeout_seconds = 42
        "#;

        let config = parse_config_file(contents).expect("legacy config should parse");

        assert_eq!(config.profiles.len(), 1);
        let profile = &config.profiles[DEFAULT_PROFILE_NAME];
        assert_eq!(profile.url.as_str(), "https://graylog.example.com/");
        assert_eq!(profile.timeout_seconds, 42);
        assert_eq!(config.active_profile.as_deref(), Some(DEFAULT_PROFILE_NAME));
    }

    #[test]
    fn legacy_graylog_preserves_updater_settings() {
        let contents = r#"
            [graylog]
            url = "https://graylog.example.com"
            token = "legacy-token"

            [updater]
            disable_auto_update = true
        "#;

        let config = parse_config_file(contents).expect("legacy config should parse");

        assert!(config.updater.disable_auto_update);
    }

    #[test]
    fn profile_format_passes_through_unchanged() {
        let contents = r#"
            active_profile = "prod"

            [profiles.prod]
            url = "https://prod.example.com"
            token = "prod-token"

            [profiles.staging]
            url = "https://staging.example.com"
            token = "staging-token"
        "#;

        let config = parse_config_file(contents).expect("profile config should parse");

        assert_eq!(config.profiles.len(), 2);
        assert_eq!(config.active_profile.as_deref(), Some("prod"));
        assert!(config.profiles.contains_key("staging"));
    }

    #[test]
    fn profile_format_without_active_profile_selects_default_then_first() {
        let contents = r#"
            [profiles.beta]
            url = "https://beta.example.com"
            token = "beta-token"

            [profiles.default]
            url = "https://graylog.example.com"
            token = "default-token"
        "#;

        let config = parse_config_file(contents).expect("profile config should parse");

        assert_eq!(config.active_profile.as_deref(), Some(DEFAULT_PROFILE_NAME));
    }

    #[test]
    fn migration_keeps_existing_default_profile_over_legacy_graylog() {
        let contents = r#"
            [graylog]
            url = "https://legacy.example.com"
            token = "legacy-token"

            [profiles.default]
            url = "https://current.example.com"
            token = "current-token"
        "#;

        let config = parse_config_file(contents).expect("mixed config should parse");

        assert_eq!(config.profiles.len(), 1);
        assert_eq!(
            config.profiles[DEFAULT_PROFILE_NAME].url.as_str(),
            "https://current.example.com/"
        );
    }

    #[test]
    fn empty_profiles_migrate_to_not_configured_state() {
        let contents = "";

        let config = parse_config_file(contents).expect("empty config should parse");

        assert!(config.profiles.is_empty());
        assert_eq!(config.active_profile, None);
    }

    #[test]
    fn malformed_config_is_rejected() {
        let contents = "not [ valid toml";

        let error = parse_config_file(contents).expect_err("malformed config should fail");

        assert!(error.to_string().contains("failed to parse config"));
    }
}
