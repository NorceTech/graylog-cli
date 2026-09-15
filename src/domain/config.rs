use std::collections::BTreeMap;

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use url::Url;

pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_FIELDS_CACHE_TTL_SECONDS: u64 = 300;
/// Profile name that legacy `[graylog]` configurations migrate into.
pub const DEFAULT_PROFILE_NAME: &str = "default";

fn default_timeout_seconds() -> u64 {
    DEFAULT_TIMEOUT_SECONDS
}
fn default_fields_cache_ttl() -> u64 {
    DEFAULT_FIELDS_CACHE_TTL_SECONDS
}
fn default_verify_tls() -> bool {
    true
}

/// Root configuration file format: named Graylog profiles plus updater settings.
///
/// Legacy files with a single `[graylog]` table are migrated in memory by the
/// infrastructure config store; the domain type only models the new shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub profiles: BTreeMap<String, GraylogConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_profile: Option<String>,
    #[serde(default)]
    pub updater: UpdaterConfig,
}

/// Returns true when `name` matches `^[A-Za-z0-9][A-Za-z0-9._-]*$`.
///
/// The rule keeps profile names usable as file-name fragments (the fields
/// cache key embeds the profile name) and as TOML table keys.
pub fn is_valid_profile_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphanumeric() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Validates a profile name, returning a message describing the rule on failure.
pub fn validate_profile_name(name: &str) -> Result<(), String> {
    if is_valid_profile_name(name) {
        Ok(())
    } else {
        Err(format!(
            "profile names must start with an ASCII letter or digit and may only contain \
             ASCII letters, digits, `.`, `_`, and `-` (got `{name}`)"
        ))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdaterConfig {
    #[serde(default)]
    pub disable_auto_update: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GraylogConfig {
    pub url: Url,
    #[serde(
        serialize_with = "serialize_secret_string",
        deserialize_with = "deserialize_secret_string"
    )]
    pub token: SecretString,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_verify_tls")]
    pub verify_tls: bool,
    #[serde(default = "default_fields_cache_ttl")]
    pub fields_cache_ttl_seconds: u64,
}

impl Clone for GraylogConfig {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            token: SecretString::new(self.token.expose_secret().to_owned().into()),
            timeout_seconds: self.timeout_seconds,
            verify_tls: self.verify_tls,
            fields_cache_ttl_seconds: self.fields_cache_ttl_seconds,
        }
    }
}

impl GraylogConfig {
    pub fn new(url: Url, token: SecretString) -> Self {
        Self {
            url,
            token,
            timeout_seconds: default_timeout_seconds(),
            verify_tls: default_verify_tls(),
            fields_cache_ttl_seconds: default_fields_cache_ttl(),
        }
    }
}

fn serialize_secret_string<S>(value: &SecretString, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(value.expose_secret())
}

fn deserialize_secret_string<'de, D>(deserializer: D) -> Result<SecretString, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(SecretString::new(s.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use secrecy::{ExposeSecret, SecretString};
    use url::Url;

    use super::{
        Config, DEFAULT_FIELDS_CACHE_TTL_SECONDS, DEFAULT_PROFILE_NAME, DEFAULT_TIMEOUT_SECONDS,
        GraylogConfig, UpdaterConfig, is_valid_profile_name,
    };

    fn test_profile() -> GraylogConfig {
        GraylogConfig {
            url: Url::parse("https://graylog.example.com").expect("test URL should parse"),
            token: SecretString::new("test-token".to_owned().into()),
            timeout_seconds: 42,
            verify_tls: false,
            fields_cache_ttl_seconds: 123,
        }
    }

    fn test_config() -> Config {
        let mut profiles = BTreeMap::new();
        profiles.insert(DEFAULT_PROFILE_NAME.to_string(), test_profile());
        Config {
            profiles,
            active_profile: Some(DEFAULT_PROFILE_NAME.to_string()),
            updater: UpdaterConfig::default(),
        }
    }

    #[test]
    fn config_serializes_to_toml() {
        let toml = toml::to_string(&test_config()).expect("config should serialize");

        assert!(toml.contains("[profiles.default]"));
        assert!(toml.contains("url = \"https://graylog.example.com/\""));
        assert!(toml.contains("token = \"test-token\""));
        assert!(!toml.contains("graylog ="));
    }

    #[test]
    fn config_round_trips_through_toml() {
        let config = test_config();
        let toml = toml::to_string(&config).expect("config should serialize");

        let deserialized: Config = toml::from_str(&toml).expect("config should deserialize");

        assert_eq!(
            deserialized.profiles[DEFAULT_PROFILE_NAME].url,
            config.profiles[DEFAULT_PROFILE_NAME].url
        );
        assert_eq!(
            deserialized.profiles[DEFAULT_PROFILE_NAME].timeout_seconds,
            config.profiles[DEFAULT_PROFILE_NAME].timeout_seconds
        );
        assert_eq!(
            deserialized.active_profile.as_deref(),
            Some(DEFAULT_PROFILE_NAME)
        );
    }

    #[test]
    fn config_without_active_profile_serializes_without_the_field() {
        let config = Config {
            profiles: BTreeMap::new(),
            active_profile: None,
            updater: UpdaterConfig::default(),
        };

        let toml = toml::to_string(&config).expect("config should serialize");

        assert!(!toml.contains("active_profile"));
    }

    #[test]
    fn config_with_empty_profiles_round_trips() {
        let config = Config {
            profiles: BTreeMap::new(),
            active_profile: None,
            updater: UpdaterConfig::default(),
        };
        let toml = toml::to_string(&config).expect("config should serialize");

        let deserialized: Config = toml::from_str(&toml).expect("config should deserialize");

        assert!(deserialized.profiles.is_empty());
        assert_eq!(deserialized.active_profile, None);
    }

    #[test]
    fn graylog_config_new_sets_defaults() {
        let config = GraylogConfig::new(
            Url::parse("https://graylog.example.com").expect("test URL should parse"),
            SecretString::new("test-token".to_owned().into()),
        );

        assert_eq!(config.timeout_seconds, DEFAULT_TIMEOUT_SECONDS);
        assert!(config.verify_tls);
        assert_eq!(
            config.fields_cache_ttl_seconds,
            DEFAULT_FIELDS_CACHE_TTL_SECONDS
        );
    }

    #[test]
    fn graylog_config_clone_preserves_token() {
        let config = GraylogConfig::new(
            Url::parse("https://graylog.example.com").expect("test URL should parse"),
            SecretString::new("test-token".to_owned().into()),
        );

        let cloned = config.clone();

        assert_eq!(cloned.token.expose_secret(), "test-token");
    }

    #[test]
    fn config_deserialization_uses_default_timeout() {
        let toml = r#"
            [profiles.default]
            url = "https://graylog.example.com"
            token = "test-token"
            verify_tls = false
            fields_cache_ttl_seconds = 123
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert_eq!(
            config.profiles[DEFAULT_PROFILE_NAME].timeout_seconds,
            DEFAULT_TIMEOUT_SECONDS
        );
    }

    #[test]
    fn config_deserialization_uses_default_verify_tls() {
        let toml = r#"
            [profiles.default]
            url = "https://graylog.example.com"
            token = "test-token"
            timeout_seconds = 42
            fields_cache_ttl_seconds = 123
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert!(config.profiles[DEFAULT_PROFILE_NAME].verify_tls);
    }

    #[test]
    fn config_deserialization_uses_default_cache_ttl() {
        let toml = r#"
            [profiles.default]
            url = "https://graylog.example.com"
            token = "test-token"
            timeout_seconds = 42
            verify_tls = false
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert_eq!(
            config.profiles[DEFAULT_PROFILE_NAME].fields_cache_ttl_seconds,
            DEFAULT_FIELDS_CACHE_TTL_SECONDS
        );
    }

    #[test]
    fn config_deserialization_defaults_active_profile_to_none() {
        let toml = r#"
            [profiles.default]
            url = "https://graylog.example.com"
            token = "test-token"
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert_eq!(config.active_profile, None);
        assert!(config.profiles.contains_key(DEFAULT_PROFILE_NAME));
    }

    #[test]
    fn config_deserialization_defaults_disable_auto_update_to_false() {
        let toml = r#"
            [profiles.default]
            url = "https://graylog.example.com"
            token = "test-token"
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert!(!config.updater.disable_auto_update);
    }

    #[test]
    fn config_deserialization_reads_disable_auto_update_override() {
        let toml = r#"
            active_profile = "default"

            [profiles.default]
            url = "https://graylog.example.com"
            token = "test-token"

            [updater]
            disable_auto_update = true
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert!(config.updater.disable_auto_update);
    }

    #[test]
    fn valid_profile_names_are_accepted() {
        for name in [
            "a",
            "A",
            "7",
            "default",
            "prod-eu-1",
            "staging.v2",
            "team_a",
            "Prod.EU-1_2",
        ] {
            assert!(is_valid_profile_name(name), "`{name}` should be valid");
        }
    }

    #[test]
    fn invalid_profile_names_are_rejected() {
        for name in [
            "",
            ".hidden",
            "-leading",
            "_leading",
            "trailing space ",
            "with space",
            "with/slash",
            "with:colon",
            "with\\backslash",
            "ünïcode",
        ] {
            assert!(!is_valid_profile_name(name), "`{name}` should be invalid");
        }
    }
}
