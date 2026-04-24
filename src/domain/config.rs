use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use url::Url;

pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_FIELDS_CACHE_TTL_SECONDS: u64 = 300;

fn default_timeout_seconds() -> u64 {
    DEFAULT_TIMEOUT_SECONDS
}
fn default_fields_cache_ttl() -> u64 {
    DEFAULT_FIELDS_CACHE_TTL_SECONDS
}
fn default_verify_tls() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub graylog: GraylogConfig,
    #[serde(default)]
    pub updater: UpdaterConfig,
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
    use secrecy::{ExposeSecret, SecretString};
    use url::Url;

    use super::{
        Config, DEFAULT_FIELDS_CACHE_TTL_SECONDS, DEFAULT_TIMEOUT_SECONDS, GraylogConfig,
        UpdaterConfig,
    };

    fn test_config() -> Config {
        Config {
            graylog: GraylogConfig {
                url: Url::parse("https://graylog.example.com").expect("test URL should parse"),
                token: SecretString::new("test-token".to_owned().into()),
                timeout_seconds: 42,
                verify_tls: false,
                fields_cache_ttl_seconds: 123,
            },
            updater: UpdaterConfig::default(),
        }
    }

    #[test]
    fn config_serializes_to_toml() {
        let toml = toml::to_string(&test_config()).expect("config should serialize");

        assert!(toml.contains("url = \"https://graylog.example.com/\""));
        assert!(toml.contains("token = \"test-token\""));
    }

    #[test]
    fn config_round_trips_through_toml() {
        let config = test_config();
        let toml = toml::to_string(&config).expect("config should serialize");

        let deserialized: Config = toml::from_str(&toml).expect("config should deserialize");

        assert_eq!(deserialized.graylog.url, config.graylog.url);
        assert_eq!(
            deserialized.graylog.timeout_seconds,
            config.graylog.timeout_seconds
        );
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
            [graylog]
            url = "https://graylog.example.com"
            token = "test-token"
            verify_tls = false
            fields_cache_ttl_seconds = 123
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert_eq!(config.graylog.timeout_seconds, DEFAULT_TIMEOUT_SECONDS);
    }

    #[test]
    fn config_deserialization_uses_default_verify_tls() {
        let toml = r#"
            [graylog]
            url = "https://graylog.example.com"
            token = "test-token"
            timeout_seconds = 42
            fields_cache_ttl_seconds = 123
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert!(config.graylog.verify_tls);
    }

    #[test]
    fn config_deserialization_uses_default_cache_ttl() {
        let toml = r#"
            [graylog]
            url = "https://graylog.example.com"
            token = "test-token"
            timeout_seconds = 42
            verify_tls = false
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert_eq!(
            config.graylog.fields_cache_ttl_seconds,
            DEFAULT_FIELDS_CACHE_TTL_SECONDS
        );
    }

    #[test]
    fn config_deserialization_defaults_disable_auto_update_to_false() {
        let toml = r#"
            [graylog]
            url = "https://graylog.example.com"
            token = "test-token"
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert!(!config.updater.disable_auto_update);
    }

    #[test]
    fn config_deserialization_reads_disable_auto_update_override() {
        let toml = r#"
            [graylog]
            url = "https://graylog.example.com"
            token = "test-token"

            [updater]
            disable_auto_update = true
        "#;

        let config: Config = toml::from_str(toml).expect("config should deserialize");

        assert!(config.updater.disable_auto_update);
    }
}
