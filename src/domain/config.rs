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
