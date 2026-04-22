use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::domain::error::ValidationError;

pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_FIELDS_CACHE_TTL_SECONDS: u64 = 300;

fn default_fields_cache_ttl() -> u64 {
    DEFAULT_FIELDS_CACHE_TTL_SECONDS
}

#[derive(Debug, Clone)]
pub struct GraylogConfig {
    pub base_url: String,
    pub token: SecretString,
    pub timeout_seconds: u64,
    pub verify_tls: bool,
    pub fields_cache_ttl_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredConfig {
    pub graylog: StoredGraylogConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredGraylogConfig {
    pub url: String,
    pub token: String,
    pub timeout_seconds: u64,
    pub verify_tls: bool,
    #[serde(default = "default_fields_cache_ttl")]
    pub fields_cache_ttl_seconds: u64,
}

impl GraylogConfig {
    pub fn new(
        base_url: impl Into<String>,
        token: SecretString,
        timeout_seconds: u64,
        verify_tls: bool,
        fields_cache_ttl_seconds: u64,
    ) -> Result<Self, ValidationError> {
        let base_url = normalize_url(base_url.into())?;

        if timeout_seconds == 0 {
            return Err(ValidationError::InvalidValue {
                field: "graylog.timeout_seconds",
                message: "must be greater than zero".to_string(),
            });
        }

        if fields_cache_ttl_seconds == 0 {
            return Err(ValidationError::InvalidValue {
                field: "graylog.fields_cache_ttl_seconds",
                message: "must be greater than zero".to_string(),
            });
        }

        if token.expose_secret().trim().is_empty() {
            return Err(ValidationError::EmptyField {
                field: "graylog.token",
            });
        }

        Ok(Self {
            base_url,
            token,
            timeout_seconds,
            verify_tls,
            fields_cache_ttl_seconds,
        })
    }

    pub fn to_stored(&self) -> StoredConfig {
        StoredConfig::from_runtime(self)
    }
}

pub fn normalize_url(value: impl Into<String>) -> Result<String, ValidationError> {
    let normalized = value.into();
    let trimmed = normalized.trim();

    if trimmed.is_empty() {
        return Err(ValidationError::EmptyField {
            field: "graylog.url",
        });
    }

    Ok(trimmed.trim_end_matches('/').to_string())
}

impl Default for StoredConfig {
    fn default() -> Self {
        Self {
            graylog: StoredGraylogConfig {
                url: String::new(),
                token: String::new(),
                timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
                verify_tls: true,
                fields_cache_ttl_seconds: DEFAULT_FIELDS_CACHE_TTL_SECONDS,
            },
        }
    }
}

impl StoredConfig {
    pub fn from_runtime(config: &GraylogConfig) -> Self {
        Self {
            graylog: StoredGraylogConfig {
                url: config.base_url.clone(),
                token: config.token.expose_secret().to_owned(),
                timeout_seconds: config.timeout_seconds,
                verify_tls: config.verify_tls,
                fields_cache_ttl_seconds: config.fields_cache_ttl_seconds,
            },
        }
    }

    pub fn into_runtime(self) -> Result<GraylogConfig, ValidationError> {
        GraylogConfig::new(
            self.graylog.url,
            SecretString::new(self.graylog.token.into()),
            self.graylog.timeout_seconds,
            self.graylog.verify_tls,
            self.graylog.fields_cache_ttl_seconds,
        )
    }
}
