use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),
    #[error("http error: {0}")]
    Http(#[from] HttpError),
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config store `{backend}` is unavailable: {message}")]
    StoreUnavailable {
        backend: &'static str,
        message: String,
    },
    #[error("config is not initialized")]
    NotConfigured,
    #[error("invalid config format: {message}")]
    InvalidFormat { message: String },
    #[error("failed to serialize config: {message}")]
    Serialization { message: String },
    #[error("failed to deserialize config: {message}")]
    Deserialization { message: String },
    #[error("filesystem error while {operation} `{path}`: {message}")]
    Filesystem {
        operation: &'static str,
        path: String,
        message: String,
    },
}

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("request could not be built: {message}")]
    RequestBuild { message: String },
    #[error("transport failure while calling Graylog: {message}")]
    Transport { message: String },
    #[error("Graylog returned HTTP {status}: {message}")]
    UnexpectedStatus { status: u16, message: String },
    #[error("graylog endpoint is unavailable: {message}")]
    Unavailable { message: String },
}

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("missing required field `{field}`")]
    MissingField { field: &'static str },
    #[error("`{field}` cannot be empty")]
    EmptyField { field: &'static str },
    #[error("`{left}` cannot be combined with `{right}`")]
    MutuallyExclusiveFields {
        left: &'static str,
        right: &'static str,
    },
    #[error("invalid value for `{field}`: {message}")]
    InvalidValue {
        field: &'static str,
        message: String,
    },
    #[error("invalid time range: {message}")]
    InvalidTimerange { message: String },
}
