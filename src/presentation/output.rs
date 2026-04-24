use std::io::{self, Write};

use serde::Serialize;

use crate::domain::error::{CliError, HttpError};

#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub ok: bool,
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl ErrorEnvelope {
    pub fn from_message(exit_code: i32, message: String) -> Self {
        Self {
            ok: false,
            code: error_kind_for_exit_code(exit_code),
            message,
            hint: None,
        }
    }

    pub fn from_cli_error(error: &CliError) -> Self {
        Self {
            ok: false,
            code: error_kind_for_cli_error(error),
            message: safe_cli_error_message(error),
            hint: None,
        }
    }
}

pub fn print_json<T>(value: &T) -> io::Result<()>
where
    T: Serialize,
{
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, value)?;
    handle.write_all(b"\n")
}

pub fn print_error_json<T>(value: &T) -> io::Result<()>
where
    T: Serialize,
{
    let stderr = io::stderr();
    let mut handle = stderr.lock();
    serde_json::to_writer_pretty(&mut handle, value)?;
    handle.write_all(b"\n")
}

pub fn exit_code_for_cli_error(error: &CliError) -> i32 {
    match error {
        CliError::Validation(_) | CliError::Config(_) | CliError::Cache(_) => 2,
        CliError::Http(http_error) => exit_code_for_http_error(http_error),
    }
}

fn exit_code_for_http_error(error: &HttpError) -> i32 {
    match error {
        HttpError::Transport { .. } => 5,
        HttpError::Unavailable { .. } => 4,
        HttpError::UnexpectedStatus { status, .. } if matches!(*status, 401 | 403) => 3,
        HttpError::UnexpectedStatus { status, .. } if matches!(*status, 404 | 405 | 501) => 4,
        HttpError::UnexpectedStatus { .. } => 1,
        HttpError::RequestBuild { .. } => 1,
    }
}

fn error_kind_for_cli_error(error: &CliError) -> &'static str {
    match error {
        CliError::Validation(_) => "validation_error",
        CliError::Config(_) => "config_error",
        CliError::Cache(_) => "internal_error",
        CliError::Http(http_error) => error_kind_for_http_error(http_error),
    }
}

const AUTH_HINT: &str = "run `graylog-cli auth -u <URL> -t <TOKEN>`";

fn error_kind_for_http_error(error: &HttpError) -> &'static str {
    match error {
        HttpError::Transport { .. } => "network_error",
        HttpError::Unavailable { .. } => "unsupported_endpoint",
        HttpError::UnexpectedStatus { status, .. } if matches!(*status, 401 | 403) => "auth_error",
        HttpError::UnexpectedStatus { status, .. } if matches!(*status, 404 | 405 | 501) => {
            "not_found"
        }
        HttpError::UnexpectedStatus { .. } => "http_error",
        HttpError::RequestBuild { .. } => "internal_error",
    }
}

fn safe_cli_error_message(error: &CliError) -> String {
    match error {
        CliError::Config(_) => "configuration error".to_string(),
        CliError::Cache(_) => "cache error".to_string(),
        CliError::Http(http_error) => safe_http_error_message(http_error),
        CliError::Validation(_) => error.to_string(),
    }
}

fn safe_http_error_message(error: &HttpError) -> String {
    match error {
        HttpError::Transport { message }
        | HttpError::Unavailable { message }
        | HttpError::RequestBuild { message }
        | HttpError::UnexpectedStatus { message, .. } => message.clone(),
    }
}

fn error_kind_for_exit_code(exit_code: i32) -> &'static str {
    match exit_code {
        2 => "validation_error",
        3 => "auth_error",
        4 => "not_found",
        5 => "network_error",
        _ => "internal_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_http_errors_map_to_exit_code_three() {
        let error = CliError::Http(HttpError::UnexpectedStatus {
            status: 401,
            message: "Graylog rejected the supplied credentials".to_string(),
        });

        assert_eq!(exit_code_for_cli_error(&error), 3);
    }

    #[test]
    fn malformed_config_messages_are_sanitized() {
        let error = CliError::Config("TOML parse error near super-secret-token".to_string());

        let envelope = ErrorEnvelope::from_cli_error(&error);

        assert_eq!(envelope.code, "config_error");
        assert_eq!(envelope.message, "configuration error");
        assert!(!envelope.message.contains("super-secret-token"));
    }
}
