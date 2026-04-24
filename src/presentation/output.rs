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
        CliError::Update(_) => 6,
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
        CliError::Update(_) => "update_error",
    }
}

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
        CliError::Update(update_error) => update_error.to_string(),
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

    #[test]
    fn transport_error_maps_to_exit_code_five() {
        let error = HttpError::Transport {
            message: "connection refused".to_string(),
        };

        assert_eq!(exit_code_for_http_error(&error), 5);
    }

    #[test]
    fn unavailable_error_maps_to_exit_code_four() {
        let error = HttpError::Unavailable {
            message: "endpoint disabled".to_string(),
        };

        assert_eq!(exit_code_for_http_error(&error), 4);
    }

    #[test]
    fn not_found_404_maps_to_exit_code_four() {
        let error = HttpError::UnexpectedStatus {
            status: 404,
            message: "not found".to_string(),
        };

        assert_eq!(exit_code_for_http_error(&error), 4);
    }

    #[test]
    fn method_not_allowed_405_maps_to_exit_code_four() {
        let error = HttpError::UnexpectedStatus {
            status: 405,
            message: "method not allowed".to_string(),
        };

        assert_eq!(exit_code_for_http_error(&error), 4);
    }

    #[test]
    fn not_implemented_501_maps_to_exit_code_four() {
        let error = HttpError::UnexpectedStatus {
            status: 501,
            message: "not implemented".to_string(),
        };

        assert_eq!(exit_code_for_http_error(&error), 4);
    }

    #[test]
    fn unexpected_500_maps_to_exit_code_one() {
        let error = HttpError::UnexpectedStatus {
            status: 500,
            message: "server error".to_string(),
        };

        assert_eq!(exit_code_for_http_error(&error), 1);
    }

    #[test]
    fn request_build_maps_to_exit_code_one() {
        let error = HttpError::RequestBuild {
            message: "invalid request".to_string(),
        };

        assert_eq!(exit_code_for_http_error(&error), 1);
    }

    #[test]
    fn validation_error_maps_to_exit_code_two() {
        let error = CliError::Validation(crate::domain::error::ValidationError::MissingField {
            field: "query",
        });

        assert_eq!(exit_code_for_cli_error(&error), 2);
    }

    #[test]
    fn cache_error_maps_to_exit_code_two() {
        let error = CliError::Cache("permission denied at /private/cache".to_string());

        assert_eq!(exit_code_for_cli_error(&error), 2);
    }

    #[test]
    fn validation_error_kind_is_validation_error() {
        let error = CliError::Validation(crate::domain::error::ValidationError::MissingField {
            field: "query",
        });

        assert_eq!(error_kind_for_cli_error(&error), "validation_error");
    }

    #[test]
    fn transport_error_kind_is_network_error() {
        let error = HttpError::Transport {
            message: "connection refused".to_string(),
        };

        assert_eq!(error_kind_for_http_error(&error), "network_error");
    }

    #[test]
    fn unavailable_error_kind_is_unsupported_endpoint() {
        let error = HttpError::Unavailable {
            message: "endpoint disabled".to_string(),
        };

        assert_eq!(error_kind_for_http_error(&error), "unsupported_endpoint");
    }

    #[test]
    fn not_found_error_kind_is_not_found() {
        let error = HttpError::UnexpectedStatus {
            status: 404,
            message: "not found".to_string(),
        };

        assert_eq!(error_kind_for_http_error(&error), "not_found");
    }

    #[test]
    fn generic_http_error_kind_is_http_error() {
        let error = HttpError::UnexpectedStatus {
            status: 500,
            message: "server error".to_string(),
        };

        assert_eq!(error_kind_for_http_error(&error), "http_error");
    }

    #[test]
    fn request_build_error_kind_is_internal_error() {
        let error = HttpError::RequestBuild {
            message: "invalid request".to_string(),
        };

        assert_eq!(error_kind_for_http_error(&error), "internal_error");
    }

    #[test]
    fn safe_http_error_message_from_transport() {
        let error = HttpError::Transport {
            message: "connection refused".to_string(),
        };

        assert_eq!(safe_http_error_message(&error), "connection refused");
    }

    #[test]
    fn safe_http_error_message_from_unexpected_status() {
        let error = HttpError::UnexpectedStatus {
            status: 500,
            message: "server error".to_string(),
        };

        assert_eq!(safe_http_error_message(&error), "server error");
    }

    #[test]
    fn cache_error_sanitized_message() {
        let error = CliError::Cache("permission denied at /private/cache".to_string());

        assert_eq!(safe_cli_error_message(&error), "cache error");
    }

    #[test]
    fn validation_error_exposes_message() {
        let error = CliError::Validation(crate::domain::error::ValidationError::MissingField {
            field: "query",
        });

        assert_eq!(
            safe_cli_error_message(&error),
            "validation error: missing required field `query`"
        );
    }

    #[test]
    fn error_kind_for_exit_code_two() {
        assert_eq!(error_kind_for_exit_code(2), "validation_error");
    }

    #[test]
    fn error_kind_for_exit_code_three() {
        assert_eq!(error_kind_for_exit_code(3), "auth_error");
    }

    #[test]
    fn error_kind_for_exit_code_four() {
        assert_eq!(error_kind_for_exit_code(4), "not_found");
    }

    #[test]
    fn error_kind_for_exit_code_five() {
        assert_eq!(error_kind_for_exit_code(5), "network_error");
    }

    #[test]
    fn error_kind_for_exit_code_unknown() {
        assert_eq!(error_kind_for_exit_code(99), "internal_error");
    }

    #[test]
    fn error_envelope_from_message() {
        let envelope = ErrorEnvelope::from_message(5, "connection refused".to_string());

        assert!(!envelope.ok);
        assert_eq!(envelope.code, "network_error");
        assert_eq!(envelope.message, "connection refused");
        assert_eq!(envelope.hint, None);
    }
}
