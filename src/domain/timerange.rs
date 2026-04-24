use crate::domain::error::ValidationError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandTimerange {
    Relative(RelativeTimerange),
    Absolute(AbsoluteTimerange),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelativeTimerange {
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbsoluteTimerange {
    from: String,
    to: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimerangeInput {
    pub relative: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

impl CommandTimerange {
    pub fn relative(value: impl Into<String>) -> Result<Self, ValidationError> {
        Ok(Self::Relative(RelativeTimerange::new(value)?))
    }

    pub fn absolute(
        from: impl Into<String>,
        to: impl Into<String>,
    ) -> Result<Self, ValidationError> {
        Ok(Self::Absolute(AbsoluteTimerange::new(from, to)?))
    }

    pub fn from_input(input: TimerangeInput) -> Result<Self, ValidationError> {
        input.into_timerange()
    }
}

impl RelativeTimerange {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = normalize_segment("timerange.relative", value.into())?;
        parse_relative_range_seconds(&value)?;
        Ok(Self { value })
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn api_range(&self) -> Result<u64, ValidationError> {
        parse_relative_range_seconds(&self.value)
    }
}

impl AbsoluteTimerange {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Result<Self, ValidationError> {
        let from = normalize_segment("timerange.from", from.into())?;
        let to = normalize_segment("timerange.to", to.into())?;

        if from == to {
            return Err(ValidationError::InvalidTimerange {
                message: "`from` and `to` must not be identical".to_string(),
            });
        }

        Ok(Self { from, to })
    }

    pub fn from(&self) -> &str {
        &self.from
    }

    pub fn to(&self) -> &str {
        &self.to
    }
}

impl TimerangeInput {
    pub fn into_timerange(self) -> Result<CommandTimerange, ValidationError> {
        match (self.relative, self.from, self.to) {
            (Some(relative), None, None) => CommandTimerange::relative(relative),
            (None, Some(from), Some(to)) => CommandTimerange::absolute(from, to),
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                Err(ValidationError::MutuallyExclusiveFields {
                    left: "timerange.relative",
                    right: "timerange.from/timerange.to",
                })
            }
            (None, Some(_), None) => Err(ValidationError::MissingField {
                field: "timerange.to",
            }),
            (None, None, Some(_)) => Err(ValidationError::MissingField {
                field: "timerange.from",
            }),
            (None, None, None) => Err(ValidationError::MissingField { field: "timerange" }),
        }
    }
}

fn normalize_segment(field: &'static str, value: String) -> Result<String, ValidationError> {
    let normalized = value.trim();

    if normalized.is_empty() {
        return Err(ValidationError::EmptyField { field });
    }

    Ok(normalized.to_string())
}

fn parse_relative_range_seconds(value: &str) -> Result<u64, ValidationError> {
    let duration =
        humantime::parse_duration(value).map_err(|_| invalid_relative_timerange(value))?;
    let secs = duration.as_secs();
    if secs == 0 {
        return Err(invalid_relative_timerange(value));
    }
    Ok(secs)
}

fn invalid_relative_timerange(value: &str) -> ValidationError {
    ValidationError::InvalidTimerange {
        message: format!(
            "relative time range `{value}` must use a positive duration like 15m, 1h, 5d, or 1w"
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::error::ValidationError;

    use super::{AbsoluteTimerange, TimerangeInput};
    use super::{CommandTimerange, RelativeTimerange};

    #[test]
    fn relative_timerange_converts_minutes_to_seconds() {
        let timerange = RelativeTimerange::new("15m").expect("relative timerange should parse");

        assert_eq!(timerange.api_range().expect("api range should parse"), 900);
    }

    #[test]
    fn command_timerange_accepts_absolute_ranges_unchanged() {
        let timerange = CommandTimerange::absolute("2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z")
            .expect("absolute timerange should parse");

        assert!(matches!(timerange, CommandTimerange::Absolute(_)));
    }

    #[test]
    fn relative_timerange_rejects_unencoded_strings() {
        let error = RelativeTimerange::new("15").expect_err("missing unit should fail");

        assert!(
            error
                .to_string()
                .contains("must use a positive duration like 15m, 1h, 5d, or 1w")
        );
    }

    #[test]
    fn absolute_timerange_rejects_identical_from_to() {
        let error = AbsoluteTimerange::new("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
            .expect_err("identical absolute bounds should fail");

        assert!(matches!(error, ValidationError::InvalidTimerange { .. }));
    }

    #[test]
    fn absolute_timerange_accepts_different_from_to() {
        let timerange = AbsoluteTimerange::new("2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z")
            .expect("different absolute bounds should parse");

        assert_eq!(timerange.from(), "2026-01-01T00:00:00Z");
        assert_eq!(timerange.to(), "2026-01-01T01:00:00Z");
    }

    #[test]
    fn timerange_input_relative_only() {
        let input = TimerangeInput {
            relative: Some("15m".to_string()),
            from: None,
            to: None,
        };

        let timerange = CommandTimerange::from_input(input).expect("relative input should parse");

        assert!(matches!(timerange, CommandTimerange::Relative(_)));
    }

    #[test]
    fn timerange_input_absolute_from_to() {
        let input = TimerangeInput {
            relative: None,
            from: Some("2026-01-01T00:00:00Z".to_string()),
            to: Some("2026-01-01T01:00:00Z".to_string()),
        };

        let timerange = CommandTimerange::from_input(input).expect("absolute input should parse");

        assert!(matches!(timerange, CommandTimerange::Absolute(_)));
    }

    #[test]
    fn timerange_input_rejects_mixed_relative_and_from() {
        let input = TimerangeInput {
            relative: Some("5m".to_string()),
            from: Some("2026-01-01T00:00:00Z".to_string()),
            to: None,
        };

        let error = CommandTimerange::from_input(input).expect_err("mixed input should fail");

        assert!(matches!(
            error,
            ValidationError::MutuallyExclusiveFields { .. }
        ));
    }

    #[test]
    fn timerange_input_rejects_mixed_relative_and_to() {
        let input = TimerangeInput {
            relative: Some("5m".to_string()),
            from: None,
            to: Some("2026-01-01T01:00:00Z".to_string()),
        };

        let error = CommandTimerange::from_input(input).expect_err("mixed input should fail");

        assert!(matches!(
            error,
            ValidationError::MutuallyExclusiveFields { .. }
        ));
    }

    #[test]
    fn timerange_input_rejects_mixed_all_three() {
        let input = TimerangeInput {
            relative: Some("5m".to_string()),
            from: Some("2026-01-01T00:00:00Z".to_string()),
            to: Some("2026-01-01T01:00:00Z".to_string()),
        };

        let error = CommandTimerange::from_input(input).expect_err("mixed input should fail");

        assert!(matches!(
            error,
            ValidationError::MutuallyExclusiveFields { .. }
        ));
    }

    #[test]
    fn timerange_input_rejects_from_without_to() {
        let input = TimerangeInput {
            relative: None,
            from: Some("2026-01-01T00:00:00Z".to_string()),
            to: None,
        };

        let error = CommandTimerange::from_input(input).expect_err("missing to should fail");

        assert!(matches!(
            error,
            ValidationError::MissingField {
                field: "timerange.to"
            }
        ));
    }

    #[test]
    fn timerange_input_rejects_to_without_from() {
        let input = TimerangeInput {
            relative: None,
            from: None,
            to: Some("2026-01-01T01:00:00Z".to_string()),
        };

        let error = CommandTimerange::from_input(input).expect_err("missing from should fail");

        assert!(matches!(
            error,
            ValidationError::MissingField {
                field: "timerange.from"
            }
        ));
    }

    #[test]
    fn timerange_input_rejects_all_none() {
        let error = CommandTimerange::from_input(TimerangeInput::default())
            .expect_err("missing timerange should fail");

        assert!(matches!(
            error,
            ValidationError::MissingField { field: "timerange" }
        ));
    }

    #[test]
    fn relative_timerange_accepts_hours() {
        let timerange = RelativeTimerange::new("1h").expect("hours should parse");

        assert_eq!(timerange.api_range().expect("api range should parse"), 3600);
    }

    #[test]
    fn relative_timerange_accepts_days() {
        let timerange = RelativeTimerange::new("1d").expect("days should parse");

        assert_eq!(
            timerange.api_range().expect("api range should parse"),
            86400
        );
    }

    #[test]
    fn relative_timerange_accepts_weeks() {
        let timerange = RelativeTimerange::new("1w").expect("weeks should parse");

        assert_eq!(
            timerange.api_range().expect("api range should parse"),
            604800
        );
    }

    #[test]
    fn relative_timerange_rejects_zero() {
        let error = RelativeTimerange::new("0s").expect_err("zero duration should fail");

        assert!(matches!(error, ValidationError::InvalidTimerange { .. }));
    }

    #[test]
    fn normalize_segment_trims_whitespace() {
        let timerange = RelativeTimerange::new("  15m  ").expect("whitespace should be trimmed");

        assert_eq!(timerange.value(), "15m");
    }
}
