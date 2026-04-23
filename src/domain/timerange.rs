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
}
