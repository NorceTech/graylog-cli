use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::domain::error::{CliError, ValidationError};
use crate::domain::models::{
    AggregateCommandInput, AggregationType, SearchCommandInput, SortDirection,
};
use crate::domain::timerange::{CommandTimerange, TimerangeInput};

#[derive(Debug, Parser)]
#[command(
    name = "graylog-cli",
    version,
    about = "Graylog command-line interface"
)]
#[command(arg_required_else_help = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn validate(&self) -> Result<(), CliError> {
        self.command.validate().map_err(CliError::from)
    }
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Persist Graylog credentials locally.
    Auth(AuthArgs),
    /// Search Graylog messages.
    Search(SearchArgs),
    /// Run an aggregation query.
    Aggregate(AggregateArgs),
    /// Count messages by log level.
    CountByLevel(CountByLevelArgs),
    /// Work with Graylog streams.
    Streams {
        #[command(subcommand)]
        command: StreamsCommands,
    },
    /// Inspect Graylog system details.
    System {
        #[command(subcommand)]
        command: SystemCommands,
    },
    /// Check that Graylog is reachable.
    Ping,
    /// List all indexed fields.
    Fields,
    /// Upgrade graylog-cli to the latest released version.
    Upgrade,
    /// Internal: background worker that checks for updates and stages a newer binary.
    #[command(name = "__self-update-worker", hide = true)]
    SelfUpdateWorker,
}

impl Commands {
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::Auth(_) | Self::Ping => Ok(()),
            Self::Search(args) => {
                args.timerange.try_into_timerange()?;
                Ok(())
            }
            Self::Aggregate(args) => args.validate(),
            Self::CountByLevel(args) => {
                args.timerange.try_into_timerange()?;
                Ok(())
            }
            Self::Streams { command } => command.validate(),
            Self::System { .. } => Ok(()),
            Self::Fields => Ok(()),
            Self::Upgrade | Self::SelfUpdateWorker => Ok(()),
        }
    }
}

#[derive(Debug, Args)]
pub struct AuthArgs {
    /// Graylog base URL.
    #[arg(short = 'u', long = "url", required = true)]
    pub url: String,
    /// Graylog access token.
    #[arg(short = 't', long = "token", required = true)]
    pub token: String,
}

#[derive(Debug, Args)]
pub struct SearchArgs {
    pub query: String,
    #[command(flatten)]
    pub timerange: TimerangeArgs,
    #[arg(long = "field")]
    pub field: Vec<String>,
    #[arg(long = "limit", value_parser = clap::value_parser!(u64).range(1..=1000))]
    pub limit: Option<u64>,
    #[arg(long = "offset")]
    pub offset: Option<u64>,
    #[arg(long = "sort")]
    pub sort: Option<String>,
    #[arg(long = "sort-direction", value_enum)]
    pub sort_direction: Option<SortDirectionArg>,
    #[arg(long = "group-by")]
    pub group_by: Option<String>,
    #[arg(long = "all-pages")]
    pub all_pages: bool,
    #[arg(long = "all-fields")]
    pub all_fields: bool,
    #[arg(long = "stream-id")]
    pub stream_id: Vec<String>,
}

impl SearchArgs {
    pub fn to_input(&self) -> Result<SearchCommandInput, ValidationError> {
        Ok(SearchCommandInput {
            query: self.query.clone(),
            timerange: self.timerange.try_into_timerange()?,
            fields: self.field.clone(),
            limit: self.limit,
            offset: self.offset,
            sort: self.sort.clone(),
            sort_direction: self.sort_direction.map(Into::into),
            group_by: self.group_by.clone(),
            all_pages: self.all_pages,
            all_fields: self.all_fields,
            streams: self.stream_id.clone(),
        })
    }
}

#[derive(Debug, Args)]
pub struct AggregateArgs {
    pub query: String,
    #[arg(long = "aggregation-type", value_enum)]
    pub aggregation_type: AggregationTypeArg,
    #[arg(long = "field")]
    pub field: String,
    #[arg(long = "size", value_parser = clap::value_parser!(u64).range(1..=100))]
    pub size: Option<u64>,
    #[arg(long = "interval")]
    pub interval: Option<String>,
    #[command(flatten)]
    pub timerange: TimerangeArgs,
}

impl AggregateArgs {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.timerange.try_into_timerange()?;

        match (self.aggregation_type, self.interval.as_deref()) {
            (AggregationTypeArg::DateHistogram, None) => Err(ValidationError::MissingField {
                field: "interval",
            }),
            (AggregationTypeArg::DateHistogram, Some(value)) if value.trim().is_empty() => {
                Err(ValidationError::EmptyField { field: "interval" })
            }
            (AggregationTypeArg::DateHistogram, Some(_)) => Ok(()),
            (_, Some(_)) => Err(ValidationError::InvalidValue {
                field: "interval",
                message: "`--interval` is only supported when `--aggregation-type date_histogram` is selected"
                    .to_string(),
            }),
            (_, None) => Ok(()),
        }
    }

    pub fn to_input(&self) -> Result<AggregateCommandInput, ValidationError> {
        Ok(AggregateCommandInput {
            query: self.query.clone(),
            timerange: self.timerange.try_into_timerange()?,
            aggregation_type: self.aggregation_type.into(),
            field: self.field.clone(),
            size: self.size,
            interval: self.interval.clone(),
            streams: Vec::new(),
        })
    }
}

#[derive(Debug, Args)]
pub struct CountByLevelArgs {
    #[command(flatten)]
    pub timerange: TimerangeArgs,
}

impl CountByLevelArgs {
    pub fn to_input(&self) -> Result<AggregateCommandInput, ValidationError> {
        Ok(AggregateCommandInput {
            query: "*".to_string(),
            timerange: self.timerange.try_into_timerange()?,
            aggregation_type: AggregationType::Terms,
            field: "level".to_string(),
            size: Some(10),
            interval: None,
            streams: Vec::new(),
        })
    }
}

#[derive(Debug, Subcommand)]
pub enum StreamsCommands {
    /// List streams.
    List,
    /// Show a stream by id.
    Show(StreamIdArgs),
    /// Find a stream by name.
    Find(StreamNameArgs),
    /// Search messages within a stream.
    Search(StreamSearchArgs),
    /// Fetch the last event for a stream.
    LastEvent(StreamIdTimerangeArgs),
}

impl StreamsCommands {
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::List | Self::Show(_) | Self::Find(_) => Ok(()),
            Self::Search(args) => {
                args.timerange.try_into_timerange()?;
                Ok(())
            }
            Self::LastEvent(args) => {
                args.timerange.try_into_timerange()?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Args)]
pub struct StreamIdArgs {
    pub stream_id: String,
}

#[derive(Debug, Args)]
pub struct StreamNameArgs {
    pub name: String,
}

#[derive(Debug, Args)]
pub struct StreamSearchArgs {
    pub stream_id: String,
    pub query: String,
    #[command(flatten)]
    pub timerange: TimerangeArgs,
    #[arg(long = "field")]
    pub field: Vec<String>,
    #[arg(long = "limit", value_parser = clap::value_parser!(u64).range(1..=100))]
    pub limit: Option<u64>,
}

impl StreamSearchArgs {
    pub fn to_input(&self) -> Result<SearchCommandInput, ValidationError> {
        Ok(SearchCommandInput {
            query: self.query.clone(),
            timerange: self.timerange.try_into_timerange()?,
            fields: self.field.clone(),
            limit: self.limit,
            offset: None,
            sort: None,
            sort_direction: None,
            group_by: None,
            all_pages: false,
            all_fields: false,
            streams: vec![self.stream_id.clone()],
        })
    }
}

#[derive(Debug, Args)]
pub struct StreamIdTimerangeArgs {
    pub stream_id: String,
    #[command(flatten)]
    pub timerange: TimerangeArgs,
}

impl StreamIdTimerangeArgs {
    pub fn timerange(&self) -> Result<Option<CommandTimerange>, ValidationError> {
        self.timerange.try_into_timerange()
    }
}

#[derive(Debug, Subcommand)]
pub enum SystemCommands {
    /// Show Graylog system information.
    Info,
}

#[derive(Debug, Clone, Default, Args)]
pub struct TimerangeArgs {
    #[arg(long = "time-range")]
    pub time_range: Option<String>,
    #[arg(long = "from")]
    pub from: Option<String>,
    #[arg(long = "to")]
    pub to: Option<String>,
}

impl TimerangeArgs {
    pub fn try_into_timerange(&self) -> Result<Option<CommandTimerange>, ValidationError> {
        if self.time_range.is_none() && self.from.is_none() && self.to.is_none() {
            return Ok(None);
        }

        CommandTimerange::from_input(TimerangeInput {
            relative: self.time_range.clone(),
            from: self.from.clone(),
            to: self.to.clone(),
        })
        .map(Some)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AggregationTypeArg {
    Terms,
    #[value(name = "date_histogram")]
    DateHistogram,
    Cardinality,
    Stats,
    Min,
    Max,
    Avg,
    Sum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SortDirectionArg {
    Asc,
    Desc,
}

impl From<SortDirectionArg> for SortDirection {
    fn from(value: SortDirectionArg) -> Self {
        match value {
            SortDirectionArg::Asc => Self::Asc,
            SortDirectionArg::Desc => Self::Desc,
        }
    }
}

impl From<AggregationTypeArg> for AggregationType {
    fn from(value: AggregationTypeArg) -> Self {
        match value {
            AggregationTypeArg::Terms => Self::Terms,
            AggregationTypeArg::DateHistogram => Self::DateHistogram,
            AggregationTypeArg::Cardinality => Self::Cardinality,
            AggregationTypeArg::Stats => Self::Stats,
            AggregationTypeArg::Min => Self::Min,
            AggregationTypeArg::Max => Self::Max,
            AggregationTypeArg::Avg => Self::Avg,
            AggregationTypeArg::Sum => Self::Sum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(args)
    }

    // --- Auth tests ---

    #[test]
    fn auth_requires_url_and_token() {
        let result = parse(&["graylog-cli", "auth"]);
        assert!(result.is_err(), "auth without --url and --token should fail");
    }

    #[test]
    fn auth_succeeds_with_url_and_token() {
        let cli = parse(&[
            "graylog-cli",
            "auth",
            "--url",
            "http://localhost:9000",
            "--token",
            "secret",
        ])
        .expect("auth with --url and --token should parse");

        match cli.command {
            Commands::Auth(args) => {
                assert_eq!(args.url, "http://localhost:9000");
                assert_eq!(args.token, "secret");
            }
            _ => panic!("expected Auth command"),
        }
    }

    // --- Search tests ---

    #[test]
    fn search_requires_query() {
        let result = parse(&["graylog-cli", "search"]);
        assert!(result.is_err(), "search without query should fail");
    }

    #[test]
    fn search_succeeds_with_query() {
        let cli =
            parse(&["graylog-cli", "search", "level:ERROR"]).expect("search with query should parse");

        match cli.command {
            Commands::Search(args) => {
                assert_eq!(args.query, "level:ERROR");
            }
            _ => panic!("expected Search command"),
        }
    }

    #[test]
    fn search_with_all_options() {
        let cli = parse(&[
            "graylog-cli",
            "search",
            "level:ERROR",
            "--field",
            "message",
            "--limit",
            "10",
            "--offset",
            "5",
            "--sort",
            "source",
            "--sort-direction",
            "asc",
            "--group-by",
            "level",
            "--all-pages",
            "--all-fields",
            "--stream-id",
            "abc123",
        ])
        .expect("search with all options should parse");

        match cli.command {
            Commands::Search(args) => {
                assert_eq!(args.query, "level:ERROR");
                assert_eq!(args.field, vec!["message"]);
                assert_eq!(args.limit, Some(10));
                assert_eq!(args.offset, Some(5));
                assert_eq!(args.sort.as_deref(), Some("source"));
                assert_eq!(args.sort_direction, Some(SortDirectionArg::Asc));
                assert_eq!(args.group_by.as_deref(), Some("level"));
                assert!(args.all_pages);
                assert!(args.all_fields);
                assert_eq!(args.stream_id, vec!["abc123"]);
            }
            _ => panic!("expected Search command"),
        }
    }

    // --- Aggregate tests ---

    #[test]
    fn aggregate_requires_query_aggregation_type_and_field() {
        assert!(
            parse(&["graylog-cli", "aggregate"]).is_err(),
            "aggregate with no args should fail"
        );
        assert!(
            parse(&["graylog-cli", "aggregate", "level:ERROR"]).is_err(),
            "aggregate without --aggregation-type and --field should fail"
        );
        assert!(
            parse(&[
                "graylog-cli",
                "aggregate",
                "level:ERROR",
                "--aggregation-type",
                "terms"
            ])
            .is_err(),
            "aggregate without --field should fail"
        );
    }

    #[test]
    fn aggregate_succeeds_with_required_args() {
        let cli = parse(&[
            "graylog-cli",
            "aggregate",
            "level:ERROR",
            "--aggregation-type",
            "terms",
            "--field",
            "level",
        ])
        .expect("aggregate with required args should parse");

        match cli.command {
            Commands::Aggregate(args) => {
                assert_eq!(args.query, "level:ERROR");
                assert_eq!(args.aggregation_type, AggregationTypeArg::Terms);
                assert_eq!(args.field, "level");
            }
            _ => panic!("expected Aggregate command"),
        }
    }

    #[test]
    fn aggregate_date_histogram_requires_interval_via_validate() {
        let cli = parse(&[
            "graylog-cli",
            "aggregate",
            "level:ERROR",
            "--aggregation-type",
            "date_histogram",
            "--field",
            "timestamp",
        ])
        .expect("clap parsing should succeed for date_histogram without --interval");

        let err = cli
            .validate()
            .expect_err("validate should reject date_histogram without --interval");

        let msg = err.to_string();
        assert!(
            msg.contains("interval"),
            "error should mention 'interval': {msg}"
        );
    }

    // --- Count-by-level tests ---

    #[test]
    fn count_by_level_needs_no_positional_args() {
        let cli = parse(&["graylog-cli", "count-by-level"])
            .expect("count-by-level with no args should parse");

        assert!(
            matches!(cli.command, Commands::CountByLevel(_)),
            "expected CountByLevel command"
        );
    }

    // --- Streams subcommand tests ---

    #[test]
    fn streams_list_parses() {
        let cli =
            parse(&["graylog-cli", "streams", "list"]).expect("streams list should parse");

        match cli.command {
            Commands::Streams { command } => {
                assert!(matches!(command, StreamsCommands::List));
            }
            _ => panic!("expected Streams command"),
        }
    }

    #[test]
    fn streams_show_parses() {
        let cli =
            parse(&["graylog-cli", "streams", "show", "abc"]).expect("streams show should parse");

        match cli.command {
            Commands::Streams { command } => match command {
                StreamsCommands::Show(args) => {
                    assert_eq!(args.stream_id, "abc");
                }
                _ => panic!("expected Show subcommand"),
            },
            _ => panic!("expected Streams command"),
        }
    }

    #[test]
    fn streams_find_parses() {
        let cli =
            parse(&["graylog-cli", "streams", "find", "test"]).expect("streams find should parse");

        match cli.command {
            Commands::Streams { command } => match command {
                StreamsCommands::Find(args) => {
                    assert_eq!(args.name, "test");
                }
                _ => panic!("expected Find subcommand"),
            },
            _ => panic!("expected Streams command"),
        }
    }

    // --- Ping tests ---

    #[test]
    fn ping_needs_no_args() {
        let cli = parse(&["graylog-cli", "ping"]).expect("ping should parse");
        assert!(matches!(cli.command, Commands::Ping));
    }

    // --- Fields tests ---

    #[test]
    fn fields_needs_no_args() {
        let cli = parse(&["graylog-cli", "fields"]).expect("fields should parse");
        assert!(matches!(cli.command, Commands::Fields));
    }

    // --- Limit validation tests ---

    #[test]
    fn search_rejects_limit_zero() {
        let result = parse(&["graylog-cli", "search", "test", "--limit", "0"]);
        assert!(
            result.is_err(),
            "search --limit 0 should be rejected by clap value parser"
        );
    }

    #[test]
    fn search_rejects_limit_above_1000() {
        let result = parse(&["graylog-cli", "search", "test", "--limit", "1001"]);
        assert!(
            result.is_err(),
            "search --limit 1001 should be rejected by clap value parser"
        );
    }

    #[test]
    fn streams_search_rejects_limit_zero() {
        let result = parse(&["graylog-cli", "streams", "search", "abc", "test", "--limit", "0"]);
        assert!(
            result.is_err(),
            "streams search --limit 0 should be rejected by clap value parser"
        );
    }

    #[test]
    fn streams_search_rejects_limit_above_100() {
        let result = parse(&[
            "graylog-cli",
            "streams",
            "search",
            "abc",
            "test",
            "--limit",
            "101",
        ]);
        assert!(
            result.is_err(),
            "streams search --limit 101 should be rejected by clap value parser"
        );
    }

    // --- Sort-direction validation tests ---

    #[test]
    fn sort_direction_accepts_asc_and_desc() {
        let cli_asc = parse(&[
            "graylog-cli",
            "search",
            "test",
            "--sort-direction",
            "asc",
        ])
        .expect("asc should be accepted");

        match cli_asc.command {
            Commands::Search(args) => {
                assert_eq!(args.sort_direction, Some(SortDirectionArg::Asc));
            }
            _ => panic!("expected Search command"),
        }

        let cli_desc = parse(&[
            "graylog-cli",
            "search",
            "test",
            "--sort-direction",
            "desc",
        ])
        .expect("desc should be accepted");

        match cli_desc.command {
            Commands::Search(args) => {
                assert_eq!(args.sort_direction, Some(SortDirectionArg::Desc));
            }
            _ => panic!("expected Search command"),
        }
    }

    #[test]
    fn sort_direction_rejects_invalid_value() {
        let result = parse(&[
            "graylog-cli",
            "search",
            "test",
            "--sort-direction",
            "invalid",
        ]);
        assert!(
            result.is_err(),
            "invalid sort-direction should be rejected"
        );
    }
}
