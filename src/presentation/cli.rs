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
