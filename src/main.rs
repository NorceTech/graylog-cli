use clap::Parser;
use graylog_cli::application::service::ApplicationService;
use graylog_cli::infrastructure::config_store::FileConfigStore;
use graylog_cli::infrastructure::graylog_client::ReqwestGraylogGatewayFactory;
use graylog_cli::presentation::cli::{Cli, Commands, StreamsCommands, SystemCommands};
use graylog_cli::presentation::output::{
    ErrorEnvelope, exit_code_for_cli_error, print_error_json, print_json,
};
use secrecy::SecretString;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let cli = match parse_cli() {
        Ok(cli) => cli,
        Err(exit_code) => std::process::exit(exit_code),
    };

    if let Err(error) = cli.validate() {
        emit_cli_error(&error);
    }

    let config_store = Arc::new(FileConfigStore::new());
    let service = ApplicationService::with_dependencies(
        config_store.clone(),
        Arc::new(ReqwestGraylogGatewayFactory),
        config_store,
    );

    if let Err(error) = run(cli.command, &service).await {
        emit_cli_error(&error);
    }
}

fn parse_cli() -> Result<Cli, i32> {
    match Cli::try_parse() {
        Ok(cli) => Ok(cli),
        Err(error) => {
            let exit_code = error.exit_code();
            let _ = error.print();
            Err(exit_code)
        }
    }
}

async fn run(
    command: Commands,
    service: &ApplicationService,
) -> Result<(), graylog_cli::domain::error::CliError> {
    match command {
        Commands::Auth(args) => {
            let status = service
                .authenticate(args.url, SecretString::new(args.token.into()))
                .await?;
            emit_json_success(&status);
        }
        Commands::Search(args) => {
            emit_json_success(&service.search(args.to_input()?).await?);
        }
        Commands::Aggregate(args) => {
            emit_json_success(&service.aggregate(args.to_input()?).await?);
        }
        Commands::CountByLevel(args) => {
            emit_json_success(&service.count_by_level(args.to_input()?).await?);
        }
        Commands::Streams {
            command: streams_command,
        } => match streams_command {
            StreamsCommands::List => {
                emit_json_success(&service.streams_list().await?);
            }
            StreamsCommands::Show(args) => {
                emit_json_success(&service.streams_show(&args.stream_id).await?);
            }
            StreamsCommands::Find(args) => {
                emit_json_success(&service.streams_find(&args.name).await?);
            }
            StreamsCommands::Search(args) => {
                emit_json_success(&service.streams_search(args.to_input()?).await?);
            }
            StreamsCommands::LastEvent(args) => {
                let timerange = args.timerange()?;
                emit_json_success(
                    &service
                        .streams_last_event(args.stream_id, timerange)
                        .await?,
                );
            }
        },
        Commands::System {
            command: system_command,
        } => match system_command {
            SystemCommands::Info => {
                emit_json_success(&service.system_info().await?);
            }
        },
        Commands::Fields => {
            emit_json_success(&service.fields().await?);
        }
        Commands::Ping => {
            emit_json_success(&service.ping().await?);
        }
    }

    Ok(())
}

fn emit_json_success<T>(value: &T)
where
    T: serde::Serialize,
{
    if let Err(error) = print_json(value) {
        let _ = print_error_json(&ErrorEnvelope::from_message(1, error.to_string()));
        std::process::exit(1);
    }
}

fn emit_cli_error(error: &graylog_cli::domain::error::CliError) -> ! {
    let exit_code = exit_code_for_cli_error(error);
    let _ = print_error_json(&ErrorEnvelope::from_cli_error(error));
    std::process::exit(exit_code);
}
