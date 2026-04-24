use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

use clap::Parser;
use graylog_cli::application::ports::config_store::ConfigStore;
use graylog_cli::application::ports::updater::UpdaterError;
use graylog_cli::application::service::ApplicationService;
use graylog_cli::application::updater_service::{DEFAULT_CHECK_INTERVAL_SECONDS, UpdaterService};
use graylog_cli::domain::error::CliError;
use graylog_cli::domain::error::ValidationError;
use graylog_cli::infrastructure::config_store::FileConfigStore;
use graylog_cli::infrastructure::graylog_client::ReqwestGraylogGatewayFactory;
use graylog_cli::infrastructure::updater::GitHubUpdaterGateway;
use graylog_cli::presentation::cli::{Cli, Commands, StreamsCommands, SystemCommands};
use graylog_cli::presentation::output::{
    ErrorEnvelope, exit_code_for_cli_error, print_error_json, print_json,
};
use secrecy::SecretString;
use url::Url;

const WORKER_ARG: &str = "__self-update-worker";
const AUTO_UPDATE_ENV: &str = "GRAYLOG_CLI_AUTO_UPDATE";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() {
    let cli = match parse_cli() {
        Ok(cli) => cli,
        Err(exit_code) => std::process::exit(exit_code),
    };

    if let Err(error) = cli.validate() {
        emit_cli_error(&error.into());
    }

    let config_store = Arc::new(FileConfigStore::new());
    let service = ApplicationService::new(
        config_store.clone(),
        Arc::new(ReqwestGraylogGatewayFactory),
        config_store.clone(),
    );

    let updater = build_updater_service(config_store.clone());

    if !matches!(cli.command, Commands::SelfUpdateWorker)
        && let Some(updater) = updater.as_ref()
    {
        let _ = updater.apply_pending_upgrade().await;
    }

    if let Err(error) = run(cli.command, &service, updater.as_deref()).await {
        emit_cli_error(&error);
    }

    if let Some(updater) = updater.as_ref()
        && auto_update_enabled(config_store.as_ref()).await
        && updater
            .should_check_now(DEFAULT_CHECK_INTERVAL_SECONDS)
            .await
    {
        spawn_background_worker();
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
    updater: Option<&UpdaterService>,
) -> Result<(), exn::Exn<CliError>> {
    match command {
        Commands::Auth(args) => {
            let url: Url = args.url.parse().map_err(|_| {
                CliError::Validation(ValidationError::InvalidValue {
                    field: "url",
                    message: "invalid URL format".to_string(),
                })
            })?;
            let status = service
                .authenticate(url, SecretString::new(args.token.into()))
                .await?;
            emit_json_success(&status);
        }
        Commands::Search(args) => {
            emit_json_success(
                &service
                    .search(args.to_input().map_err(CliError::from)?)
                    .await?,
            );
        }
        Commands::Aggregate(args) => {
            emit_json_success(
                &service
                    .aggregate(args.to_input().map_err(CliError::from)?)
                    .await?,
            );
        }
        Commands::CountByLevel(args) => {
            emit_json_success(
                &service
                    .count_by_level(args.to_input().map_err(CliError::from)?)
                    .await?,
            );
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
                emit_json_success(
                    &service
                        .streams_search(args.to_input().map_err(CliError::from)?)
                        .await?,
                );
            }
            StreamsCommands::LastEvent(args) => {
                let timerange = args.timerange().map_err(CliError::from)?;
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
        Commands::Upgrade => {
            let updater = updater.ok_or_else(|| {
                CliError::Update(UpdaterError::Unavailable(
                    "updater is not available for this build".to_string(),
                ))
            })?;
            let status = updater.upgrade_now().await.map_err(CliError::from)?;
            emit_json_success(&status);
        }
        Commands::SelfUpdateWorker => {
            if let Some(updater) = updater {
                let _ = updater.stage_update_if_newer().await;
            }
        }
    }

    Ok(())
}

fn build_updater_service(cache_store: Arc<FileConfigStore>) -> Option<Arc<UpdaterService>> {
    let gateway = GitHubUpdaterGateway::new().ok()?;
    let staged_dir = staged_binary_dir()?;
    Some(Arc::new(UpdaterService::new(
        Arc::new(gateway),
        cache_store,
        CURRENT_VERSION.to_string(),
        staged_dir,
    )))
}

fn staged_binary_dir() -> Option<PathBuf> {
    Some(dirs::config_dir()?.join("graylog-cli"))
}

async fn auto_update_enabled(config_store: &dyn ConfigStore) -> bool {
    if let Ok(value) = std::env::var(AUTO_UPDATE_ENV) {
        return !matches!(value.trim(), "0" | "false" | "no" | "off");
    }
    match config_store.load().await {
        Ok(Some(config)) => !config.updater.disable_auto_update,
        _ => true,
    }
}

fn spawn_background_worker() {
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let _ = Command::new(exe)
        .arg(WORKER_ARG)
        .env(AUTO_UPDATE_ENV, "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
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

fn emit_cli_error(error: &exn::Exn<CliError>) -> ! {
    let cli_error: &CliError = error;
    let exit_code = exit_code_for_cli_error(cli_error);
    let _ = print_error_json(&ErrorEnvelope::from_cli_error(cli_error));
    std::process::exit(exit_code);
}
