use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

fn graylog_cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_graylog-cli"))
}

fn run(args: &[&str]) -> std::process::Output {
    let mut cmd = graylog_cli();
    cmd.args(args);
    cmd.output().expect("failed to run graylog-cli")
}

struct TestEnv {
    home: TempDir,
    config_path: PathBuf,
}

impl TestEnv {
    fn new() -> Self {
        let home = tempfile::tempdir().expect("temp dir should create");
        let config_path = config_file_path(home.path());
        std::fs::create_dir_all(
            config_path
                .parent()
                .expect("config path should have a parent"),
        )
        .expect("config dir should create");
        Self { home, config_path }
    }

    fn write_config(&self, contents: &str) {
        std::fs::write(&self.config_path, contents).expect("config should write");
    }

    fn read_config(&self) -> String {
        std::fs::read_to_string(&self.config_path).expect("config should read")
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut cmd = graylog_cli();
        cmd.args(args)
            .env("HOME", self.config_home())
            .env("XDG_CONFIG_HOME", self.config_home())
            // dirs::config_dir() reads %APPDATA% on Windows, ignoring HOME.
            .env("APPDATA", self.config_home())
            .env("GRAYLOG_CLI_AUTO_UPDATE", "0")
            .env_remove("GRAYLOG_PROFILE")
            .env_remove("GRAYLOG_TOKEN");
        cmd
    }

    fn run(&self, args: &[&str]) -> std::process::Output {
        self.command(args)
            .output()
            .expect("failed to run graylog-cli")
    }

    fn config_home(&self) -> &Path {
        self.home.path()
    }
}

fn config_file_path(config_home: &Path) -> PathBuf {
    if cfg!(target_os = "macos") {
        config_home.join("Library/Application Support/graylog-cli/config.toml")
    } else {
        config_home.join("graylog-cli/config.toml")
    }
}

const LEGACY_CONFIG: &str = r#"
[graylog]
url = "https://legacy.example.com"
token = "legacy-secret-token"
timeout_seconds = 42

[updater]
disable_auto_update = true
"#;

const PROFILES_CONFIG: &str = r#"
active_profile = "alpha"

[profiles.alpha]
url = "https://alpha.example.com"
token = "alpha-secret-token"

[profiles.beta]
url = "https://beta.example.com"
token = "beta-secret-token"
"#;

#[test]
fn help_flag_exits_zero() {
    let output = run(&["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("graylog-cli"));
    assert!(stdout.contains("search"));
    assert!(stdout.contains("auth"));
    assert!(stdout.contains("--profile"));
}

#[test]
fn version_flag_exits_zero() {
    let output = run(&["--version"]);
    assert!(output.status.success());
}

#[test]
fn no_args_shows_help() {
    let output = run(&[]);
    assert!(!output.status.success());
}

#[test]
fn unknown_command_fails() {
    let output = run(&["nonexistent"]);
    assert!(!output.status.success());
}

#[test]
fn profiles_help_lists_subcommands() {
    let output = run(&["profiles", "--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for subcommand in ["list", "use", "show", "delete"] {
        assert!(
            stdout.contains(subcommand),
            "expected `{subcommand}` in help"
        );
    }
}

#[test]
fn legacy_config_migrates_without_reauth_and_without_token_leak() {
    let env = TestEnv::new();
    env.write_config(LEGACY_CONFIG);

    let output = env.run(&["profiles", "list"]);
    assert!(output.status.success(), "profiles list should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("default"),
        "legacy profile migrates to `default`"
    );
    assert!(
        stdout.contains("https://legacy.example.com/"),
        "legacy URL is preserved: {stdout}"
    );
    assert!(
        !stdout.contains("legacy-secret-token"),
        "token must never appear in output"
    );
    assert!(stdout.contains("\"active_profile\": \"default\""));

    // Loading never rewrites a legacy file.
    let on_disk = env.read_config();
    assert!(
        on_disk.contains("[graylog]"),
        "legacy config must not be rewritten by load"
    );
    assert!(!on_disk.contains("[profiles"));
}

#[test]
fn legacy_config_profiles_show_defaults_to_migrated_profile() {
    let env = TestEnv::new();
    env.write_config(LEGACY_CONFIG);

    let output = env.run(&["profiles", "show"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"name\": \"default\""));
    assert!(stdout.contains("\"active\": true"));
    assert!(stdout.contains("\"timeout_seconds\": 42"));
    assert!(!stdout.contains("legacy-secret-token"));
}

#[test]
fn global_profile_flag_selects_profile_for_show() {
    let env = TestEnv::new();
    env.write_config(PROFILES_CONFIG);

    let output = env.run(&["--profile", "beta", "profiles", "show"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("https://beta.example.com/"));
    assert!(stdout.contains("\"active\": true"));
    assert!(!stdout.contains("beta-secret-token"));
}

#[test]
fn profile_flag_works_after_subcommand() {
    let env = TestEnv::new();
    env.write_config(PROFILES_CONFIG);

    let output = env.run(&["profiles", "show", "--profile", "beta"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("https://beta.example.com/"));
}

#[test]
fn graylog_profile_env_var_selects_profile() {
    let env = TestEnv::new();
    env.write_config(PROFILES_CONFIG);

    let mut cmd = env.command(&["profiles", "show"]);
    cmd.env("GRAYLOG_PROFILE", "beta");
    let output = cmd.output().expect("failed to run graylog-cli");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("https://beta.example.com/"));
}

#[test]
fn profiles_use_persists_active_profile() {
    let env = TestEnv::new();
    env.write_config(PROFILES_CONFIG);

    let output = env.run(&["profiles", "use", "beta"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"command\": \"profiles.use\""));
    assert!(stdout.contains("\"name\": \"beta\""));

    let on_disk = env.read_config();
    assert!(on_disk.contains("active_profile = \"beta\""));

    let follow_up = env.run(&["profiles", "show"]);
    assert!(follow_up.status.success());
    let stdout = String::from_utf8_lossy(&follow_up.stdout);
    assert!(stdout.contains("https://beta.example.com/"));
}

#[test]
fn profiles_delete_active_clears_active_and_leaves_file_valid() {
    let env = TestEnv::new();
    env.write_config(PROFILES_CONFIG);

    let output = env.run(&["profiles", "delete", "alpha"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"remaining_profiles\": 1"));

    let on_disk = env.read_config();
    assert!(!on_disk.contains("[profiles.alpha]"));
    assert!(on_disk.contains("[profiles.beta]"));
    assert!(!on_disk.contains("active_profile = \"alpha\""));

    // Remaining profile is still usable without re-selecting it.
    let follow_up = env.run(&["profiles", "show"]);
    assert!(follow_up.status.success());
    let stdout = String::from_utf8_lossy(&follow_up.stdout);
    assert!(stdout.contains("https://beta.example.com/"));
}

#[test]
fn profiles_delete_last_profile_leads_to_not_configured() {
    let env = TestEnv::new();
    env.write_config(LEGACY_CONFIG);

    let deleted = env.run(&["profiles", "delete", "default"]);
    assert!(deleted.status.success());

    let output = env.run(&["profiles", "show"]);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("validation_error"));
    assert!(stderr.contains("graylog is not configured"));
}

#[test]
fn unknown_profile_reports_validation_error() {
    let env = TestEnv::new();
    env.write_config(PROFILES_CONFIG);

    let output = env.run(&["--profile", "gamma", "profiles", "show"]);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("validation_error"), "got: {stderr}");
    assert!(stderr.contains("unknown profile `gamma`"), "got: {stderr}");
    assert!(stderr.contains("alpha"), "got: {stderr}");
}

#[test]
fn invalid_profile_name_is_rejected_by_the_parser() {
    let output = run(&["--profile", "not valid", "ping"]);
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--profile"), "got: {stderr}");
}

#[test]
fn auth_without_profile_writes_default_profile() {
    let env = TestEnv::new();

    let output = env.run(&[
        "auth",
        "--url",
        "http://localhost:9000",
        "--token",
        "fresh-token",
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"profile\": \"default\""));
    assert!(!stdout.contains("fresh-token"));

    let on_disk = env.read_config();
    assert!(on_disk.contains("[profiles.default]"));
    assert!(on_disk.contains("active_profile = \"default\""));
    assert!(!on_disk.contains("[graylog]"));
}

#[test]
fn auth_with_global_profile_writes_named_profile() {
    let env = TestEnv::new();
    env.write_config(LEGACY_CONFIG);

    let output = env.run(&[
        "--profile",
        "prod",
        "auth",
        "--url",
        "http://prod:9000",
        "--token",
        "prod-token",
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"profile\": \"prod\""));

    let on_disk = env.read_config();
    assert!(
        on_disk.contains("[profiles.default]"),
        "legacy profile is kept"
    );
    assert!(on_disk.contains("[profiles.prod]"));
    assert!(on_disk.contains("active_profile = \"prod\""));
    assert!(
        on_disk.contains("disable_auto_update = true"),
        "updater settings survive"
    );
    assert!(
        !on_disk.contains("[graylog]"),
        "save always writes the new format"
    );
}

#[test]
fn ping_reports_profile_and_available_profiles() {
    use std::time::Duration;

    // One-shot HTTP server: a single accept with a read timeout, one fixed
    // 200 JSON response, then exit. No keep-alive, no second connection.
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let addr = listener.local_addr().expect("local addr");
    let (served_tx, served_rx) = std::sync::mpsc::channel::<bool>();
    let server = std::thread::spawn(move || {
        let served = (|| -> Option<()> {
            let (mut stream, _) = listener.accept().ok()?;
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .ok()?;
            let mut seen = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let n = stream.read(&mut buffer).ok()?;
                if n == 0 {
                    return None;
                }
                seen.extend_from_slice(&buffer[..n]);
                if String::from_utf8_lossy(&seen).contains("\r\n\r\n") {
                    break;
                }
            }
            let body = "{}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).ok()?;
            stream.flush().ok()?;
            Some(())
        })()
        .is_some();
        let _ = served_tx.send(served);
    });

    let env = TestEnv::new();
    env.write_config(&format!(
        r#"
active_profile = "alpha"

[profiles.alpha]
url = "http://{addr}"
token = "alpha-secret-token"

[profiles.beta]
url = "http://beta.example.com"
token = "beta-secret-token"
"#
    ));

    let output = env.run(&["ping"]);
    assert!(
        served_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("server should serve exactly one request"),
        "server should have served the ping request"
    );
    // The server thread has sent its result and returns immediately, so this
    // join cannot block.
    server.join().expect("server thread should finish");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"profile\": \"alpha\""), "got: {stdout}");
    assert!(stdout.contains("\"available_profiles\""));
    assert!(stdout.contains("alpha"));
    assert!(stdout.contains("beta"));
    assert!(!stdout.contains("alpha-secret-token"));
}
