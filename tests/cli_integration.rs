use std::process::Command;

fn graylog_cli() -> Command {
    Command::new("cargo")
}

fn run(args: &[&str]) -> std::process::Output {
    let mut cmd = graylog_cli();
    cmd.args(["run", "--"]).args(args);
    cmd.output().expect("failed to run graylog-cli")
}

#[test]
fn help_flag_exits_zero() {
    let output = run(&["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("graylog-cli"));
    assert!(stdout.contains("search"));
    assert!(stdout.contains("auth"));
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
