use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_cli_runs() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.assert().success();
}

#[test]
fn test_help_flag() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage"));
}

#[test]
fn test_version_flag() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("0.1.0"));
}
