use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_cli_runs() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    // With the updated CLI, we need to provide either -d or -i
    // Test that it fails with the expected error message when neither is provided
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Either --data-dir (-d) or --import (-i) must be specified"));
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

#[test]
fn test_assets_function() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("assets")
        .assert()
        .success()
        .stdout(predicate::str::contains("Total assets:"))
        .stdout(predicate::str::contains("Asset distribution by ETF count:"));
}

#[test]
fn test_assets_function_with_output() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_assets.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("assets")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Assets saved to:"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify the file has content
    let content = fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("Symbol,Name,ETF_Count,ETFs"));
}

#[test]
fn test_assets_function_default_extension() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_assets");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("assets")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created with .csv extension
    let csv_path = temp_dir.path().join("test_assets.csv");
    assert!(csv_path.exists());
}

#[test]
fn test_unique_function() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("unique")
        .assert()
        .success()
        .stdout(predicate::str::contains("unique assets"));
}

#[test]
fn test_unique_function_with_output() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_unique.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("unique")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Unique assets saved to:"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify column order: Symbol, Name, Weight, ETF
    let content = fs::read_to_string(&output_path).unwrap();
    let first_line = content.lines().next().unwrap();
    assert_eq!(first_line, "Symbol,Name,Weight,ETF");
}

#[test]
fn test_export_function_csv() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_export.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("export")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Successfully exported to:"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify it's a valid CSV with expected columns
    let content = fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("ETF,Symbol,Name,Weight"));
}

#[test]
fn test_export_function_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_export.parquet");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("export")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created
    assert!(output_path.exists());
}

#[test]
fn test_import_function() {
    let temp_dir = TempDir::new().unwrap();
    let export_path = temp_dir.path().join("test_export.parquet");

    // First export data
    let mut export_cmd = Command::cargo_bin("etf_analyzer").unwrap();
    export_cmd
        .arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("export")
        .arg("-o")
        .arg(&export_path)
        .arg("--force")
        .assert()
        .success();

    // Then import it back
    let mut import_cmd = Command::cargo_bin("etf_analyzer").unwrap();
    import_cmd
        .arg("-i")
        .arg(&export_path)
        .arg("-f")
        .arg("assets")
        .assert()
        .success()
        .stdout(predicate::str::contains("Total assets:"));
}

#[test]
fn test_etf_filter() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("--etfs")
        .arg("IVW,IWF")
        .arg("-f")
        .arg("assets")
        .assert()
        .success()
        .stdout(predicate::str::contains("Total assets:"));
}

#[test]
fn test_etf_filter_case_insensitive() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("--etfs")
        .arg("ivw,iwf")
        .arg("-f")
        .arg("assets")
        .assert()
        .success();
}

#[test]
fn test_assets_sort_by_count() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("assets")
        .arg("--sort-by")
        .arg("count")
        .assert()
        .success();
}
