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

#[test]
fn test_overlap_function() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("overlap")
        .assert()
        .success()
        .stdout(predicate::str::contains("overlapping assets"));
}

#[test]
fn test_overlap_function_with_output() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_overlap.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("overlap")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Overlapping assets saved to:"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify column order: Symbol, Name, ETF_Count, ETFs
    let content = fs::read_to_string(&output_path).unwrap();
    let first_line = content.lines().next().unwrap();
    assert_eq!(first_line, "Symbol,Name,ETF_Count,ETFs");
}

#[test]
fn test_overlap_function_default_extension() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_overlap");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("overlap")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created with .csv extension
    let csv_path = temp_dir.path().join("test_overlap.csv");
    assert!(csv_path.exists());
}

#[test]
fn test_overlap_sort_by_count() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_overlap_count.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("overlap")
        .arg("--sort-by")
        .arg("count")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created
    assert!(output_path.exists());

    // Read the file and verify sorting (first data row should have highest count)
    let content = fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert!(lines.len() > 2); // Header + at least 2 data rows

    // Parse second line (first data row) to get ETF_Count
    let first_data = lines[1].split(',').collect::<Vec<&str>>();
    let first_count: u32 = first_data[2].parse().unwrap();

    // Parse last line to get ETF_Count
    let last_data = lines[lines.len() - 1].split(',').collect::<Vec<&str>>();
    let last_count: u32 = last_data[2].parse().unwrap();

    // Verify descending order by count
    assert!(first_count >= last_count);
}

#[test]
fn test_mapping_function() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("mapping")
        .assert()
        .success()
        .stdout(predicate::str::contains("Total assets:"))
        .stdout(predicate::str::contains("Asset distribution by ETF count:"));
}

#[test]
fn test_mapping_function_with_output() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_mapping.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("mapping")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Asset mapping saved to:"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify column order: Symbol, Name, ETF_Count, ETFs
    let content = fs::read_to_string(&output_path).unwrap();
    let first_line = content.lines().next().unwrap();
    assert_eq!(first_line, "Symbol,Name,ETF_Count,ETFs");
}

#[test]
fn test_mapping_function_default_extension() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_mapping");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("mapping")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created with .csv extension
    let csv_path = temp_dir.path().join("test_mapping.csv");
    assert!(csv_path.exists());
}

#[test]
fn test_mapping_sort_by_count() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_mapping_count.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("mapping")
        .arg("--sort-by")
        .arg("count")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created
    assert!(output_path.exists());

    // Read the file and verify sorting (first data row should have highest count)
    let content = fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert!(lines.len() > 2); // Header + at least 2 data rows

    // Parse second line (first data row) to get ETF_Count
    let first_data = lines[1].split(',').collect::<Vec<&str>>();
    let first_count: u32 = first_data[2].parse().unwrap();

    // Parse last line to get ETF_Count
    let last_data = lines[lines.len() - 1].split(',').collect::<Vec<&str>>();
    let last_count: u32 = last_data[2].parse().unwrap();

    // Verify descending order by count
    assert!(first_count >= last_count);
}

#[test]
fn test_list_function() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("Found"))
        .stdout(predicate::str::contains("ETFs:"));
}

#[test]
fn test_list_function_with_output() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_list.txt");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("list")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("ETF list saved to:"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify the file has content (one ETF per line)
    let content = fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert!(lines.len() > 0);

    // Verify ETF names are in the file
    assert!(content.len() > 0);
}

#[test]
fn test_list_function_default_extension() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_list");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("list")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created with .txt extension
    let txt_path = temp_dir.path().join("test_list.txt");
    assert!(txt_path.exists());
}

#[test]
fn test_list_function_with_filter() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_list_filter.txt");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("--etfs")
        .arg("IVW,IWF")
        .arg("-f")
        .arg("list")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Found 2 ETFs:"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify content contains exactly 2 ETFs
    let content = fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2);

    // Verify the ETFs are IVW and IWF (sorted alphabetically)
    assert!(lines.contains(&"IVW"));
    assert!(lines.contains(&"IWF"));
}

#[test]
fn test_summary_function() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("summary")
        .assert()
        .success()
        .stdout(predicate::str::contains("Total ETFs:"))
        .stdout(predicate::str::contains("Largest ETF contains"))
        .stdout(predicate::str::contains("Smallest ETF contains"));
}

#[test]
fn test_summary_function_with_output() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_summary.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("summary")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("ETF summary saved to:"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify column order: ETF, Asset_Count, Assets
    let content = fs::read_to_string(&output_path).unwrap();
    let first_line = content.lines().next().unwrap();
    assert_eq!(first_line, "ETF,Asset_Count,Assets");
}

#[test]
fn test_summary_function_default_extension() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_summary");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("summary")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created with .csv extension
    let csv_path = temp_dir.path().join("test_summary.csv");
    assert!(csv_path.exists());
}

#[test]
fn test_summary_function_with_filter() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_summary_filter.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("--etfs")
        .arg("IVW,IWF")
        .arg("-f")
        .arg("summary")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Total ETFs: 2"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify content contains exactly 2 ETFs (+ header)
    let content = fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 3); // Header + 2 ETFs
}
