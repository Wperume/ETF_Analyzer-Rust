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

#[test]
fn test_compare_function() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_compare.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("--etfs")
        .arg("IVW,IWF")
        .arg("-f")
        .arg("compare")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Comparing 2 ETFs"));

    // Verify the file was created
    assert!(output_path.exists());

    // Verify column structure: Symbol, IVW, IWF
    let content = fs::read_to_string(&output_path).unwrap();
    let first_line = content.lines().next().unwrap();
    assert_eq!(first_line, "Symbol,IVW,IWF");
}

#[test]
fn test_compare_function_default_extension() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_compare");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("--etfs")
        .arg("IVW,IWF")
        .arg("-f")
        .arg("compare")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify the file was created with .csv extension
    let csv_path = temp_dir.path().join("test_compare.csv");
    assert!(csv_path.exists());
}

#[test]
fn test_compare_function_requires_etfs() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test_compare.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("-f")
        .arg("compare")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Compare function requires --etfs to be specified"));
}

#[test]
fn test_compare_function_requires_output() {
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg("./example-data")
        .arg("--etfs")
        .arg("IVW,IWF")
        .arg("-f")
        .arg("compare")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Compare function requires --output (-o) to be specified"));
}

#[test]
fn test_column_override_symbol_col() {
    // Create a temporary directory with a test CSV file that has different column names
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test-etf-holdings.csv");

    // Write a CSV with custom column names (Ticker instead of Symbol)
    fs::write(&test_file, "Ticker,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\nMSFT,Microsoft,8%,80,2\n").unwrap();

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg(temp_dir.path())
        .arg("--symbol-col")
        .arg("Ticker")
        .arg("-f")
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("Found 1 ETFs"));
}

#[test]
fn test_column_override_multiple_columns() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("custom-etf-holdings.csv");

    // Write a CSV with all custom column names
    fs::write(&test_file, "Ticker,CompanyName,Weighting,Holdings,RowNum\nAAPL,Apple Inc.,10%,100,1\nMSFT,Microsoft,8%,80,2\n").unwrap();

    let output_path = temp_dir.path().join("output.csv");

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg(temp_dir.path())
        .arg("--symbol-col")
        .arg("Ticker")
        .arg("--name-col")
        .arg("CompanyName")
        .arg("--weight-col")
        .arg("Weighting")
        .arg("--shares-col")
        .arg("Holdings")
        .arg("--number-col")
        .arg("RowNum")
        .arg("-f")
        .arg("summary")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Total ETFs: 1"));

    // Verify the output file was created and contains the expected data
    assert!(output_path.exists());
    let content = fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("AAPL"));
    assert!(content.contains("MSFT"));
}

#[test]
fn test_column_override_with_different_order() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("reordered-etf-holdings.csv");

    // Write a CSV with columns in a different order than usual
    fs::write(&test_file, "CompanyName,Weighting,Ticker,Holdings,RowNum\nApple Inc.,10%,AAPL,100,1\nMicrosoft,8%,MSFT,80,2\n").unwrap();

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg(temp_dir.path())
        .arg("--symbol-col")
        .arg("Ticker")
        .arg("--name-col")
        .arg("CompanyName")
        .arg("--weight-col")
        .arg("Weighting")
        .arg("--shares-col")
        .arg("Holdings")
        .arg("--number-col")
        .arg("RowNum")
        .arg("-f")
        .arg("assets")
        .assert()
        .success()
        .stdout(predicate::str::contains("Total assets: 2"));
}

#[test]
fn test_column_override_verbose_output() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test-etf-holdings.csv");

    fs::write(&test_file, "Ticker,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.arg("-d")
        .arg(temp_dir.path())
        .arg("--symbol-col")
        .arg("Ticker")
        .arg("-f")
        .arg("list")
        .arg("-v")
        .assert()
        .success()
        .stdout(predicate::str::contains("Using custom column configuration"))
        .stdout(predicate::str::contains("Symbol column: Ticker"));
}

#[test]
fn test_config_file_loads_data_dir() {
    let temp_dir = TempDir::new().unwrap();

    // Create example-data directory structure in temp dir
    let data_dir = temp_dir.path().join("test_data");
    fs::create_dir(&data_dir).unwrap();
    let test_file = data_dir.join("test-etf-holdings.csv");
    fs::write(&test_file, "Symbol,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    // Create config file in temp directory
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    fs::write(&config_file, format!("data_dir = \"{}\"", data_dir.display())).unwrap();

    // Run from temp directory so config is loaded
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .arg("-f")
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("Found 1 ETFs"));
}

#[test]
fn test_config_file_verbose_output() {
    let temp_dir = TempDir::new().unwrap();

    // Create example-data directory structure in temp dir
    let data_dir = temp_dir.path().join("test_data");
    fs::create_dir(&data_dir).unwrap();
    let test_file = data_dir.join("test-etf-holdings.csv");
    fs::write(&test_file, "Symbol,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    // Create config file with verbose = true
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    fs::write(&config_file, format!("data_dir = \"{}\"\nverbose = true", data_dir.display())).unwrap();

    // Run from temp directory
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .arg("-f")
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("ETF Analyzer starting..."))
        .stdout(predicate::str::contains("Loading portfolio from directory"));
}

#[test]
fn test_config_file_cli_overrides() {
    let temp_dir = TempDir::new().unwrap();

    // Create two data directories
    let config_data = temp_dir.path().join("config_data");
    fs::create_dir(&config_data).unwrap();
    let config_file_path = config_data.join("config-etf-holdings.csv");
    fs::write(&config_file_path, "Symbol,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    let cli_data = temp_dir.path().join("cli_data");
    fs::create_dir(&cli_data).unwrap();
    let cli_file_path = cli_data.join("cli-etf-holdings.csv");
    fs::write(&cli_file_path, "Symbol,Name,% Weight,Shares,No.\nMSFT,Microsoft,10%,100,1\nGOOG,Google,10%,100,2\n").unwrap();

    // Create config file pointing to config_data
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    fs::write(&config_file, format!("data_dir = \"{}\"", config_data.display())).unwrap();

    // Run from temp directory but override data_dir with CLI arg
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .arg("-d")
        .arg(&cli_data)
        .arg("-f")
        .arg("assets")
        .assert()
        .success()
        .stdout(predicate::str::contains("Total assets: 2")); // cli_data has 2 assets, not 1
}

#[test]
fn test_config_file_function_default() {
    let temp_dir = TempDir::new().unwrap();

    // Create data directory
    let data_dir = temp_dir.path().join("test_data");
    fs::create_dir(&data_dir).unwrap();
    let test_file = data_dir.join("test-etf-holdings.csv");
    fs::write(&test_file, "Symbol,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    // Create config file with function = "list"
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    fs::write(&config_file, format!("data_dir = \"{}\"\nfunction = \"list\"", data_dir.display())).unwrap();

    // Run without specifying function (should use config default)
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Found 1 ETFs"));
}

#[test]
fn test_config_file_sort_by() {
    let temp_dir = TempDir::new().unwrap();

    // Create data directory with multiple files
    let data_dir = temp_dir.path().join("test_data");
    fs::create_dir(&data_dir).unwrap();

    let file1 = data_dir.join("etf1-etf-holdings.csv");
    fs::write(&file1, "Symbol,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    let file2 = data_dir.join("etf2-etf-holdings.csv");
    fs::write(&file2, "Symbol,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\nMSFT,Microsoft,10%,100,2\n").unwrap();

    let output_path = temp_dir.path().join("output.csv");

    // Create config file with sort_by = "count"
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    fs::write(&config_file, format!("data_dir = \"{}\"\nsort_by = \"count\"", data_dir.display())).unwrap();

    // Run assets function (should sort by count from config)
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .arg("-f")
        .arg("assets")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify sorting: AAPL should be first (appears in 2 ETFs)
    let content = fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert!(lines[1].starts_with("AAPL")); // First data row should be AAPL
}

#[test]
fn test_config_file_column_overrides() {
    let temp_dir = TempDir::new().unwrap();

    // Create data directory with custom column names
    let data_dir = temp_dir.path().join("test_data");
    fs::create_dir(&data_dir).unwrap();
    let test_file = data_dir.join("test-etf-holdings.csv");
    fs::write(&test_file, "Ticker,CompanyName,Weighting,Holdings,RowNum\nAAPL,Apple Inc.,10%,100,1\nMSFT,Microsoft,8%,80,2\n").unwrap();

    // Create config file with column overrides
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    let config_content = format!(
        "data_dir = \"{}\"\n\n[columns]\nsymbol_col = \"Ticker\"\nname_col = \"CompanyName\"\nweight_col = \"Weighting\"\nshares_col = \"Holdings\"\nnumber_col = \"RowNum\"",
        data_dir.display()
    );
    fs::write(&config_file, config_content).unwrap();

    // Run from temp directory
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .arg("-f")
        .arg("assets")
        .assert()
        .success()
        .stdout(predicate::str::contains("Total assets: 2"));
}

#[test]
fn test_config_file_column_overrides_cli_priority() {
    let temp_dir = TempDir::new().unwrap();

    // Create data directory with custom column names
    let data_dir = temp_dir.path().join("test_data");
    fs::create_dir(&data_dir).unwrap();
    let test_file = data_dir.join("test-etf-holdings.csv");
    fs::write(&test_file, "CLICol,CompanyName,Weighting,Holdings,RowNum\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    // Create config file with different symbol column name
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    let config_content = format!(
        "data_dir = \"{}\"\n\n[columns]\nsymbol_col = \"ConfigCol\"\nname_col = \"CompanyName\"\nweight_col = \"Weighting\"",
        data_dir.display()
    );
    fs::write(&config_file, config_content).unwrap();

    // Run with CLI override for symbol_col (should use CLICol, not ConfigCol)
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .arg("--symbol-col")
        .arg("CLICol")
        .arg("--name-col")
        .arg("CompanyName")
        .arg("--weight-col")
        .arg("Weighting")
        .arg("--number-col")
        .arg("RowNum")
        .arg("-f")
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("Found 1 ETFs"));
}

#[test]
fn test_config_file_etf_filter() {
    let temp_dir = TempDir::new().unwrap();

    // Create data directory with multiple ETF files
    let data_dir = temp_dir.path().join("test_data");
    fs::create_dir(&data_dir).unwrap();

    let file1 = data_dir.join("vti-etf-holdings.csv");
    fs::write(&file1, "Symbol,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    let file2 = data_dir.join("voo-etf-holdings.csv");
    fs::write(&file2, "Symbol,Name,% Weight,Shares,No.\nMSFT,Microsoft,10%,100,1\n").unwrap();

    let file3 = data_dir.join("spy-etf-holdings.csv");
    fs::write(&file3, "Symbol,Name,% Weight,Shares,No.\nGOOG,Google,10%,100,1\n").unwrap();

    // Create config file with ETF filter
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    fs::write(&config_file, format!("data_dir = \"{}\"\netfs = [\"VTI\", \"VOO\"]", data_dir.display())).unwrap();

    // Run list function (should only show VTI and VOO)
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .arg("-f")
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("Found 2 ETFs"));
}

#[test]
fn test_config_file_force_option() {
    let temp_dir = TempDir::new().unwrap();

    // Create data directory
    let data_dir = temp_dir.path().join("test_data");
    fs::create_dir(&data_dir).unwrap();
    let test_file = data_dir.join("test-etf-holdings.csv");
    fs::write(&test_file, "Symbol,Name,% Weight,Shares,No.\nAAPL,Apple Inc.,10%,100,1\n").unwrap();

    let output_path = temp_dir.path().join("output.csv");

    // Create initial output file
    fs::write(&output_path, "existing content").unwrap();

    // Create config file with force = true
    let config_file = temp_dir.path().join(".etf_analyzer.toml");
    fs::write(&config_file, format!("data_dir = \"{}\"\nforce = true", data_dir.display())).unwrap();

    // Run export (should overwrite without prompting due to force in config)
    let mut cmd = Command::cargo_bin("etf_analyzer").unwrap();
    cmd.current_dir(temp_dir.path())
        .arg("-f")
        .arg("export")
        .arg("-o")
        .arg(&output_path)
        .assert()
        .success();

    // Verify file was overwritten
    let content = fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("ETF,Symbol,Name,Weight"));
}

