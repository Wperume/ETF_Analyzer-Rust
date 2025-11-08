use polars::prelude::*;
use std::path::Path;
use std::fs;
use std::io::{self, Write};
use rayon::prelude::*;
use crate::Result;

/// Load ETF data from a CSV file
pub fn load_csv<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path.as_ref().to_path_buf()))?
        .finish()?;

    Ok(df)
}

/// Save DataFrame to CSV file
pub fn save_csv<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    CsvWriter::new(&mut file)
        .finish(&mut df.clone())?;

    Ok(())
}

/// Save DataFrame to JSON file (requires "json" feature)
/// For now, this saves as CSV. Enable "json" feature in Cargo.toml to use JSON output.
pub fn save_json<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
    // Fallback to CSV since json feature is disabled to reduce rust-analyzer load
    save_csv(df, path)
}

/// Extract ETF name from filename pattern: {etf_name}-etf-holdings.csv
fn extract_etf_name<P: AsRef<Path>>(path: P) -> Result<String> {
    let path = path.as_ref();
    let filename = path
        .file_name()
        .and_then(|f| f.to_str())
        .ok_or_else(|| crate::Error::Parse("Invalid filename".to_string()))?;

    // Remove .csv extension
    let name_without_ext = filename.strip_suffix(".csv").unwrap_or(filename);

    // Extract ETF name from pattern: {etf_name}-etf-holdings
    if let Some(etf_name) = name_without_ext.strip_suffix("-etf-holdings") {
        Ok(etf_name.to_uppercase())
    } else {
        // Fallback: use filename without extension
        Ok(name_without_ext.to_uppercase())
    }
}

/// Load ETF holdings CSV file with specific format
/// Expected columns: "No.", "Name", "Ticker", "Asset Class", "% Weight", "Shares"
/// The function will:
/// - Synthesize Symbol values for empty/null/n/a entries using format: {ETF}-{No.}
/// - Rename "% Weight" to "weight"
/// - Add an "etf" column with the ETF name extracted from filename
/// - Reorder columns so "etf" is first
pub fn load_holdings_csv<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let path_ref = path.as_ref();
    let etf_name = extract_etf_name(path_ref)?;

    // Load CSV
    let mut df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path_ref.to_path_buf()))?
        .finish()?;

    // Get the "No." column before we process it
    let no_col = if df.column("No.").is_ok() {
        Some(df.column("No.")?.clone())
    } else {
        None
    };

    // Synthesize Symbol values for empty/null/n/a entries
    if let Some(no_series) = &no_col {
        if df.column("Symbol").is_ok() {
            let symbol_col = df.column("Symbol")?.str()?;
            let no_values = no_series.cast(&DataType::String)?;
            let no_str = no_values.str()?;

            let synthesized: Vec<Option<String>> = symbol_col
                .into_iter()
                .zip(no_str.into_iter())
                .map(|(symbol, no)| {
                    match symbol {
                        Some(s) if !s.is_empty() && s.to_lowercase() != "n/a" => {
                            Some(s.to_string())
                        }
                        _ => {
                            // Synthesize: {ETF}-{No}
                            no.map(|n| format!("{}-{}", etf_name, n))
                        }
                    }
                })
                .collect();

            let new_symbol_col = Series::new("Symbol".into(), synthesized);
            df.replace("Symbol", new_symbol_col)?;
        }
    }

    // Drop the "No." column if it exists (we don't need it anymore)
    if df.column("No.").is_ok() {
        df = df.drop("No.")?;
    }

    // Rename "% Weight" to "Weight"
    if df.column("% Weight").is_ok() {
        df.rename("% Weight", "Weight".into())?;
    }

    // Add ETF name column
    let etf_col = Series::new(
        "ETF".into(),
        vec![etf_name.as_str(); df.height()]
    );
    df.with_column(etf_col)?;

    // Reorder columns to put "ETF" first
    let column_names: Vec<String> = df.get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut new_order: Vec<String> = vec!["ETF".to_string()];

    // Add all other columns except "ETF"
    for col in &column_names {
        if col != "ETF" {
            new_order.push(col.clone());
        }
    }

    df = df.select(new_order)?;

    Ok(df)
}

/// Load multiple ETF holdings files and combine them into a single DataFrame
/// Uses parallel processing with Rayon for improved performance when loading many files
pub fn load_multiple_holdings<P: AsRef<Path> + Send + Sync>(paths: Vec<P>) -> Result<DataFrame> {
    if paths.is_empty() {
        return Err(crate::Error::Other("No paths provided".to_string()));
    }

    // Load all files in parallel using Rayon
    let results: Vec<Result<DataFrame>> = paths
        .par_iter()
        .map(|path| load_holdings_csv(path))
        .collect();

    // Collect results and handle errors
    let mut dataframes = Vec::new();
    for result in results {
        dataframes.push(result?);
    }

    // Vertically concatenate all DataFrames
    if dataframes.len() == 1 {
        return Ok(dataframes.into_iter().next().unwrap());
    }

    let mut combined = dataframes[0].clone();
    for df in dataframes.iter().skip(1) {
        combined.vstack_mut(df)?;
    }

    Ok(combined)
}

/// Load all ETF holdings CSV files from a directory
/// Looks for files matching pattern: *-etf-holdings.csv
pub fn load_portfolio_from_directory<P: AsRef<Path>>(dir_path: P) -> Result<DataFrame> {
    let dir_path = dir_path.as_ref();

    if !dir_path.exists() {
        return Err(crate::Error::Other(
            format!("Directory does not exist: {}", dir_path.display())
        ));
    }

    if !dir_path.is_dir() {
        return Err(crate::Error::Other(
            format!("Path is not a directory: {}", dir_path.display())
        ));
    }

    // Read directory and find all ETF holdings CSV files
    let entries = fs::read_dir(dir_path)?;
    let mut csv_files: Vec<_> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file() &&
            path.extension().map_or(false, |ext| ext == "csv") &&
            path.file_name()
                .and_then(|name| name.to_str())
                .map_or(false, |name| name.ends_with("-etf-holdings.csv"))
        })
        .collect();

    if csv_files.is_empty() {
        return Err(crate::Error::Other(
            format!("No ETF holdings CSV files found in directory: {}", dir_path.display())
        ));
    }

    // Sort for consistent ordering
    csv_files.sort();

    load_multiple_holdings(csv_files)
}

/// Determine file format from extension
#[derive(Debug, PartialEq)]
pub enum FileFormat {
    Csv,
    Parquet,
}

impl FileFormat {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        path.as_ref()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| match ext.to_lowercase().as_str() {
                "parquet" | "pq" => FileFormat::Parquet,
                _ => FileFormat::Csv,
            })
            .unwrap_or(FileFormat::Parquet) // Default to Parquet if no extension
    }
}

/// Import DataFrame from file (auto-detects CSV or Parquet based on extension)
/// If the exact path doesn't exist, tries adding .parquet extension
/// If both exist, uses the exact path specified
pub fn import_dataframe<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let path_ref = path.as_ref();

    // Determine which path to use
    let actual_path = if path_ref.exists() {
        // Exact path exists, use it
        path_ref.to_path_buf()
    } else {
        // Check if path with .parquet extension exists
        let parquet_path = if path_ref.extension().is_none() {
            // No extension, try adding .parquet
            let mut p = path_ref.to_path_buf();
            p.set_extension("parquet");
            p
        } else {
            // Has extension, don't modify
            path_ref.to_path_buf()
        };

        if parquet_path.exists() && parquet_path != path_ref {
            // .parquet version exists
            parquet_path
        } else {
            // Neither exists, return error with both paths tried
            let error_msg = if parquet_path != path_ref {
                format!(
                    "Import file does not exist. Tried:\n  - {}\n  - {}",
                    path_ref.display(),
                    parquet_path.display()
                )
            } else {
                format!("Import file does not exist: {}", path_ref.display())
            };
            return Err(crate::Error::Other(error_msg));
        }
    };

    match FileFormat::from_path(&actual_path) {
        FileFormat::Csv => load_csv(&actual_path),
        FileFormat::Parquet => {
            let file = std::fs::File::open(&actual_path)?;
            let df = ParquetReader::new(file).finish()?;
            Ok(df)
        }
    }
}

/// Export DataFrame to file (auto-detects CSV or Parquet based on extension)
/// Returns true if file was written, false if user cancelled overwrite
pub fn export_dataframe<P: AsRef<Path>>(
    df: &DataFrame,
    path: P,
    force: bool,
) -> Result<bool> {
    let path_ref = path.as_ref();

    // Check if file exists and prompt for overwrite unless --force is specified
    if path_ref.exists() && !force {
        print!("File '{}' already exists. Overwrite? [y/N]: ", path_ref.display());
        io::stdout().flush()?;

        let mut response = String::new();
        io::stdin().read_line(&mut response)?;

        let response = response.trim().to_lowercase();
        if response != "y" && response != "yes" {
            println!("Export cancelled.");
            return Ok(false);
        }
    }

    match FileFormat::from_path(path_ref) {
        FileFormat::Csv => {
            save_csv(df, path_ref)?;
        }
        FileFormat::Parquet => {
            let file = std::fs::File::create(path_ref)?;
            ParquetWriter::new(file)
                .finish(&mut df.clone())?;
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_csv_validates_path() {
        let result = load_csv("nonexistent.csv");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_etf_name_standard_format() {
        let result = extract_etf_name("spy-etf-holdings.csv").unwrap();
        assert_eq!(result, "SPY");

        let result = extract_etf_name("/path/to/voo-etf-holdings.csv").unwrap();
        assert_eq!(result, "VOO");
    }

    #[test]
    fn test_extract_etf_name_fallback() {
        let result = extract_etf_name("other-file.csv").unwrap();
        assert_eq!(result, "OTHER-FILE");
    }

    #[test]
    fn test_extract_etf_name_lowercase() {
        let result = extract_etf_name("qqq-etf-holdings.csv").unwrap();
        assert_eq!(result, "QQQ");
    }

    #[test]
    fn test_load_multiple_holdings_empty() {
        let paths: Vec<&str> = vec![];
        let result = load_multiple_holdings(paths);
        assert!(result.is_err());
    }
}
