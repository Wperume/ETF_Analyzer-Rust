use polars::prelude::*;
use std::path::Path;
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
/// - Skip the "No." column (index)
/// - Rename "% Weight" to "weight"
/// - Add an "etf" column with the ETF name extracted from filename
pub fn load_holdings_csv<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let path_ref = path.as_ref();
    let etf_name = extract_etf_name(path_ref)?;

    // Load CSV
    let mut df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path_ref.to_path_buf()))?
        .finish()?;

    // Drop the "No." column if it exists (it's just an index)
    if df.column("No.").is_ok() {
        df = df.drop("No.")?;
    }

    // Rename "% Weight" to "weight"
    if df.column("% Weight").is_ok() {
        df.rename("% Weight", "weight".into())?;
    }

    // Add ETF name column
    let etf_col = Series::new(
        "etf".into(),
        vec![etf_name.as_str(); df.height()]
    );
    df.with_column(etf_col)?;

    Ok(df)
}

/// Load multiple ETF holdings files and combine them into a single DataFrame
pub fn load_multiple_holdings<P: AsRef<Path>>(paths: Vec<P>) -> Result<DataFrame> {
    if paths.is_empty() {
        return Err(crate::Error::Other("No paths provided".to_string()));
    }

    let mut dataframes = Vec::new();

    for path in paths {
        let df = load_holdings_csv(path)?;
        dataframes.push(df);
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
