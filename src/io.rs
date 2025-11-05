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

/// Save DataFrame to JSON file
pub fn save_json<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    JsonWriter::new(&mut file)
        .finish(&mut df.clone())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_csv_validates_path() {
        let result = load_csv("nonexistent.csv");
        assert!(result.is_err());
    }
}
