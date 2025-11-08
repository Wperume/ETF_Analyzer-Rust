use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::fs;
use crate::Result;

/// Configuration for ETF Analyzer
/// Can be loaded from a TOML file to set default values for CLI parameters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// Default data directory
    pub data_dir: Option<String>,

    /// Default function to perform
    pub function: Option<String>,

    /// Default output file
    pub output: Option<String>,

    /// Default sort order
    pub sort_by: Option<String>,

    /// Default ETFs to filter
    pub etfs: Option<Vec<String>>,

    /// Force overwrite without prompting
    pub force: Option<bool>,

    /// Verbose mode
    pub verbose: Option<bool>,

    /// Column name overrides
    #[serde(default)]
    pub columns: ColumnConfig,
}

/// Column name configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnConfig {
    pub symbol_col: Option<String>,
    pub name_col: Option<String>,
    pub weight_col: Option<String>,
    pub shares_col: Option<String>,
    pub number_col: Option<String>,
}

impl Default for ColumnConfig {
    fn default() -> Self {
        Self {
            symbol_col: None,
            name_col: None,
            weight_col: None,
            shares_col: None,
            number_col: None,
        }
    }
}

impl Config {
    /// Load configuration from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| crate::Error::Other(format!("Failed to parse config file: {}", e)))?;
        Ok(config)
    }

    /// Load configuration from default locations
    /// Checks in order:
    /// 1. ./.etf_analyzer.toml (current directory)
    /// 2. ~/.config/etf_analyzer/config.toml (user config directory)
    /// 3. ~/.etf_analyzer.toml (home directory)
    pub fn load_default() -> Result<Option<Self>> {
        // Check current directory
        let current_dir_config = PathBuf::from(".etf_analyzer.toml");
        if current_dir_config.exists() {
            return Ok(Some(Self::from_file(current_dir_config)?));
        }

        // Check user config directory
        if let Some(config_dir) = Self::get_config_dir() {
            let config_path = config_dir.join("etf_analyzer").join("config.toml");
            if config_path.exists() {
                return Ok(Some(Self::from_file(config_path)?));
            }
        }

        // Check home directory
        if let Some(home_dir) = Self::get_home_dir() {
            let home_config = home_dir.join(".etf_analyzer.toml");
            if home_config.exists() {
                return Ok(Some(Self::from_file(home_config)?));
            }
        }

        Ok(None)
    }

    /// Get the user's config directory
    fn get_config_dir() -> Option<PathBuf> {
        if let Ok(config_dir) = std::env::var("XDG_CONFIG_HOME") {
            Some(PathBuf::from(config_dir))
        } else if let Some(home_dir) = Self::get_home_dir() {
            Some(home_dir.join(".config"))
        } else {
            None
        }
    }

    /// Get the user's home directory
    fn get_home_dir() -> Option<PathBuf> {
        std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .ok()
            .map(PathBuf::from)
    }

    /// Merge config with CLI arguments, giving CLI arguments priority
    pub fn merge_with_cli(&self, cli_args: &mut crate::cli::Args) {
        // Only set from config if CLI arg is None/default
        if cli_args.data_dir.is_none() {
            cli_args.data_dir = self.data_dir.clone();
        }

        // Only override function if it's still the default "summary"
        if cli_args.function == "summary" && self.function.is_some() {
            cli_args.function = self.function.clone().unwrap();
        }

        if cli_args.output.is_none() {
            cli_args.output = self.output.clone();
        }

        // Only override sort_by if it's the default "symbol"
        if cli_args.sort_by == "symbol" && self.sort_by.is_some() {
            cli_args.sort_by = self.sort_by.clone().unwrap();
        }

        if cli_args.etfs.is_none() {
            cli_args.etfs = self.etfs.clone();
        }

        // Booleans: only set from config if CLI flag wasn't explicitly set
        if !cli_args.force && self.force == Some(true) {
            cli_args.force = true;
        }

        if !cli_args.verbose && self.verbose == Some(true) {
            cli_args.verbose = true;
        }

        // Column overrides
        if cli_args.symbol_col.is_none() {
            cli_args.symbol_col = self.columns.symbol_col.clone();
        }
        if cli_args.name_col.is_none() {
            cli_args.name_col = self.columns.name_col.clone();
        }
        if cli_args.weight_col.is_none() {
            cli_args.weight_col = self.columns.weight_col.clone();
        }
        if cli_args.shares_col.is_none() {
            cli_args.shares_col = self.columns.shares_col.clone();
        }
        if cli_args.number_col.is_none() {
            cli_args.number_col = self.columns.number_col.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parsing() {
        let toml_str = r#"
            data_dir = "./test_data"
            function = "assets"
            sort_by = "count"
            verbose = true

            [columns]
            symbol_col = "Ticker"
            weight_col = "Weighting"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.data_dir, Some("./test_data".to_string()));
        assert_eq!(config.function, Some("assets".to_string()));
        assert_eq!(config.sort_by, Some("count".to_string()));
        assert_eq!(config.verbose, Some(true));
        assert_eq!(config.columns.symbol_col, Some("Ticker".to_string()));
        assert_eq!(config.columns.weight_col, Some("Weighting".to_string()));
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.data_dir.is_none());
        assert!(config.function.is_none());
        assert!(config.columns.symbol_col.is_none());
    }

    #[test]
    fn test_merge_with_cli_preserves_cli_args() {
        let config = Config {
            data_dir: Some("./config_data".to_string()),
            function: Some("assets".to_string()),
            sort_by: Some("count".to_string()),
            verbose: Some(true),
            force: Some(true),
            ..Default::default()
        };

        let mut args = crate::cli::Args {
            data_dir: Some("./cli_data".to_string()),
            function: "summary".to_string(),
            sort_by: "symbol".to_string(),
            verbose: false,
            force: false,
            import: None,
            output: None,
            etfs: None,
            symbol_col: None,
            name_col: None,
            weight_col: None,
            shares_col: None,
            number_col: None,
        };

        config.merge_with_cli(&mut args);

        // CLI args should be preserved
        assert_eq!(args.data_dir, Some("./cli_data".to_string()));
    }

    #[test]
    fn test_merge_with_cli_uses_config_defaults() {
        let config = Config {
            data_dir: Some("./config_data".to_string()),
            function: Some("assets".to_string()),
            sort_by: Some("count".to_string()),
            verbose: Some(true),
            output: Some("output.csv".to_string()),
            etfs: Some(vec!["VTI".to_string(), "VOO".to_string()]),
            ..Default::default()
        };

        let mut args = crate::cli::Args {
            data_dir: None,
            function: "summary".to_string(),
            sort_by: "symbol".to_string(),
            verbose: false,
            force: false,
            import: None,
            output: None,
            etfs: None,
            symbol_col: None,
            name_col: None,
            weight_col: None,
            shares_col: None,
            number_col: None,
        };

        config.merge_with_cli(&mut args);

        // Config values should be used when CLI args are None/default
        assert_eq!(args.data_dir, Some("./config_data".to_string()));
        assert_eq!(args.function, "assets");
        assert_eq!(args.sort_by, "count");
        assert_eq!(args.verbose, true);
        assert_eq!(args.output, Some("output.csv".to_string()));
        assert_eq!(args.etfs, Some(vec!["VTI".to_string(), "VOO".to_string()]));
    }

    #[test]
    fn test_merge_with_cli_column_overrides() {
        let config = Config {
            columns: ColumnConfig {
                symbol_col: Some("Ticker".to_string()),
                weight_col: Some("Weighting".to_string()),
                name_col: Some("CompanyName".to_string()),
                shares_col: None,
                number_col: None,
            },
            ..Default::default()
        };

        let mut args = crate::cli::Args {
            data_dir: None,
            function: "summary".to_string(),
            sort_by: "symbol".to_string(),
            verbose: false,
            force: false,
            import: None,
            output: None,
            etfs: None,
            symbol_col: None,
            name_col: None,
            weight_col: None,
            shares_col: None,
            number_col: None,
        };

        config.merge_with_cli(&mut args);

        // Config column overrides should be applied
        assert_eq!(args.symbol_col, Some("Ticker".to_string()));
        assert_eq!(args.weight_col, Some("Weighting".to_string()));
        assert_eq!(args.name_col, Some("CompanyName".to_string()));
        assert_eq!(args.shares_col, None);
        assert_eq!(args.number_col, None);
    }

    #[test]
    fn test_merge_with_cli_column_cli_priority() {
        let config = Config {
            columns: ColumnConfig {
                symbol_col: Some("Ticker".to_string()),
                weight_col: Some("Weighting".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut args = crate::cli::Args {
            data_dir: None,
            function: "summary".to_string(),
            sort_by: "symbol".to_string(),
            verbose: false,
            force: false,
            import: None,
            output: None,
            etfs: None,
            symbol_col: Some("Symbol_CLI".to_string()),
            name_col: None,
            weight_col: None,
            shares_col: None,
            number_col: None,
        };

        config.merge_with_cli(&mut args);

        // CLI symbol_col should be preserved, config weight_col should be used
        assert_eq!(args.symbol_col, Some("Symbol_CLI".to_string()));
        assert_eq!(args.weight_col, Some("Weighting".to_string()));
    }

    #[test]
    fn test_config_partial_fields() {
        let toml_str = r#"
            data_dir = "./data"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.data_dir, Some("./data".to_string()));
        assert!(config.function.is_none());
        assert!(config.verbose.is_none());
        assert!(config.columns.symbol_col.is_none());
    }

    #[test]
    fn test_config_with_etf_list() {
        let toml_str = r#"
            etfs = ["VTI", "VOO", "SPY"]
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.etfs, Some(vec!["VTI".to_string(), "VOO".to_string(), "SPY".to_string()]));
    }
}
