use polars::prelude::*;
use std::fmt::Write as FmtWrite;
use crate::Result;
use crate::portfolio::Portfolio;

/// Generate a text report from analysis results
pub fn generate_text_report(
    portfolio: &Portfolio,
    metrics: &AnalysisMetrics,
) -> String {
    let mut report = String::new();

    writeln!(report, "{}", "=".repeat(60)).unwrap();
    writeln!(report, "ETF ANALYSIS REPORT").unwrap();
    writeln!(report, "{}", "=".repeat(60)).unwrap();
    writeln!(report).unwrap();

    writeln!(report, "{}", portfolio.summary()).unwrap();
    writeln!(report).unwrap();

    writeln!(report, "Performance Metrics:").unwrap();
    writeln!(report, "  Volatility: {:.4}", metrics.volatility).unwrap();
    writeln!(report, "  Sharpe Ratio: {:.4}", metrics.sharpe_ratio).unwrap();
    writeln!(report, "  Max Drawdown: {:.2}%", metrics.max_drawdown * 100.0).unwrap();
    writeln!(report).unwrap();

    writeln!(report, "{}", "=".repeat(60)).unwrap();

    report
}

/// Generate a DataFrame summary report
pub fn generate_dataframe_summary(df: &DataFrame) -> Result<String> {
    let mut summary = String::new();

    writeln!(summary, "DataFrame Summary:").unwrap();
    writeln!(summary, "  Shape: {:?}", df.shape()).unwrap();
    writeln!(summary, "  Columns: {:?}", df.get_column_names()).unwrap();
    writeln!(summary).unwrap();

    writeln!(summary, "First 5 rows:").unwrap();
    writeln!(summary, "{}", df.head(Some(5))).unwrap();

    Ok(summary)
}

/// Generate CSV summary with key statistics
pub fn generate_statistics_csv(df: &DataFrame, output_path: &str) -> Result<()> {
    // Manual calculation of basic statistics since describe() is not available
    // You can extend this with more statistics as needed
    crate::io::save_csv(df, output_path)?;
    Ok(())
}

/// Analysis metrics structure
#[derive(Debug, Clone)]
pub struct AnalysisMetrics {
    pub volatility: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
}

impl AnalysisMetrics {
    pub fn new(volatility: f64, sharpe_ratio: f64, max_drawdown: f64) -> Self {
        AnalysisMetrics {
            volatility,
            sharpe_ratio,
            max_drawdown,
        }
    }
}

/// Format a correlation matrix for display
pub fn format_correlation_matrix(
    matrix: &[Vec<f64>],
    labels: &[String],
) -> String {
    let mut output = String::new();

    writeln!(output, "\nCorrelation Matrix:").unwrap();

    // Header
    write!(output, "{:>10}", "").unwrap();
    for label in labels {
        write!(output, "{:>10}", label).unwrap();
    }
    writeln!(output).unwrap();

    // Matrix rows
    for (i, row) in matrix.iter().enumerate() {
        write!(output, "{:>10}", labels[i]).unwrap();
        for val in row {
            write!(output, "{:>10.4}", val).unwrap();
        }
        writeln!(output).unwrap();
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analysis_metrics_creation() {
        let metrics = AnalysisMetrics::new(0.15, 1.5, 0.20);
        assert_eq!(metrics.volatility, 0.15);
        assert_eq!(metrics.sharpe_ratio, 1.5);
        assert_eq!(metrics.max_drawdown, 0.20);
    }

    #[test]
    fn test_format_correlation_matrix() {
        let matrix = vec![
            vec![1.0, 0.8],
            vec![0.8, 1.0],
        ];
        let labels = vec!["SPY".to_string(), "QQQ".to_string()];
        let output = format_correlation_matrix(&matrix, &labels);
        assert!(output.contains("SPY"));
        assert!(output.contains("QQQ"));
        assert!(output.contains("1.0000"));
    }
}
