use polars::prelude::*;
use crate::Result;

/// Portfolio configuration and state
pub struct Portfolio {
    pub etfs: Vec<String>,
    pub weights: Vec<f64>,
    pub data: Option<DataFrame>,
}

impl Portfolio {
    /// Create a new portfolio with equal weights
    pub fn new(etfs: Vec<String>) -> Self {
        let n = etfs.len();
        let weight = if n > 0 { 1.0 / n as f64 } else { 0.0 };
        let weights = vec![weight; n];

        Portfolio {
            etfs,
            weights,
            data: None,
        }
    }

    /// Create a portfolio with custom weights
    pub fn with_weights(etfs: Vec<String>, weights: Vec<f64>) -> Result<Self> {
        if etfs.len() != weights.len() {
            return Err(crate::Error::Other(
                "Number of ETFs must match number of weights".to_string()
            ));
        }

        let sum: f64 = weights.iter().sum();
        if (sum - 1.0).abs() > 1e-6 {
            return Err(crate::Error::Other(
                format!("Weights must sum to 1.0, got {}", sum)
            ));
        }

        Ok(Portfolio {
            etfs,
            weights,
            data: None,
        })
    }

    /// Load data for all ETFs in the portfolio
    pub fn load_data(&mut self, df: DataFrame) -> Result<()> {
        self.data = Some(df);
        Ok(())
    }

    /// Calculate portfolio return given individual ETF returns
    pub fn calculate_portfolio_return(&self, returns: &[f64]) -> Result<f64> {
        if returns.len() != self.weights.len() {
            return Err(crate::Error::Other(
                "Returns length must match weights length".to_string()
            ));
        }

        let portfolio_return: f64 = returns
            .iter()
            .zip(self.weights.iter())
            .map(|(r, w)| r * w)
            .sum();

        Ok(portfolio_return)
    }

    /// Rebalance portfolio to equal weights
    pub fn rebalance_equal(&mut self) {
        let n = self.etfs.len();
        let weight = if n > 0 { 1.0 / n as f64 } else { 0.0 };
        self.weights = vec![weight; n];
    }

    /// Get portfolio summary
    pub fn summary(&self) -> String {
        let mut summary = String::from("Portfolio Summary:\n");
        summary.push_str(&format!("ETFs: {}\n", self.etfs.len()));

        for (etf, weight) in self.etfs.iter().zip(self.weights.iter()) {
            summary.push_str(&format!("  {} - {:.2}%\n", etf, weight * 100.0));
        }

        summary
    }
}

/// Calculate correlation matrix for multiple return series
pub fn calculate_correlation(df: &DataFrame, columns: &[&str]) -> Result<Vec<Vec<f64>>> {
    let n = columns.len();
    let mut corr_matrix = vec![vec![0.0; n]; n];

    for (i, col1) in columns.iter().enumerate() {
        for (j, col2) in columns.iter().enumerate() {
            if i == j {
                corr_matrix[i][j] = 1.0;
            } else if i < j {
                let series1 = df.column(col1)?.f64()?;
                let series2 = df.column(col2)?.f64()?;

                // Simple Pearson correlation
                let corr = pearson_correlation(&series1, &series2)?;
                corr_matrix[i][j] = corr;
                corr_matrix[j][i] = corr;
            }
        }
    }

    Ok(corr_matrix)
}

/// Calculate Pearson correlation coefficient
fn pearson_correlation(x: &Float64Chunked, y: &Float64Chunked) -> Result<f64> {
    let x_vec: Vec<f64> = x.into_iter().flatten().collect();
    let y_vec: Vec<f64> = y.into_iter().flatten().collect();

    if x_vec.len() != y_vec.len() || x_vec.is_empty() {
        return Ok(0.0);
    }

    let n = x_vec.len() as f64;
    let mean_x: f64 = x_vec.iter().sum::<f64>() / n;
    let mean_y: f64 = y_vec.iter().sum::<f64>() / n;

    let mut numerator = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;

    for (xi, yi) in x_vec.iter().zip(y_vec.iter()) {
        let dx = xi - mean_x;
        let dy = yi - mean_y;
        numerator += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }

    if var_x == 0.0 || var_y == 0.0 {
        return Ok(0.0);
    }

    Ok(numerator / (var_x * var_y).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_portfolio_new_equal_weights() {
        let portfolio = Portfolio::new(vec!["SPY".to_string(), "QQQ".to_string()]);
        assert_eq!(portfolio.weights, vec![0.5, 0.5]);
    }

    #[test]
    fn test_portfolio_weights_must_sum_to_one() {
        let result = Portfolio::with_weights(
            vec!["SPY".to_string()],
            vec![0.5]
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_portfolio_return_calculation() {
        let portfolio = Portfolio::new(vec!["SPY".to_string(), "QQQ".to_string()]);
        let returns = vec![0.10, 0.20];
        let portfolio_return = portfolio.calculate_portfolio_return(&returns).unwrap();
        assert!((portfolio_return - 0.15).abs() < 1e-6);
    }
}
