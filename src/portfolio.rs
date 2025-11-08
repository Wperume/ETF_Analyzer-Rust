use polars::prelude::*;
use rayon::prelude::*;
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

    // Set diagonal to 1.0 (correlation of a series with itself)
    for i in 0..n {
        corr_matrix[i][i] = 1.0;
    }

    // Generate all unique pairs (i, j) where i < j
    let pairs: Vec<(usize, usize)> = (0..n)
        .flat_map(|i| ((i + 1)..n).map(move |j| (i, j)))
        .collect();

    // Calculate correlations in parallel
    let results: Vec<((usize, usize), f64)> = pairs
        .par_iter()
        .map(|(i, j)| {
            let series1 = df.column(columns[*i]).unwrap().f64().unwrap();
            let series2 = df.column(columns[*j]).unwrap().f64().unwrap();
            let corr = pearson_correlation(&series1, &series2).unwrap_or(0.0);
            ((*i, *j), corr)
        })
        .collect();

    // Populate the correlation matrix (both upper and lower triangles)
    for ((i, j), corr) in results {
        corr_matrix[i][j] = corr;
        corr_matrix[j][i] = corr;
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

    #[test]
    fn test_correlation_matrix_parallel() {
        // Create a simple DataFrame with 3 columns for testing
        let col1 = Column::new("A".into(), &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let col2 = Column::new("B".into(), &[2.0, 4.0, 6.0, 8.0, 10.0]); // Perfect positive correlation with A
        let col3 = Column::new("C".into(), &[5.0, 4.0, 3.0, 2.0, 1.0]); // Perfect negative correlation with A

        let df = DataFrame::new(vec![col1, col2, col3]).unwrap();
        let columns = vec!["A", "B", "C"];

        let corr_matrix = calculate_correlation(&df, &columns).unwrap();

        // Verify matrix dimensions
        assert_eq!(corr_matrix.len(), 3);
        assert_eq!(corr_matrix[0].len(), 3);

        // Verify diagonal is all 1.0
        assert!((corr_matrix[0][0] - 1.0).abs() < 1e-10);
        assert!((corr_matrix[1][1] - 1.0).abs() < 1e-10);
        assert!((corr_matrix[2][2] - 1.0).abs() < 1e-10);

        // Verify symmetry
        assert!((corr_matrix[0][1] - corr_matrix[1][0]).abs() < 1e-10);
        assert!((corr_matrix[0][2] - corr_matrix[2][0]).abs() < 1e-10);
        assert!((corr_matrix[1][2] - corr_matrix[2][1]).abs() < 1e-10);

        // Verify A and B have perfect positive correlation (~1.0)
        assert!((corr_matrix[0][1] - 1.0).abs() < 1e-10);

        // Verify A and C have perfect negative correlation (~-1.0)
        assert!((corr_matrix[0][2] + 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_correlation_matrix_larger() {
        // Test with more columns to verify parallel execution
        let data: Vec<Column> = (0..10)
            .map(|i| {
                let values: Vec<f64> = (0..100).map(|j| (j as f64) * (i as f64 + 1.0)).collect();
                Column::new(format!("col_{}", i).into(), &values)
            })
            .collect();

        let df = DataFrame::new(data).unwrap();
        let columns: Vec<&str> = (0..10).map(|i| match i {
            0 => "col_0",
            1 => "col_1",
            2 => "col_2",
            3 => "col_3",
            4 => "col_4",
            5 => "col_5",
            6 => "col_6",
            7 => "col_7",
            8 => "col_8",
            9 => "col_9",
            _ => unreachable!(),
        }).collect();

        let corr_matrix = calculate_correlation(&df, &columns).unwrap();

        // Verify matrix dimensions (10x10 = 45 unique pairs)
        assert_eq!(corr_matrix.len(), 10);

        // Verify all diagonals are 1.0
        for i in 0..10 {
            assert!((corr_matrix[i][i] - 1.0).abs() < 1e-10);
        }

        // Verify symmetry for all pairs
        for i in 0..10 {
            for j in 0..10 {
                assert!((corr_matrix[i][j] - corr_matrix[j][i]).abs() < 1e-10);
            }
        }

        // All columns should have perfect positive correlation since they're linear multiples
        for i in 0..10 {
            for j in 0..10 {
                if i != j {
                    assert!((corr_matrix[i][j] - 1.0).abs() < 1e-10);
                }
            }
        }
    }
}
