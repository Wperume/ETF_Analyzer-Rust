use polars::prelude::*;
use rayon::prelude::*;
use crate::Result;

/// Calculate daily returns for a price column
pub fn calculate_returns(df: &DataFrame, price_col: &str) -> Result<DataFrame> {
    let prices = df.column(price_col)?.f64()?;

    // Manually calculate percentage change
    let mut returns_vec = Vec::with_capacity(prices.len());
    returns_vec.push(None); // First value is always null

    for i in 1..prices.len() {
        let prev = prices.get(i - 1);
        let curr = prices.get(i);

        match (prev, curr) {
            (Some(p), Some(c)) if p != 0.0 => {
                returns_vec.push(Some((c - p) / p));
            }
            _ => returns_vec.push(None),
        }
    }

    let returns_series = Series::new("daily_return".into(), returns_vec);
    let mut result = df.clone();
    result.with_column(returns_series)?;

    Ok(result)
}

/// Calculate volatility (standard deviation of returns)
pub fn calculate_volatility(df: &DataFrame, returns_col: &str) -> Result<f64> {
    let returns_series = df.column(returns_col)?;

    let volatility = returns_series
        .f64()?
        .std(1) // ddof=1 for sample std dev
        .unwrap_or(0.0);

    Ok(volatility)
}

/// Calculate Sharpe ratio
/// Assumes returns_col contains daily returns and risk_free_rate is annual
pub fn calculate_sharpe_ratio(
    df: &DataFrame,
    returns_col: &str,
    risk_free_rate: f64,
) -> Result<f64> {
    let returns_series = df.column(returns_col)?;
    let returns = returns_series.f64()?;

    let mean_return = returns.mean().unwrap_or(0.0);
    let std_dev = returns.std(1).unwrap_or(0.0);

    // Annualize (assuming daily returns, 252 trading days)
    let annual_return = mean_return * 252.0;
    let annual_volatility = std_dev * (252.0_f64).sqrt();

    if annual_volatility == 0.0 {
        return Ok(0.0);
    }

    let sharpe = (annual_return - risk_free_rate) / annual_volatility;
    Ok(sharpe)
}

/// Calculate maximum drawdown
pub fn calculate_max_drawdown(df: &DataFrame, price_col: &str) -> Result<f64> {
    let prices = df.column(price_col)?.f64()?;

    let mut peak = f64::MIN;
    let mut max_dd = 0.0;

    for price in prices.into_iter().flatten() {
        if price > peak {
            peak = price;
        }
        let drawdown = (peak - price) / peak;
        if drawdown > max_dd {
            max_dd = drawdown;
        }
    }

    Ok(max_dd)
}

/// Compare multiple ETFs using parallel processing
pub fn compare_etfs(dfs: Vec<DataFrame>, metric_fn: fn(&DataFrame) -> Result<f64>) -> Result<Vec<f64>> {
    let results: Vec<Result<f64>> = dfs
        .par_iter()
        .map(metric_fn)
        .collect();

    results.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_volatility_zero_variance() {
        let df = df! {
            "returns" => &[0.0, 0.0, 0.0]
        }.unwrap();

        let vol = calculate_volatility(&df, "returns").unwrap();
        assert_eq!(vol, 0.0);
    }

    #[test]
    fn test_sharpe_ratio_zero_volatility() {
        let df = df! {
            "returns" => &[0.0, 0.0, 0.0]
        }.unwrap();

        let sharpe = calculate_sharpe_ratio(&df, "returns", 0.02).unwrap();
        assert_eq!(sharpe, 0.0);
    }
}
