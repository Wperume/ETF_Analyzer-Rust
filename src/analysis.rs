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

/// Filter DataFrame to only include specified ETFs
/// Returns filtered DataFrame containing only rows where ETF column matches one of the specified ETF symbols
pub fn filter_etfs(df: &DataFrame, etf_symbols: &[String]) -> Result<DataFrame> {
    if etf_symbols.is_empty() {
        return Ok(df.clone());
    }

    // Convert ETF symbols to uppercase for case-insensitive matching
    let etf_symbols_upper: Vec<String> = etf_symbols
        .iter()
        .map(|s| s.to_uppercase())
        .collect();

    // Get the ETF column and filter
    let etf_col = df.column("ETF")?;
    let etf_str = etf_col.str()?;

    // Create boolean mask for matching ETFs
    let mut mask = BooleanChunked::from_iter(
        std::iter::repeat(false).take(df.height())
    );

    for etf_symbol in &etf_symbols_upper {
        let matches = etf_str
            .into_iter()
            .map(|opt_str| {
                opt_str.map_or(false, |s| s.to_uppercase() == *etf_symbol)
            });
        let current_mask = BooleanChunked::from_iter(matches);
        mask = mask | current_mask;
    }

    let filtered = df.filter(&mask)?;

    Ok(filtered)
}

/// Sort order for assets aggregation
#[derive(Debug, Clone, PartialEq)]
pub enum AssetsSortBy {
    Symbol,      // Sort alphabetically by symbol
    EtfCount,    // Sort by ETF count (descending), then by symbol
}

impl AssetsSortBy {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "symbol" | "alpha" | "alphabetical" => AssetsSortBy::Symbol,
            "count" | "etf_count" | "etfs" => AssetsSortBy::EtfCount,
            _ => AssetsSortBy::Symbol, // Default
        }
    }
}

/// Aggregate assets across all ETFs
/// Returns a DataFrame with columns: Symbol, Name, ETF_Count, ETFs
pub fn aggregate_assets(df: &DataFrame, sort_by: AssetsSortBy) -> Result<DataFrame> {
    // Group by Symbol and aggregate
    let grouped = df
        .clone()
        .lazy()
        .group_by([col("Symbol")])
        .agg([
            // Take the first Name for each Symbol (they should all be the same)
            col("Name").first().alias("Name"),
            // Count unique ETFs
            col("ETF").n_unique().alias("ETF_Count"),
            // Collect all unique ETF names as a list
            col("ETF").unique().alias("ETF_List"),
        ])
        .collect()?;

    // Convert ETF_List to comma-separated string
    let etf_list_col = grouped.column("ETF_List")?;
    let etf_strings: Vec<String> = etf_list_col
        .list()?
        .into_iter()
        .map(|opt_series| {
            opt_series
                .map(|series| {
                    series
                        .str()
                        .map(|ca| {
                            ca.into_iter()
                                .flatten()
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_default()
                })
                .unwrap_or_default()
        })
        .collect();

    // Create new DataFrame with the ETFs column as string
    let mut result = grouped
        .select(["Symbol", "Name", "ETF_Count"])?;

    let etfs_col = Series::new("ETFs".into(), etf_strings);
    result.with_column(etfs_col)?;

    // Sort based on the sort_by parameter
    let result = match sort_by {
        AssetsSortBy::Symbol => {
            result
                .sort(["Symbol"], SortMultipleOptions::default())?
        }
        AssetsSortBy::EtfCount => {
            result
                .sort(
                    ["ETF_Count", "Symbol"],
                    SortMultipleOptions::default()
                        .with_order_descending_multi([true, false])
                )?
        }
    };

    Ok(result)
}

/// Generate summary statistics for assets aggregation
/// Returns a string summarizing how many assets appear in N ETFs
pub fn summarize_assets(df: &DataFrame) -> Result<String> {
    use std::collections::BTreeMap;

    let etf_count_col = df.column("ETF_Count")?.u32()?;

    // Count how many assets have each ETF_Count value
    let mut count_map: BTreeMap<u32, usize> = BTreeMap::new();
    for count in etf_count_col.into_iter().flatten() {
        *count_map.entry(count).or_insert(0) += 1;
    }

    let total_assets = df.height();
    let mut summary = format!("Total assets: {}\n\n", total_assets);
    summary.push_str("Asset distribution by ETF count:\n");

    // Sort by ETF count (descending) for better readability
    for (etf_count, asset_count) in count_map.iter().rev() {
        let plural = if *asset_count == 1 { "asset" } else { "assets" };
        let etf_plural = if *etf_count == 1 { "ETF" } else { "ETFs" };
        summary.push_str(&format!("  {} {} found in {} {}\n", asset_count, plural, etf_count, etf_plural));
    }

    Ok(summary)
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
