use etf_analyzer::{cli, io, portfolio, report, Result};

fn main() -> Result<()> {
    let args = cli::parse_args();

    if args.verbose {
        println!("ETF Analyzer starting...");
        println!("Loading portfolio from directory: {}", args.data_dir);
    }

    // Load all ETF holdings from the specified directory
    let df = io::load_portfolio_from_directory(&args.data_dir)?;

    // Extract unique ETF names from the "etf" column
    let etf_names: Vec<String> = df
        .column("etf")
        .ok()
        .and_then(|col| col.str().ok())
        .map(|s| {
            s.into_iter()
                .flatten()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_else(|| vec!["Unknown".to_string()]);

    if args.verbose {
        println!("Found {} ETFs: {}", etf_names.len(), etf_names.join(", "));
        println!("{}", report::generate_dataframe_summary(&df)?);
    }

    // Create a Portfolio struct to manage state
    let mut portfolio = portfolio::Portfolio::new(etf_names);
    portfolio.load_data(df)?;

    if args.verbose {
        println!("{}", portfolio.summary());
    }

    // Generate output
    if let Some(output_path) = args.output {
        if args.verbose {
            println!("Saving results to: {}", output_path);
        }
        if let Some(data) = &portfolio.data {
            io::save_csv(data, &output_path)?;
        }
    }

    if args.verbose {
        println!("ETF Analyzer finished.");
    }

    Ok(())
}
