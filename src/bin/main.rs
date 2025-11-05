use etf_analyzer::{cli, io, portfolio, report, Result};

fn main() -> Result<()> {
    let args = cli::parse_args();

    if args.verbose {
        println!("ETF Analyzer starting...");
    }

    // Determine which mode to use: holdings or regular input
    let has_holdings = !args.holdings.is_empty();
    let holdings_paths = args.holdings.clone();

    let df = if has_holdings {
        // Load ETF holdings files
        if args.verbose {
            println!("Loading {} ETF holdings files...", holdings_paths.len());
            for file in &holdings_paths {
                println!("  - {}", file);
            }
        }

        io::load_multiple_holdings(holdings_paths)?
    } else if let Some(input_path) = args.input {
        // Load single CSV file
        if args.verbose {
            println!("Loading data from: {}", input_path);
        }

        io::load_csv(&input_path)?
    } else {
        println!("No input file specified. Use --help for usage information.");
        println!("\nExamples:");
        println!("  Single file:    cargo run -- --input data.csv");
        println!("  Holdings files: cargo run -- --holdings spy-etf-holdings.csv,voo-etf-holdings.csv");
        return Ok(());
    };

    if args.verbose {
        println!("{}", report::generate_dataframe_summary(&df)?);
    }

    // Perform analysis
    // Note: Adjust column names based on your data
    // For holdings files, you can filter by ETF:
    // let spy_holdings = df.filter(&df.column("etf")?.equal("SPY")?)?;

    // Create a Portfolio struct to manage state
    let etf_names: Vec<String> = if !args.holdings.is_empty() {
        // Extract unique ETF names from the holdings
        args.holdings
            .iter()
            .filter_map(|path| {
                path.split('-').next().map(|s| s.to_uppercase())
            })
            .collect()
    } else {
        vec!["Sample ETF".to_string()]
    };

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
