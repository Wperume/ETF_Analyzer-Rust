use etf_analyzer::{cli, io, portfolio, report, Result};

fn main() -> Result<()> {
    let args = cli::parse_args();

    if args.verbose {
        println!("ETF Analyzer starting...");
    }

    // Example workflow demonstrating the hybrid architecture:
    // 1. Load data using free functions from io module
    if let Some(input_path) = args.input {
        if args.verbose {
            println!("Loading data from: {}", input_path);
        }

        let df = io::load_csv(&input_path)?;

        if args.verbose {
            println!("{}", report::generate_dataframe_summary(&df)?);
        }

        // 2. Perform analysis using free functions
        // Note: This is a simple example - adjust column names as needed
        // let returns_df = analysis::calculate_returns(&df, "Close")?;

        // 3. Create a Portfolio struct to manage state
        let mut portfolio = portfolio::Portfolio::new(vec![
            "Sample ETF".to_string(),
        ]);
        portfolio.load_data(df)?;

        if args.verbose {
            println!("{}", portfolio.summary());
        }

        // 4. Generate output using report module
        if let Some(output_path) = args.output {
            if args.verbose {
                println!("Saving results to: {}", output_path);
            }
            // Save processed data
            if let Some(data) = &portfolio.data {
                io::save_csv(data, &output_path)?;
            }
        }
    } else {
        println!("No input file specified. Use --help for usage information.");
    }

    if args.verbose {
        println!("ETF Analyzer finished.");
    }

    Ok(())
}
