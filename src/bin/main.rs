use etf_analyzer::{analysis, cli, io, portfolio, report, Result};

fn main() -> Result<()> {
    let args = cli::parse_args();

    if args.verbose {
        println!("ETF Analyzer starting...");
    }

    // Validate that either -d or -i is provided
    if args.data_dir.is_none() && args.import.is_none() {
        return Err(etf_analyzer::Error::Other(
            "Either --data-dir (-d) or --import (-i) must be specified".to_string()
        ));
    }

    // Load DataFrame from either import file or data directory
    let mut df = if let Some(import_path) = &args.import {
        if args.verbose {
            println!("Importing DataFrame from: {}", import_path);
        }
        io::import_dataframe(import_path)?
    } else if let Some(data_dir) = &args.data_dir {
        if args.verbose {
            println!("Loading portfolio from directory: {}", data_dir);
        }
        io::load_portfolio_from_directory(data_dir)?
    } else {
        unreachable!("Either data_dir or import must be Some");
    };

    // Apply ETF filter if specified
    if let Some(etf_list) = &args.etfs {
        if args.verbose {
            println!("Filtering to ETFs: {}", etf_list.join(", "));
        }
        df = analysis::filter_etfs(&df, etf_list)?;

        if args.verbose {
            println!("Filtered DataFrame contains {} rows", df.height());
        }

        if df.height() == 0 {
            return Err(etf_analyzer::Error::Other(
                "No data found for the specified ETFs. Check that ETF symbols are correct.".to_string()
            ));
        }
    }

    // Handle the export function
    if args.function == "export" {
        if let Some(output_path) = &args.output {
            if args.verbose {
                println!("Exporting DataFrame to: {}", output_path);
            }
            let written = io::export_dataframe(&df, output_path, args.force)?;
            if written {
                println!("Successfully exported to: {}", output_path);
            }
        } else {
            return Err(etf_analyzer::Error::Other(
                "Export function requires --output (-o) to be specified".to_string()
            ));
        }
        return Ok(());
    }

    // Handle the assets function
    if args.function == "assets" {
        let sort_by = analysis::AssetsSortBy::from_str(&args.sort_by);

        if args.verbose {
            println!("Aggregating assets by symbol...");
        }

        let assets_df = analysis::aggregate_assets(&df, sort_by)?;

        // Always print summary to stdout
        let summary = analysis::summarize_assets(&assets_df)?;
        println!("{}", summary);

        // If output file is specified, save with default .csv extension if no extension provided
        if let Some(output_path) = &args.output {
            // Add .csv extension if no extension is present
            let output_path_with_ext = if std::path::Path::new(output_path).extension().is_none() {
                format!("{}.csv", output_path)
            } else {
                output_path.to_string()
            };

            if args.verbose {
                println!("Saving assets to: {}", output_path_with_ext);
            }
            let written = io::export_dataframe(&assets_df, &output_path_with_ext, args.force)?;
            if written {
                println!("Assets saved to: {}", output_path_with_ext);
            }
        }

        return Ok(());
    }

    // Extract unique ETF names from the "ETF" column
    let etf_names: Vec<String> = df
        .column("ETF")
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
    if let Some(output_path) = &args.output {
        if args.verbose {
            println!("Saving results to: {}", output_path);
        }
        if let Some(data) = &portfolio.data {
            io::save_csv(data, output_path)?;
        }
    }

    if args.verbose {
        println!("ETF Analyzer finished.");
    }

    Ok(())
}
