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

    // Handle the summary function
    if args.function == "summary" {
        if args.verbose {
            println!("Generating ETF summary...");
        }

        let summary_df = analysis::get_etf_summary(&df)?;

        // Always print summary statistics to stdout
        let summary = analysis::summarize_etfs(&summary_df)?;
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
                println!("Saving ETF summary to: {}", output_path_with_ext);
            }
            let written = io::export_dataframe(&summary_df, &output_path_with_ext, args.force)?;
            if written {
                println!("ETF summary saved to: {}", output_path_with_ext);
            }
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

    // Handle the unique function
    if args.function == "unique" {
        if args.verbose {
            println!("Finding unique assets (appear in only one ETF)...");
        }

        let unique_df = analysis::get_unique_assets(&df)?;

        println!("Found {} unique assets (appear in only one ETF)", unique_df.height());

        // If output file is specified, save with default .csv extension if no extension provided
        if let Some(output_path) = &args.output {
            // Add .csv extension if no extension is present
            let output_path_with_ext = if std::path::Path::new(output_path).extension().is_none() {
                format!("{}.csv", output_path)
            } else {
                output_path.to_string()
            };

            if args.verbose {
                println!("Saving unique assets to: {}", output_path_with_ext);
            }
            let written = io::export_dataframe(&unique_df, &output_path_with_ext, args.force)?;
            if written {
                println!("Unique assets saved to: {}", output_path_with_ext);
            }
        }

        return Ok(());
    }

    // Handle the overlap function
    if args.function == "overlap" {
        let sort_by = analysis::AssetsSortBy::from_str(&args.sort_by);

        if args.verbose {
            println!("Finding overlapping assets (appear in multiple ETFs)...");
        }

        let overlap_df = analysis::get_overlap_assets(&df, sort_by)?;

        println!("Found {} overlapping assets (appear in multiple ETFs)", overlap_df.height());

        // If output file is specified, save with default .csv extension if no extension provided
        if let Some(output_path) = &args.output {
            // Add .csv extension if no extension is present
            let output_path_with_ext = if std::path::Path::new(output_path).extension().is_none() {
                format!("{}.csv", output_path)
            } else {
                output_path.to_string()
            };

            if args.verbose {
                println!("Saving overlapping assets to: {}", output_path_with_ext);
            }
            let written = io::export_dataframe(&overlap_df, &output_path_with_ext, args.force)?;
            if written {
                println!("Overlapping assets saved to: {}", output_path_with_ext);
            }
        }

        return Ok(());
    }

    // Handle the mapping function
    if args.function == "mapping" {
        let sort_by = analysis::AssetsSortBy::from_str(&args.sort_by);

        if args.verbose {
            println!("Creating asset-to-ETF mapping...");
        }

        let mapping_df = analysis::get_asset_mapping(&df, sort_by)?;

        // Always print summary to stdout
        let summary = analysis::summarize_assets(&mapping_df)?;
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
                println!("Saving asset mapping to: {}", output_path_with_ext);
            }
            let written = io::export_dataframe(&mapping_df, &output_path_with_ext, args.force)?;
            if written {
                println!("Asset mapping saved to: {}", output_path_with_ext);
            }
        }

        return Ok(());
    }

    // Handle the list function
    if args.function == "list" {
        if args.verbose {
            println!("Getting list of ETFs...");
        }

        let etf_list = analysis::get_etf_list(&df)?;

        // Print to stdout
        println!("Found {} ETFs:", etf_list.len());
        for etf in &etf_list {
            println!("  {}", etf);
        }

        // If output file is specified, save with default .txt extension if no extension provided
        if let Some(output_path) = &args.output {
            // Add .txt extension if no extension is present
            let output_path_with_ext = if std::path::Path::new(output_path).extension().is_none() {
                format!("{}.txt", output_path)
            } else {
                output_path.to_string()
            };

            if args.verbose {
                println!("Saving ETF list to: {}", output_path_with_ext);
            }

            // Write ETF list to text file (one per line)
            let content = etf_list.join("\n") + "\n";
            std::fs::write(&output_path_with_ext, content)?;
            println!("ETF list saved to: {}", output_path_with_ext);
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
