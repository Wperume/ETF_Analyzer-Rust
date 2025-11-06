use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Directory containing ETF holdings CSV files (pattern: {etf_name}-etf-holdings.csv)
    #[arg(short = 'd', long)]
    pub data_dir: Option<String>,

    /// Import previously exported DataFrame (CSV or Parquet)
    #[arg(short = 'i', long)]
    pub import: Option<String>,

    /// Function/operation to perform (summary, list, assets, unique, overlap, compare, mapping, export)
    #[arg(short = 'f', long, default_value = "summary")]
    pub function: String,

    /// Output file path
    #[arg(short, long)]
    pub output: Option<String>,

    /// Force overwrite of existing output files without prompting
    #[arg(long)]
    pub force: bool,

    /// Verbose mode
    #[arg(short, long)]
    pub verbose: bool,
}

pub fn parse_args() -> Args {
    Args::parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Args::command().debug_assert();
    }
}
