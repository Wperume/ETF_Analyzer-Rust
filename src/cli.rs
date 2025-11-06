use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Directory containing ETF holdings CSV files (pattern: {etf_name}-etf-holdings.csv)
    #[arg(short = 'd', long, default_value = "./data")]
    pub data_dir: String,

    /// Output file path
    #[arg(short, long)]
    pub output: Option<String>,

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
