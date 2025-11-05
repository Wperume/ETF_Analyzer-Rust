use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Input file path
    #[arg(short, long)]
    pub input: Option<String>,

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
