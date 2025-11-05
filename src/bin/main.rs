use etf_analyzer::{cli, run, Result};

fn main() -> Result<()> {
    let _args = cli::parse_args();
    run()
}
