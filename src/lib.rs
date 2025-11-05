pub mod cli;
pub mod error;

pub use error::{Error, Result};

/// Main library functionality
pub fn run() -> Result<()> {
    println!("ETF Analyzer running...");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run() {
        assert!(run().is_ok());
    }
}
