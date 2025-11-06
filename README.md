# ETF Analyzer

A command-line tool for analyzing Exchange-Traded Fund (ETF) portfolios using Rust and Polars.

## Features

- Load ETF holdings from CSV files
- Import/export portfolio data in CSV or Parquet formats
- Analyze portfolio composition and overlap
- Compare multiple ETFs
- High-performance data processing with Polars

## Installation

### Prerequisites

- Rust 1.70 or later
- Cargo (comes with Rust)

If you don't have Rust installed, you can install it from:
- **Official Rust Website**: [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)
- **rustup** (recommended): The official Rust installer and version management tool
  ```bash
  # On macOS, Linux, or WSL
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

  # On Windows, download and run rustup-init.exe from:
  # https://rustup.rs/
  ```
- After installation, verify with: `rustc --version` and `cargo --version`

### Build from Source

```bash
git clone <repository-url>
cd ETF_Analyzer
cargo build --release
```

The compiled binary will be available at `./target/release/etf_analyzer`.

## Usage

### Basic Syntax

```bash
etf_analyzer [OPTIONS]
```

### Command-Line Options

- `-d DIR` or `--data-dir DIR`: Directory containing ETF CSV files
- `-i FILE` or `--import FILE`: Import previously exported DataFrame
- `-f FUNCTION` or `--function FUNCTION`: Operation to perform
  - `summary` (default): Display ETF portfolio summary (CSV export includes asset lists)
  - `list`: List all ETF symbols
  - `assets`: Show all assets with ETF associations (sorted by symbol)
  - `unique`: Show assets that appear in only one ETF (ETF_Count = 1)
  - `overlap`: Show assets that appear in multiple ETFs (ETF_Count > 1, sorted by count)
  - `compare`: Compare specific ETFs side-by-side (requires `--etfs`)
  - `mapping`: Show asset-to-ETF mapping
  - `export`: Export DataFrame to file (requires `-o`)
- `-o FILE` or `--output FILE`: Output file (if not specified, print to stdout)
- `--symbol-col COLUMN`: Column name for asset symbol (default: Symbol)
- `--name-col COLUMN`: Column name for asset name (default: Name)
- `--weight-col COLUMN`: Column name for weight/percentage (default: % Weight)
- `--shares-col COLUMN`: Column name for shares (default: Shares)
- `--etfs ETF1,ETF2,...`: Comma-separated list of ETF symbols to compare (for compare function)
- `--sort-assets {weight,alpha}`: Sort assets by weight (default) or alphabetically (for summary and compare functions)
- `--sort-etfs {weight,alpha}`: Sort ETFs by asset weight (default) or alphabetically (for overlap function)
- `--force`: Force overwrite of existing output files without prompting
- `-v` or `--verbose`: Enable verbose output

### Notes

- Either `-d` or `-i` must be specified
- The `-o` option is required for `-f export`
- Column overrides (`--symbol-col`, etc.) are useful when your CSV files use different column names
- Only specify the column overrides you need; others will use defaults
- **File Overwrite Protection**: If the output file exists, you'll be prompted to confirm overwrite. Use `--force` to skip the prompt for automated scripts.

## CSV File Format

### ETF Holdings Files

ETF holdings files should follow the naming pattern: `{etf_name}-etf-holdings.csv`

Expected columns:
- `No.`: Index/row number
- `Symbol`: Asset ticker symbol (can be empty/null/"n/a" - will be auto-generated)
- `Name`: Asset name
- `Asset Class`: Type of asset
- `% Weight`: Percentage weight in the ETF
- `Shares`: Number of shares held

Example: `spy-etf-holdings.csv`, `voo-etf-holdings.csv`

The tool will:
- Extract the ETF name from the filename
- Synthesize Symbol values for empty/null/n/a entries using format: `{ETF}-{No.}`
- Rename `% Weight` to `Weight`
- Add an `ETF` column with the ETF name
- Reorder columns so `ETF` is first

## Examples

### Load Portfolio from Directory

```bash
# Load all ETF CSV files from the data directory
etf_analyzer -d ./data -v
```

### Export Portfolio Data

```bash
# Export to Parquet (default format when no extension specified)
etf_analyzer -d ./data -f export -o portfolio.parquet

# Export to CSV
etf_analyzer -d ./data -f export -o portfolio.csv

# Force overwrite without prompting
etf_analyzer -d ./data -f export -o portfolio.parquet --force
```

### Import Previously Exported Data

```bash
# Import from Parquet file
etf_analyzer -i portfolio.parquet -v

# Import and re-export to different format
etf_analyzer -i portfolio.parquet -f export -o portfolio.csv
```

### Analyze Portfolio (Coming Soon)

```bash
# Show portfolio summary
etf_analyzer -d ./data -f summary

# List all ETF symbols
etf_analyzer -d ./data -f list

# Show all assets
etf_analyzer -d ./data -f assets

# Show unique assets (appear in only one ETF)
etf_analyzer -d ./data -f unique

# Show overlapping assets (appear in multiple ETFs)
etf_analyzer -d ./data -f overlap

# Compare specific ETFs
etf_analyzer -d ./data -f compare --etfs SPY,VOO,QQQ
```

## File Formats

### Parquet

Parquet is a columnar storage format that offers:
- Better compression (smaller file sizes)
- Faster read/write performance for large datasets
- Type preservation (no string conversions)
- Default format when no file extension is provided

### CSV

CSV (Comma-Separated Values) offers:
- Human-readable format
- Universal compatibility
- Easy to inspect and edit manually

The tool automatically detects the format based on file extension:
- `.parquet` or `.pq` → Parquet format
- `.csv` or any other extension → CSV format
- No extension → Parquet (default)

## Project Structure

```
ETF_Analyzer/
├── src/
│   ├── bin/
│   │   └── main.rs          # CLI entry point
│   ├── lib.rs               # Library root
│   ├── cli.rs               # Command-line argument parsing
│   ├── error.rs             # Error types
│   ├── io.rs                # File I/O operations
│   ├── analysis.rs          # Portfolio analysis functions
│   ├── portfolio.rs         # Portfolio data structure
│   └── report.rs            # Report generation
├── tests/
│   └── integration_test.rs  # Integration tests
├── example-data/            # Example ETF holdings CSV files
├── data/                    # User data directory (gitignored)
├── analysis/                # Analysis output directory (gitignored)
├── Cargo.toml               # Project configuration
└── README.md                # This file
```

## Development

### Running Tests

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_name
```

### Building for Release

```bash
cargo build --release
```

The optimized binary will be at `./target/release/etf_analyzer`.

### Debug vs Release Builds

```bash
# Debug build (faster compilation, slower execution)
cargo build
./target/debug/etf_analyzer -d ./data

# Release build (slower compilation, faster execution)
cargo build --release
./target/release/etf_analyzer -d ./data
```

## Dependencies

- **polars**: High-performance DataFrame library (features: lazy, csv, parquet)
- **clap**: Command-line argument parser
- **rayon**: Data parallelism library
- **csv**: CSV reading/writing
- **anyhow**: Error handling

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
