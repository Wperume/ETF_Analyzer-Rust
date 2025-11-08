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
  - `summary` (default): Display ETF portfolio summary (export includes asset lists)
  - `export`: Export DataFrame to file (requires `-o`)
  - `assets`: Show all assets with ETF associations and aggregated ETF counts
  - `unique`: Show assets that appear in only one ETF
  - `overlap`: Show assets that appear in multiple ETFs (with ETF_Count column)
  - `mapping`: Show asset-to-ETF mapping with summary statistics
  - `compare`: Compare specific ETFs side-by-side showing asset weights across ETFs
  - `list`: List all ETF symbols in the DataFrame
- `-o FILE` or `--output FILE`: Output file (if not specified, print to stdout)
- `--etfs ETF1,ETF2,...`: Comma-separated list of ETF symbols to include in analysis (e.g., VTI,VOO,SPY)
- `--sort-by {symbol,count}`: Sort order for assets, overlap, and mapping functions - 'symbol' (alphabetical, default) or 'count' (by ETF count descending)
- `--symbol-col COLUMN`: Column name for asset symbol in input CSV (default: "Symbol")
- `--name-col COLUMN`: Column name for asset name in input CSV (default: "Name")
- `--weight-col COLUMN`: Column name for weight/percentage in input CSV (default: "% Weight")
- `--shares-col COLUMN`: Column name for shares in input CSV (default: "Shares")
- `--number-col COLUMN`: Column name for row number in input CSV (default: "No.")
- `--force`: Force overwrite of existing output files without prompting
- `-v` or `--verbose`: Enable verbose output

### Notes

- Either `-d` or `-i` must be specified
- The `-o` option is required for `-f export` and `-f compare`
- **Column Name Overrides**: Use `--symbol-col`, `--name-col`, `--weight-col`, `--shares-col`, and `--number-col` to specify custom column names when your input CSV files use different column names than the defaults
  - Only specify the column overrides you need; others will use defaults
  - Column names are case-sensitive
  - The tool works correctly regardless of column order in the CSV files (columns are accessed by name, not position)
- **File Overwrite Protection**: If the output file exists, you'll be prompted to confirm overwrite. Use `--force` to skip the prompt for automated scripts.

## File Formats

### Summary Output Format

When using `-f summary`, the output shows ETF-level statistics:

**Columns:**
- `ETF`: ETF symbol
- `Asset_Count`: Number of assets in the ETF
- `Assets`: Comma-separated list of all asset symbols in the ETF

**Summary Output (stdout):**
- Total number of ETFs
- Largest ETF asset count
- Smallest ETF asset count

**Example:**
```
Total ETFs: 6
Largest ETF contains 503 assets
Smallest ETF contains 25 assets
```

### ETF Holdings Files (Input)

ETF holdings files should follow the naming pattern: `{etf_name}-etf-holdings.csv`

**Expected columns:**
- `No.`: Index/row number
- `Symbol`: Asset ticker symbol (can be empty/null/"n/a" - will be auto-generated)
- `Name`: Asset name
- `Asset Class`: Type of asset
- `% Weight`: Percentage weight in the ETF
- `Shares`: Number of shares held

**Example:** `spy-etf-holdings.csv`, `voo-etf-holdings.csv`

**The tool will:**
- Extract the ETF name from the filename
- Synthesize Symbol values for empty/null/n/a entries using format: `{ETF}-{No.}`
- Rename `% Weight` to `Weight`
- Add an `ETF` column with the ETF name
- Reorder columns so `ETF` is first

### Assets Output Format

When using `-f assets`, the output contains:

**Columns:**
- `Symbol`: Asset ticker symbol
- `Name`: Asset name
- `ETF_Count`: Number of ETFs containing this asset
- `ETFs`: Comma-separated list of ETF symbols containing this asset

**Summary Output (stdout):**
- Total number of unique assets
- Distribution showing how many assets appear in N ETFs

**Example:**
```
Total assets: 659

Asset distribution by ETF count:
  41 assets found in 3 ETFs
  191 assets found in 2 ETFs
  427 assets found in 1 ETF
```

### Unique Assets Output Format

When using `-f unique`, the output contains assets that appear in only one ETF:

**Columns:**
- `Symbol`: Asset ticker symbol
- `Name`: Asset name
- `Weight`: Asset weight/percentage
- `ETF`: ETF symbol containing this asset

**Summary Output (stdout):**
- Count of unique assets (assets appearing in only one ETF)

**Example:**
```
Found 217 unique assets (appear in only one ETF)
```

### Overlap Assets Output Format

When using `-f overlap`, the output contains assets that appear in multiple ETFs:

**Columns:**
- `Symbol`: Asset ticker symbol
- `Name`: Asset name
- `ETF_Count`: Number of ETFs containing this asset (always > 1)
- `Weight`: Asset weight/percentage in this specific ETF
- `ETF`: ETF symbol

**Note:** Each asset will have multiple rows (one per ETF it appears in).

**Sorting:**
- `--sort-by symbol` (default): Alphabetical by asset symbol
- `--sort-by count`: Descending by ETF_Count, then alphabetical by symbol

**Summary Output (stdout):**
- Count of overlapping assets (assets appearing in multiple ETFs)

**Example:**
```
Found 53 overlapping assets (appear in multiple ETFs)
```

### Asset Mapping Output Format

When using `-f mapping`, the output shows the complete asset-to-ETF mapping:

**Columns:**
- `Symbol`: Asset ticker symbol
- `Name`: Asset name
- `ETF_Count`: Number of ETFs containing this asset
- `ETFs`: Comma-separated list of ETF symbols containing this asset

**Sorting:**
- `--sort-by symbol` (default): Alphabetical by asset symbol
- `--sort-by count`: Descending by ETF_Count, then alphabetical by symbol

**Summary Output (stdout):**
- Total number of unique assets
- Distribution showing how many assets appear in N ETFs

**Example:**
```
Total assets: 243

Asset distribution by ETF count:
  1 asset found in 3 ETFs
  25 assets found in 2 ETFs
  217 assets found in 1 ETF
```

### List ETFs Output Format

When using `-f list`, the output shows all unique ETF symbols in the DataFrame:

**Output (stdout):**
- Count of ETFs found
- List of ETF symbols (sorted alphabetically), one per line

**Output File (optional):**
- Text file (.txt) with one ETF symbol per line
- Default extension: `.txt`

**Example:**
```
Found 3 ETFs:
  IVW
  IWF
  VTV
```

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

### ETF Summary

```bash
# Show summary of all ETFs
etf_analyzer -d ./data -f summary

# Save ETF summary to CSV
etf_analyzer -d ./data -f summary -o etf_summary.csv

# Save without extension (default .csv will be added)
etf_analyzer -d ./data -f summary -o etf_summary

# Summary for specific ETFs only
etf_analyzer -d ./data --etfs VTV,IVW,IWF -f summary
```

### Analyze Assets

```bash
# Show all assets with ETF associations (sorted alphabetically)
etf_analyzer -d ./data -f assets

# Sort by ETF count (assets appearing in most ETFs first)
etf_analyzer -d ./data -f assets --sort-by count

# Filter to specific ETFs and analyze their assets
etf_analyzer -d ./data -f assets --etfs VTV,IVW,IWF

# Save results to CSV (default format when no extension specified)
etf_analyzer -d ./data -f assets --sort-by count -o assets

# Save results to CSV (explicit .csv extension)
etf_analyzer -d ./data -f assets --sort-by count -o assets.csv

# Save results to Parquet (explicitly specify .parquet extension)
etf_analyzer -d ./data -f assets --sort-by count -o assets.parquet

# Complete example with filtering and verbose output
etf_analyzer -d ./data -f assets --etfs VTV,IVW --sort-by count -o filtered-assets.csv --verbose
```

### Analyze Unique Assets

```bash
# Show assets that appear in only one ETF
etf_analyzer -d ./data -f unique

# Save to CSV (default extension automatically added)
etf_analyzer -d ./data -f unique -o unique_assets

# Save to CSV (explicit .csv extension)
etf_analyzer -d ./data -f unique -o unique_assets.csv

# Save to Parquet (explicitly specify .parquet extension)
etf_analyzer -d ./data -f unique -o unique_assets.parquet

# With specific ETF filter
etf_analyzer -d ./data --etfs VTV,IVW,IWF -f unique -o unique.csv
```

### Analyze Overlapping Assets

```bash
# Show assets that appear in multiple ETFs (sorted by symbol)
etf_analyzer -d ./data -f overlap

# Sort by ETF count (assets in most ETFs first)
etf_analyzer -d ./data -f overlap --sort-by count

# Save to CSV (default format when no extension specified)
etf_analyzer -d ./data -f overlap --sort-by count -o overlaps

# Save to CSV (explicit .csv extension)
etf_analyzer -d ./data -f overlap --sort-by count -o overlaps.csv

# Save to Parquet (explicitly specify .parquet extension)
etf_analyzer -d ./data -f overlap --sort-by count -o overlaps.parquet

# Filter to specific ETFs and find their overlaps
etf_analyzer -d ./data --etfs VTV,IVW,IWF -f overlap -o overlap_value_etfs
```

### Asset-to-ETF Mapping

```bash
# Show complete asset-to-ETF mapping with summary
etf_analyzer -d ./data -f mapping

# Sort by ETF count (assets in most ETFs first)
etf_analyzer -d ./data -f mapping --sort-by count

# Save to CSV (default format when no extension specified)
etf_analyzer -d ./data -f mapping -o asset_mapping

# Save to CSV (explicit .csv extension)
etf_analyzer -d ./data -f mapping -o asset_mapping.csv

# Save to Parquet (explicitly specify .parquet extension)
etf_analyzer -d ./data -f mapping -o asset_mapping.parquet

# Map assets for specific ETFs only
etf_analyzer -d ./data --etfs VTV,IVW,IWF -f mapping --sort-by count -o value_etf_mapping
```

### Compare ETFs

```bash
# Compare specific ETFs side-by-side (requires --etfs and --output)
etf_analyzer -d ./data --etfs IVE,IVW,IWF -f compare -o etf_comparison.csv

# Compare without extension (default .csv will be added)
etf_analyzer -d ./data --etfs IVE,IVW,IWF -f compare -o etf_comparison

# Compare two ETFs to see differences
etf_analyzer -d ./data --etfs VTV,VBR -f compare -o value_comparison.csv

# Compare multiple ETFs with verbose output
etf_analyzer -d ./data --etfs IVE,IVW,IWF,IWS -f compare -o four_etf_compare.csv --verbose
```

**Compare Output Format:**
- `Symbol`: Asset symbol
- One column per ETF (column name = ETF symbol)
- Each ETF column shows the weight (percentage) of that asset in the ETF
- "N/A" appears when an asset is not in a particular ETF

**Example output:**
```
Symbol,IVE,IVW,IWF
AAPL,8.16%,5.66%,11.28%
MSFT,7.23%,N/A,N/A
GOOGL,N/A,4.12%,3.89%
```

### List ETFs

```bash
# List all ETFs in the DataFrame
etf_analyzer -d ./data -f list

# Save ETF list to text file
etf_analyzer -d ./data -f list -o etf_list.txt

# Save without extension (default .txt will be added)
etf_analyzer -d ./data -f list -o etf_list

# List ETFs after filtering
etf_analyzer -d ./data --etfs VTV,IVW,IWF -f list
```

### Filter by ETF

The `--etfs` option works with all functions to filter the analysis to specific ETFs:

```bash
# Export only specific ETFs
etf_analyzer -d ./data --etfs VTI,VOO,SPY -f export -o filtered.parquet

# Analyze assets in specific ETFs
etf_analyzer -d ./data --etfs CORN,GLDM -f assets

# Find overlaps between specific ETFs
etf_analyzer -d ./data --etfs IVW,IWF,VTV -f overlap --sort-by count

# Map assets for specific ETFs
etf_analyzer -d ./data --etfs VTV,VBR -f mapping
```

### Using Custom Column Names

If your input CSV files use different column names than the defaults, you can override them:

```bash
# If your CSV uses "Ticker" instead of "Symbol" and "Weighting" instead of "% Weight"
etf_analyzer -d ./data --symbol-col Ticker --weight-col Weighting -f assets

# Multiple column overrides
etf_analyzer -d ./data \
  --symbol-col Ticker \
  --name-col CompanyName \
  --weight-col Weighting \
  --shares-col Holdings \
  -f summary -o summary.csv

# With verbose output to see the configuration being used
etf_analyzer -d ./data --symbol-col Ticker --weight-col Weighting -f list -v
```

**Notes:**
- Column names are case-sensitive
- Only override the columns that differ from defaults
- The tool works correctly regardless of column order in your CSV files
- All functions will use the specified column configuration

### Export File Formats

#### Parquet

Parquet is a columnar storage format that offers:
- Better compression (smaller file sizes)
- Faster read/write performance for large datasets
- Type preservation (no string conversions)
- Default format when no file extension is provided

#### CSV

CSV (Comma-Separated Values) offers:
- Human-readable format
- Universal compatibility
- Easy to inspect and edit manually

The tool automatically detects the format based on file extension:
- `.parquet` or `.pq` → Parquet format
- `.csv` → CSV format
- No extension:
  - **Export function**: Defaults to Parquet format
  - **Other functions** (summary, assets, unique, overlap, mapping): Default to CSV format

**Examples:**
```bash
# Export function defaults to Parquet
etf_analyzer -d ./data -f export -o portfolio         # Creates portfolio (Parquet)
etf_analyzer -d ./data -f export -o portfolio.parquet # Creates portfolio.parquet
etf_analyzer -d ./data -f export -o portfolio.csv     # Creates portfolio.csv

# Other functions default to CSV
etf_analyzer -d ./data -f summary -o summary          # Creates summary.csv
etf_analyzer -d ./data -f summary -o summary.csv      # Creates summary.csv
etf_analyzer -d ./data -f summary -o summary.parquet  # Creates summary.parquet
```

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
