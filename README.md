# ETF Analyzer

A command-line tool for ETF analysis written in Rust.

## Building

```bash
cargo build --release
```

## Running

```bash
cargo run
```

## Testing

Run unit tests:
```bash
cargo test
```

Run integration tests:
```bash
cargo test --test integration_test
```

Run all tests with output:
```bash
cargo test -- --nocapture
```

## Usage

```bash
etf_analyzer --help
```

## Project Structure

```
.
├── Cargo.toml          # Project configuration and dependencies
├── src/
│   ├── lib.rs          # Library root with core functionality
│   ├── cli.rs          # Command-line argument parsing
│   ├── error.rs        # Error types and handling
│   └── bin/
│       └── main.rs     # Binary entry point
├── tests/              # Integration tests
├── benches/            # Benchmarks (optional)
└── examples/           # Example usage (optional)
```
