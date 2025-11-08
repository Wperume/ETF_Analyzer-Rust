use criterion::{black_box, criterion_group, criterion_main, Criterion};
use etf_analyzer::portfolio::calculate_correlation;
use polars::prelude::*;

fn create_test_dataframe(n_cols: usize, n_rows: usize) -> DataFrame {
    let data: Vec<Column> = (0..n_cols)
        .map(|i| {
            let values: Vec<f64> = (0..n_rows)
                .map(|j| (j as f64) * (i as f64 + 1.0) + (j as f64).sin())
                .collect();
            Column::new(format!("col_{}", i).into(), &values)
        })
        .collect();
    DataFrame::new(data).unwrap()
}

fn benchmark_correlation_small(c: &mut Criterion) {
    let df = create_test_dataframe(5, 1000);
    let columns: Vec<&str> = (0..5)
        .map(|i| match i {
            0 => "col_0",
            1 => "col_1",
            2 => "col_2",
            3 => "col_3",
            4 => "col_4",
            _ => unreachable!(),
        })
        .collect();

    c.bench_function("correlation_5_etfs_1000_rows", |b| {
        b.iter(|| calculate_correlation(black_box(&df), black_box(&columns)))
    });
}

fn benchmark_correlation_medium(c: &mut Criterion) {
    let df = create_test_dataframe(10, 1000);
    let columns: Vec<&str> = (0..10)
        .map(|i| match i {
            0 => "col_0",
            1 => "col_1",
            2 => "col_2",
            3 => "col_3",
            4 => "col_4",
            5 => "col_5",
            6 => "col_6",
            7 => "col_7",
            8 => "col_8",
            9 => "col_9",
            _ => unreachable!(),
        })
        .collect();

    c.bench_function("correlation_10_etfs_1000_rows", |b| {
        b.iter(|| calculate_correlation(black_box(&df), black_box(&columns)))
    });
}

fn benchmark_correlation_large(c: &mut Criterion) {
    let df = create_test_dataframe(20, 1000);
    let columns: Vec<&str> = (0..20)
        .map(|i| match i {
            0 => "col_0", 1 => "col_1", 2 => "col_2", 3 => "col_3", 4 => "col_4",
            5 => "col_5", 6 => "col_6", 7 => "col_7", 8 => "col_8", 9 => "col_9",
            10 => "col_10", 11 => "col_11", 12 => "col_12", 13 => "col_13", 14 => "col_14",
            15 => "col_15", 16 => "col_16", 17 => "col_17", 18 => "col_18", 19 => "col_19",
            _ => unreachable!(),
        })
        .collect();

    c.bench_function("correlation_20_etfs_1000_rows", |b| {
        b.iter(|| calculate_correlation(black_box(&df), black_box(&columns)))
    });
}

criterion_group!(benches, benchmark_correlation_small, benchmark_correlation_medium, benchmark_correlation_large);
criterion_main!(benches);
