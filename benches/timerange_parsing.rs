use criterion::{Criterion, black_box, criterion_group, criterion_main};
use graylog_cli::domain::timerange::{CommandTimerange, TimerangeInput};

fn bench_relative_timerange(c: &mut Criterion) {
    let input = TimerangeInput {
        relative: Some("15m".to_string()),
        from: None,
        to: None,
    };
    c.bench_function("relative_timerange_15m", |b| {
        b.iter(|| CommandTimerange::from_input(black_box(input.clone())).unwrap())
    });
}

fn bench_absolute_timerange(c: &mut Criterion) {
    let input = TimerangeInput {
        relative: None,
        from: Some("2026-01-01T00:00:00Z".to_string()),
        to: Some("2026-01-02T00:00:00Z".to_string()),
    };
    c.bench_function("absolute_timerange", |b| {
        b.iter(|| CommandTimerange::from_input(black_box(input.clone())).unwrap())
    });
}

criterion_group!(benches, bench_relative_timerange, bench_absolute_timerange);
criterion_main!(benches);
