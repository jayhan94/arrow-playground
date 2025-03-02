mod common;

use crate::common::COUNT;
use arrow_playground::exec::ExecutionPlan;
use arrow_playground::traditional::CompiledProject;
use common::traditional_source_data;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;

// benchmark of compiled traditional execution
pub fn bench_compiled_traditional(c: &mut Criterion) {
    let mut group = c.benchmark_group("compiled_traditional");
    let project = CompiledProject::new(traditional_source_data(COUNT));
    group.bench_function("consume_stream", |bencher| {
        bencher.iter(|| {
            let stream = project.execute();
            black_box(common::consume_stream(stream));
        });
    });
}

criterion_group!(
    name = compiled_traditional;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(3));
    targets = bench_compiled_traditional);
criterion_main!(compiled_traditional);
