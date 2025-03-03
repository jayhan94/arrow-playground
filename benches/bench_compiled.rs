mod common;

use crate::common::COUNT;
use arrow_playground::compiled::{CompiledProject, RowData};
use arrow_playground::exec::ExecutionPlan;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::time::Duration;

// benchmark of compiled traditional execution
pub fn bench_compiled(c: &mut Criterion) {
    let mut group = c.benchmark_group("compiled_traditional");
    let project = CompiledProject::new(source_data(COUNT));
    group.bench_function("consume_stream", |bencher| {
        bencher.iter(|| {
            let stream = project.execute();
            black_box(common::consume_stream(stream));
        });
    });
}

pub fn source_data(count: i32) -> Arc<Vec<Arc<RowData>>> {
    let mut dataset = Vec::with_capacity(count as usize);
    for i in 0..count {
        let data = Arc::new(RowData {
            a: i,
            b: i,
            c: i % 2 == 0,
            d: format!("hello world {}", i),
        });
        dataset.push(data);
    }
    Arc::new(dataset)
}

criterion_group!(
    name = compiled;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(3));
    targets = bench_compiled);
criterion_main!(compiled);
