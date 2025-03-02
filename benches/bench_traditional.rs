mod common;

use crate::common::{consume_stream, COUNT};
use arrow_playground::exec::ExecutionPlan;
use arrow_playground::traditional::DataSource as TraditionalDataSource;
use arrow_playground::traditional::Filter as TraditionalFilter;
use arrow_playground::traditional::Project as TraditionalProject;
use common::traditional_source_data;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;

// benchmark of traditional execution
pub fn bench_traditional(c: &mut Criterion) {
    let mut group = c.benchmark_group("traditional");
    let source = TraditionalDataSource::new(traditional_source_data(COUNT));
    let filter = TraditionalFilter::new(Box::new(source));
    let project = TraditionalProject::new(Box::new(filter));
    group.bench_function("consume_stream", |bencher| {
        bencher.iter(|| {
            let stream = project.execute();
            black_box(consume_stream(stream));
        });
    });
}

criterion_group!(
    name = traditional;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(3));
    targets = bench_traditional);
criterion_main!(traditional);
