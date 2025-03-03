mod common;

use crate::common::{consume_stream, COUNT};
use arrow_playground::exec::ExecutionPlan;
use arrow_playground::row::Filter as TraditionalFilter;
use arrow_playground::row::Project as TraditionalProject;
use arrow_playground::row::{DataSource as TraditionalDataSource, Row};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::time::Duration;

// benchmark of traditional execution
pub fn bench_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("row");
    let source = TraditionalDataSource::new(source_data(COUNT));
    let filter = TraditionalFilter::new(Box::new(source));
    let project = TraditionalProject::new(Box::new(filter));
    group.bench_function("consume_stream", |bencher| {
        bencher.iter(|| {
            let stream = project.execute();
            black_box(consume_stream(stream));
        });
    });
}

pub fn source_data(count: i32) -> Arc<Vec<Arc<Row>>> {
    let mut dataset = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut row = Row::empty(4);
        row.set(0, Some(Arc::new(Box::new(i))));
        row.set(1, Some(Arc::new(Box::new(i))));
        row.set(2, Some(Arc::new(Box::new(i % 2 == 0))));
        row.set(3, Some(Arc::new(Box::new(format!("hello world {}", i)))));
        dataset.push(Arc::new(row));
    }
    Arc::new(dataset)
}

criterion_group!(
    name = row;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(3));
    targets = bench_row);
criterion_main!(row);
