extern crate arrow_playground;

use arrow::array::{BooleanArray, Int32Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_playground::columnar::{
    DataSource as ColumnarDataSource, Filter as ColumnarFilter, Project as ColumnarProject,
};
use arrow_playground::exec::DataStream;
use arrow_playground::exec::ExecutionPlan;
use arrow_playground::traditional;
use arrow_playground::traditional::Filter as TraditionalFilter;
use arrow_playground::traditional::Project as TraditionalProject;
use arrow_playground::traditional::{DataSource as TraditionalDataSource, RowData};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

const COUNT: i32 = 1000000;

// benchmark of traditional execution
pub fn bench_traditional(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_vs_row");
    let source = TraditionalDataSource::new(traditional_source_data(COUNT));
    let filter = TraditionalFilter::new(Box::new(source));
    let project = TraditionalProject::new(Box::new(filter));
    group.bench_function("traditional", |bencher| {
        bencher.iter(|| {
            let stream = project.execute();
            black_box(consume_stream(stream));
        });
    });
}

// benchmark of arrow-based execution
pub fn bench_columnar(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_vs_row");
    let source = ColumnarDataSource::new(columnar_source_data(COUNT), Arc::new(vec![0, 1, 2]));
    let filter = ColumnarFilter::new(Box::new(source));
    let project = ColumnarProject::new(Box::new(filter));
    group.bench_function("columnar", |bencher| {
        bencher.iter(|| {
            let stream = project.execute();
            black_box(consume_stream(stream));
        });
    });
}

pub fn consume_stream(stream: Box<dyn DataStream>) {
    let mut stream = stream;
    while let Some(_value) = stream.deref_mut().poll_next() {}
}

fn traditional_source_data(count: i32) -> Arc<Vec<Arc<RowData>>> {
    let mut dataset = Vec::with_capacity(count as usize);
    for i in 0..count {
        let data = Arc::new(traditional::RowData {
            a: i,
            b: i,
            c: i % 2 == 0,
            d: format!("hello world {}", i),
        });
        dataset.push(data);
    }
    Arc::new(dataset)
}

fn columnar_source_data(count: i32) -> Arc<Vec<Arc<RecordBatch>>> {
    let mut dataset = Vec::new();
    (0..count)
        .collect::<Vec<i32>>()
        .chunks(8192)
        .for_each(|chunk| {
            let a = Arc::new(Int32Array::from(chunk.to_vec()));
            let b = Arc::new(Int32Array::from(chunk.to_vec()));
            let c = Arc::new(BooleanArray::from(
                chunk.iter().map(|i| i % 2 == 0).collect::<Vec<bool>>(),
            ));
            let d = Arc::new(StringViewArray::from(
                chunk
                    .iter()
                    .map(|i| format!("hello world {}", i))
                    .collect::<Vec<String>>(),
            ));
            let field_a = Field::new("a", DataType::Int32, false);
            let field_b = Field::new("b", DataType::Int32, false);
            let field_c = Field::new("c", DataType::Boolean, false);
            let field_d = Field::new("d", DataType::Utf8View, false);

            let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c, field_d]));
            let one_batch = RecordBatch::try_new(schema, vec![a, b, c, d]).unwrap();
            dataset.push(Arc::new(one_batch));
        });
    Arc::new(dataset)
}

criterion_group!(
    name = columnar_vs_row;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(3));
    targets = bench_traditional, bench_columnar);
criterion_main!(columnar_vs_row);
