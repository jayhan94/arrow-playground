use arrow::array::{AsArray, BooleanArray, Int32Array, RecordBatch, StringArray};

use crate::exec::{DataStream, ExecutionPlan};
use crate::DATA_COUNT;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_arith::numeric::add;
use arrow_select::filter::filter_record_batch;
use std::any::Any;
use std::sync::Arc;

pub(crate) struct DataSource;

pub(crate) struct DataSourceStream {
    dataset: Vec<Arc<RecordBatch>>,
    i: usize,
}

impl DataSource {
    pub fn new() -> Self {
        DataSource {}
    }
}

impl ExecutionPlan for DataSource {
    fn execute(&self) -> Box<dyn DataStream> {
        let mut dataset = Vec::new();
        (0..DATA_COUNT)
            .collect::<Vec<i32>>()
            .chunks(8192)
            .for_each(|chunk| {
                let a = Arc::new(Int32Array::from(chunk.to_vec()));
                let b = Arc::new(Int32Array::from(chunk.to_vec()));
                let c = Arc::new(BooleanArray::from(
                    chunk.iter().map(|i| i % 2 == 0).collect::<Vec<bool>>(),
                ));
                let d = Arc::new(StringArray::from(
                    chunk
                        .iter()
                        .map(|i| format!("hello world {}", i))
                        .collect::<Vec<String>>(),
                ));
                let field_a = Field::new("a", DataType::Int32, false);
                let field_b = Field::new("b", DataType::Int32, false);
                let field_c = Field::new("c", DataType::Boolean, false);
                let field_d = Field::new("d", DataType::Utf8, false);

                let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c, field_d]));
                let one_batch = RecordBatch::try_new(schema, vec![a, b, c, d]).unwrap();
                dataset.push(Arc::new(one_batch));
            });
        Box::new(DataSourceStream { dataset, i: 0 })
    }
}

impl DataStream for DataSourceStream {
    fn poll_next(&mut self) -> Option<Arc<dyn Any + Send + Sync>> {
        if self.i >= self.dataset.len() {
            return None;
        }
        let result = self.dataset[self.i].clone();
        self.i += 1;
        Some(result)
    }
}

pub(crate) struct Filter {
    child: Box<dyn ExecutionPlan>,
}

pub(crate) struct FilterStream {
    child_stream: Box<dyn DataStream>,
}

impl Filter {
    pub fn new(child: Box<dyn ExecutionPlan>) -> Self {
        Self { child }
    }
}

impl ExecutionPlan for Filter {
    fn execute(&self) -> Box<dyn DataStream> {
        Box::new(FilterStream {
            child_stream: self.child.execute(),
        })
    }
}

impl DataStream for FilterStream {
    fn poll_next(&mut self) -> Option<Arc<dyn Any + Send + Sync>> {
        if let Some(value) = self.child_stream.poll_next() {
            let batch = value.downcast_ref::<RecordBatch>().unwrap();
            return Some(Arc::new(
                filter_record_batch(batch, batch.column_by_name("c").unwrap().as_boolean())
                    .unwrap(),
            ));
        }
        None
    }
}

pub(crate) struct Project {
    child: Box<dyn ExecutionPlan>,
}

pub(crate) struct ProjectStream {
    schema: Arc<Schema>,
    child_stream: Box<dyn DataStream>,
}

impl Project {
    pub fn new(child: Box<dyn ExecutionPlan>) -> Self {
        Self { child }
    }
}

impl ExecutionPlan for Project {
    fn execute(&self) -> Box<dyn DataStream> {
        Box::new(ProjectStream {
            schema: Arc::new(Schema::new(vec![Field::new("sum", DataType::Int32, false)])),
            child_stream: self.child.execute(),
        })
    }
}

impl DataStream for ProjectStream {
    fn poll_next(&mut self) -> Option<Arc<dyn Any + Send + Sync>> {
        if let Some(value) = self.child_stream.poll_next() {
            let batch = value.downcast_ref::<RecordBatch>().unwrap();
            let sum = add(
                batch.column_by_name("a").unwrap(),
                batch.column_by_name("b").unwrap(),
            )
            .unwrap();
            return Some(Arc::new(
                RecordBatch::try_new(self.schema.clone(), vec![sum]).unwrap(),
            ));
        }
        None
    }
}
