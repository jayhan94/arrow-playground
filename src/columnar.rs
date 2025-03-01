use arrow::array::{AsArray, RecordBatch};

use crate::exec::{DataStream, ExecutionPlan};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_arith::numeric::add;
use arrow_select::filter::filter_record_batch;
use std::any::Any;
use std::sync::Arc;

pub struct DataSource {
    dataset: Arc<Vec<Arc<RecordBatch>>>,
    project: Arc<Vec<usize>>,
}

#[allow(dead_code)]
pub struct DataSourceStream {
    dataset: Arc<Vec<Arc<RecordBatch>>>,
    i: usize,
    project: Arc<Vec<usize>>,
}

impl DataSource {
    pub fn new(dataset: Arc<Vec<Arc<RecordBatch>>>, project: Arc<Vec<usize>>) -> Self {
        DataSource { dataset, project }
    }
}

impl ExecutionPlan for DataSource {
    fn execute(&self) -> Box<dyn DataStream> {
        Box::new(DataSourceStream {
            dataset: self.dataset.clone(),
            i: 0,
            project: self.project.clone(),
        })
    }
}

impl DataStream for DataSourceStream {
    fn poll_next(&mut self) -> Option<Arc<dyn Any + Send + Sync>> {
        if self.i >= self.dataset.len() {
            return None;
        }
        // projection is zero-copy
        let result = Arc::new(self.dataset[self.i].project(&self.project[..]).unwrap());
        self.i += 1;
        Some(result)
    }
}

pub struct Filter {
    child: Box<dyn ExecutionPlan>,
}

pub struct FilterStream {
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

pub struct Project {
    child: Box<dyn ExecutionPlan>,
}

pub struct ProjectStream {
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
