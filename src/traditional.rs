use crate::exec::{DataStream, ExecutionPlan};
use crate::DATA_COUNT;
use std::any::Any;
use std::sync::Arc;

#[allow(dead_code)]
struct RowData {
    a: i32,
    b: i32,
    c: bool,
    d: String,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ResultData {
    sum: i32,
}

unsafe impl Send for RowData {}

unsafe impl Sync for RowData {}

pub(crate) struct DataSource;

pub(crate) struct DataSourceStream {
    dataset: Vec<Arc<RowData>>,
    i: usize,
}

impl DataSource {
    pub fn new() -> Self {
        DataSource {}
    }
}

impl ExecutionPlan for DataSource {
    fn execute(&self) -> Box<dyn DataStream> {
        let mut dataset = vec![];
        for i in 0..DATA_COUNT {
            let data = Arc::new(RowData {
                a: i,
                b: i,
                c: i % 2 == 0,
                d: format!("hello world {}", i),
            });
            dataset.push(data);
        }
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
        while let Some(value) = self.child_stream.poll_next() {
            let data = value.downcast::<RowData>().unwrap();
            if data.c {
                return Some(data);
            }
        }
        None
    }
}

pub(crate) struct Project {
    child: Box<dyn ExecutionPlan>,
}

pub(crate) struct ProjectStream {
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
            child_stream: self.child.execute(),
        })
    }
}

impl DataStream for ProjectStream {
    fn poll_next(&mut self) -> Option<Arc<dyn Any + Send + Sync>> {
        if let Some(value) = self.child_stream.poll_next() {
            let data = value.downcast::<RowData>().unwrap();
            return Some(Arc::new(ResultData {
                sum: data.a + data.b,
            }));
        }
        None
    }
}
