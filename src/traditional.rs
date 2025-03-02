use crate::exec::{DataStream, ExecutionPlan};
use std::any::Any;
use std::sync::Arc;

#[allow(dead_code)]
pub struct RowData {
    pub a: i32,
    pub b: i32,
    pub c: bool,
    pub d: String,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ResultData {
    sum: i32,
}

unsafe impl Send for RowData {}

unsafe impl Sync for RowData {}

pub struct DataSource {
    dataset: Arc<Vec<Arc<RowData>>>,
}

pub struct DataSourceStream {
    dataset: Arc<Vec<Arc<RowData>>>,
    i: usize,
}

impl DataSource {
    pub fn new(dataset: Arc<Vec<Arc<RowData>>>) -> Self {
        DataSource { dataset }
    }
}

impl ExecutionPlan for DataSource {
    fn execute(&self) -> Box<dyn DataStream> {
        Box::new(DataSourceStream {
            dataset: self.dataset.clone(),
            i: 0,
        })
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
        while let Some(value) = self.child_stream.poll_next() {
            let data = value.downcast::<RowData>().unwrap();
            if data.c {
                return Some(data);
            }
        }
        None
    }
}

pub struct Project {
    child: Box<dyn ExecutionPlan>,
}

pub struct ProjectStream {
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

pub struct CompiledProject {
    dataset: Arc<Vec<Arc<RowData>>>,
}

impl CompiledProject {
    pub fn new(dataset: Arc<Vec<Arc<RowData>>>) -> Self {
        Self { dataset }
    }
}

impl ExecutionPlan for CompiledProject {
    fn execute(&self) -> Box<dyn DataStream> {
        Box::new(CompiledProjectStream::new(self.dataset.clone()))
    }
}

pub struct CompiledProjectStream {
    dataset: Arc<Vec<Arc<RowData>>>,
    i: usize,
}

impl CompiledProjectStream {
    pub fn new(dataset: Arc<Vec<Arc<RowData>>>) -> Self {
        Self { dataset, i: 0 }
    }
}

// this is a mock for compilation
impl DataStream for CompiledProjectStream {
    fn poll_next(&mut self) -> Option<Arc<dyn Any + Send + Sync>> {
        while self.i < self.dataset.len() {
            let row = self.dataset[self.i].clone();
            self.i += 1;
            if !row.c {
                continue;
            }
            return Some(Arc::new(ResultData { sum: row.a + row.b }));
        }
        None
    }
}
