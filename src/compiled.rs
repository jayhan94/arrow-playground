use crate::exec::{DataStream, ExecutionPlan};
use crate::shared_data::SharedData;
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
    fn poll_next(&mut self) -> Option<SharedData> {
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
