use crate::exec::{DataStream, ExecutionPlan};
use crate::shared_data::SharedData;
use std::sync::Arc;

pub struct Row {
    columns: Arc<Vec<Option<SharedData>>>,
}

impl Row {
    pub fn empty(size: usize) -> Self {
        Self {
            columns: Arc::new(vec![None; size]),
        }
    }

    pub fn set(&mut self, i: usize, value: Option<SharedData>) {
        if let Some(mut_vec) = Arc::get_mut(&mut self.columns) {
            mut_vec[i] = value;
        } else {
            eprintln!("unexpected column index {}", i);
        }
    }

    pub fn get(&self, i: usize) -> &Option<SharedData> {
        &self.columns[i]
    }
}

pub struct DataSource {
    dataset: Arc<Vec<Arc<Row>>>,
}

pub struct DataSourceStream {
    dataset: Arc<Vec<Arc<Row>>>,
    i: usize,
}

impl DataSource {
    pub fn new(dataset: Arc<Vec<Arc<Row>>>) -> Self {
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
    fn poll_next(&mut self) -> Option<SharedData> {
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
    fn poll_next(&mut self) -> Option<SharedData> {
        while let Some(value) = self.child_stream.poll_next() {
            let row = value.downcast::<Row>().unwrap();
            if **row
                .get(2)
                .as_ref()
                .unwrap()
                .downcast_ref::<Box<bool>>()
                .unwrap()
            {
                return Some(row);
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
    fn poll_next(&mut self) -> Option<SharedData> {
        if let Some(value) = self.child_stream.poll_next() {
            let row = value.downcast::<Row>().unwrap();
            let a = **row
                .get(0)
                .as_ref()
                .unwrap()
                .downcast_ref::<Box<i32>>()
                .unwrap();
            let b = **row
                .get(1)
                .as_ref()
                .unwrap()
                .downcast_ref::<Box<i32>>()
                .unwrap();
            let sum = Some(Arc::new(Box::new(a + b)) as SharedData);
            let mut ret_row = Row::empty(1);
            ret_row.set(0, sum);
            return Some(Arc::new(ret_row));
        }
        None
    }
}
