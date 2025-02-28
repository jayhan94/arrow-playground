use std::any::Any;
use std::sync::Arc;

pub trait DataStream {
    fn poll_next(&mut self) -> Option<Arc<dyn Any + Send + Sync>>;
}

pub trait ExecutionPlan {
    fn execute(&self) -> Box<dyn DataStream>;
}
