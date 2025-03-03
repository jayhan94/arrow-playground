use crate::shared_data::SharedData;

pub trait DataStream {
    fn poll_next(&mut self) -> Option<SharedData>;
}

pub trait ExecutionPlan {
    fn execute(&self) -> Box<dyn DataStream>;
}
