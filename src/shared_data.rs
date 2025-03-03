use std::any::Any;
use std::sync::Arc;

pub type SharedData = Arc<dyn Any + Send + Sync>;
