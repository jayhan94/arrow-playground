mod columnar;
mod exec;
mod traditional;

use crate::columnar::DataSource as ColumnarDataSource;
use crate::columnar::Filter as ColumnarFilter;
use crate::columnar::Project as ColumnarProject;
use crate::exec::ExecutionPlan;
use crate::traditional::DataSource as TraditionalDataSource;
use crate::traditional::Filter as TraditionalFilter;
use crate::traditional::Project as TraditionalProject;
use std::ops::DerefMut;

pub const DATA_COUNT: i32 = 10000000;

#[allow(unused_variables)]
fn main() {
    let start_time = std::time::Instant::now();
    let source = TraditionalDataSource::new();
    let filter = TraditionalFilter::new(Box::new(source));
    let project = TraditionalProject::new(Box::new(filter));
    let mut stream = project.execute();
    while let Some(value) = stream.deref_mut().poll_next() {}
    println!("traditional: cost {} ms", start_time.elapsed().as_millis());

    let start_time = std::time::Instant::now();
    let source = ColumnarDataSource::new();
    let filter = ColumnarFilter::new(Box::new(source));
    let project = ColumnarProject::new(Box::new(filter));
    let mut stream = project.execute();
    while let Some(value) = stream.deref_mut().poll_next() {}
    println!("columnar: cost {} ms", start_time.elapsed().as_millis());
}
