extern crate arrow_playground;

use arrow_playground::exec::DataStream;
use arrow_playground::traditional;
use arrow_playground::traditional::RowData;
use std::ops::DerefMut;
use std::sync::Arc;

pub const COUNT: i32 = 1000000;

pub fn consume_stream(stream: Box<dyn DataStream>) {
    let mut stream = stream;
    while let Some(_value) = stream.deref_mut().poll_next() {}
}

pub fn traditional_source_data(count: i32) -> Arc<Vec<Arc<RowData>>> {
    let mut dataset = Vec::with_capacity(count as usize);
    for i in 0..count {
        let data = Arc::new(traditional::RowData {
            a: i,
            b: i,
            c: i % 2 == 0,
            d: format!("hello world {}", i),
        });
        dataset.push(data);
    }
    Arc::new(dataset)
}
