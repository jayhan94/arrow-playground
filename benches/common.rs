extern crate arrow_playground;

use arrow_playground::exec::DataStream;
use std::ops::DerefMut;

pub const COUNT: i32 = 1000000;

pub fn consume_stream(stream: Box<dyn DataStream>) {
    let mut stream = stream;
    while let Some(_value) = stream.deref_mut().poll_next() {}
}
