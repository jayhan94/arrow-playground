pub mod columnar;
pub mod compiled;
pub mod exec;
pub mod row;
mod shared_data;

#[cfg(test)]
mod tests {
    use crate::exec::ExecutionPlan;
    use crate::row::{DataSource, Filter, Project, Row};
    use std::sync::Arc;

    #[test]
    fn test_row_impl() {
        let source = DataSource::new(source_data(100));
        let filter = Filter::new(Box::new(source));
        let project = Project::new(Box::new(filter));
        let mut stream = project.execute();
        while let Some(value) = stream.poll_next() {
            let r = value.downcast::<Row>().unwrap();
            println!(
                "{:?}",
                r.get(0)
                    .as_ref()
                    .unwrap()
                    .downcast_ref::<Box<i32>>()
                    .unwrap()
            );
        }
    }

    pub fn source_data(count: i32) -> Arc<Vec<Arc<Row>>> {
        let mut dataset = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut row = Row::empty(4);
            row.set(0, Some(Arc::new(Box::new(i))));
            row.set(1, Some(Arc::new(Box::new(i))));
            row.set(2, Some(Arc::new(Box::new(i % 2 == 0))));
            row.set(3, Some(Arc::new(Box::new(format!("hello world {}", i)))));
            dataset.push(Arc::new(row));
        }
        Arc::new(dataset)
    }
}
