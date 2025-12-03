//! Table scan iterator implementation

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::RowIterator;

/// Iterator that scans rows from a materialized table
///
/// This is the simplest RowIterator - it wraps a `Vec<Row>` and produces
/// rows one at a time. While the input is still materialized, this provides
/// a uniform iterator interface for composition with other operators.
///
/// # Example
///
/// ```text
/// let rows = vec![row1, row2, row3];
/// let scan = TableScanIterator::new(schema, rows);
/// for row in scan {
///     println!("{:?}", row?);
/// }
/// ```
pub struct TableScanIterator {
    schema: CombinedSchema,
    rows: std::vec::IntoIter<vibesql_storage::Row>,
    #[allow(dead_code)]
    total_count: usize,
}

impl TableScanIterator {
    /// Create a new table scan iterator from a schema and materialized rows
    ///
    /// # Arguments
    /// * `schema` - The schema describing the structure of rows
    /// * `rows` - The rows to iterate over (consumed and moved)
    pub fn new(schema: CombinedSchema, rows: Vec<vibesql_storage::Row>) -> Self {
        let total_count = rows.len();
        Self { schema, rows: rows.into_iter(), total_count }
    }
}

impl Iterator for TableScanIterator {
    type Item = Result<vibesql_storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(Ok)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl RowIterator for TableScanIterator {
    fn schema(&self) -> &CombinedSchema {
        &self.schema
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.size_hint();
        (lower, upper)
    }
}
