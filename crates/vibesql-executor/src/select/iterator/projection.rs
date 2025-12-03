//! Projection iterator implementation

#![allow(dead_code)]

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::RowIterator;

/// Iterator that projects (transforms) rows using a projection function
///
/// This iterator wraps another RowIterator and applies a transformation
/// to each row. The transformation can change the row's values and/or schema
/// (e.g., selecting specific columns, computing expressions, renaming columns).
///
/// # Example
///
/// ```text
/// let scan = TableScanIterator::new(schema, rows);
///
/// // Project to select only certain columns
/// let project_fn = |row: vibesql_storage::Row| -> Result<vibesql_storage::Row, ExecutorError> {
///     Ok(vibesql_storage::Row::new(vec![row.values[0].clone(), row.values[2].clone()]))
/// };
///
/// let projected_schema = CombinedSchema::new(/* ... */);
/// let project = ProjectionIterator::new(scan, projected_schema, project_fn);
///
/// for row in project {
///     println!("{:?}", row?);  // Only contains projected columns
/// }
/// ```
pub struct ProjectionIterator<I, F>
where
    I: RowIterator,
    F: FnMut(vibesql_storage::Row) -> Result<vibesql_storage::Row, ExecutorError>,
{
    source: I,
    schema: CombinedSchema,
    projection_fn: F,
}

impl<I, F> ProjectionIterator<I, F>
where
    I: RowIterator,
    F: FnMut(vibesql_storage::Row) -> Result<vibesql_storage::Row, ExecutorError>,
{
    /// Create a new projection iterator
    ///
    /// # Arguments
    /// * `source` - The source iterator to project
    /// * `schema` - The schema of rows after projection
    /// * `projection_fn` - The function to transform each row
    pub fn new(source: I, schema: CombinedSchema, projection_fn: F) -> Self {
        Self { source, schema, projection_fn }
    }
}

impl<I, F> Iterator for ProjectionIterator<I, F>
where
    I: RowIterator,
    F: FnMut(vibesql_storage::Row) -> Result<vibesql_storage::Row, ExecutorError>,
{
    type Item = Result<vibesql_storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = match self.source.next()? {
            Ok(row) => row,
            Err(e) => return Some(Err(e)),
        };

        Some((self.projection_fn)(row))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Projection doesn't change row count
        self.source.size_hint()
    }
}

impl<I, F> RowIterator for ProjectionIterator<I, F>
where
    I: RowIterator,
    F: FnMut(vibesql_storage::Row) -> Result<vibesql_storage::Row, ExecutorError>,
{
    fn schema(&self) -> &CombinedSchema {
        &self.schema
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        // Projection doesn't change row count
        self.source.row_size_hint()
    }
}
