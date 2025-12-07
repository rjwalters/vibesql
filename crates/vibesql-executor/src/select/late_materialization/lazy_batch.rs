//! Lazy Materialized Batch
//!
//! A batch that holds source data and selection, materializing rows on demand.

use std::sync::Arc;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::SelectionVector;
use crate::errors::ExecutorError;
use crate::select::columnar::{ColumnArray, ColumnarBatch};

/// Source data format for lazy materialization
#[derive(Debug, Clone)]
pub enum SourceData {
    /// Row-based source data (Vec<Row>)
    Rows(Arc<Vec<Row>>),
    /// Columnar source data (ColumnarBatch)
    Columnar(Arc<ColumnarBatch>),
}

impl SourceData {
    /// Get the number of rows in the source data
    pub fn row_count(&self) -> usize {
        match self {
            SourceData::Rows(rows) => rows.len(),
            SourceData::Columnar(batch) => batch.row_count(),
        }
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        match self {
            SourceData::Rows(rows) => rows.first().map(|r| r.len()).unwrap_or(0),
            SourceData::Columnar(batch) => batch.column_count(),
        }
    }

    /// Get a value at a specific position
    pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Result<SqlValue, ExecutorError> {
        match self {
            SourceData::Rows(rows) => rows
                .get(row_idx)
                .and_then(|row| row.get(col_idx))
                .cloned()
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_idx }),
            SourceData::Columnar(batch) => batch.get_value(row_idx, col_idx),
        }
    }

    /// Get a row at a specific index (materializes if columnar)
    pub fn get_row(&self, row_idx: usize) -> Result<Row, ExecutorError> {
        match self {
            SourceData::Rows(rows) => rows
                .get(row_idx)
                .cloned()
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: row_idx }),
            SourceData::Columnar(batch) => {
                let mut values = Vec::with_capacity(batch.column_count());
                for col_idx in 0..batch.column_count() {
                    values.push(batch.get_value(row_idx, col_idx)?);
                }
                Ok(Row::new(values))
            }
        }
    }
}

/// A lazy materialized batch that defers row materialization
///
/// This is the key abstraction for late materialization. It holds:
/// - Source data (either row-based or columnar)
/// - A selection vector indicating which rows are active
///
/// Rows are only materialized when explicitly requested, typically
/// at the output boundary of query execution.
///
/// # Performance Benefits
///
/// 1. **Memory**: Only stores indices (4 bytes each) instead of full rows
/// 2. **Cache**: Selection vectors are cache-friendly during iteration
/// 3. **Composition**: Multiple operations can share source data via Arc
/// 4. **Lazy Evaluation**: Skips materialization for filtered rows
///
/// # Example
///
/// ```text
/// // Create a lazy batch from source data
/// let source = SourceData::Rows(Arc::new(rows));
/// let lazy_batch = LazyMaterializedBatch::new(source);
///
/// // Apply filter (just updates selection, no materialization)
/// let filtered = lazy_batch.filter(&filter_bitmap);
///
/// // Only materialize at output
/// let result_rows = filtered.materialize_selected(&[0, 2, 3])?; // Only these columns
/// ```
#[derive(Debug, Clone)]
pub struct LazyMaterializedBatch {
    /// The underlying source data
    source: SourceData,
    /// Selection vector (which rows are active)
    selection: SelectionVector,
    /// Column names (optional metadata)
    column_names: Option<Vec<String>>,
}

impl LazyMaterializedBatch {
    /// Create a new lazy batch from source data (selects all rows)
    pub fn new(source: SourceData) -> Self {
        let row_count = source.row_count();
        Self { source, selection: SelectionVector::all(row_count), column_names: None }
    }

    /// Create a new lazy batch with a specific selection
    pub fn with_selection(source: SourceData, selection: SelectionVector) -> Self {
        Self { source, selection, column_names: None }
    }

    /// Create from row-based data
    pub fn from_rows(rows: Vec<Row>) -> Self {
        Self::new(SourceData::Rows(Arc::new(rows)))
    }

    /// Create from columnar data
    pub fn from_columnar(batch: ColumnarBatch) -> Self {
        Self::new(SourceData::Columnar(Arc::new(batch)))
    }

    /// Set column names
    pub fn with_column_names(mut self, names: Vec<String>) -> Self {
        self.column_names = Some(names);
        self
    }

    /// Get the source data
    pub fn source(&self) -> &SourceData {
        &self.source
    }

    /// Get the selection vector
    pub fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    /// Get column names if available
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    /// Number of selected (active) rows
    pub fn len(&self) -> usize {
        self.selection.len()
    }

    /// Check if no rows are selected
    pub fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }

    /// Number of columns in the source
    pub fn column_count(&self) -> usize {
        self.source.column_count()
    }

    /// Total rows in source (before selection)
    pub fn source_row_count(&self) -> usize {
        self.source.row_count()
    }

    /// Apply a filter bitmap, returning a new lazy batch with refined selection
    ///
    /// This is a lazy operation - it updates the selection vector without
    /// materializing any row data.
    pub fn filter(&self, bitmap: &[bool]) -> Self {
        let new_selection = self.selection.filter(bitmap, |&idx| bitmap[idx as usize]);
        Self {
            source: self.source.clone(),
            selection: new_selection,
            column_names: self.column_names.clone(),
        }
    }

    /// Apply a filter to the current selection
    ///
    /// The predicate receives the source row index and should return true
    /// to keep the row.
    pub fn filter_with<F>(&self, predicate: F) -> Self
    where
        F: Fn(usize) -> bool,
    {
        let new_indices: Vec<u32> =
            self.selection.iter().filter(|&idx| predicate(idx as usize)).collect();

        Self {
            source: self.source.clone(),
            selection: SelectionVector::from_indices(new_indices),
            column_names: self.column_names.clone(),
        }
    }

    /// Intersect selection with another selection vector
    pub fn intersect_selection(&self, other: &SelectionVector) -> Self {
        Self {
            source: self.source.clone(),
            selection: self.selection.intersect(other),
            column_names: self.column_names.clone(),
        }
    }

    /// Union selection with another selection vector
    pub fn union_selection(&self, other: &SelectionVector) -> Self {
        Self {
            source: self.source.clone(),
            selection: self.selection.union(other),
            column_names: self.column_names.clone(),
        }
    }

    /// Materialize all selected rows
    ///
    /// This is typically called at the output boundary when results
    /// need to be returned to the caller.
    pub fn materialize(&self) -> Result<Vec<Row>, ExecutorError> {
        let mut rows = Vec::with_capacity(self.selection.len());

        for idx in self.selection.iter() {
            let row = self.source.get_row(idx as usize)?;
            rows.push(row);
        }

        Ok(rows)
    }

    /// Materialize only specific columns for selected rows
    ///
    /// This is the most efficient output path - only materializes
    /// columns that appear in the final SELECT projection.
    pub fn materialize_columns(&self, column_indices: &[usize]) -> Result<Vec<Row>, ExecutorError> {
        let mut rows = Vec::with_capacity(self.selection.len());

        for idx in self.selection.iter() {
            let mut values = Vec::with_capacity(column_indices.len());
            for &col_idx in column_indices {
                let value = self.source.get_value(idx as usize, col_idx)?;
                values.push(value);
            }
            rows.push(Row::new(values));
        }

        Ok(rows)
    }

    /// Materialize a single column for selected rows
    ///
    /// Useful for extracting join keys or aggregation inputs.
    pub fn materialize_column(&self, column_idx: usize) -> Result<Vec<SqlValue>, ExecutorError> {
        let mut values = Vec::with_capacity(self.selection.len());

        for idx in self.selection.iter() {
            let value = self.source.get_value(idx as usize, column_idx)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Get a single value from selected row
    pub fn get_selected_value(
        &self,
        selection_idx: usize,
        column_idx: usize,
    ) -> Result<SqlValue, ExecutorError> {
        let row_idx = self
            .selection
            .get(selection_idx)
            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: selection_idx })?;

        self.source.get_value(row_idx as usize, column_idx)
    }

    /// Iterate over selected row indices
    pub fn iter_indices(&self) -> impl Iterator<Item = u32> + '_ {
        self.selection.iter()
    }

    /// Create a child batch with remapped selection
    ///
    /// Used when chaining operations: if we filter a filtered result,
    /// we need to maintain the chain of selections.
    pub fn remap_selection(&self, child_selection: &SelectionVector) -> Self {
        let remapped = child_selection.remap(&self.selection);
        Self {
            source: self.source.clone(),
            selection: remapped,
            column_names: self.column_names.clone(),
        }
    }

    /// Get the raw column array if source is columnar
    ///
    /// Returns None if source is row-based.
    pub fn column_array(&self, column_idx: usize) -> Option<&ColumnArray> {
        match &self.source {
            SourceData::Columnar(batch) => batch.column(column_idx),
            SourceData::Rows(_) => None,
        }
    }

    /// Check if the source is columnar
    pub fn is_columnar(&self) -> bool {
        matches!(&self.source, SourceData::Columnar(_))
    }

    /// Convert to columnar format if not already
    ///
    /// This is useful when downstream operations benefit from columnar access.
    pub fn to_columnar(&self) -> Result<LazyMaterializedBatch, ExecutorError> {
        match &self.source {
            SourceData::Columnar(_) => Ok(self.clone()),
            SourceData::Rows(rows) => {
                let batch = ColumnarBatch::from_rows(rows)?;
                Ok(LazyMaterializedBatch {
                    source: SourceData::Columnar(Arc::new(batch)),
                    selection: self.selection.clone(),
                    column_names: self.column_names.clone(),
                })
            }
        }
    }

    /// Selectivity ratio
    pub fn selectivity(&self) -> f64 {
        self.selection.selectivity(self.source_row_count())
    }
}

/// Builder for creating lazy batches with joined sources
///
/// Used for join operations that combine multiple source tables.
pub struct JoinedLazyBatchBuilder {
    /// Left source
    left_source: SourceData,
    /// Right source
    right_source: SourceData,
    /// Left selection indices
    left_indices: Vec<u32>,
    /// Right selection indices (u32::MAX for outer join NULL rows)
    right_indices: Vec<u32>,
}

impl JoinedLazyBatchBuilder {
    /// Create a new builder for joining two sources
    pub fn new(left_source: SourceData, right_source: SourceData) -> Self {
        Self { left_source, right_source, left_indices: Vec::new(), right_indices: Vec::new() }
    }

    /// Add a matched pair of indices
    pub fn add_match(&mut self, left_idx: u32, right_idx: u32) {
        self.left_indices.push(left_idx);
        self.right_indices.push(right_idx);
    }

    /// Add a left-only row (for LEFT OUTER join)
    pub fn add_left_only(&mut self, left_idx: u32) {
        self.left_indices.push(left_idx);
        self.right_indices.push(u32::MAX); // NULL marker
    }

    /// Add a right-only row (for RIGHT OUTER join)
    pub fn add_right_only(&mut self, right_idx: u32) {
        self.left_indices.push(u32::MAX); // NULL marker
        self.right_indices.push(right_idx);
    }

    /// Get left indices
    pub fn left_indices(&self) -> &[u32] {
        &self.left_indices
    }

    /// Get right indices
    pub fn right_indices(&self) -> &[u32] {
        &self.right_indices
    }

    /// Build the result count
    pub fn result_count(&self) -> usize {
        self.left_indices.len()
    }

    /// Materialize the joined result
    ///
    /// Combines columns from both sources for each matching pair.
    pub fn materialize(&self) -> Result<Vec<Row>, ExecutorError> {
        let left_cols = self.left_source.column_count();
        let right_cols = self.right_source.column_count();
        let total_cols = left_cols + right_cols;

        let mut rows = Vec::with_capacity(self.left_indices.len());

        for (&left_idx, &right_idx) in self.left_indices.iter().zip(&self.right_indices) {
            let mut values = Vec::with_capacity(total_cols);

            // Left columns
            if left_idx == u32::MAX {
                // NULL row for RIGHT OUTER join
                for _ in 0..left_cols {
                    values.push(SqlValue::Null);
                }
            } else {
                for col_idx in 0..left_cols {
                    values.push(self.left_source.get_value(left_idx as usize, col_idx)?);
                }
            }

            // Right columns
            if right_idx == u32::MAX {
                // NULL row for LEFT OUTER join
                for _ in 0..right_cols {
                    values.push(SqlValue::Null);
                }
            } else {
                for col_idx in 0..right_cols {
                    values.push(self.right_source.get_value(right_idx as usize, col_idx)?);
                }
            }

            rows.push(Row::new(values));
        }

        Ok(rows)
    }

    /// Materialize only specific columns
    ///
    /// `left_columns` and `right_columns` specify which columns to include from each source.
    pub fn materialize_columns(
        &self,
        left_columns: &[usize],
        right_columns: &[usize],
    ) -> Result<Vec<Row>, ExecutorError> {
        let total_cols = left_columns.len() + right_columns.len();
        let mut rows = Vec::with_capacity(self.left_indices.len());

        for (&left_idx, &right_idx) in self.left_indices.iter().zip(&self.right_indices) {
            let mut values = Vec::with_capacity(total_cols);

            // Selected left columns
            if left_idx == u32::MAX {
                for _ in 0..left_columns.len() {
                    values.push(SqlValue::Null);
                }
            } else {
                for &col_idx in left_columns {
                    values.push(self.left_source.get_value(left_idx as usize, col_idx)?);
                }
            }

            // Selected right columns
            if right_idx == u32::MAX {
                for _ in 0..right_columns.len() {
                    values.push(SqlValue::Null);
                }
            } else {
                for &col_idx in right_columns {
                    values.push(self.right_source.get_value(right_idx as usize, col_idx)?);
                }
            }

            rows.push(Row::new(values));
        }

        Ok(rows)
    }
}

#[cfg(test)]
mod lazy_batch_tests {
    use super::*;

    fn sample_rows() -> Vec<Row> {
        vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Carol"))]),
            Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar(arcstr::ArcStr::from("Dave"))]),
            Row::new(vec![SqlValue::Integer(5), SqlValue::Varchar(arcstr::ArcStr::from("Eve"))]),
        ]
    }

    #[test]
    fn test_lazy_batch_creation() {
        let rows = sample_rows();
        let batch = LazyMaterializedBatch::from_rows(rows);

        assert_eq!(batch.len(), 5);
        assert_eq!(batch.column_count(), 2);
        assert_eq!(batch.source_row_count(), 5);
    }

    #[test]
    fn test_lazy_batch_filter() {
        let rows = sample_rows();
        let batch = LazyMaterializedBatch::from_rows(rows);

        // Filter to rows where id > 2
        let filtered = batch.filter_with(|idx| {
            if let SourceData::Rows(rows) = batch.source() {
                if let Some(SqlValue::Integer(id)) = rows[idx].get(0) {
                    return *id > 2;
                }
            }
            false
        });

        assert_eq!(filtered.len(), 3); // Carol, Dave, Eve
    }

    #[test]
    fn test_lazy_batch_materialize() {
        let rows = sample_rows();
        let batch = LazyMaterializedBatch::from_rows(rows);

        let materialized = batch.materialize().unwrap();
        assert_eq!(materialized.len(), 5);
        assert_eq!(materialized[0].get(0), Some(&SqlValue::Integer(1)));
    }

    #[test]
    fn test_lazy_batch_materialize_columns() {
        let rows = sample_rows();
        let batch = LazyMaterializedBatch::from_rows(rows);

        // Only materialize the name column
        let names = batch.materialize_columns(&[1]).unwrap();
        assert_eq!(names.len(), 5);
        assert_eq!(names[0].len(), 1); // Only 1 column
        assert_eq!(names[0].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
    }

    #[test]
    fn test_lazy_batch_with_selection() {
        let rows = sample_rows();
        let selection = SelectionVector::from_indices(vec![0, 2, 4]); // Alice, Carol, Eve
        let batch =
            LazyMaterializedBatch::with_selection(SourceData::Rows(Arc::new(rows)), selection);

        assert_eq!(batch.len(), 3);

        let materialized = batch.materialize().unwrap();
        assert_eq!(materialized[0].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
        assert_eq!(materialized[1].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Carol"))));
        assert_eq!(materialized[2].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Eve"))));
    }

    #[test]
    fn test_joined_batch_builder() {
        let left_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("A"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("B"))]),
        ];
        let right_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("X"))]),
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Y"))]),
        ];

        let mut builder = JoinedLazyBatchBuilder::new(
            SourceData::Rows(Arc::new(left_rows)),
            SourceData::Rows(Arc::new(right_rows)),
        );

        // Left row 0 matches right rows 0 and 1
        builder.add_match(0, 0);
        builder.add_match(0, 1);
        // Left row 1 has no match (left outer join)
        builder.add_left_only(1);

        assert_eq!(builder.result_count(), 3);

        let rows = builder.materialize().unwrap();
        assert_eq!(rows.len(), 3);

        // First match: (1, A, 1, X)
        assert_eq!(rows[0].get(0), Some(&SqlValue::Integer(1)));
        assert_eq!(rows[0].get(2), Some(&SqlValue::Integer(1)));
        assert_eq!(rows[0].get(3), Some(&SqlValue::Varchar(arcstr::ArcStr::from("X"))));

        // Second match: (1, A, 1, Y)
        assert_eq!(rows[1].get(3), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Y"))));

        // Left-only: (2, B, NULL, NULL)
        assert_eq!(rows[2].get(0), Some(&SqlValue::Integer(2)));
        assert_eq!(rows[2].get(2), Some(&SqlValue::Null));
        assert_eq!(rows[2].get(3), Some(&SqlValue::Null));
    }

    #[test]
    fn test_selectivity() {
        let rows = sample_rows();
        let selection = SelectionVector::from_indices(vec![0, 2]); // 2 out of 5
        let batch =
            LazyMaterializedBatch::with_selection(SourceData::Rows(Arc::new(rows)), selection);

        assert!((batch.selectivity() - 0.4).abs() < 0.001);
    }
}
