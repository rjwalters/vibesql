//! Pipeline Input/Output Types
//!
//! This module defines polymorphic types for pipeline data that can represent
//! rows, batches, or columnar data depending on the execution strategy.
//!
//! ## Design
//!
//! `PipelineInput` and `PipelineOutput` are polymorphic types that allow pipelines
//! to work with different data representations:
//! - **Rows**: Traditional row-oriented Vec<Row> format
//! - **Batch**: Columnar batch format (ColumnarBatch) for SIMD-accelerated execution
//! - **NativeColumnar**: Direct reference to columnar storage (zero-copy)
//!
//! ## Zero-Copy Conversions
//!
//! Where possible, conversions avoid copying data:
//! - `PipelineOutput::Rows` → `into_rows()` is zero-copy (move)
//! - `PipelineInput::Rows` → `as_rows()` is zero-copy (borrow)
//! - Batch ↔ Rows conversions require materialization

use crate::select::columnar::ColumnarBatch;
use vibesql_storage::Row;

/// Polymorphic input for pipeline stages.
///
/// Different execution strategies work with different data representations:
/// - Row-oriented: Works with borrowed or owned row vectors
/// - Columnar: Works with columnar batches (Arrow-like format)
/// - Native columnar: Works directly with table storage
///
/// This enum allows pipeline stages to accept any of these formats and
/// convert as needed for their specific execution path.
///
/// # Example
///
/// ```text
/// use vibesql_executor::pipeline::{PipelineInput, PipelineOutput};
///
/// // Row-based input (borrowed)
/// let rows = vec![Row::new(vec![SqlValue::Integer(1)])];
/// let input = PipelineInput::from_rows(&rows);
///
/// // Columnar batch input
/// let batch = ColumnarBatch::from_rows(&rows)?;
/// let input = PipelineInput::from_batch(batch);
///
/// // Convert to rows when needed
/// let rows = input.into_rows();
/// ```
#[derive(Debug)]
pub enum PipelineInput<'a> {
    /// Borrowed slice of rows (zero-copy for row-oriented)
    Rows(&'a [Row]),

    /// Owned vector of rows (when ownership transfer is needed)
    RowsOwned(Vec<Row>),

    /// Columnar batch for SIMD-accelerated execution
    ///
    /// This variant enables high-performance columnar operations without
    /// row-by-row iteration. Use this for aggregations, filters, and
    /// projections that can benefit from SIMD vectorization.
    Batch(ColumnarBatch),

    /// Native columnar access (zero-copy from storage)
    /// Contains table reference and column indices to project
    NativeColumnar {
        /// Table name for storage lookup
        table_name: String,
        /// Column indices to project (empty = all columns)
        column_indices: Vec<usize>,
    },

    /// Empty input (for expression-only queries like SELECT 1+1)
    Empty,
}

impl<'a> PipelineInput<'a> {
    /// Create input from a borrowed row slice.
    #[inline]
    pub fn from_rows(rows: &'a [Row]) -> Self {
        PipelineInput::Rows(rows)
    }

    /// Create input from an owned row vector.
    #[inline]
    pub fn from_rows_owned(rows: Vec<Row>) -> Self {
        PipelineInput::RowsOwned(rows)
    }

    /// Create input from a columnar batch.
    ///
    /// This enables SIMD-accelerated execution for pipelines that support it.
    #[inline]
    pub fn from_batch(batch: ColumnarBatch) -> Self {
        PipelineInput::Batch(batch)
    }

    /// Create empty input for expression-only queries.
    #[inline]
    pub fn empty() -> Self {
        PipelineInput::Empty
    }

    /// Create native columnar input.
    #[inline]
    pub fn native_columnar(table_name: String, column_indices: Vec<usize>) -> Self {
        PipelineInput::NativeColumnar { table_name, column_indices }
    }

    /// Convert to owned rows, consuming the input.
    ///
    /// This is used when the pipeline stage needs ownership of the data.
    /// For columnar batches, this performs a materialization to rows.
    ///
    /// # Performance
    ///
    /// - `Rows`: Clones the slice (O(n))
    /// - `RowsOwned`: Zero-copy move (O(1))
    /// - `Batch`: Materializes to rows (O(n * m) where m = columns)
    /// - `NativeColumnar`: Returns empty (should be handled at storage layer)
    /// - `Empty`: Returns single empty row
    pub fn into_rows(self) -> Vec<Row> {
        match self {
            PipelineInput::Rows(rows) => rows.to_vec(),
            PipelineInput::RowsOwned(rows) => rows,
            PipelineInput::Batch(batch) => {
                // Materialize columnar batch to rows
                batch.to_rows().unwrap_or_default()
            }
            PipelineInput::NativeColumnar { .. } => {
                // Native columnar should be converted at the storage layer
                // This fallback returns empty for safety
                Vec::new()
            }
            PipelineInput::Empty => Vec::new(),
        }
    }

    /// Get the number of rows in the input.
    ///
    /// For native columnar, this may require a table lookup.
    pub fn row_count(&self) -> usize {
        match self {
            PipelineInput::Rows(rows) => rows.len(),
            PipelineInput::RowsOwned(rows) => rows.len(),
            PipelineInput::Batch(batch) => batch.row_count(),
            PipelineInput::NativeColumnar { .. } => 0, // Unknown without table lookup
            PipelineInput::Empty => 1,
        }
    }

    /// Check if the input is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            PipelineInput::Rows(rows) => rows.is_empty(),
            PipelineInput::RowsOwned(rows) => rows.is_empty(),
            PipelineInput::Batch(batch) => batch.row_count() == 0,
            PipelineInput::NativeColumnar { .. } => false, // Assume non-empty
            PipelineInput::Empty => false,                 // Empty input has one implicit row
        }
    }

    /// Try to get rows as a slice (only works for row-based inputs).
    ///
    /// Returns `None` for `Batch` and `NativeColumnar` variants.
    /// Use `into_rows()` if you need rows from those variants.
    pub fn as_rows(&self) -> Option<&[Row]> {
        match self {
            PipelineInput::Rows(rows) => Some(rows),
            PipelineInput::RowsOwned(rows) => Some(rows),
            _ => None,
        }
    }

    /// Try to get the columnar batch (only works for Batch variant).
    ///
    /// Returns `None` for row-based and native columnar variants.
    pub fn as_batch(&self) -> Option<&ColumnarBatch> {
        match self {
            PipelineInput::Batch(batch) => Some(batch),
            _ => None,
        }
    }

    /// Consume and return the columnar batch if this is a Batch variant.
    ///
    /// Returns `None` for other variants.
    pub fn into_batch(self) -> Option<ColumnarBatch> {
        match self {
            PipelineInput::Batch(batch) => Some(batch),
            _ => None,
        }
    }

    /// Check if this input is in columnar batch format.
    #[inline]
    pub fn is_batch(&self) -> bool {
        matches!(self, PipelineInput::Batch(_))
    }

    /// Check if this input is in row format (borrowed or owned).
    #[inline]
    pub fn is_rows(&self) -> bool {
        matches!(self, PipelineInput::Rows(_) | PipelineInput::RowsOwned(_))
    }

    /// Check if this input is native columnar format.
    #[inline]
    pub fn is_native_columnar(&self) -> bool {
        matches!(self, PipelineInput::NativeColumnar { .. })
    }
}

/// Polymorphic output from pipeline stages.
///
/// Output can be in row or columnar batch format, allowing efficient chaining
/// of pipeline stages with format-appropriate optimizations.
///
/// # Example
///
/// ```text
/// use vibesql_executor::pipeline::PipelineOutput;
///
/// // Row-based output
/// let output = PipelineOutput::from_rows(rows);
/// let rows = output.into_rows(); // Zero-copy for Rows variant
///
/// // Columnar batch output
/// let output = PipelineOutput::from_batch(batch);
/// let rows = output.into_rows(); // Materializes batch to rows
/// ```
#[derive(Debug, Default)]
pub enum PipelineOutput {
    /// Row-based output (traditional format)
    Rows(Vec<Row>),

    /// Columnar batch output for SIMD-accelerated pipelines
    ///
    /// This variant enables keeping data in columnar format throughout
    /// the pipeline for maximum performance.
    Batch(ColumnarBatch),

    /// Empty output (zero rows)
    #[default]
    Empty,
}

impl PipelineOutput {
    /// Create output from rows.
    #[inline]
    pub fn from_rows(rows: Vec<Row>) -> Self {
        PipelineOutput::Rows(rows)
    }

    /// Create output from a columnar batch.
    #[inline]
    pub fn from_batch(batch: ColumnarBatch) -> Self {
        PipelineOutput::Batch(batch)
    }

    /// Create empty output.
    #[inline]
    pub fn empty() -> Self {
        PipelineOutput::Empty
    }

    /// Convert to rows, consuming the output.
    ///
    /// This is the final conversion when returning results to the caller.
    ///
    /// # Performance
    ///
    /// - `Rows`: Zero-copy move (O(1))
    /// - `Batch`: Materializes to rows (O(n * m) where m = columns)
    /// - `Empty`: Returns empty Vec (O(1))
    pub fn into_rows(self) -> Vec<Row> {
        match self {
            PipelineOutput::Rows(rows) => rows,
            PipelineOutput::Batch(batch) => {
                // Materialize columnar batch to rows
                batch.to_rows().unwrap_or_default()
            }
            PipelineOutput::Empty => Vec::new(),
        }
    }

    /// Get the number of rows in the output.
    pub fn row_count(&self) -> usize {
        match self {
            PipelineOutput::Rows(rows) => rows.len(),
            PipelineOutput::Batch(batch) => batch.row_count(),
            PipelineOutput::Empty => 0,
        }
    }

    /// Check if the output is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            PipelineOutput::Rows(rows) => rows.is_empty(),
            PipelineOutput::Batch(batch) => batch.row_count() == 0,
            PipelineOutput::Empty => true,
        }
    }

    /// Try to get rows as a slice (only works for Rows variant).
    ///
    /// Returns `None` for `Batch` and `Empty` variants.
    pub fn as_rows(&self) -> Option<&[Row]> {
        match self {
            PipelineOutput::Rows(rows) => Some(rows),
            _ => None,
        }
    }

    /// Try to get the columnar batch (only works for Batch variant).
    ///
    /// Returns `None` for `Rows` and `Empty` variants.
    pub fn as_batch(&self) -> Option<&ColumnarBatch> {
        match self {
            PipelineOutput::Batch(batch) => Some(batch),
            _ => None,
        }
    }

    /// Consume and return the columnar batch if this is a Batch variant.
    ///
    /// Returns `None` for other variants.
    pub fn into_batch(self) -> Option<ColumnarBatch> {
        match self {
            PipelineOutput::Batch(batch) => Some(batch),
            _ => None,
        }
    }

    /// Check if this output is in columnar batch format.
    #[inline]
    pub fn is_batch(&self) -> bool {
        matches!(self, PipelineOutput::Batch(_))
    }

    /// Check if this output is in row format.
    #[inline]
    pub fn is_rows(&self) -> bool {
        matches!(self, PipelineOutput::Rows(_))
    }

    /// Convert to PipelineInput for chaining pipeline stages.
    ///
    /// This enables fluent chaining: `filter().project().aggregate()`
    /// Preserves the data format (rows stay rows, batches stay batches).
    pub fn into_input(self) -> PipelineInput<'static> {
        match self {
            PipelineOutput::Rows(rows) => PipelineInput::RowsOwned(rows),
            PipelineOutput::Batch(batch) => PipelineInput::Batch(batch),
            PipelineOutput::Empty => PipelineInput::Empty,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    fn make_test_row(values: Vec<i64>) -> Row {
        Row::new(values.into_iter().map(SqlValue::Integer).collect::<Vec<_>>())
    }

    fn make_test_rows() -> Vec<Row> {
        vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(10.5)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Double(20.5)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Double(30.5)]),
        ]
    }

    // ============== PipelineInput Tests ==============

    #[test]
    fn test_pipeline_input_from_rows() {
        let rows = vec![make_test_row(vec![1, 2]), make_test_row(vec![3, 4])];

        let input = PipelineInput::from_rows(&rows);
        assert_eq!(input.row_count(), 2);
        assert!(!input.is_empty());
        assert!(input.is_rows());
        assert!(!input.is_batch());
    }

    #[test]
    fn test_pipeline_input_from_rows_owned() {
        let rows = vec![make_test_row(vec![1, 2])];
        let input = PipelineInput::from_rows_owned(rows);

        assert_eq!(input.row_count(), 1);
        assert!(input.is_rows());

        let output = input.into_rows();
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn test_pipeline_input_from_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let input = PipelineInput::from_batch(batch);
        assert_eq!(input.row_count(), 3);
        assert!(!input.is_empty());
        assert!(input.is_batch());
        assert!(!input.is_rows());
    }

    #[test]
    fn test_pipeline_input_as_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let input = PipelineInput::from_batch(batch);
        let batch_ref = input.as_batch();
        assert!(batch_ref.is_some());
        assert_eq!(batch_ref.unwrap().row_count(), 3);
    }

    #[test]
    fn test_pipeline_input_into_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let input = PipelineInput::from_batch(batch);
        let batch_owned = input.into_batch();
        assert!(batch_owned.is_some());
        assert_eq!(batch_owned.unwrap().row_count(), 3);
    }

    #[test]
    fn test_pipeline_input_batch_into_rows() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let input = PipelineInput::from_batch(batch);
        let output_rows = input.into_rows();

        // Verify row count matches
        assert_eq!(output_rows.len(), 3);

        // Verify first row values
        assert_eq!(output_rows[0].get(0), Some(&SqlValue::Integer(1)));
        assert_eq!(output_rows[0].get(1), Some(&SqlValue::Double(10.5)));
    }

    #[test]
    fn test_pipeline_input_as_rows_returns_none_for_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let input = PipelineInput::from_batch(batch);
        assert!(input.as_rows().is_none());
    }

    #[test]
    fn test_pipeline_input_as_batch_returns_none_for_rows() {
        let rows = make_test_rows();
        let input = PipelineInput::from_rows(&rows);
        assert!(input.as_batch().is_none());
    }

    #[test]
    fn test_pipeline_input_empty() {
        let input = PipelineInput::empty();
        assert_eq!(input.row_count(), 1); // Empty has one implicit row
        assert!(!input.is_empty());
        assert!(!input.is_rows());
        assert!(!input.is_batch());
    }

    #[test]
    fn test_pipeline_input_native_columnar() {
        let input = PipelineInput::native_columnar("test_table".to_string(), vec![0, 1, 2]);
        assert!(input.is_native_columnar());
        assert!(!input.is_rows());
        assert!(!input.is_batch());
        assert_eq!(input.row_count(), 0); // Unknown without table lookup
    }

    // ============== PipelineOutput Tests ==============

    #[test]
    fn test_pipeline_output_from_rows() {
        let rows = vec![make_test_row(vec![1, 2])];
        let output = PipelineOutput::from_rows(rows);

        assert_eq!(output.row_count(), 1);
        assert!(!output.is_empty());
        assert!(output.is_rows());
        assert!(!output.is_batch());
    }

    #[test]
    fn test_pipeline_output_from_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let output = PipelineOutput::from_batch(batch);
        assert_eq!(output.row_count(), 3);
        assert!(!output.is_empty());
        assert!(output.is_batch());
        assert!(!output.is_rows());
    }

    #[test]
    fn test_pipeline_output_as_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let output = PipelineOutput::from_batch(batch);
        let batch_ref = output.as_batch();
        assert!(batch_ref.is_some());
        assert_eq!(batch_ref.unwrap().row_count(), 3);
    }

    #[test]
    fn test_pipeline_output_into_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let output = PipelineOutput::from_batch(batch);
        let batch_owned = output.into_batch();
        assert!(batch_owned.is_some());
        assert_eq!(batch_owned.unwrap().row_count(), 3);
    }

    #[test]
    fn test_pipeline_output_batch_into_rows() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let output = PipelineOutput::from_batch(batch);
        let output_rows = output.into_rows();

        assert_eq!(output_rows.len(), 3);
        assert_eq!(output_rows[0].get(0), Some(&SqlValue::Integer(1)));
    }

    #[test]
    fn test_pipeline_output_as_rows_returns_none_for_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let output = PipelineOutput::from_batch(batch);
        assert!(output.as_rows().is_none());
    }

    #[test]
    fn test_pipeline_output_chaining_rows() {
        let rows = vec![make_test_row(vec![1, 2])];
        let output = PipelineOutput::from_rows(rows);

        let input = output.into_input();
        assert_eq!(input.row_count(), 1);
        assert!(input.is_rows());
    }

    #[test]
    fn test_pipeline_output_chaining_batch() {
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let output = PipelineOutput::from_batch(batch);
        let input = output.into_input();

        // Batch should stay as batch through chaining
        assert_eq!(input.row_count(), 3);
        assert!(input.is_batch());
    }

    #[test]
    fn test_empty_pipeline_output() {
        let output = PipelineOutput::empty();
        assert!(output.is_empty());
        assert_eq!(output.row_count(), 0);
        assert_eq!(output.into_rows().len(), 0);
    }

    #[test]
    fn test_pipeline_output_default() {
        let output = PipelineOutput::default();
        assert!(output.is_empty());
    }

    // ============== Roundtrip Tests ==============

    #[test]
    fn test_rows_to_batch_to_rows_roundtrip() {
        let original_rows = make_test_rows();

        // Convert rows -> batch
        let batch = ColumnarBatch::from_rows(&original_rows).unwrap();
        let input = PipelineInput::from_batch(batch);

        // Convert batch -> rows
        let converted_rows = input.into_rows();

        assert_eq!(converted_rows.len(), original_rows.len());
        for (orig, conv) in original_rows.iter().zip(converted_rows.iter()) {
            assert_eq!(orig.len(), conv.len());
            for i in 0..orig.len() {
                assert_eq!(orig.get(i), conv.get(i));
            }
        }
    }

    #[test]
    fn test_pipeline_chaining_preserves_format() {
        // Test that chaining preserves row format
        let rows = make_test_rows();
        let output_rows = PipelineOutput::from_rows(rows);
        let input_from_rows = output_rows.into_input();
        assert!(input_from_rows.is_rows());

        // Test that chaining preserves batch format
        let rows = make_test_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        let output_batch = PipelineOutput::from_batch(batch);
        let input_from_batch = output_batch.into_input();
        assert!(input_from_batch.is_batch());

        // Test that chaining empty stays empty
        let output_empty = PipelineOutput::empty();
        let input_from_empty = output_empty.into_input();
        assert!(matches!(input_from_empty, PipelineInput::Empty));
    }
}
