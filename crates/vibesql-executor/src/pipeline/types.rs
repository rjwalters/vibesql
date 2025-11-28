//! Pipeline Input/Output Types
//!
//! This module defines polymorphic types for pipeline data that can represent
//! rows, batches, or columnar data depending on the execution strategy.

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
#[derive(Debug)]
pub enum PipelineInput<'a> {
    /// Borrowed slice of rows (zero-copy for row-oriented)
    Rows(&'a [Row]),

    /// Owned vector of rows (when ownership transfer is needed)
    RowsOwned(Vec<Row>),

    // Note: Columnar batch support is available via the select::columnar module
    // but we don't include it directly in PipelineInput to avoid coupling.
    // Instead, columnar execution paths convert to/from rows at boundaries.

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

    /// Create empty input for expression-only queries.
    #[inline]
    pub fn empty() -> Self {
        PipelineInput::Empty
    }

    /// Create native columnar input.
    #[inline]
    pub fn native_columnar(table_name: String, column_indices: Vec<usize>) -> Self {
        PipelineInput::NativeColumnar {
            table_name,
            column_indices,
        }
    }

    /// Convert to owned rows, consuming the input.
    ///
    /// This is used when the pipeline stage needs ownership of the data.
    pub fn into_rows(self) -> Vec<Row> {
        match self {
            PipelineInput::Rows(rows) => rows.to_vec(),
            PipelineInput::RowsOwned(rows) => rows,
            PipelineInput::NativeColumnar { .. } => {
                // Native columnar should be converted at the storage layer
                // This fallback returns empty for safety
                Vec::new()
            }
            PipelineInput::Empty => vec![Row::new(vec![])],
        }
    }

    /// Get the number of rows in the input.
    ///
    /// For native columnar, this may require a table lookup.
    pub fn row_count(&self) -> usize {
        match self {
            PipelineInput::Rows(rows) => rows.len(),
            PipelineInput::RowsOwned(rows) => rows.len(),
            PipelineInput::NativeColumnar { .. } => 0, // Unknown without table lookup
            PipelineInput::Empty => 1,
        }
    }

    /// Check if the input is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            PipelineInput::Rows(rows) => rows.is_empty(),
            PipelineInput::RowsOwned(rows) => rows.is_empty(),
            PipelineInput::NativeColumnar { .. } => false, // Assume non-empty
            PipelineInput::Empty => false, // Empty input has one implicit row
        }
    }

    /// Try to get rows as a slice (only works for row-based inputs).
    pub fn as_rows(&self) -> Option<&[Row]> {
        match self {
            PipelineInput::Rows(rows) => Some(rows),
            PipelineInput::RowsOwned(rows) => Some(rows),
            _ => None,
        }
    }
}

/// Polymorphic output from pipeline stages.
///
/// Output can be in row format, allowing efficient chaining of pipeline stages.
/// Note: Columnar batch conversions happen at execution strategy boundaries,
/// not within the pipeline abstraction.
#[derive(Debug)]
pub enum PipelineOutput {
    /// Row-based output (traditional format)
    Rows(Vec<Row>),

    /// Empty output (zero rows)
    Empty,
}

impl PipelineOutput {
    /// Create output from rows.
    #[inline]
    pub fn from_rows(rows: Vec<Row>) -> Self {
        PipelineOutput::Rows(rows)
    }

    /// Create empty output.
    #[inline]
    pub fn empty() -> Self {
        PipelineOutput::Empty
    }

    /// Convert to rows, consuming the output.
    ///
    /// This is the final conversion when returning results to the caller.
    pub fn into_rows(self) -> Vec<Row> {
        match self {
            PipelineOutput::Rows(rows) => rows,
            PipelineOutput::Empty => Vec::new(),
        }
    }

    /// Get the number of rows in the output.
    pub fn row_count(&self) -> usize {
        match self {
            PipelineOutput::Rows(rows) => rows.len(),
            PipelineOutput::Empty => 0,
        }
    }

    /// Check if the output is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            PipelineOutput::Rows(rows) => rows.is_empty(),
            PipelineOutput::Empty => true,
        }
    }

    /// Convert to PipelineInput for chaining pipeline stages.
    ///
    /// This enables fluent chaining: `filter().project().aggregate()`
    pub fn into_input(self) -> PipelineInput<'static> {
        match self {
            PipelineOutput::Rows(rows) => PipelineInput::RowsOwned(rows),
            PipelineOutput::Empty => PipelineInput::Empty,
        }
    }
}

impl Default for PipelineOutput {
    fn default() -> Self {
        PipelineOutput::Empty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    fn make_test_row(values: Vec<i64>) -> Row {
        Row::new(values.into_iter().map(SqlValue::Integer).collect())
    }

    #[test]
    fn test_pipeline_input_from_rows() {
        let rows = vec![make_test_row(vec![1, 2]), make_test_row(vec![3, 4])];

        let input = PipelineInput::from_rows(&rows);
        assert_eq!(input.row_count(), 2);
        assert!(!input.is_empty());
    }

    #[test]
    fn test_pipeline_input_into_rows() {
        let rows = vec![make_test_row(vec![1, 2])];
        let input = PipelineInput::from_rows_owned(rows);

        let output = input.into_rows();
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn test_pipeline_output_from_rows() {
        let rows = vec![make_test_row(vec![1, 2])];
        let output = PipelineOutput::from_rows(rows);

        assert_eq!(output.row_count(), 1);
        assert!(!output.is_empty());
    }

    #[test]
    fn test_pipeline_output_chaining() {
        let rows = vec![make_test_row(vec![1, 2])];
        let output = PipelineOutput::from_rows(rows);

        let input = output.into_input();
        assert_eq!(input.row_count(), 1);
    }

    #[test]
    fn test_empty_pipeline_output() {
        let output = PipelineOutput::empty();
        assert!(output.is_empty());
        assert_eq!(output.into_rows().len(), 0);
    }
}
