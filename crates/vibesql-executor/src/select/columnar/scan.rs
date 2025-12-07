//! Columnar scan - zero-copy column extraction from row slices

use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// Columnar scan over a slice of rows
///
/// Provides zero-copy access to individual columns without materializing
/// full Row objects. This is the foundation for columnar execution.
///
/// # Example
///
/// ```text
/// let scan = ColumnarScan::new(&rows);
/// let prices: Vec<&SqlValue> = scan.column(2).collect();
/// let quantities: Vec<&SqlValue> = scan.column(4).collect();
/// ```
pub struct ColumnarScan<'a> {
    rows: &'a [Row],
}

impl<'a> ColumnarScan<'a> {
    /// Create a new columnar scan over a row slice
    pub fn new(rows: &'a [Row]) -> Self {
        Self { rows }
    }

    /// Get an iterator over a specific column
    ///
    /// Returns references to SqlValues, avoiding clones.
    /// Returns None for rows that don't have the column index.
    pub fn column(&self, index: usize) -> ColumnIterator<'a> {
        ColumnIterator { rows: self.rows, column_index: index, row_index: 0 }
    }

    /// Get number of rows in this scan
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if scan is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get a row by index (returns reference, no clone)
    pub fn row(&self, index: usize) -> Option<&'a Row> {
        self.rows.get(index)
    }
}

/// Iterator over a single column's values
///
/// Yields `&SqlValue` references for zero-copy access.
pub struct ColumnIterator<'a> {
    rows: &'a [Row],
    column_index: usize,
    row_index: usize,
}

impl<'a> Iterator for ColumnIterator<'a> {
    type Item = Option<&'a SqlValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_index >= self.rows.len() {
            return None;
        }

        let row = &self.rows[self.row_index];
        self.row_index += 1;

        // Return reference to the value at column_index, or None if column doesn't exist
        Some(row.get(self.column_index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len() - self.row_index;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for ColumnIterator<'a> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_columnar_scan() {
        let rows = vec![
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Double(10.5),
                SqlValue::Varchar(arcstr::ArcStr::from("A")),
            ]),
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Double(20.5),
                SqlValue::Varchar(arcstr::ArcStr::from("B")),
            ]),
        ];

        let scan = ColumnarScan::new(&rows);
        assert_eq!(scan.len(), 2);

        // Test column extraction
        let col0: Vec<Option<&SqlValue>> = scan.column(0).collect();
        assert_eq!(col0.len(), 2);
        assert!(matches!(col0[0], Some(&SqlValue::Integer(1))));
        assert!(matches!(col0[1], Some(&SqlValue::Integer(2))));

        // Test column 1
        let col1: Vec<Option<&SqlValue>> = scan.column(1).collect();
        assert!(matches!(col1[0], Some(&SqlValue::Double(10.5))));
        assert!(matches!(col1[1], Some(&SqlValue::Double(20.5))));
    }

    #[test]
    fn test_column_iterator_size_hint() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
        ];

        let scan = ColumnarScan::new(&rows);
        let mut iter = scan.column(0);

        assert_eq!(iter.size_hint(), (3, Some(3)));
        iter.next();
        assert_eq!(iter.size_hint(), (2, Some(2)));
    }
}
