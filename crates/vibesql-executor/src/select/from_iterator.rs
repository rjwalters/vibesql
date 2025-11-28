//! Iterator-based FROM clause execution
//!
//! This module provides an iterator-based abstraction for FROM clause results,
//! enabling lazy evaluation and streaming of table scan results without materializing
//! all rows in memory.

//!
//! ## Design
//!
//! The iterator infrastructure supports different sources:
//! - Table scans (lazy, streaming directly from storage)
//! - Materialized results (JOINs, CTEs that need to be cached)
//! - Subqueries
//!
//! ## Benefits
//!
//! - **Memory efficiency**: Only materialize rows that pass WHERE filters and LIMIT
//! - **Performance**: O(LIMIT) complexity for simple queries instead of O(N)
//! - **Streaming**: Can process large tables without loading all rows

use vibesql_storage::Row;

/// Trait for iterating over rows from a FROM clause source
///
/// This trait abstracts over different row sources:
/// - Table scans (streaming from storage)
/// - Materialized vectors (from joins, CTEs)
/// - Transformed iterators (filtered, mapped)
#[allow(dead_code)]
pub trait RowIterator: Iterator<Item = Row> {
    /// Clone this iterator if possible (for operations that need multiple passes)
    ///
    /// Not all iterators can be cloned (e.g., consumable iterators).
    /// Returns None if cloning is not supported.
    fn try_clone(&self) -> Option<Box<dyn RowIterator>>;

    /// Hint about the number of rows (if known)
    ///
    /// Used for memory estimation and optimization decisions.
    /// Returns (lower_bound, optional_upper_bound).
    fn size_hint_rows(&self) -> (usize, Option<usize>) {
        self.size_hint()
    }
}

/// Iterator wrapper for materialized row vectors
///
/// This adapter allows Vec<Row> to implement RowIterator,
/// providing a migration path and supporting joins/CTEs that
/// need materialized results.
#[derive(Clone)]
pub struct VecRowIterator {
    rows: Vec<Row>,
    pos: usize,
}

impl VecRowIterator {
    /// Create a new iterator from a materialized vector
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows, pos: 0 }
    }

    /// Get the underlying rows (consuming the iterator)
    #[allow(dead_code)]
    pub fn into_vec(self) -> Vec<Row> {
        self.rows
    }

    /// Get a reference to the underlying rows
    pub fn as_slice(&self) -> &[Row] {
        &self.rows[self.pos..]
    }
}

impl Iterator for VecRowIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.rows.len() {
            let row = self.rows[self.pos].clone();
            self.pos += 1;
            Some(row)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len() - self.pos;
        (remaining, Some(remaining))
    }
}

impl RowIterator for VecRowIterator {
    fn try_clone(&self) -> Option<Box<dyn RowIterator>> {
        Some(Box::new(self.clone()))
    }

    fn size_hint_rows(&self) -> (usize, Option<usize>) {
        self.size_hint()
    }
}

/// Iterator for table scans that streams from storage
///
/// This iterator lazily yields rows from a table without materializing
/// them all in memory. It owns a clone of the storage vector but yields
/// rows one at a time.
#[derive(Clone)]
pub struct TableScanIterator {
    rows: Vec<Row>,
    pos: usize,
}

impl TableScanIterator {
    /// Create a new table scan iterator
    ///
    /// For now, this takes a Vec<Row> from table.scan().
    /// Future optimization: take a reference to the table and scan lazily.
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows, pos: 0 }
    }
}

impl Iterator for TableScanIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.rows.len() {
            let row = self.rows[self.pos].clone();
            self.pos += 1;
            Some(row)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len() - self.pos;
        (remaining, Some(remaining))
    }
}

impl RowIterator for TableScanIterator {
    fn try_clone(&self) -> Option<Box<dyn RowIterator>> {
        Some(Box::new(self.clone()))
    }

    fn size_hint_rows(&self) -> (usize, Option<usize>) {
        self.size_hint()
    }
}

/// Enum representing different iterator sources for FROM clause results
///
/// This enum allows us to switch between materialized and lazy execution
/// strategies based on the query type.
pub enum FromIterator {
    /// Materialized vector of rows (for JOINs, CTEs, subqueries)
    Materialized(VecRowIterator),

    /// Table scan iterator (lazy evaluation)
    TableScan(TableScanIterator),
}

impl FromIterator {
    /// Create from a materialized vector
    pub fn from_vec(rows: Vec<Row>) -> Self {
        Self::Materialized(VecRowIterator::new(rows))
    }

    /// Create from a table scan
    pub fn from_table_scan(rows: Vec<Row>) -> Self {
        Self::TableScan(TableScanIterator::new(rows))
    }

    /// Collect the iterator into a Vec
    ///
    /// This materializes all remaining rows.
    pub fn collect_vec(self) -> Vec<Row> {
        match self {
            Self::Materialized(iter) => iter.collect(),
            Self::TableScan(iter) => iter.collect(),
        }
    }

    /// Get a reference to the underlying rows without materializing via iterator
    ///
    /// This is a zero-cost operation that accesses the underlying Vec<Row> directly
    /// without triggering iteration or collection. This is critical for columnar
    /// execution performance, avoiding the 137ms row materialization bottleneck.
    pub fn as_slice(&self) -> &[Row] {
        match self {
            Self::Materialized(iter) => iter.as_slice(),
            Self::TableScan(iter) => &iter.rows[iter.pos..],
        }
    }

    /// Get size hint
    #[allow(dead_code)]
    pub fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Materialized(iter) => iter.size_hint(),
            Self::TableScan(iter) => iter.size_hint(),
        }
    }
}

impl Iterator for FromIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Materialized(iter) => iter.next(),
            Self::TableScan(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Materialized(iter) => iter.size_hint(),
            Self::TableScan(iter) => iter.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    fn test_row(id: i32) -> Row {
        Row::new(vec![SqlValue::Integer(id as i64)])
    }

    #[test]
    fn test_vec_row_iterator() {
        let rows = vec![test_row(1), test_row(2), test_row(3)];
        let mut iter = VecRowIterator::new(rows.clone());

        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.next().unwrap(), rows[0]);
        assert_eq!(iter.next().unwrap(), rows[1]);
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.next().unwrap(), rows[2]);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_table_scan_iterator() {
        let rows = vec![test_row(10), test_row(20)];
        let mut iter = TableScanIterator::new(rows.clone());

        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.next().unwrap(), rows[0]);
        assert_eq!(iter.next().unwrap(), rows[1]);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_from_iterator_materialized() {
        let rows = vec![test_row(1), test_row(2)];
        let mut iter = FromIterator::from_vec(rows.clone());

        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.next().unwrap(), rows[0]);
        assert_eq!(iter.next().unwrap(), rows[1]);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_from_iterator_table_scan() {
        let rows = vec![test_row(100)];
        let mut iter = FromIterator::from_table_scan(rows.clone());

        assert_eq!(iter.next().unwrap(), rows[0]);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_from_iterator_collect() {
        let rows = vec![test_row(1), test_row(2), test_row(3)];
        let iter = FromIterator::from_vec(rows.clone());

        let collected = iter.collect_vec();
        assert_eq!(collected, rows);
    }
}
