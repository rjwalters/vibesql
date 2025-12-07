//! Row Reference Implementation
//!
//! A row reference is a lightweight pointer to a row in source data,
//! avoiding data copying during intermediate query operations.

use std::sync::Arc;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// A lightweight reference to a row in source data
///
/// Instead of cloning entire rows during query processing, `RowReference`
/// stores just enough information to locate the row when materialization
/// is needed.
///
/// # Memory Comparison
///
/// For a row with 10 columns averaging 32 bytes each:
/// - Full Row: 320+ bytes (plus heap allocations for strings)
/// - RowReference: 16 bytes (table_id + row_index)
///
/// This is **20x** more memory efficient for intermediate results.
///
/// # Example
///
/// ```text
/// // Create references instead of copying rows
/// let refs: Vec<RowReference> = qualifying_indices
///     .iter()
///     .map(|&idx| RowReference::new(0, idx as u32))
///     .collect();
///
/// // Only materialize at output boundary
/// let output_rows: Vec<Row> = refs
///     .iter()
///     .map(|r| source_tables[r.table_id()].row(r.row_index()))
///     .collect();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowReference {
    /// Identifier for the source table (index into a table registry)
    table_id: u16,
    /// Row index within the source table
    row_index: u32,
}

impl RowReference {
    /// Create a new row reference
    #[inline]
    pub const fn new(table_id: u16, row_index: u32) -> Self {
        Self { table_id, row_index }
    }

    /// Get the table identifier
    #[inline]
    pub const fn table_id(&self) -> u16 {
        self.table_id
    }

    /// Get the row index within the table
    #[inline]
    pub const fn row_index(&self) -> u32 {
        self.row_index
    }

    /// Create a vector of row references for a range of rows
    #[inline]
    pub fn range(table_id: u16, start: u32, end: u32) -> Vec<Self> {
        (start..end).map(|idx| Self::new(table_id, idx)).collect()
    }

    /// Create row references from a selection vector
    #[inline]
    pub fn from_selection(table_id: u16, indices: &[u32]) -> Vec<Self> {
        indices.iter().map(|&idx| Self::new(table_id, idx)).collect()
    }
}

/// A pair of row references for join results
///
/// When joining tables, we track which rows from each side matched
/// without materializing the combined row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinedRowRef {
    /// Reference to the left (probe) side row
    pub left: RowReference,
    /// Reference to the right (build) side row
    /// None for LEFT OUTER join when there's no match
    pub right: Option<RowReference>,
}

impl JoinedRowRef {
    /// Create a new joined row reference (inner join match)
    #[inline]
    pub const fn matched(left: RowReference, right: RowReference) -> Self {
        Self { left, right: Some(right) }
    }

    /// Create a new joined row reference (left outer, no match)
    #[inline]
    pub const fn left_only(left: RowReference) -> Self {
        Self { left, right: None }
    }

    /// Check if this is a matched pair
    #[inline]
    pub const fn is_matched(&self) -> bool {
        self.right.is_some()
    }
}

/// A resolver that can materialize row references into actual rows
///
/// This trait abstracts over different source data formats (row-based, columnar)
/// allowing late materialization to work with various storage layouts.
pub trait RowResolver {
    /// Get a row by its reference
    fn resolve(&self, reference: &RowReference) -> Option<&Row>;

    /// Get a specific column value from a row reference
    fn resolve_column(&self, reference: &RowReference, column_idx: usize) -> Option<&SqlValue>;

    /// Batch resolve multiple row references
    ///
    /// Default implementation calls resolve() for each, but implementations
    /// can override for better performance with columnar storage.
    fn resolve_batch(&self, references: &[RowReference]) -> Vec<Option<&Row>> {
        references.iter().map(|r| self.resolve(r)).collect()
    }
}

/// Simple row resolver backed by a vector of rows
///
/// This is the most common case for row-based table scans.
pub struct VecRowResolver<'a> {
    table_id: u16,
    rows: &'a [Row],
}

impl<'a> VecRowResolver<'a> {
    /// Create a new resolver for a specific table
    pub fn new(table_id: u16, rows: &'a [Row]) -> Self {
        Self { table_id, rows }
    }
}

impl<'a> RowResolver for VecRowResolver<'a> {
    fn resolve(&self, reference: &RowReference) -> Option<&Row> {
        if reference.table_id == self.table_id {
            self.rows.get(reference.row_index as usize)
        } else {
            None
        }
    }

    fn resolve_column(&self, reference: &RowReference, column_idx: usize) -> Option<&SqlValue> {
        self.resolve(reference).and_then(|row| row.get(column_idx))
    }
}

/// Multi-table row resolver for joins
///
/// Resolves row references across multiple source tables.
pub struct MultiTableResolver<'a> {
    tables: Vec<(u16, &'a [Row])>,
}

impl<'a> MultiTableResolver<'a> {
    /// Create a new multi-table resolver
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    /// Register a table with the resolver
    pub fn add_table(&mut self, table_id: u16, rows: &'a [Row]) {
        self.tables.push((table_id, rows));
    }

    /// Create from a list of tables
    pub fn from_tables(tables: Vec<(u16, &'a [Row])>) -> Self {
        Self { tables }
    }
}

impl<'a> Default for MultiTableResolver<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> RowResolver for MultiTableResolver<'a> {
    fn resolve(&self, reference: &RowReference) -> Option<&Row> {
        for (table_id, rows) in &self.tables {
            if *table_id == reference.table_id {
                return rows.get(reference.row_index as usize);
            }
        }
        None
    }

    fn resolve_column(&self, reference: &RowReference, column_idx: usize) -> Option<&SqlValue> {
        self.resolve(reference).and_then(|row| row.get(column_idx))
    }
}

/// Owned row data for late materialization
///
/// When rows need to be owned (e.g., for cross-thread operations),
/// this wrapper allows efficient reference-based access while
/// maintaining ownership of the underlying data.
pub struct OwnedRowSource {
    table_id: u16,
    rows: Arc<Vec<Row>>,
}

impl OwnedRowSource {
    /// Create a new owned row source
    pub fn new(table_id: u16, rows: Vec<Row>) -> Self {
        Self { table_id, rows: Arc::new(rows) }
    }

    /// Get the table ID
    #[inline]
    pub fn table_id(&self) -> u16 {
        self.table_id
    }

    /// Get the number of rows
    #[inline]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get a reference to the rows
    #[inline]
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Create a row reference for an index
    #[inline]
    pub fn reference(&self, row_index: u32) -> RowReference {
        RowReference::new(self.table_id, row_index)
    }

    /// Resolve a row reference
    #[inline]
    pub fn resolve(&self, reference: &RowReference) -> Option<&Row> {
        if reference.table_id == self.table_id {
            self.rows.get(reference.row_index as usize)
        } else {
            None
        }
    }

    /// Clone the Arc (cheap, just bumps reference count)
    pub fn share(&self) -> Self {
        Self { table_id: self.table_id, rows: Arc::clone(&self.rows) }
    }
}

impl Clone for OwnedRowSource {
    fn clone(&self) -> Self {
        self.share()
    }
}

#[cfg(test)]
mod row_ref_tests {
    use super::*;

    #[test]
    fn test_row_reference_creation() {
        let r = RowReference::new(1, 42);
        assert_eq!(r.table_id(), 1);
        assert_eq!(r.row_index(), 42);
    }

    #[test]
    fn test_row_reference_range() {
        let refs = RowReference::range(0, 10, 15);
        assert_eq!(refs.len(), 5);
        assert_eq!(refs[0].row_index(), 10);
        assert_eq!(refs[4].row_index(), 14);
    }

    #[test]
    fn test_vec_resolver() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
        ];

        let resolver = VecRowResolver::new(0, &rows);

        let ref1 = RowReference::new(0, 1);
        let resolved = resolver.resolve(&ref1).unwrap();
        assert_eq!(resolved.get(0), Some(&SqlValue::Integer(2)));

        // Wrong table_id returns None
        let ref_wrong = RowReference::new(1, 1);
        assert!(resolver.resolve(&ref_wrong).is_none());
    }

    #[test]
    fn test_multi_table_resolver() {
        let table0 = [Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A"))])];
        let table1 = [Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B"))])];

        let resolver = MultiTableResolver::from_tables(vec![(0, &table0[..]), (1, &table1[..])]);

        let ref0 = RowReference::new(0, 0);
        let ref1 = RowReference::new(1, 0);

        assert_eq!(resolver.resolve_column(&ref0, 0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("A"))));
        assert_eq!(resolver.resolve_column(&ref1, 0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("B"))));
    }

    #[test]
    fn test_joined_row_ref() {
        let left = RowReference::new(0, 10);
        let right = RowReference::new(1, 20);

        let matched = JoinedRowRef::matched(left, right);
        assert!(matched.is_matched());
        assert_eq!(matched.left.row_index(), 10);
        assert_eq!(matched.right.unwrap().row_index(), 20);

        let left_only = JoinedRowRef::left_only(left);
        assert!(!left_only.is_matched());
    }

    #[test]
    fn test_owned_row_source() {
        let rows =
            vec![Row::new(vec![SqlValue::Integer(100)]), Row::new(vec![SqlValue::Integer(200)])];

        let source = OwnedRowSource::new(0, rows);
        assert_eq!(source.row_count(), 2);

        let r = source.reference(1);
        assert_eq!(source.resolve(&r).unwrap().get(0), Some(&SqlValue::Integer(200)));

        // Test sharing (Arc clone)
        let shared = source.share();
        assert_eq!(shared.row_count(), 2);
    }
}
