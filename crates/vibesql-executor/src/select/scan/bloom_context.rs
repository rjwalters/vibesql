//! Bloom filter context for scan-time filtering
//!
//! This module provides infrastructure for using Bloom filters during table scans
//! to skip rows that cannot possibly match join conditions. This is similar to
//! SQLite's `WHERE_BLOOMFILTER` optimization.
//!
//! # Architecture
//!
//! During multi-way join execution:
//! 1. After each join completes, we extract the values of the join key column
//!    that will be used to connect to the next table
//! 2. We build a Bloom filter from those values
//! 3. When scanning the next table, we check each row against the Bloom filter
//!    BEFORE including it in the result
//!
//! This is more efficient than post-scan filtering because:
//! - We never allocate memory for rows that won't match
//! - We avoid cloning rows that will be discarded
//! - The Bloom filter check is O(1) per row with excellent cache behavior

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use vibesql_types::SqlValue;

use crate::select::join::BloomFilter;

/// Context for Bloom filter-based scan filtering
///
/// Contains a Bloom filter and the column index to check against.
#[derive(Debug)]
pub struct BloomFilterScanContext {
    /// The Bloom filter containing valid join key values
    pub filter: BloomFilter,
    /// Name of the column to check (lowercase for case-insensitive matching)
    pub column_name: String,
}

impl BloomFilterScanContext {
    /// Create a new Bloom filter scan context
    pub fn new(filter: BloomFilter, column_name: String) -> Self {
        Self { filter, column_name: column_name.to_lowercase() }
    }

    /// Check if a value might be in the filter
    #[inline]
    #[allow(dead_code)] // Infrastructure for future Bloom filter scan optimization
    pub fn might_contain(&self, value: &SqlValue) -> bool {
        let hash = hash_value(value);
        self.filter.might_contain_hash(hash)
    }
}

/// Build a Bloom filter from the values of a specific column in a result set.
///
/// # Arguments
/// * `rows` - The rows to extract values from
/// * `col_index` - The column index to extract values from
/// * `false_positive_rate` - Target false positive rate (e.g., 0.01 for 1%)
///
/// Returns `None` if there are no rows.
pub fn build_bloom_filter_from_rows(
    rows: &[vibesql_storage::Row],
    col_index: usize,
    false_positive_rate: f64,
) -> Option<BloomFilter> {
    if rows.is_empty() {
        return None;
    }

    let mut bloom = BloomFilter::new(rows.len(), false_positive_rate);

    for row in rows {
        if let Some(value) = row.values.get(col_index) {
            let hash = hash_value(value);
            bloom.insert_hash(hash);
        }
    }

    Some(bloom)
}

/// Hash a SqlValue for Bloom filter operations.
///
/// Uses AHash for fast, high-quality hashing consistent with the rest of the system.
#[inline]
pub fn hash_value(value: &SqlValue) -> u64 {
    let mut hasher = ahash::AHasher::default();

    match value {
        SqlValue::Integer(i) => i.hash(&mut hasher),
        SqlValue::Bigint(i) => i.hash(&mut hasher),
        SqlValue::Smallint(i) => i.hash(&mut hasher),
        SqlValue::Unsigned(u) => u.hash(&mut hasher),
        SqlValue::Numeric(f) => f.to_bits().hash(&mut hasher),
        SqlValue::Float(f) => f.to_bits().hash(&mut hasher),
        SqlValue::Real(f) => f.to_bits().hash(&mut hasher),
        SqlValue::Double(f) => f.to_bits().hash(&mut hasher),
        SqlValue::Character(s) => s.as_str().hash(&mut hasher),
        SqlValue::Varchar(s) => s.as_str().hash(&mut hasher),
        SqlValue::Boolean(b) => b.hash(&mut hasher),
        SqlValue::Null => 0u64.hash(&mut hasher),
        SqlValue::Date(d) => d.hash(&mut hasher),
        SqlValue::Time(t) => t.hash(&mut hasher),
        SqlValue::Timestamp(ts) => ts.hash(&mut hasher),
        SqlValue::Interval(i) => i.hash(&mut hasher),
        SqlValue::Vector(v) => {
            for f in v {
                hasher.write_u32(f.to_bits());
            }
        }
        SqlValue::Blob(b) => b.hash(&mut hasher),
    }

    hasher.finish()
}

/// Extract equijoin key column information from a condition.
///
/// For a condition like `t1.col_a = t2.col_b`, this returns information about which columns
/// from which tables are being compared.
///
/// Returns `(left_table, left_column, right_table, right_column)` if the condition is a simple
/// equijoin between two columns.
pub fn extract_equijoin_columns(
    condition: &vibesql_ast::Expression,
    column_to_table: &HashMap<String, String>,
) -> Option<(String, String, String, String)> {
    use vibesql_ast::{BinaryOperator, Expression};

    match condition {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            let (left_table, left_col) = extract_column_table_and_name(left, column_to_table)?;
            let (right_table, right_col) = extract_column_table_and_name(right, column_to_table)?;

            // Ensure the two columns are from different tables
            if left_table.to_lowercase() != right_table.to_lowercase() {
                Some((left_table, left_col, right_table, right_col))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract the table name and column name from a column reference expression.
fn extract_column_table_and_name(
    expr: &vibesql_ast::Expression,
    column_to_table: &HashMap<String, String>,
) -> Option<(String, String)> {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_some() => {
            let t = col_id.table_canonical().unwrap();
            let column = col_id.column_canonical();
            Some((t.to_lowercase(), column.to_lowercase()))
        }
        Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() => {
            // Try to resolve unqualified column using schema-based mapping
            let col_lower = col_id.column_canonical().to_lowercase();
            column_to_table.get(&col_lower).map(|t| (t.to_lowercase(), col_lower))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_storage::Row;

    #[test]
    fn test_build_bloom_filter_from_rows() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("a".into())]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("b".into())]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("c".into())]),
        ];

        // Build filter from first column (integers)
        let filter = build_bloom_filter_from_rows(&rows, 0, 0.01).unwrap();

        // Values that were inserted should pass
        assert!(filter.might_contain_hash(hash_value(&SqlValue::Integer(1))));
        assert!(filter.might_contain_hash(hash_value(&SqlValue::Integer(2))));
        assert!(filter.might_contain_hash(hash_value(&SqlValue::Integer(3))));

        // Values that were NOT inserted should (usually) fail
        // Note: Bloom filters can have false positives, but 100 is very unlikely to match
        let mut false_positives = 0;
        for i in 100..200 {
            if filter.might_contain_hash(hash_value(&SqlValue::Integer(i))) {
                false_positives += 1;
            }
        }
        // With 1% FPR and 100 tests, expect ~1 false positive
        assert!(false_positives < 10, "Too many false positives: {}", false_positives);
    }

    #[test]
    fn test_bloom_filter_scan_context() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];

        let filter = build_bloom_filter_from_rows(&rows, 0, 0.01).unwrap();
        let ctx = BloomFilterScanContext::new(filter, "id".to_string());

        // Values in the filter should pass
        assert!(ctx.might_contain(&SqlValue::Integer(10)));
        assert!(ctx.might_contain(&SqlValue::Integer(20)));
        assert!(ctx.might_contain(&SqlValue::Integer(30)));
    }

    #[test]
    fn test_hash_value_consistency() {
        // Same values should hash the same
        let v1 = SqlValue::Integer(42);
        let v2 = SqlValue::Integer(42);
        assert_eq!(hash_value(&v1), hash_value(&v2));

        // Different values should (usually) hash differently
        let v3 = SqlValue::Integer(43);
        assert_ne!(hash_value(&v1), hash_value(&v3));
    }
}
