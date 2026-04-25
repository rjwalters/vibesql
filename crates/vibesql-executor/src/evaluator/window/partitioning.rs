//! Row partitioning for window functions
//!
//! Groups rows into partitions based on PARTITION BY expressions.

use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// A partition of rows for window function evaluation
#[derive(Debug, Clone)]
pub struct Partition {
    pub rows: Vec<Row>,
    /// Original indices of rows before partitioning/sorting
    pub original_indices: Vec<usize>,
    /// Column name to index mapping (for resolving named column references)
    pub column_map: std::collections::HashMap<String, usize>,
}

impl Partition {
    pub fn new(rows: Vec<Row>) -> Self {
        let original_indices = (0..rows.len()).collect();
        Self { rows, original_indices, column_map: std::collections::HashMap::new() }
    }

    pub fn with_indices(rows: Vec<Row>, original_indices: Vec<usize>) -> Self {
        Self { rows, original_indices, column_map: std::collections::HashMap::new() }
    }

    pub fn with_column_map(
        rows: Vec<Row>,
        original_indices: Vec<usize>,
        column_map: std::collections::HashMap<String, usize>,
    ) -> Self {
        Self { rows, original_indices, column_map }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get column index by name (case-insensitive)
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        // First try exact match
        if let Some(&idx) = self.column_map.get(name) {
            return Some(idx);
        }
        // Try lowercase match
        let lower = name.to_lowercase();
        self.column_map.get(&lower).copied()
    }
}

/// Partition rows by PARTITION BY expressions
///
/// Groups rows into partitions based on partition expressions.
/// If no PARTITION BY clause, all rows go into a single partition.
///
/// Partitions are ordered by their key values (using BTreeMap with SqlValue keys
/// for proper numeric/string ordering), matching SQLite's behavior.
pub fn partition_rows<F>(
    rows: Vec<Row>,
    partition_by: &Option<Vec<Expression>>,
    eval_fn: F,
) -> Vec<Partition>
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    partition_rows_with_collations(rows, partition_by, &[], eval_fn)
}

/// Partition rows by PARTITION BY expressions, respecting column collations
///
/// `collations` maps each PARTITION BY expression index to its resolved collation.
/// For NOCASE collation, partition keys are compared case-insensitively.
pub fn partition_rows_with_collations<F>(
    rows: Vec<Row>,
    partition_by: &Option<Vec<Expression>>,
    collations: &[Option<String>],
    eval_fn: F,
) -> Vec<Partition>
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    // If no PARTITION BY, return all rows in single partition
    let Some(partition_exprs) = partition_by else {
        return vec![Partition::new(rows)];
    };

    if partition_exprs.is_empty() {
        return vec![Partition::new(rows)];
    }

    // Group rows by partition key values
    // Use BTreeMap with SqlValue keys for proper value ordering (not string ordering)
    // This ensures Integer(3) < Integer(7) < Integer(11) as expected
    let mut partitions_map: std::collections::BTreeMap<Vec<SqlValue>, Vec<(usize, Row)>> =
        std::collections::BTreeMap::new();

    for (original_idx, row) in rows.into_iter().enumerate() {
        // Evaluate partition expressions for this row
        let mut partition_key = Vec::new();

        for (i, expr) in partition_exprs.iter().enumerate() {
            let value = eval_fn(expr, &row).unwrap_or(SqlValue::Null);
            // Apply collation to partition key for case-insensitive grouping
            let collation = collations.get(i).and_then(|c| c.as_deref());
            if let Some(coll) = collation {
                if coll.eq_ignore_ascii_case("nocase") {
                    let normalized = match &value {
                        SqlValue::Varchar(s) => {
                            SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase()))
                        }
                        SqlValue::Character(s) => {
                            SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase()))
                        }
                        other => other.clone(),
                    };
                    partition_key.push(normalized);
                    continue;
                }
            }
            partition_key.push(value);
        }

        partitions_map.entry(partition_key).or_default().push((original_idx, row));
    }

    // Convert BTreeMap to Vec<Partition>, preserving sorted partition key order
    partitions_map
        .into_values()
        .map(|rows_with_indices| {
            let (indices, rows): (Vec<_>, Vec<_>) = rows_with_indices.into_iter().unzip();
            Partition::with_indices(rows, indices)
        })
        .collect()
}
