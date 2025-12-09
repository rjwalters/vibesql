//! Delta computation for subscription updates
//!
//! This module provides types and functions for computing the differences
//! between result sets, enabling efficient incremental updates to subscribers.

use super::hash::hash_row;
use super::types::{SubscriptionId, SubscriptionUpdate};

// ============================================================================
// Partial Row Delta
// ============================================================================

/// A partial row update containing only changed columns plus primary key columns
///
/// Used for efficient updates when only a subset of columns have changed.
/// The `column_indices` field indicates which columns are present in `values`.
#[derive(Debug, Clone)]
pub struct PartialRowDelta {
    /// Indices of columns that are included in this partial update
    /// (primary key columns + changed columns, sorted)
    pub column_indices: Vec<usize>,
    /// Old values for the included columns
    pub old_values: Vec<vibesql_types::SqlValue>,
    /// New values for the included columns
    pub new_values: Vec<vibesql_types::SqlValue>,
}

impl PartialRowDelta {
    /// Create a new partial row delta from old and new rows
    ///
    /// # Arguments
    /// * `old_row` - The previous row values
    /// * `new_row` - The current row values
    /// * `pk_columns` - Primary key column indices (always included)
    ///
    /// # Returns
    /// * `Some(PartialRowDelta)` if the rows differ
    /// * `None` if the rows are identical
    pub fn from_rows(
        old_row: &crate::Row,
        new_row: &crate::Row,
        pk_columns: &[usize],
    ) -> Option<Self> {
        if old_row.values.len() != new_row.values.len() {
            return None;
        }

        // Find changed columns
        let mut changed_columns = Vec::new();
        for (idx, (old_val, new_val)) in
            old_row.values.iter().zip(new_row.values.iter()).enumerate()
        {
            if old_val != new_val {
                changed_columns.push(idx);
            }
        }

        // If no columns changed, return None
        if changed_columns.is_empty() {
            return None;
        }

        // Build included columns: PK columns + changed columns, sorted
        let mut column_indices: Vec<usize> = pk_columns.to_vec();
        for &idx in &changed_columns {
            if !column_indices.contains(&idx) {
                column_indices.push(idx);
            }
        }
        column_indices.sort_unstable();

        // Extract values for included columns
        let old_values: Vec<vibesql_types::SqlValue> =
            column_indices.iter().map(|&idx| old_row.values[idx].clone()).collect();
        let new_values: Vec<vibesql_types::SqlValue> =
            column_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

        Some(Self { column_indices, old_values, new_values })
    }
}

// ============================================================================
// Delta Computation
// ============================================================================

/// Compute delta between old and new result sets
///
/// This function compares two result sets and produces a delta update
/// containing the inserts, updates, and deletes needed to transform
/// the old result into the new result.
///
/// # Algorithm
///
/// Uses row hashing to efficiently detect changes:
/// - Rows in new but not in old are inserts
/// - Rows in old but not in new are deletes
/// - Updates are not detected in this implementation (would appear as delete + insert)
///
/// For proper update detection, use `compute_delta_with_pk()` with primary key information.
///
/// # Returns
///
/// Returns `Some(SubscriptionUpdate::Delta)` if there are changes,
/// or `None` if the result sets are identical.
pub fn compute_delta(
    subscription_id: SubscriptionId,
    old: &[crate::Row],
    new: &[crate::Row],
) -> Option<SubscriptionUpdate> {
    // Delegate to PK-based implementation with empty pk_columns for backward compatibility
    compute_delta_with_pk(subscription_id, old, new, &[])
}

/// Compute delta between old and new result sets using primary key columns
///
/// This function compares two result sets and produces a delta update
/// containing the inserts, updates, and deletes needed to transform
/// the old result into the new result.
///
/// # Algorithm
///
/// When `pk_columns` is provided and non-empty:
/// - Builds a lookup map of old rows indexed by their PK values
/// - For each new row, looks up by PK to determine if it's an INSERT or UPDATE
/// - Rows in old but not in new (by PK) are DELETEs
/// - Rows with same PK but different content are UPDATEs
///
/// When `pk_columns` is empty, falls back to hash-based matching:
/// - Rows in new but not in old are inserts
/// - Rows in old but not in new are deletes
/// - Updates appear as delete + insert pairs
///
/// # Arguments
///
/// * `subscription_id` - The subscription ID for the delta update
/// * `old` - Previous result set rows
/// * `new` - Current result set rows
/// * `pk_columns` - Indices of primary key columns in the result set
///
/// # Returns
///
/// Returns `Some(SubscriptionUpdate::Delta)` if there are changes,
/// or `None` if the result sets are identical.
pub fn compute_delta_with_pk(
    subscription_id: SubscriptionId,
    old: &[crate::Row],
    new: &[crate::Row],
    pk_columns: &[usize],
) -> Option<SubscriptionUpdate> {
    use std::collections::HashMap;

    // If no PK columns provided, use hash-based matching
    if pk_columns.is_empty() {
        return compute_delta_hash_based(subscription_id, old, new);
    }

    // Validate PK columns are within bounds for both old and new rows
    let valid_pk = old.iter().chain(new.iter()).all(|row| {
        pk_columns.iter().all(|&idx| idx < row.values.len())
    });

    if !valid_pk {
        // Fall back to hash-based if PK columns are out of bounds
        return compute_delta_hash_based(subscription_id, old, new);
    }

    // Build a lookup map of old rows indexed by PK values
    // Key: PK values as a vector, Value: list of (index, row) for handling duplicates
    let mut old_by_pk: HashMap<Vec<&vibesql_types::SqlValue>, Vec<(usize, &crate::Row)>> =
        HashMap::new();
    for (idx, row) in old.iter().enumerate() {
        let pk_values: Vec<&vibesql_types::SqlValue> =
            pk_columns.iter().map(|&i| &row.values[i]).collect();
        old_by_pk.entry(pk_values).or_default().push((idx, row));
    }

    let mut inserts = Vec::new();
    let mut updates: Vec<(crate::Row, crate::Row)> = Vec::new();
    let mut matched_old_indices = std::collections::HashSet::new();

    // Process each new row
    for new_row in new {
        let pk_values: Vec<&vibesql_types::SqlValue> =
            pk_columns.iter().map(|&i| &new_row.values[i]).collect();

        if let Some(old_rows) = old_by_pk.get_mut(&pk_values) {
            // Found matching PK in old - check if it's an update or unchanged
            if let Some((old_idx, old_row)) = old_rows.pop() {
                matched_old_indices.insert(old_idx);

                // Compare full row content to detect changes
                if old_row.values != new_row.values {
                    // Content differs - this is an UPDATE
                    updates.push((old_row.clone(), new_row.clone()));
                }
                // If content is identical, row is unchanged - no action needed
            } else {
                // No more old rows with this PK - treat as insert
                // (handles case where new has more duplicates than old)
                inserts.push(new_row.clone());
            }
        } else {
            // No matching PK in old - this is an INSERT
            inserts.push(new_row.clone());
        }
    }

    // Find deletes: old rows that weren't matched
    let deletes: Vec<crate::Row> = old
        .iter()
        .enumerate()
        .filter(|(idx, _)| !matched_old_indices.contains(idx))
        .map(|(_, row)| row.clone())
        .collect();

    // If no changes, return None
    if inserts.is_empty() && updates.is_empty() && deletes.is_empty() {
        return None;
    }

    Some(SubscriptionUpdate::Delta { subscription_id, inserts, updates, deletes })
}

/// Hash-based delta computation (original algorithm)
///
/// This is the fallback when PK columns are not available.
fn compute_delta_hash_based(
    subscription_id: SubscriptionId,
    old: &[crate::Row],
    new: &[crate::Row],
) -> Option<SubscriptionUpdate> {
    use std::collections::HashMap;

    // Build hash maps for efficient lookup
    // Map from row hash -> (count, row reference)
    // We use count to handle duplicate rows correctly
    let mut old_map: HashMap<u64, Vec<&crate::Row>> = HashMap::new();
    for row in old {
        let hash = hash_row(row);
        old_map.entry(hash).or_default().push(row);
    }

    let mut new_map: HashMap<u64, Vec<&crate::Row>> = HashMap::new();
    for row in new {
        let hash = hash_row(row);
        new_map.entry(hash).or_default().push(row);
    }

    let mut inserts = Vec::new();
    let mut deletes = Vec::new();

    // Find inserts: rows in new but not in old (or with higher count in new)
    for (hash, new_rows) in &new_map {
        let old_rows = old_map.get(hash).map(|v| v.as_slice()).unwrap_or(&[]);

        // For each row in new that exceeds the count in old, it's an insert
        if new_rows.len() > old_rows.len() {
            for row in new_rows.iter().skip(old_rows.len()) {
                inserts.push((*row).clone());
            }
        }
    }

    // Find deletes: rows in old but not in new (or with higher count in old)
    for (hash, old_rows) in &old_map {
        let new_rows = new_map.get(hash).map(|v| v.as_slice()).unwrap_or(&[]);

        // For each row in old that exceeds the count in new, it's a delete
        if old_rows.len() > new_rows.len() {
            for row in old_rows.iter().skip(new_rows.len()) {
                deletes.push((*row).clone());
            }
        }
    }

    // If no changes, return None
    if inserts.is_empty() && deletes.is_empty() {
        return None;
    }

    // Updates are not detected in hash-based mode
    // A row update would appear as a delete of the old row + insert of the new row
    let updates = Vec::new();

    Some(SubscriptionUpdate::Delta { subscription_id, inserts, updates, deletes })
}
