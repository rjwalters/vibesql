//! Result hashing for subscription change detection
//!
//! This module provides functions for hashing result sets to detect changes
//! efficiently without storing and comparing full result sets.

use std::hash::{Hash, Hasher};

/// Compute a hash of result rows for change detection
///
/// This function hashes the row contents to detect changes without
/// storing the full result set. When the hash changes, we know the
/// results have changed and need to notify subscribers.
pub fn hash_rows(rows: &[crate::Row]) -> u64 {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();

    // Hash the number of rows first
    rows.len().hash(&mut hasher);

    // Hash each row's values
    for row in rows {
        for value in &row.values {
            // Hash the SqlValue - using debug format as a simple approach
            // In production, you'd implement proper hashing for SqlValue
            format!("{:?}", value).hash(&mut hasher);
        }
    }

    hasher.finish()
}

/// Compute a hash for a single row (for delta computation)
pub(crate) fn hash_row(row: &crate::Row) -> u64 {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    for value in &row.values {
        value.hash(&mut hasher);
    }
    hasher.finish()
}
