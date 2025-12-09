//! Selective column updates for efficient subscription notifications
//!
//! This module provides types and functions for computing partial row updates
//! that only include changed columns plus primary key columns, reducing bandwidth
//! for wide tables with few column changes.

use serde::{Deserialize, Serialize};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for selective column updates
///
/// This config controls when selective column updates (0xF7 messages) are used
/// instead of full row updates. Selective updates only send changed columns
/// plus primary key columns, reducing bandwidth for wide tables with few changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectiveColumnConfig {
    /// Enable selective column updates
    #[serde(default = "default_selective_enabled")]
    pub enabled: bool,
    /// Column indices that are primary key columns (always included)
    /// This is per-subscription and not configurable via config file
    #[serde(skip)]
    pub pk_columns: Vec<usize>,
    /// Minimum columns that must change to use selective update
    /// If fewer columns change, send full row instead
    #[serde(default = "default_min_changed_columns")]
    pub min_changed_columns: usize,
    /// Maximum ratio of changed columns before falling back to full row
    /// E.g., 0.5 means if >50% of columns changed, send full row instead
    #[serde(default = "default_max_changed_columns_ratio")]
    pub max_changed_columns_ratio: f64,
}

fn default_selective_enabled() -> bool {
    true
}

fn default_min_changed_columns() -> usize {
    1
}

fn default_max_changed_columns_ratio() -> f64 {
    0.5
}

impl Default for SelectiveColumnConfig {
    fn default() -> Self {
        Self {
            enabled: default_selective_enabled(),
            pk_columns: vec![0], // Assume first column is PK by default
            min_changed_columns: default_min_changed_columns(),
            max_changed_columns_ratio: default_max_changed_columns_ratio(),
        }
    }
}

impl SelectiveColumnConfig {
    /// Create a copy of this config with the specified pk_columns
    ///
    /// Useful for creating subscription-specific configs from a server-level template.
    pub fn with_pk_columns(&self, pk_columns: Vec<usize>) -> Self {
        Self {
            enabled: self.enabled,
            pk_columns,
            min_changed_columns: self.min_changed_columns,
            max_changed_columns_ratio: self.max_changed_columns_ratio,
        }
    }
}

// ============================================================================
// Column Diff
// ============================================================================

/// Result of column-level diff computation
#[derive(Debug, Clone)]
pub struct ColumnDiff {
    /// Indices of columns that changed
    pub changed_columns: Vec<usize>,
    /// Indices of columns to include (PK + changed)
    pub included_columns: Vec<usize>,
}

/// Compute which columns differ between two rows
///
/// # Arguments
/// * `old_row` - The previous row values
/// * `new_row` - The current row values
/// * `pk_columns` - Indices of primary key columns (always included even if unchanged)
///
/// # Returns
/// * `Some(ColumnDiff)` if rows have same column count and some columns differ
/// * `None` if rows have different column counts or are identical
pub fn compute_column_diff(
    old_row: &crate::Row,
    new_row: &crate::Row,
    pk_columns: &[usize],
) -> Option<ColumnDiff> {
    // Rows must have same number of columns
    if old_row.values.len() != new_row.values.len() {
        return None;
    }

    let mut changed_columns = Vec::new();

    // Compare each column
    for (idx, (old_val, new_val)) in old_row.values.iter().zip(new_row.values.iter()).enumerate() {
        if old_val != new_val {
            changed_columns.push(idx);
        }
    }

    // If no columns changed, return None
    if changed_columns.is_empty() {
        return None;
    }

    // Build included columns: PK columns + changed columns
    let mut included_columns: Vec<usize> = pk_columns.to_vec();
    for &idx in &changed_columns {
        if !included_columns.contains(&idx) {
            included_columns.push(idx);
        }
    }
    included_columns.sort_unstable();

    Some(ColumnDiff { changed_columns, included_columns })
}

// ============================================================================
// Selective Update Decision Functions
// ============================================================================

/// Determine if selective update should be used based on configuration
///
/// Returns true if:
/// - Selective updates are enabled
/// - Number of changed columns meets minimum threshold
/// - Changed column ratio doesn't exceed maximum
pub fn should_use_selective_update(
    diff: &ColumnDiff,
    total_columns: usize,
    config: &SelectiveColumnConfig,
) -> bool {
    if !config.enabled {
        return false;
    }

    // Check minimum changed columns
    if diff.changed_columns.len() < config.min_changed_columns {
        return false;
    }

    // Check maximum ratio
    let changed_ratio = diff.changed_columns.len() as f64 / total_columns as f64;
    if changed_ratio > config.max_changed_columns_ratio {
        return false;
    }

    true
}

/// Determine if selective update should be used, with metrics recording
pub fn should_use_selective_update_with_metrics(
    diff: &ColumnDiff,
    total_columns: usize,
    config: &SelectiveColumnConfig,
    metrics: Option<&crate::observability::metrics::ServerMetrics>,
) -> bool {
    if !config.enabled {
        if let Some(m) = metrics {
            m.record_partial_update_fallback("disabled");
        }
        return false;
    }

    // Check minimum changed columns
    if diff.changed_columns.len() < config.min_changed_columns {
        return false;
    }

    // Check maximum ratio
    let changed_ratio = diff.changed_columns.len() as f64 / total_columns as f64;
    if changed_ratio > config.max_changed_columns_ratio {
        if let Some(m) = metrics {
            m.record_partial_update_fallback("threshold_exceeded");
        }
        return false;
    }

    true
}

// ============================================================================
// Partial Row Update Creation
// ============================================================================

/// Create a partial row update from old and new rows
///
/// # Arguments
/// * `old_row` - The previous row values (wire format)
/// * `new_row` - The current row values (wire format)
/// * `pk_columns` - Primary key column indices
/// * `config` - Selective column configuration
///
/// # Returns
/// * `Some(PartialRowUpdate)` if selective update should be used
/// * `None` if full row should be sent instead
pub fn create_partial_row_update(
    old_row: &[Option<Vec<u8>>],
    new_row: &[Option<Vec<u8>>],
    pk_columns: &[usize],
    config: &SelectiveColumnConfig,
) -> Option<crate::protocol::messages::PartialRowUpdate> {
    // Rows must have same number of columns
    if old_row.len() != new_row.len() {
        return None;
    }

    let total_columns = new_row.len();
    let mut changed_columns = Vec::new();

    // Compare each column
    for (idx, (old_val, new_val)) in old_row.iter().zip(new_row.iter()).enumerate() {
        if old_val != new_val {
            changed_columns.push(idx);
        }
    }

    // If no columns changed, return None
    if changed_columns.is_empty() {
        return None;
    }

    // Check if we should use selective update
    let changed_ratio = changed_columns.len() as f64 / total_columns as f64;
    if !config.enabled || changed_ratio > config.max_changed_columns_ratio {
        return None;
    }

    // Build included columns: PK columns + changed columns, sorted
    let mut included_columns: Vec<usize> = pk_columns.to_vec();
    for &idx in &changed_columns {
        if !included_columns.contains(&idx) {
            included_columns.push(idx);
        }
    }
    included_columns.sort_unstable();

    // Extract values for included columns
    let values: Vec<Option<Vec<u8>>> =
        included_columns.iter().map(|&idx| new_row[idx].clone()).collect();

    // Convert to u16 for protocol
    let present_columns: Vec<u16> = included_columns.iter().map(|&idx| idx as u16).collect();

    Some(crate::protocol::messages::PartialRowUpdate::new(
        total_columns as u16,
        &present_columns,
        values,
    ))
}

/// Create a partial row update from old and new rows with metrics recording
///
/// # Arguments
/// * `old_row` - The previous row values (wire format)
/// * `new_row` - The current row values (wire format)
/// * `pk_columns` - Primary key column indices
/// * `config` - Selective column configuration
/// * `metrics` - Optional metrics for recording fallback reasons
///
/// # Returns
/// * `Some(PartialRowUpdate)` if selective update should be used
/// * `None` if full row should be sent instead
pub fn create_partial_row_update_with_metrics(
    old_row: &[Option<Vec<u8>>],
    new_row: &[Option<Vec<u8>>],
    pk_columns: &[usize],
    config: &SelectiveColumnConfig,
    metrics: Option<&crate::observability::metrics::ServerMetrics>,
) -> Option<crate::protocol::messages::PartialRowUpdate> {
    // Rows must have same number of columns
    if old_row.len() != new_row.len() {
        if let Some(m) = metrics {
            m.record_partial_update_fallback("row_count_mismatch");
        }
        return None;
    }

    let total_columns = new_row.len();
    let mut changed_columns = Vec::new();

    // Compare each column
    for (idx, (old_val, new_val)) in old_row.iter().zip(new_row.iter()).enumerate() {
        if old_val != new_val {
            changed_columns.push(idx);
        }
    }

    // If no columns changed, return None
    if changed_columns.is_empty() {
        if let Some(m) = metrics {
            m.record_partial_update_fallback("no_changes");
        }
        return None;
    }

    // Check if we should use selective update
    let changed_ratio = changed_columns.len() as f64 / total_columns as f64;
    if !config.enabled || changed_ratio > config.max_changed_columns_ratio {
        if let Some(m) = metrics {
            if !config.enabled {
                m.record_partial_update_fallback("disabled");
            } else {
                m.record_partial_update_fallback("threshold_exceeded");
            }
        }
        return None;
    }

    // Build included columns: PK columns + changed columns, sorted
    let mut included_columns: Vec<usize> = pk_columns.to_vec();
    for &idx in &changed_columns {
        if !included_columns.contains(&idx) {
            included_columns.push(idx);
        }
    }
    included_columns.sort_unstable();

    // Extract values for included columns
    let values: Vec<Option<Vec<u8>>> =
        included_columns.iter().map(|&idx| new_row[idx].clone()).collect();

    // Convert to u16 for protocol
    let present_columns: Vec<u16> = included_columns.iter().map(|&idx| idx as u16).collect();

    Some(crate::protocol::messages::PartialRowUpdate::new(
        total_columns as u16,
        &present_columns,
        values,
    ))
}
