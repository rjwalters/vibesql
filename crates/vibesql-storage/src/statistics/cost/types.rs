//! Cost estimation types and utilities
//!
//! This module contains types used by the cost estimator:
//! - `AccessMethod` - Represents chosen access methods for queries
//! - `TableIndexInfo` - Metadata about table indexes for DML cost estimation
//! - Row size estimation functions for WAL cost scaling

use vibesql_types::DataType;

/// Base row size in bytes for WAL cost scaling (64 bytes)
/// This represents a minimal row with a few small columns.
/// Rows larger than this will have proportionally higher WAL costs.
pub const BASE_ROW_SIZE: f64 = 64.0;

/// Maximum WAL size scaling factor (10x)
/// Caps the row size multiplier to prevent extreme cost estimates
/// for tables with very large rows.
pub const MAX_WAL_SIZE_FACTOR: f64 = 10.0;

/// Represents the chosen access method for a query
#[derive(Debug, Clone, PartialEq)]
pub enum AccessMethod {
    /// Sequential scan of entire table
    TableScan {
        /// Estimated cost of this access method
        estimated_cost: f64,
    },

    /// Index scan with optional filtering
    IndexScan {
        /// Estimated cost of this access method
        estimated_cost: f64,
        /// Estimated number of rows to be returned
        estimated_rows: usize,
    },
}

impl AccessMethod {
    /// Get the estimated cost of this access method
    pub fn cost(&self) -> f64 {
        match self {
            AccessMethod::TableScan { estimated_cost } => *estimated_cost,
            AccessMethod::IndexScan { estimated_cost, .. } => *estimated_cost,
        }
    }

    /// Check if this is an index scan
    pub fn is_index_scan(&self) -> bool {
        matches!(self, AccessMethod::IndexScan { .. })
    }
}

/// Metadata about table indexes for DML cost estimation
#[derive(Debug, Clone, Default)]
pub struct TableIndexInfo {
    /// Number of hash indexes (PK + unique constraints)
    pub hash_index_count: usize,
    /// Number of user-defined B-tree indexes
    pub btree_index_count: usize,
    /// Whether the table uses native columnar storage
    pub is_native_columnar: bool,
    /// Current ratio of deleted rows (0.0 to 1.0)
    /// Used to estimate compaction probability
    pub deleted_ratio: f64,
    /// Average row size in bytes (estimated from schema)
    /// Used to scale WAL cost based on actual row size.
    /// Defaults to BASE_ROW_SIZE (64 bytes) if unknown.
    pub avg_row_size: usize,
}

impl TableIndexInfo {
    /// Create new table index info
    pub fn new(
        hash_index_count: usize,
        btree_index_count: usize,
        is_native_columnar: bool,
        deleted_ratio: f64,
        avg_row_size: usize,
    ) -> Self {
        Self {
            hash_index_count,
            btree_index_count,
            is_native_columnar,
            deleted_ratio,
            avg_row_size,
        }
    }

    /// Calculate the WAL size scaling factor based on average row size.
    ///
    /// The factor is clamped between 1.0 (for small rows <= BASE_ROW_SIZE)
    /// and MAX_WAL_SIZE_FACTOR (for very large rows).
    ///
    /// # Returns
    /// A multiplier to apply to the per-row WAL write cost.
    #[inline]
    pub fn wal_size_factor(&self) -> f64 {
        let size_factor = self.avg_row_size as f64 / BASE_ROW_SIZE;
        size_factor.clamp(1.0, MAX_WAL_SIZE_FACTOR)
    }
}

/// Estimate the average row size in bytes for a given data type.
///
/// These are heuristic estimates used for WAL cost estimation:
/// - Fixed-size types: actual size
/// - Variable-size types: typical/average fill based on field definition
///
/// # Arguments
/// * `data_type` - The SQL data type
///
/// # Returns
/// Estimated size in bytes for storing a value of this type.
pub fn estimate_type_size(data_type: &DataType) -> usize {
    match data_type {
        // Boolean: 1 byte
        DataType::Boolean => 1,

        // Integer types
        DataType::Smallint => 2,
        DataType::Integer => 4,
        DataType::Bigint | DataType::Unsigned => 8,

        // Decimal/Numeric: 16 bytes (typical for DECIMAL storage)
        DataType::Numeric { .. } | DataType::Decimal { .. } => 16,

        // Floating point
        DataType::Real => 4,
        DataType::DoublePrecision => 8,
        DataType::Float { precision } => {
            if *precision <= 24 {
                4
            } else {
                8
            }
        }

        // Character types
        DataType::Character { length } => *length,
        DataType::Varchar { max_length } => {
            // For VARCHAR, use half the max length or 32 bytes, whichever is smaller
            match max_length {
                Some(len) => (*len / 2).min(32),
                None => 32, // Default for unbounded VARCHAR
            }
        }
        DataType::CharacterLargeObject => 64, // CLOB: heuristic average
        DataType::Name => 32,                 // NAME type: typically short identifiers

        // Date/time types
        DataType::Date => 4,
        DataType::Time { .. } => 8,
        DataType::Timestamp { .. } => 8,
        DataType::Interval { .. } => 16,

        // Binary types
        DataType::BinaryLargeObject => 128, // BLOB: heuristic average
        DataType::Bit { length } => {
            match length {
                Some(len) => (*len).div_ceil(8), // Convert bits to bytes
                None => 1,                       // Default BIT(1)
            }
        }

        // Vector types: dimensions * 8 bytes (f64 per dimension)
        DataType::Vector { dimensions } => *dimensions as usize * 8,

        // User-defined types: estimate as 64 bytes (unknown size)
        DataType::UserDefined { .. } => 64,

        // Null: 0 bytes (just a marker)
        DataType::Null => 0,
    }
}

/// Estimate the average row size for a table schema.
///
/// Sums the estimated size of each column plus a small overhead per row
/// for metadata (e.g., null bitmap, row header).
///
/// # Arguments
/// * `columns` - Slice of column data types
///
/// # Returns
/// Estimated average row size in bytes.
pub fn estimate_row_size(columns: &[DataType]) -> usize {
    // Per-row overhead: null bitmap + row header (estimate 8 bytes)
    const ROW_OVERHEAD: usize = 8;

    let column_size: usize = columns.iter().map(estimate_type_size).sum();
    (column_size + ROW_OVERHEAD).max(BASE_ROW_SIZE as usize)
}
