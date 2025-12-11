//! Protocol type definitions
//!
//! Core types used across protocol messages including configuration,
//! errors, and field descriptions.

use std::io;

use thiserror::Error;

/// Wire protocol configuration for selective column updates
///
/// Sent by clients to override server-level selective update thresholds
/// on a per-subscription basis.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectiveUpdatesConfig {
    /// Enable/disable selective updates for this subscription
    pub enabled: Option<bool>,
    /// Minimum columns that must change to use selective update
    /// If fewer columns change, send full row instead
    pub min_changed_columns: Option<usize>,
    /// Maximum ratio of changed columns before falling back to full row
    /// E.g., 0.5 means if >50% of columns changed, send full row instead
    pub max_changed_columns_ratio: Option<f64>,
}

/// PostgreSQL protocol errors
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),

    #[error("Message too short")]
    MessageTooShort,

    #[error("Invalid message length: {0}")]
    InvalidMessageLength(i32),

    #[error("Invalid string encoding")]
    InvalidString,

    #[error("Unexpected message: {0}")]
    #[allow(dead_code)]
    UnexpectedMessage(String),
}

/// Subscription update type for SubscriptionData message
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubscriptionUpdateType {
    Full = 0,
    DeltaInsert = 1,
    DeltaUpdate = 2,
    DeltaDelete = 3,
    /// Selective column update - only changed columns are sent
    /// Used with SubscriptionPartialData message
    SelectiveUpdate = 4,
}

/// A partial row update containing only changed columns
///
/// Used for selective column updates to reduce bandwidth when only
/// a few columns change in a wide table.
#[derive(Debug, Clone, PartialEq)]
pub struct PartialRowUpdate {
    /// Total number of columns in the full row (for bitmap sizing)
    pub total_columns: u16,
    /// Bitmap indicating which columns are present (1 bit per column)
    /// Bit 0 = column 0, Bit 1 = column 1, etc.
    /// A set bit means the column value is included in `values`
    pub column_mask: Vec<u8>,
    /// Values for columns with set bits in column_mask, in column order
    /// None = NULL, Some(bytes) = value data
    pub values: Vec<Option<Vec<u8>>>,
}

impl PartialRowUpdate {
    /// Create a new partial row update
    ///
    /// # Arguments
    /// * `total_columns` - Total number of columns in the full row
    /// * `present_columns` - Indices of columns that are present in this update
    /// * `values` - Values for the present columns, in same order as present_columns
    pub fn new(total_columns: u16, present_columns: &[u16], values: Vec<Option<Vec<u8>>>) -> Self {
        debug_assert_eq!(present_columns.len(), values.len());

        // Create bitmap
        let bitmap_bytes = (total_columns as usize).div_ceil(8);
        let mut column_mask = vec![0u8; bitmap_bytes];

        for &col_idx in present_columns {
            if (col_idx as usize) < total_columns as usize {
                let byte_idx = col_idx as usize / 8;
                let bit_idx = col_idx as usize % 8;
                column_mask[byte_idx] |= 1 << bit_idx;
            }
        }

        Self { total_columns, column_mask, values }
    }

    /// Check if a column is present in this update
    pub fn is_column_present(&self, col_idx: u16) -> bool {
        if col_idx >= self.total_columns {
            return false;
        }
        let byte_idx = col_idx as usize / 8;
        let bit_idx = col_idx as usize % 8;
        if byte_idx < self.column_mask.len() {
            (self.column_mask[byte_idx] & (1 << bit_idx)) != 0
        } else {
            false
        }
    }

    /// Get the number of present columns
    pub fn present_column_count(&self) -> usize {
        self.column_mask.iter().map(|b| b.count_ones() as usize).sum()
    }
}

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Idle (not in a transaction)
    Idle,
    /// In a transaction block
    #[allow(dead_code)]
    InTransaction,
    /// In a failed transaction block
    #[allow(dead_code)]
    FailedTransaction,
}

impl TransactionStatus {
    pub fn as_byte(&self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::FailedTransaction => b'E',
        }
    }
}

/// Field description for row data
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attr_number: i16,
    pub data_type_oid: i32,
    pub data_type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16, // 0 = text, 1 = binary
}
