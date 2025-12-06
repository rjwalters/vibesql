// ============================================================================
// Errors
// ============================================================================

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq)]
pub enum StorageError {
    TableNotFound(String),
    ColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    ColumnIndexOutOfBounds {
        index: usize,
    },
    CatalogError(String),
    TransactionError(String),
    RowNotFound,
    IndexAlreadyExists(String),
    IndexNotFound(String),
    ColumnNotFound {
        column_name: String,
        table_name: String,
    },
    NullConstraintViolation {
        column: String,
    },
    TypeMismatch {
        column: String,
        expected: String,
        actual: String,
    },
    UniqueConstraintViolation(String),
    InvalidIndexColumn(String),
    NotImplemented(String),
    IoError(String),
    InvalidPageSize {
        expected: usize,
        actual: usize,
    },
    InvalidPageId(u64),
    LockError(String),
    MemoryBudgetExceeded {
        used: usize,
        budget: usize,
    },
    NoIndexToEvict,
    /// Generic error for other cases
    Other(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use vibesql_l10n::vibe_msg;
        match self {
            StorageError::TableNotFound(name) => {
                write!(f, "{}", vibe_msg!("storage-table-not-found", name = name.as_str()))
            }
            StorageError::ColumnCountMismatch { expected, actual } => {
                write!(f, "{}", vibe_msg!("storage-column-count-mismatch", expected = *expected as i64, actual = *actual as i64))
            }
            StorageError::ColumnIndexOutOfBounds { index } => {
                write!(f, "{}", vibe_msg!("storage-column-index-out-of-bounds", index = *index as i64))
            }
            StorageError::CatalogError(msg) => {
                write!(f, "{}", vibe_msg!("storage-catalog-error", message = msg.as_str()))
            }
            StorageError::TransactionError(msg) => {
                write!(f, "{}", vibe_msg!("storage-transaction-error", message = msg.as_str()))
            }
            StorageError::RowNotFound => {
                write!(f, "{}", vibe_msg!("storage-row-not-found"))
            }
            StorageError::IndexAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("storage-index-already-exists", name = name.as_str()))
            }
            StorageError::IndexNotFound(name) => {
                write!(f, "{}", vibe_msg!("storage-index-not-found", name = name.as_str()))
            }
            StorageError::ColumnNotFound { column_name, table_name } => {
                write!(f, "{}", vibe_msg!("storage-column-not-found", column_name = column_name.as_str(), table_name = table_name.as_str()))
            }
            StorageError::NullConstraintViolation { column } => {
                write!(f, "{}", vibe_msg!("storage-null-constraint-violation", column = column.as_str()))
            }
            StorageError::TypeMismatch { column, expected, actual } => {
                write!(f, "{}", vibe_msg!("storage-type-mismatch", column = column.as_str(), expected = expected.as_str(), actual = actual.as_str()))
            }
            StorageError::UniqueConstraintViolation(msg) => {
                write!(f, "{}", vibe_msg!("storage-unique-constraint-violation", message = msg.as_str()))
            }
            StorageError::InvalidIndexColumn(msg) => {
                write!(f, "{}", vibe_msg!("storage-invalid-index-column", message = msg.as_str()))
            }
            StorageError::NotImplemented(msg) => {
                write!(f, "{}", vibe_msg!("storage-not-implemented", message = msg.as_str()))
            }
            StorageError::IoError(msg) => {
                write!(f, "{}", vibe_msg!("storage-io-error", message = msg.as_str()))
            }
            StorageError::InvalidPageSize { expected, actual } => {
                write!(f, "{}", vibe_msg!("storage-invalid-page-size", expected = *expected as i64, actual = *actual as i64))
            }
            StorageError::InvalidPageId(page_id) => {
                write!(f, "{}", vibe_msg!("storage-invalid-page-id", page_id = *page_id as i64))
            }
            StorageError::LockError(msg) => {
                write!(f, "{}", vibe_msg!("storage-lock-error", message = msg.as_str()))
            }
            StorageError::MemoryBudgetExceeded { used, budget } => {
                write!(f, "{}", vibe_msg!("storage-memory-budget-exceeded", used = *used as i64, budget = *budget as i64))
            }
            StorageError::NoIndexToEvict => {
                write!(f, "{}", vibe_msg!("storage-no-index-to-evict"))
            }
            StorageError::Other(msg) => {
                write!(f, "{}", vibe_msg!("storage-other", message = msg.as_str()))
            }
        }
    }
}

impl std::error::Error for StorageError {}
