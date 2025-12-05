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
        match self {
            StorageError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            StorageError::ColumnCountMismatch { expected, actual } => {
                write!(f, "Column count mismatch: expected {}, got {}", expected, actual)
            }
            StorageError::ColumnIndexOutOfBounds { index } => {
                write!(f, "Column index {} out of bounds", index)
            }
            StorageError::CatalogError(msg) => write!(f, "Catalog error: {}", msg),
            StorageError::TransactionError(msg) => write!(f, "Transaction error: {}", msg),
            StorageError::RowNotFound => write!(f, "Row not found"),
            StorageError::IndexAlreadyExists(name) => write!(f, "Index '{}' already exists", name),
            StorageError::IndexNotFound(name) => write!(f, "Index '{}' not found", name),
            StorageError::ColumnNotFound { column_name, table_name } => {
                write!(f, "Column '{}' not found in table '{}'", column_name, table_name)
            }
            StorageError::NullConstraintViolation { column } => {
                write!(f, "NOT NULL constraint violation: column '{}' cannot be NULL", column)
            }
            StorageError::TypeMismatch { column, expected, actual } => {
                write!(
                    f,
                    "Type mismatch in column '{}': expected {}, got {}",
                    column, expected, actual
                )
            }
            StorageError::UniqueConstraintViolation(msg) => write!(f, "{}", msg),
            StorageError::InvalidIndexColumn(msg) => write!(f, "{}", msg),
            StorageError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
            StorageError::IoError(msg) => write!(f, "I/O error: {}", msg),
            StorageError::InvalidPageSize { expected, actual } => {
                write!(f, "Invalid page size: expected {}, got {}", expected, actual)
            }
            StorageError::InvalidPageId(page_id) => write!(f, "Invalid page ID: {}", page_id),
            StorageError::LockError(msg) => write!(f, "Lock error: {}", msg),
            StorageError::MemoryBudgetExceeded { used, budget } => {
                write!(
                    f,
                    "Memory budget exceeded: using {} bytes, budget is {} bytes",
                    used, budget
                )
            }
            StorageError::NoIndexToEvict => {
                write!(f, "No index available to evict (all indexes are already disk-backed)")
            }
            StorageError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for StorageError {}
