// ============================================================================
// Index Metadata - Types and helpers for index definitions
// ============================================================================

use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use parking_lot::Mutex;

#[cfg(target_arch = "wasm32")]
use std::sync::Mutex;

use vibesql_ast::IndexColumn;
use vibesql_types::SqlValue;

use crate::btree::BTreeIndex;
use crate::page::PageManager;
use crate::StorageError;

use super::hnsw::HnswIndex;
use super::ivfflat::IVFFlatIndex;

/// Normalize an index name to uppercase for case-insensitive comparison
/// This follows SQL standard identifier rules
pub(super) fn normalize_index_name(name: &str) -> String {
    name.to_uppercase()
}

/// Threshold for choosing disk-backed indexes (number of table rows)
/// Tables with more rows than this will use disk-backed B+ tree indexes
/// Set to very high value (100K) to keep Phase 2 conservative - disk-backed
/// indexes are functional but not enabled by default yet
///
/// For tests, disable disk-backed indexes entirely (use usize::MAX) to ensure
/// fast test execution. The specific test that verifies disk-backed functionality
/// is marked with #[ignore] and must be run explicitly.
#[cfg(not(test))]
pub(super) const DISK_BACKED_THRESHOLD: usize = 100_000;

#[cfg(test)]
pub(super) const DISK_BACKED_THRESHOLD: usize = usize::MAX;

/// Helper function to safely acquire a lock on a BTreeIndex mutex
///
/// # Arguments
/// * `btree` - Arc<Mutex<BTreeIndex>> to lock
///
/// # Returns
/// * `Ok(MutexGuard)` - Successfully acquired lock
/// * `Err(StorageError::LockError)` - Mutex was poisoned (thread panicked while holding lock)
///
/// # Poisoned Mutex Handling
/// When a thread panics while holding a mutex, the mutex becomes "poisoned" to indicate
/// potential data corruption. This function returns an error rather than attempting recovery,
/// forcing callers to handle the exceptional condition explicitly.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn acquire_btree_lock(
    btree: &Arc<Mutex<BTreeIndex>>,
) -> Result<parking_lot::MutexGuard<'_, BTreeIndex>, StorageError> {
    Ok(btree.lock())
}

#[cfg(target_arch = "wasm32")]
pub(super) fn acquire_btree_lock(
    btree: &Arc<Mutex<BTreeIndex>>,
) -> Result<std::sync::MutexGuard<'_, BTreeIndex>, StorageError> {
    btree.lock().map_err(|e| {
        StorageError::LockError(format!(
            "Failed to acquire BTreeIndex lock: mutex poisoned ({})",
            e
        ))
    })
}

/// Index metadata
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub index_name: String,
    pub table_name: String,
    pub unique: bool,
    pub columns: Vec<IndexColumn>,
}

/// Backend type for index storage
#[derive(Debug, Clone)]
pub enum IndexData {
    /// In-memory BTreeMap (for small indexes or backward compatibility)
    InMemory { data: BTreeMap<Vec<SqlValue>, Vec<usize>> },
    /// Disk-backed B+ tree (for large indexes or persistence)
    /// Note: The B+ tree stores (key, row_id) pairs. For non-unique indexes,
    /// we serialize Vec<usize> as the row_id value to support multiple rows per key.
    DiskBacked { btree: Arc<Mutex<BTreeIndex>>, page_manager: Arc<PageManager> },
    /// IVFFlat index for approximate nearest neighbor search on vectors
    IVFFlat { index: IVFFlatIndex },
    /// HNSW index for high-performance approximate nearest neighbor search
    Hnsw { index: HnswIndex },
}
