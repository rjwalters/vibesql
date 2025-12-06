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
/// For tests and benchmarks (with `in-memory-indexes` feature), disable disk-backed
/// indexes entirely (use usize::MAX) to ensure fast execution. The specific test that
/// verifies disk-backed functionality is marked with #[ignore] and must be run explicitly.
#[cfg(all(not(test), not(feature = "in-memory-indexes")))]
pub(super) const DISK_BACKED_THRESHOLD: usize = 100_000;

#[cfg(any(test, feature = "in-memory-indexes"))]
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
    ///
    /// The `pending_deletions` field tracks row indices that have been deleted but whose
    /// effects haven't been applied to all index entries yet. Instead of O(n) adjustment
    /// on every delete, we defer the adjustment and apply it lazily during lookups.
    /// This makes single-row deletes O(1) instead of O(n).
    ///
    /// The pending_deletions Vec is kept sorted in ascending order.
    InMemory {
        data: BTreeMap<Vec<SqlValue>, Vec<usize>>,
        /// Sorted list of row indices that have been deleted.
        /// During lookups, row indices are adjusted by subtracting the count of
        /// pending deletions that are less than each row index.
        pending_deletions: Vec<usize>,
    },
    /// Disk-backed B+ tree (for large indexes or persistence)
    /// Note: The B+ tree stores (key, row_id) pairs. For non-unique indexes,
    /// we serialize Vec<usize> as the row_id value to support multiple rows per key.
    DiskBacked { btree: Arc<Mutex<BTreeIndex>>, page_manager: Arc<PageManager> },
    /// IVFFlat index for approximate nearest neighbor search on vectors
    IVFFlat { index: IVFFlatIndex },
    /// HNSW index for high-performance approximate nearest neighbor search
    Hnsw { index: HnswIndex },
}

/// Threshold for compacting pending deletions.
/// When pending_deletions.len() exceeds this, we apply them to the index data.
/// This balances the O(1) delete benefit against lookup overhead.
pub(super) const PENDING_DELETIONS_COMPACT_THRESHOLD: usize = 1000;

impl IndexData {
    /// Adjust a row index by accounting for pending deletions.
    /// Returns the adjusted row index (decremented by the count of deletions before it).
    #[inline]
    pub fn adjust_row_index(&self, row_idx: usize) -> usize {
        match self {
            IndexData::InMemory { pending_deletions, .. } => {
                if pending_deletions.is_empty() {
                    row_idx
                } else {
                    // Binary search to find count of deletions < row_idx
                    let decrement = pending_deletions.partition_point(|&d| d < row_idx);
                    row_idx - decrement
                }
            }
            // Other index types don't use pending deletions
            _ => row_idx,
        }
    }

    /// Adjust a vector of row indices by accounting for pending deletions.
    /// This is more efficient than calling adjust_row_index in a loop when
    /// returning multiple row indices.
    #[inline]
    pub fn adjust_row_indices(&self, row_indices: Vec<usize>) -> Vec<usize> {
        match self {
            IndexData::InMemory { pending_deletions, .. } => {
                if pending_deletions.is_empty() {
                    row_indices
                } else {
                    row_indices
                        .into_iter()
                        .map(|row_idx| {
                            let decrement = pending_deletions.partition_point(|&d| d < row_idx);
                            row_idx - decrement
                        })
                        .collect()
                }
            }
            // Other index types don't use pending deletions
            _ => row_indices,
        }
    }

    /// Check if the pending deletions need compaction.
    #[inline]
    pub fn needs_compaction(&self) -> bool {
        match self {
            IndexData::InMemory { pending_deletions, .. } => {
                pending_deletions.len() >= PENDING_DELETIONS_COMPACT_THRESHOLD
            }
            _ => false,
        }
    }

    /// Compact pending deletions by applying them to the index data.
    /// This should be called periodically when pending_deletions gets large.
    pub fn compact_pending_deletions(&mut self) {
        if let IndexData::InMemory { data, pending_deletions } = self {
            if pending_deletions.is_empty() {
                return;
            }

            // Apply pending deletions to all row indices
            for row_indices in data.values_mut() {
                for row_idx in row_indices.iter_mut() {
                    let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                    *row_idx -= decrement;
                }
            }

            // Clear pending deletions after applying
            pending_deletions.clear();
        }
    }
}
