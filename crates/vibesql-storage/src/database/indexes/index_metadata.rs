// ============================================================================
// Index Metadata - Types and helpers for index definitions
// ============================================================================

#[cfg(target_arch = "wasm32")]
use std::sync::Mutex;
use std::{collections::BTreeMap, sync::Arc};

#[cfg(not(target_arch = "wasm32"))]
use parking_lot::Mutex;
use vibesql_ast::IndexColumn;
use vibesql_types::SqlValue;

use super::{hnsw::HnswIndex, ivfflat::IVFFlatIndex};
use crate::{btree::BTreeIndex, page::PageManager, StorageError};

/// Normalize an index name to lowercase for case-insensitive comparison
/// This follows SQL standard identifier rules
pub(super) fn normalize_index_name(name: &str) -> String {
    name.to_lowercase()
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
    /// Optional WHERE predicate for partial indexes (CREATE INDEX ... WHERE expr).
    ///
    /// When set, only rows for which the predicate evaluates to a truthy value
    /// (SQLite semantics: non-NULL and non-zero) are stored in the index body.
    /// The storage layer cannot evaluate expressions on its own; the executor
    /// crate is responsible for evaluating the predicate and invoking the
    /// dedicated `*_partial_indexes_*` maintenance entry points with a set of
    /// indexes that should include the row. The default index-maintenance
    /// methods (`add_to_indexes_for_insert`, `update_indexes_for_update`,
    /// `update_indexes_for_delete`, and their `batch_*` variants) skip partial
    /// indexes to avoid silently inserting rows that don't satisfy the
    /// predicate. This mirrors how expression indexes are handled.
    pub where_clause: Option<Box<vibesql_ast::Expression>>,
}

impl IndexMetadata {
    /// Returns `true` when this index is partial (has a WHERE predicate).
    #[inline]
    pub fn is_partial(&self) -> bool {
        self.where_clause.is_some()
    }
}

/// Backend type for index storage
#[derive(Debug, Clone)]
pub enum IndexData {
    /// In-memory BTreeMap (for small indexes or backward compatibility)
    InMemory {
        data: BTreeMap<Vec<SqlValue>, Vec<usize>>,
    },
    /// Disk-backed B+ tree (for large indexes or persistence)
    /// Note: The B+ tree stores (key, row_id) pairs. For non-unique indexes,
    /// we serialize Vec<usize> as the row_id value to support multiple rows per key.
    ///
    /// **Transaction rollback (#5425, follow-on of #5413/#5419):** `Clone`
    /// for this variant is a *shallow* `Arc` bump — the cloned snapshot and
    /// the live index share the same `Arc<Mutex<BTreeIndex>>`. The #5413 /
    /// #5419 copy-on-write rollback snapshot of `Operations` therefore cannot
    /// undo mutations to a spilled index by restoring the shallow clone alone
    /// (it would just re-point at the *same, already-mutated* B+ tree).
    ///
    /// To fix this, disk-backed mutations are undone via a per-tree **undo-log**
    /// (#5425): when a transaction's first index mutation arms the COW snapshot,
    /// [`BTreeIndex::begin_undo_logging`] is also called on every disk-backed
    /// tree (see [`IndexManager::begin_disk_undo_logging`]). ROLLBACK reverses
    /// the log ([`IndexManager::rollback_disk_undo_logs`]); COMMIT discards it
    /// ([`IndexManager::clear_disk_undo_logs`]). The cost is proportional to the
    /// transaction's own mutations, not the (by definition large) spilled index
    /// size, which would defeat the purpose of spilling. Note: this covers
    /// full-transaction ROLLBACK; savepoint-scoped rollback of disk-backed
    /// mutations is tracked as a separate follow-on.
    ///
    /// This path is **not reachable in replicated mode**: the state machine
    /// builds the database via `Database::new()` (no `database_path`), so index
    /// spilling — which requires a path — never triggers there. It applies to
    /// standalone-with-path databases that spill an index and then ROLLBACK.
    ///
    /// [`IndexManager::begin_disk_undo_logging`]: super::IndexManager::begin_disk_undo_logging
    /// [`IndexManager::rollback_disk_undo_logs`]: super::IndexManager::rollback_disk_undo_logs
    /// [`IndexManager::clear_disk_undo_logs`]: super::IndexManager::clear_disk_undo_logs
    DiskBacked { btree: Arc<Mutex<BTreeIndex>>, page_manager: Arc<PageManager> },
    /// IVFFlat index for approximate nearest neighbor search on vectors
    IVFFlat { index: IVFFlatIndex },
    /// HNSW index for high-performance approximate nearest neighbor search
    Hnsw { index: HnswIndex },
}

impl IndexData {
    /// Sample the stored key value at a given column position.
    ///
    /// For in-memory indexes, returns the first non-NULL `SqlValue` found at
    /// position `col_idx` across the index's stored keys. For disk-backed B+
    /// trees, synthesizes a representative value from the persisted
    /// `key_schema` (page-0 metadata, in memory after load — no I/O), which
    /// works even for empty or all-NULL indexes (issue #5337); only temporal
    /// schema types yield a sample, preserving current caller behavior for
    /// non-temporal keys. Returns `None` if the key type cannot be
    /// determined (empty/all-NULL in-memory index, non-temporal disk-backed
    /// schema, vector indexes).
    ///
    /// This is used by the executor to determine the stored key *type* so
    /// that string probe bounds can be coerced to match temporal keys
    /// (issue #5333). Only the type of the returned value is meaningful.
    pub fn first_key_value_sample(&self, col_idx: usize) -> Option<SqlValue> {
        match self {
            IndexData::InMemory { data, .. } => data
                .keys()
                .filter_map(|key| key.get(col_idx))
                .find(|v| !matches!(v, SqlValue::Null))
                .cloned(),
            IndexData::DiskBacked { btree, .. } => {
                use vibesql_types::DataType;

                let guard = acquire_btree_lock(btree).ok()?;
                // Synthesize a representative value of the declared key type;
                // only the type is meaningful per this method's contract.
                let min_date = vibesql_types::Date { year: 1, month: 1, day: 1 };
                let midnight = vibesql_types::Time { hour: 0, minute: 0, second: 0, nanosecond: 0 };
                match guard.key_schema().get(col_idx)? {
                    DataType::Date => Some(SqlValue::Date(min_date)),
                    DataType::Timestamp { .. } => {
                        Some(SqlValue::Timestamp(vibesql_types::Timestamp::new(min_date, midnight)))
                    }
                    DataType::Time { .. } => Some(SqlValue::Time(midnight)),
                    // Non-temporal schema types: callers treat None as
                    // "unknown key type" and skip coercion (no behavior
                    // change for non-temporal disk-backed indexes).
                    _ => None,
                }
            }
            // Vector indexes don't store comparable scalar keys. Callers
            // treat None as "unknown key type" and skip coercion.
            _ => None,
        }
    }

}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::str::FromStr;

    use tempfile::TempDir;
    use vibesql_types::DataType;

    use super::*;
    use crate::NativeStorage;

    /// Build a disk-backed `IndexData` with the given key schema and keys.
    /// Returns the TempDir so the backing file outlives the assertions.
    fn disk_backed_index(
        key_schema: Vec<DataType>,
        keys: Vec<Vec<SqlValue>>,
    ) -> (IndexData, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test_index.idx", storage).unwrap());

        let mut sorted_entries: Vec<(Vec<SqlValue>, usize)> =
            keys.into_iter().enumerate().map(|(i, key)| (key, i)).collect();
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let btree =
            BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager.clone()).unwrap();
        let index = IndexData::DiskBacked { btree: Arc::new(Mutex::new(btree)), page_manager };
        (index, temp_dir)
    }

    fn ts(s: &str) -> SqlValue {
        SqlValue::Timestamp(vibesql_types::Timestamp::from_str(s).unwrap())
    }

    #[test]
    fn disk_backed_sample_reports_timestamp_from_schema() {
        let (index, _dir) = disk_backed_index(
            vec![DataType::Timestamp { with_timezone: false }],
            vec![vec![ts("2024-01-01 12:00:00")]],
        );
        assert!(matches!(index.first_key_value_sample(0), Some(SqlValue::Timestamp(_))));
    }

    #[test]
    fn disk_backed_sample_reports_date_from_schema() {
        let (index, _dir) = disk_backed_index(
            vec![DataType::Date],
            vec![vec![SqlValue::Date(vibesql_types::Date::from_str("2024-01-01").unwrap())]],
        );
        assert!(matches!(index.first_key_value_sample(0), Some(SqlValue::Date(_))));
    }

    #[test]
    fn disk_backed_sample_reports_time_from_schema() {
        let (index, _dir) = disk_backed_index(
            vec![DataType::Time { with_timezone: false }],
            vec![vec![SqlValue::Time(vibesql_types::Time::from_str("12:00:00").unwrap())]],
        );
        assert!(matches!(index.first_key_value_sample(0), Some(SqlValue::Time(_))));
    }

    #[test]
    fn disk_backed_sample_returns_none_for_non_temporal_schema() {
        // Non-temporal disk-backed indexes keep the pre-#5337 behavior:
        // callers treat None as "unknown key type" and skip coercion.
        let (index, _dir) =
            disk_backed_index(vec![DataType::Integer], vec![vec![SqlValue::Integer(42)]]);
        assert_eq!(index.first_key_value_sample(0), None);
    }

    #[test]
    fn disk_backed_sample_works_for_empty_index() {
        // The schema-based sample comes from page-0 metadata, so it works
        // even when the index holds no keys (impossible for the in-memory
        // sampling approach).
        let (index, _dir) =
            disk_backed_index(vec![DataType::Timestamp { with_timezone: false }], vec![]);
        assert!(matches!(index.first_key_value_sample(0), Some(SqlValue::Timestamp(_))));
    }

    #[test]
    fn disk_backed_sample_uses_per_column_schema() {
        // Multi-column index with the temporal column at position 1: the
        // fast-path point lookup samples each column index separately.
        let (index, _dir) = disk_backed_index(
            vec![DataType::Integer, DataType::Timestamp { with_timezone: false }],
            vec![vec![SqlValue::Integer(1), ts("2024-01-01 12:00:00")]],
        );
        assert_eq!(index.first_key_value_sample(0), None);
        assert!(matches!(index.first_key_value_sample(1), Some(SqlValue::Timestamp(_))));
        assert_eq!(index.first_key_value_sample(2), None);
    }
}
