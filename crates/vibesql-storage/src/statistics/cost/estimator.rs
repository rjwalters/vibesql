//! Cost estimator core struct and parameters
//!
//! This module contains the `CostEstimator` struct with tunable cost parameters
//! for query plan cost estimation. The parameters are based on the PostgreSQL
//! cost model with adjustments for in-memory databases.

/// Cost estimator for access methods (scans, index lookups) and DML operations
///
/// Cost parameters are based on the PostgreSQL cost model:
/// - Sequential I/O is cheaper than random I/O
/// - Index scans have overhead for traversing the B-tree
/// - Cache effects are approximated by page costs
///
/// DML cost parameters are derived from TPC-C profiling (#3862):
/// - DELETE operations have significant index maintenance overhead
/// - Compaction occurs when >50% of rows are deleted
/// - Columnar cache invalidation/rebuild adds overhead
#[derive(Debug, Clone)]
pub struct CostEstimator {
    /// Cost of reading a page sequentially (default: 1.0)
    pub seq_page_cost: f64,

    /// Cost of reading a page randomly (default: 4.0, reflecting disk seek penalty)
    pub random_page_cost: f64,

    /// Cost of processing a single row (CPU cost, default: 0.01)
    pub cpu_tuple_cost: f64,

    /// Cost of processing a single index entry (default: 0.005)
    pub cpu_index_tuple_cost: f64,

    /// Estimated rows per page (default: 100 for 8KB pages)
    pub rows_per_page: f64,

    // ============================================================================
    // DML Cost Parameters
    // ============================================================================
    /// Base cost of inserting a single row (default: 0.1)
    /// Includes row storage and basic overhead
    pub insert_tuple_cost: f64,

    /// Cost of updating a hash index entry (PK/unique constraint) per row (default: 0.05)
    /// Applied per constraint index on insert/update/delete
    pub hash_index_update_cost: f64,

    /// Cost of updating a B-tree index entry per row (default: 0.15)
    /// B-tree operations are more expensive than hash updates due to tree rebalancing
    pub btree_index_update_cost: f64,

    /// Cost of updating/deleting a single row (default: 0.08)
    /// Includes bitmap marking and row modification
    pub update_tuple_cost: f64,

    /// Cost of deleting a single row (default: 0.05)
    /// Uses O(1) bitmap marking, cheaper than update
    pub delete_tuple_cost: f64,

    /// Cost multiplier when table compaction is likely (default: 2.0)
    /// Applied when deleted_ratio > 0.5
    pub compaction_cost_multiplier: f64,

    /// Base cost of rebuilding columnar representation (default: 0.02)
    /// Per-row cost for native columnar tables after DML
    pub columnar_rebuild_cost: f64,

    /// Cost of invalidating columnar cache (default: 0.1)
    /// Fixed cost for row-oriented tables with columnar cache
    pub columnar_cache_invalidation_cost: f64,

    // ============================================================================
    // WAL Cost Parameters (derived from TPC-C profiling #3862)
    // ============================================================================
    /// Cost of writing a single WAL entry per row (default: 0.12)
    /// Based on profiling showing WAL as 56% of DELETE operation time.
    /// WAL entries include: operation type, row data, and metadata.
    pub wal_write_cost: f64,

    /// Fixed overhead for WAL sync/flush operations (default: 0.5)
    /// Applied once per DML operation (amortized across batch operations).
    /// Includes fsync or equivalent durability guarantee.
    pub wal_sync_cost: f64,
}

impl Default for CostEstimator {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            // For in-memory databases (BTreeMap), random access is fast.
            // Using 1.5 instead of 4.0 (disk-based) to better reflect reality.
            random_page_cost: 1.5,
            cpu_tuple_cost: 0.01,
            cpu_index_tuple_cost: 0.005,
            rows_per_page: 100.0,
            // DML cost parameters derived from TPC-C profiling (#3862)
            insert_tuple_cost: 0.1,
            hash_index_update_cost: 0.05,
            btree_index_update_cost: 0.15,
            update_tuple_cost: 0.08,
            delete_tuple_cost: 0.05,
            compaction_cost_multiplier: 2.0,
            columnar_rebuild_cost: 0.02,
            columnar_cache_invalidation_cost: 0.1,
            // WAL cost parameters derived from TPC-C profiling (#3862)
            // WAL writes were 56% of DELETE time (600µs of 1.08ms total)
            // Row removal was 21% (230µs), so WAL is ~2.6x row removal cost
            wal_write_cost: 0.12,
            wal_sync_cost: 0.5,
        }
    }
}

impl CostEstimator {
    /// Create a cost estimator with custom read parameters (uses defaults for DML)
    pub fn new(
        seq_page_cost: f64,
        random_page_cost: f64,
        cpu_tuple_cost: f64,
        cpu_index_tuple_cost: f64,
    ) -> Self {
        let default = Self::default();
        Self {
            seq_page_cost,
            random_page_cost,
            cpu_tuple_cost,
            cpu_index_tuple_cost,
            rows_per_page: 100.0,
            // Use defaults for DML parameters
            insert_tuple_cost: default.insert_tuple_cost,
            hash_index_update_cost: default.hash_index_update_cost,
            btree_index_update_cost: default.btree_index_update_cost,
            update_tuple_cost: default.update_tuple_cost,
            delete_tuple_cost: default.delete_tuple_cost,
            compaction_cost_multiplier: default.compaction_cost_multiplier,
            columnar_rebuild_cost: default.columnar_rebuild_cost,
            columnar_cache_invalidation_cost: default.columnar_cache_invalidation_cost,
            wal_write_cost: default.wal_write_cost,
            wal_sync_cost: default.wal_sync_cost,
        }
    }
}
