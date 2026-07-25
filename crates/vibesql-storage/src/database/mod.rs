// ============================================================================
// Database Module
// ============================================================================

mod access_signal;
mod cache;
mod change_events_api;
mod columnar_policy;
mod config;
mod constructors;
mod core;
mod debug;
mod index_ops;
mod lifecycle;
mod metadata;
mod operations;
mod persistence_api;
mod point_lookup;
mod resource_tracker;
mod session;
mod table_api;
mod transaction_api;

pub mod indexes;
pub mod transactions;

#[cfg(test)]
mod tests;

pub use access_signal::AccessSignalSnapshot;
pub use columnar_policy::{
    columnar_hotness, should_use_columnar, MIN_COLUMNAR_ROWS, SCAN_DOMINANCE_FACTOR,
};
pub use core::{Database, ExportedSpatialIndexMetadata as SpatialIndexMetadata};
// The WAL recovery path (`wal::recovery`) shares the CreateTable schema-blob
// layout with the emit path here; its tests round-trip through the real
// serializer (issue #5883). Test-only: production replay only deserializes.
#[cfg(test)]
pub(crate) use table_api::serialize_table_schema;

pub use config::{DatabaseConfig, SpillPolicy, DEFAULT_COLUMNAR_CACHE_BUDGET};
pub use index_ops::{
    print_delete_profile_summary, reset_delete_profile_stats, DeleteProfileStats,
    DELETE_PROFILE_STATS,
};
pub use indexes::{IndexData, IndexManager, IndexMetadata, OwnedStreamingRangeScan};
pub use operations::SpatialIndexMetadata as OperationsSpatialIndexMetadata;
pub use resource_tracker::{IndexBackend, IndexStats, ResourceTracker};
pub use transactions::{
    DeferredFkViolation, DeferredFkViolationKind, Savepoint, TransactionChange, TransactionManager,
    TransactionState,
};
