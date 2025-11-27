// ============================================================================
// Database Module
// ============================================================================

mod cache;
mod config;
mod constructors;
mod core;
mod debug;
mod index_ops;
mod lifecycle;
mod metadata;
mod operations;
mod resource_tracker;
mod session;

pub mod indexes;
pub mod transactions;

#[cfg(test)]
mod tests;

pub use config::{DatabaseConfig, SpillPolicy, DEFAULT_COLUMNAR_CACHE_BUDGET};
pub use core::{Database, ExportedSpatialIndexMetadata as SpatialIndexMetadata};
pub use operations::SpatialIndexMetadata as OperationsSpatialIndexMetadata;

pub use indexes::{IndexData, IndexManager, IndexMetadata};
pub use resource_tracker::{IndexBackend, IndexStats, ResourceTracker};
pub use transactions::{Savepoint, TransactionChange, TransactionManager, TransactionState};
