// ============================================================================
// Index Management Module - User-defined index operations
// ============================================================================
//
// This module has been refactored into focused submodules for better
// maintainability and code organization:
//
// - index_metadata: Types and helpers for index definitions
// - index_operations: Query methods on IndexData (range_scan, multi_lookup, etc.)
//   - value_normalization: Canonical value forms for comparison
//   - range_bounds: Value increment logic for range operations
//   - point_lookup: Single-value equality operations
//   - range_scan: Range query implementation
//   - prefix_match: Multi-column index prefix matching
//   - reverse_scan: Reverse iteration for DESC ORDER BY optimization
// - index_maintenance: CRUD operations (create, drop, rebuild, update)
// - index_manager: Core IndexManager coordination and queries
// - ivfflat: IVFFlat index for approximate nearest neighbor search on vectors
// - hnsw: HNSW index for high-performance approximate nearest neighbor search

pub mod hnsw;
mod index_maintenance;
mod index_manager;
mod index_metadata;
mod index_operations;
pub mod ivfflat;
mod point_lookup;
mod prefix_match;
mod range_bounds;
mod range_scan;
mod reverse_scan;
mod streaming;
mod value_normalization;

// Re-export public API
pub use hnsw::HnswIndex;
pub use index_manager::IndexManager;
pub use index_metadata::{IndexData, IndexMetadata};
pub use ivfflat::IVFFlatIndex;
pub use streaming::OwnedStreamingRangeScan;
