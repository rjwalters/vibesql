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
// Key normalization used when storing/looking up index keys. Exported so the
// executor's in-batch unique-index duplicate tracking (issue #6346) can build
// keys with the exact same canonical form the stored index bodies use
// (Integer 1 and Real 1.0 must collide, matching `IndexData::get`).
// `point_probe_needs_exact_reverification` (issue #6586) is exported for the
// executor's index-scan planner: an equality/IN-list probe whose literal is
// outside f64's exact-integer range is only a *candidate* filter, and the
// caller must re-verify candidates against the original row values.
pub use value_normalization::{
    normalize_cow, normalize_for_comparison, point_probe_needs_exact_reverification,
};
