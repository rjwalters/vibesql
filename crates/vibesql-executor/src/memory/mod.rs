//! Memory management utilities for efficient query execution
//!
//! This module provides memory-bounded execution for SQL query operators,
//! enabling processing of datasets larger than available memory through
//! disk spilling.
//!
//! # Components
//!
//! - **Memory Controller** (`MemoryController`): Budget management and tracking
//! - **Memory Reservation** (`MemoryReservation`): Per-operator memory tracking
//! - **External Sort** (`ExternalSort`): Disk-spilling merge sort
//! - **External Aggregate** (`ExternalAggregate`): Partition-based GROUP BY
//! - **External Hash Join** (`ExternalHashJoin`): Grace hash join with spilling
//! - **Spill Files** (`SpillFile`): Temporary file management with auto-cleanup
//! - **Arena Allocator** (`QueryArena`): Fast bump-pointer allocator
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       MemoryController                          │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
//! │  │ Budget Pool │  │  Tracking   │  │       Metrics           │ │
//! │  │ (configurable)│ │ (per-operator)│ │ (spills, peak, etc.) │ │
//! │  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────┘
//!            │                │                │
//!            ▼                ▼                ▼
//! ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
//! │ External     │  │ External     │  │ External     │
//! │ Sort         │  │ Aggregate    │  │ Hash Join    │
//! │ (merge sort) │  │ (partitioned)│  │ (grace join) │
//! └──────────────┘  └──────────────┘  └──────────────┘
//!            │                │                │
//!            ▼                ▼                ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        SpillFile (temp files)                   │
//! │         Auto-cleanup on drop, buffered I/O, seeking             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Memory-Bounded Execution
//!
//! ```text
//! use std::sync::Arc;
//! use vibesql_executor::memory::{MemoryController, MemoryConfig};
//!
//! // Create controller with 1GB budget
//! let controller = Arc::new(MemoryController::with_budget(1024 * 1024 * 1024));
//!
//! // Operators create reservations to track their memory
//! let mut reservation = controller.create_reservation();
//!
//! // When memory is exhausted, spill to disk
//! if !reservation.try_grow(batch_size) {
//!     spill_to_disk(&data);
//!     reservation.shrink(data.size());
//! }
//!
//! // Check statistics after execution
//! let stats = controller.stats();
//! println!("{}", stats); // "Memory: 512MB/1GB (50%), peak: 950MB, spilled: 2GB (3 ops)"
//! ```
//!
//! # External Operators
//!
//! ## External Sort
//!
//! Two-phase external merge sort:
//! 1. **Run generation**: Sort in-memory chunks, spill as sorted runs
//! 2. **K-way merge**: Merge runs using a tournament tree
//!
//! ```text
//! let mut sort = ExternalSort::new(controller, config, sort_keys);
//! for row in input {
//!     sort.add_row(&row)?;  // Automatically spills when needed
//! }
//! for result in sort.finish()? {
//!     // Rows come out in sorted order
//! }
//! ```
//!
//! ## External Aggregate
//!
//! Partition-based aggregation for GROUP BY:
//! 1. Hash rows to partitions
//! 2. Spill partitions when memory exhausted
//! 3. Process each partition's groups
//!
//! ```text
//! let specs = vec![AggregateSpec { function_name: "SUM".into(), .. }];
//! let mut agg = ExternalAggregate::new(controller, config, specs, 2);
//! for row in input {
//!     agg.add_row(&row)?;
//! }
//! for result in agg.finish()? {
//!     // (group_key..., aggregate_values...)
//! }
//! ```
//!
//! ## External Hash Join
//!
//! Grace hash join with partition-based spilling:
//! 1. Partition both build and probe sides by join key hash
//! 2. Spill partitions when memory exhausted
//! 3. Process matching partitions together
//!
//! ```text
//! let mut join = ExternalHashJoin::new(
//!     controller, config,
//!     vec![0],  // build key columns
//!     vec![0],  // probe key columns
//!     JoinType::Inner,
//! );
//! for row in build_side { join.add_build_row(&row)?; }
//! for row in probe_side { join.add_probe_row(&row)?; }
//! for result in join.finish()? {
//!     // Joined rows
//! }
//! ```
//!
//! # Configuration
//!
//! Environment variables:
//!
//! | Variable | Description | Default |
//! |----------|-------------|---------|
//! | `VIBESQL_MEMORY_LIMIT` | Total memory budget (e.g., "4GB") | 1GB |
//! | `VIBESQL_TEMP_DIR` | Directory for spill files | system temp |
//! | `VIBESQL_SPILL_THRESHOLD` | When to start spilling (0.0-1.0) | 0.8 |
//! | `VIBESQL_PARTITION_SIZE` | Target partition size | 64MB |

mod arena;
mod controller;
mod external_aggregate;
mod external_hash_join;
mod external_sort;
pub mod row_serialization;
mod spill;

pub use arena::QueryArena;
pub use controller::{
    MemoryConfig, MemoryController, MemoryReservation, MemoryStats, DEFAULT_MEMORY_BUDGET,
    DEFAULT_SPILL_THRESHOLD, DEFAULT_TARGET_PARTITION_BYTES, MIN_OPERATOR_MEMORY,
};
pub use external_aggregate::{
    AggregateResultIterator, AggregateSpec, ExternalAggregate, ExternalAggregateConfig,
};
pub use external_hash_join::{
    ExternalHashJoin, ExternalHashJoinConfig, HashJoinResultIterator, JoinType,
};
pub use external_sort::{ExternalSort, ExternalSortConfig, SortKey, SortedIterator};
pub use spill::{SpillFile, SpillFileSet};
