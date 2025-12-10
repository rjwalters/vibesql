//! Memory management utilities for efficient query execution
//!
//! This module provides:
//!
//! - **Arena allocator** (`QueryArena`): Bump-pointer allocator for fast, query-scoped allocations
//! - **Memory controller** (`MemoryController`): Budget management for memory-bounded operators
//! - **Spill files** (`SpillFile`): Temporary file management for disk spilling
//!
//! # Memory-Bounded Execution
//!
//! For operators that may process more data than fits in memory, the memory controller
//! provides a budget-based system:
//!
//! ```rust,ignore
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
//! ```
//!
//! # Configuration
//!
//! Memory limits can be configured via environment variables:
//!
//! - `VIBESQL_MEMORY_LIMIT`: Total memory budget (e.g., "4GB", "512MB")
//! - `VIBESQL_TEMP_DIR`: Directory for spill files
//! - `VIBESQL_SPILL_THRESHOLD`: When to start spilling (0.0-1.0, default 0.8)
//! - `VIBESQL_PARTITION_SIZE`: Target partition size for external operators

mod arena;
mod controller;
mod external_sort;
pub mod row_serialization;
mod spill;

pub use arena::QueryArena;
pub use controller::{
    MemoryConfig, MemoryController, MemoryReservation, DEFAULT_MEMORY_BUDGET,
    DEFAULT_SPILL_THRESHOLD, DEFAULT_TARGET_PARTITION_BYTES, MIN_OPERATOR_MEMORY,
};
pub use external_sort::{ExternalSort, ExternalSortConfig, SortKey, SortedIterator};
pub use spill::{SpillFile, SpillFileSet};
