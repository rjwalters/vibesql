//! Morsel-driven parallel execution with work-stealing
//!
//! This module implements the morsel-driven parallelism model from Leis et al. (SIGMOD 2014).
//! Instead of static partitioning (dividing rows into N equal chunks), morsels provide
//! dynamic load balancing through work-stealing, enabling near-linear scaling to 16+ cores.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │  Traditional (Static)         vs    Morsel-Driven           │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Divide into N equal parts    │    Morsel queue (~50K rows) │
//! │  at query start               │    Workers steal as needed  │
//! │  (fixed assignment)           │    (dynamic load balancing) │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Benefits
//!
//! - **Load Balancing**: If one morsel has expensive rows (complex expressions, many joins), other
//!   workers can steal remaining morsels instead of sitting idle.
//! - **Cache Efficiency**: Morsel size is tuned to L3 cache for optimal memory bandwidth.
//! - **Scalability**: Near-linear scaling to 16+ cores (>85% efficiency).
//!
//! # Usage
//!
//! ```text
//! use vibesql_executor::select::morsel::{morsel_parallel_filter, MorselConfig};
//!
//! let config = MorselConfig::default();
//! let results = morsel_parallel_filter(&rows, &config, |row| predicate(row));
//! ```
//!
//! # References
//!
//! - [Leis et al., SIGMOD 2014](https://dl.acm.org/doi/10.1145/2588555.2610507)

mod config;
mod join;
mod parallel;
mod sort;

#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex};

// Re-export public API
pub use config::{global_config, MorselConfig};
use crossbeam_deque::{Injector, Steal, Worker};
pub use join::morsel_parallel_probe_sqlvalue;
pub use parallel::{
    morsel_filter, morsel_map, morsel_parallel_filter, morsel_parallel_filter_map,
    morsel_parallel_group, morsel_parallel_map, morsel_parallel_reduce,
};
pub use sort::{morsel_parallel_sort, morsel_sort_by};
use vibesql_storage::Row;

/// Thread-safe container for collecting morsel results with ordering info
pub(crate) type MorselResultsOrdered = Arc<Mutex<Vec<(usize, Vec<Row>)>>>;

/// Environment variable to enable morsel execution debug logging
const MORSEL_DEBUG_ENV: &str = "MORSEL_DEBUG";

/// Check if morsel debug logging is enabled
pub(crate) fn morsel_debug_enabled() -> bool {
    std::env::var(MORSEL_DEBUG_ENV).is_ok()
}

/// A unit of work containing a slice of rows to process.
///
/// Morsels are the fundamental unit of work distribution in morsel-driven execution.
/// Each morsel contains a contiguous slice of rows sized to fit in L3 cache.
#[derive(Debug, Clone)]
pub struct Morsel {
    /// Starting row index in source data
    start_idx: usize,
    /// Number of rows in this morsel
    row_count: usize,
}

impl Morsel {
    /// Create a new morsel with the given start index and row count
    pub fn new(start_idx: usize, row_count: usize) -> Self {
        Self { start_idx, row_count }
    }

    /// Get the starting index of this morsel in the source data
    #[inline]
    pub fn start_idx(&self) -> usize {
        self.start_idx
    }

    /// Get the number of rows in this morsel
    #[inline]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the ending index (exclusive) of this morsel in the source data
    #[inline]
    pub fn end_idx(&self) -> usize {
        self.start_idx + self.row_count
    }

    /// Extract the rows for this morsel from the source data
    #[inline]
    pub fn rows<'a>(&self, source: &'a [Row]) -> &'a [Row] {
        &source[self.start_idx..self.end_idx()]
    }
}

/// Create morsels from a row count.
pub(crate) fn create_morsels(total_rows: usize, morsel_size: usize) -> Vec<Morsel> {
    let mut morsels = Vec::with_capacity(total_rows.div_ceil(morsel_size));
    let mut start = 0;

    while start < total_rows {
        let count = (total_rows - start).min(morsel_size);
        morsels.push(Morsel::new(start, count));
        start += count;
    }

    morsels
}

/// Helper to steal a morsel from the injector queue
pub(crate) fn steal_morsel(injector: &Injector<Morsel>, worker: &Worker<Morsel>) -> Option<Morsel> {
    // Try local queue first
    worker.pop().or_else(|| {
        // Try to steal from global injector
        loop {
            match injector.steal() {
                Steal::Success(m) => return Some(m),
                Steal::Empty => return None,
                Steal::Retry => continue,
            }
        }
    })
}
