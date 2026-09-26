//! Lazy iterator-based query execution infrastructure
//!
//! This module provides a foundation for streaming query execution using iterators
//! instead of materializing results. This reduces memory usage and enables early
//! termination for LIMIT queries.
//!
//! # Architecture
//!
//! The core trait is `RowIterator`, which extends `Iterator<Item = Result<Row, ExecutorError>>`
//! with additional query-specific methods. All query operators (scan, filter, project, join)
//! are implemented as iterators that can be composed.
//!
//! # Benefits
//!
//! - **Memory efficiency**: O(max_single_table) instead of O(product)
//! - **Streaming**: Rows flow through pipeline without buffering
//! - **Early termination**: LIMIT 10 only computes 10 rows
//! - **Composability**: Iterators naturally chain together
//!
//! # Example
//!
//! ```text
//! // Create a table scan iterator
//! let scan_iter = TableScanIterator::new(schema, rows);
//!
//! // Add a filter
//! let filter_iter = FilterIterator::new(scan_iter, predicate, evaluator);
//!
//! // Add a projection
//! let project_iter = ProjectionIterator::new(filter_iter, projection_fn);
//!
//! // Consume only what we need (e.g., LIMIT 10)
//! for row in project_iter.take(10) {
//!     println!("{:?}", row?);
//! }
//! ```
//!
//! # Phase C Integration (Proof of Concept)
//!
//! The proof-of-concept tests in `tests/phase_c.rs` demonstrate how to build an iterator
//! pipeline for simple SELECT queries (without ORDER BY, DISTINCT, or window functions).
//! This serves as a proof-of-concept for full integration into the executor.

use crate::{errors::ExecutorError, schema::CombinedSchema};

// Module declarations
mod filter;
mod join;
mod projection;
mod scan;

// Re-export public types
pub use filter::FilterIterator;
pub use scan::TableScanIterator;

/// Core trait for row-producing iterators in the query execution pipeline
///
/// This trait extends the standard Iterator trait with query-specific metadata
/// and methods. All query operators (scans, filters, joins, projections) implement
/// this trait to enable composable, streaming query execution.
///
/// # Why not just use Iterator?
///
/// While we could use `Iterator<Item = Result<Row, ExecutorError>>` directly,
/// this trait adds query-specific capabilities:
/// - Access to the output schema (for type checking and column resolution)
/// - Size hints for query optimization
/// - Future: Statistics, cost estimates, etc.
pub trait RowIterator: Iterator<Item = Result<vibesql_storage::Row, ExecutorError>> {
    /// Get the schema of rows produced by this iterator
    ///
    /// The schema defines the structure and types of columns in output rows.
    /// It remains constant throughout iteration and must match the schema
    /// of all rows produced.
    fn schema(&self) -> &CombinedSchema;

    /// Provide a hint about the number of rows this iterator will produce
    ///
    /// This follows the same semantics as `Iterator::size_hint()`:
    /// - Returns `(lower_bound, upper_bound)`
    /// - `lower_bound` is always `<= actual count <= upper_bound.unwrap_or(usize::MAX)`
    /// - None for upper_bound means "unknown" or "unbounded"
    ///
    /// These hints can be used for:
    /// - Allocating appropriately-sized buffers
    /// - Choosing between nested loop vs hash join
    /// - Query planning and optimization
    ///
    /// The default implementation delegates to the underlying Iterator::size_hint()
    fn row_size_hint(&self) -> (usize, Option<usize>) {
        self.size_hint()
    }
}

// Implement RowIterator for Box<dyn RowIterator> to allow boxing
// Note: Box<T> already implements Iterator if T implements Iterator,
// so we only need to implement the RowIterator trait methods
impl<'a> RowIterator for Box<dyn RowIterator + 'a> {
    fn schema(&self) -> &CombinedSchema {
        (**self).schema()
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        (**self).row_size_hint()
    }
}

#[cfg(test)]
mod tests;
