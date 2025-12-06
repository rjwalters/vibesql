pub mod columnar;
pub(crate) mod cte;
mod executor;
pub(crate) mod filter;
mod from_iterator;
pub(crate) mod grouping;
mod helpers;
mod iterator;
pub mod join;
#[allow(dead_code)] // Experimental feature with tests, not yet enabled in production
pub mod late_materialization;
mod order;
#[cfg(feature = "parallel")]
mod parallel;
pub(crate) mod projection;
mod projection_simd;
pub(crate) mod scan;
mod set_operations;
mod vectorized;
pub(crate) mod window;

pub use cte::CteResult;
pub use iterator::{RowIterator, TableScanIterator};
pub use late_materialization::{
    gather_columns, gather_single_column, LazyMaterializedBatch, RowReference, SelectionVector,
};
pub use window::WindowFunctionKey;

/// Result of a SELECT query including column metadata
#[derive(Debug)]
pub struct SelectResult {
    /// Column names derived from the SELECT list
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<vibesql_storage::Row>,
}

pub use executor::SelectExecutor;
pub use executor::{is_simple_point_query, is_streaming_aggregate_query};
