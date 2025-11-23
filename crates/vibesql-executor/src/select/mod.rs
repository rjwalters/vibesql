pub mod columnar;
pub(crate) mod cte;
mod executor;
mod filter;
mod from_iterator;
mod grouping;
mod helpers;
mod iterator;
pub mod join;
mod join_executor;
mod join_reorder_wrapper;
mod monomorphic;
mod order;
#[cfg(feature = "parallel")]
mod parallel;
#[cfg(feature = "parallel")]
mod predicate_graph;
mod vectorized;
mod projection;
pub(crate) mod scan;
mod set_operations;
mod window;

pub use cte::CteResult;
pub use iterator::{RowIterator, TableScanIterator};
pub use window::WindowFunctionKey;

/// Result of a SELECT query including column metadata
pub struct SelectResult {
    /// Column names derived from the SELECT list
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<vibesql_storage::Row>,
}

pub use executor::SelectExecutor;
