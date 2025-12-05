//! Parallel execution support for expression evaluation
//!
//! This module provides utilities for parallel evaluation of expressions,
//! including component extraction and reconstruction for thread-safe execution.

#[cfg(feature = "parallel")]
use crate::{
    schema::CombinedSchema,
    select::{cte::CteResult, WindowFunctionKey},
};
#[cfg(feature = "parallel")]
use std::collections::HashMap;

/// Components returned by get_parallel_components for parallel execution
///
/// These components can be safely shared across threads and used to
/// reconstruct evaluators in parallel contexts.
///
/// Issue #3562: Added CTE context to enable IN subqueries referencing CTEs
/// during parallel predicate evaluation.
#[cfg(feature = "parallel")]
pub(super) type ParallelComponents<'a> = (
    &'a CombinedSchema,
    Option<&'a vibesql_storage::Database>,
    Option<&'a vibesql_storage::Row>,
    Option<&'a CombinedSchema>,
    Option<&'a HashMap<WindowFunctionKey, usize>>,
    Option<&'a HashMap<String, CteResult>>, // cte_context
    bool,                                   // enable_cse
);
