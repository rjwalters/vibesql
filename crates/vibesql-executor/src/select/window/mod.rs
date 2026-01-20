//! Window function integration for SELECT executor
//!
//! This module handles evaluation of window functions (with OVER clause) in SELECT queries.
//! Window functions are different from regular aggregates - they don't collapse rows.

mod collection;
mod detection;
mod evaluation;
mod order_by;
mod types;

use std::collections::HashMap;

// Re-export detection helpers for use by select module
pub(super) use detection::{expression_has_window_function, has_window_functions};
// Re-export ORDER BY support functions
pub(super) use order_by::{collect_order_by_window_functions, evaluate_order_by_window_functions};
// Re-export public types
pub use types::WindowFunctionKey;
use vibesql_ast::{SelectItem, WindowDefinition};
use vibesql_storage::Row;

use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};

/// Evaluate window functions and add results to rows
///
/// This processes all window functions in the SELECT list and adds computed values
/// to each row. Window functions don't collapse rows like GROUP BY - each input row
/// produces one output row with window function values added.
///
/// # Returns
/// Returns a tuple of (rows_with_window_values, mapping) where:
/// - rows_with_window_values: Original rows with window function results appended
/// - mapping: HashMap mapping WindowFunctionKey to column index in extended row
///
/// # Algorithm
/// 1. Find all window functions in SELECT list (resolving named window references)
/// 2. Group window functions by their window specification (for optimization)
/// 3. For each unique window spec:
///    - Partition rows according to PARTITION BY
///    - Sort each partition according to ORDER BY
///    - For each row, calculate frame and evaluate window functions
///    - Store results
/// 4. Extend each row with window function results
/// 5. Build mapping from WindowFunctionKey to column index
pub(super) fn evaluate_window_functions(
    mut rows: Vec<Row>,
    select_list: &[SelectItem],
    window_definitions: Option<&Vec<WindowDefinition>>,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<(Vec<Row>, HashMap<WindowFunctionKey, usize>), ExecutorError> {
    // Find all window functions in SELECT list, resolving named window references
    let window_functions = collection::collect_window_functions(select_list, window_definitions)?;

    if window_functions.is_empty() {
        return Ok((rows, HashMap::new()));
    }

    // For each window function, compute values for all rows
    // We'll build a Vec<Vec<SqlValue>> where outer vec is window functions,
    // inner vec is values for each row (in partition order if PARTITION BY present)
    let mut window_results: Vec<Vec<::vibesql_types::SqlValue>> = Vec::new();
    let mut window_mapping = HashMap::new();

    // Track the column index where window function results start
    let base_column_count = if rows.is_empty() { 0 } else { rows[0].values.len() };

    // Track row reordering from the first window function with PARTITION BY
    let mut row_reordering: Option<Vec<usize>> = None;

    for (idx, win_func) in window_functions.iter().enumerate() {
        let result = evaluation::evaluate_single_window_function(&rows, win_func, evaluator)?;
        window_results.push(result.values);

        // Capture row reordering from the first window function that has PARTITION BY
        if row_reordering.is_none() {
            row_reordering = result.partition_order;
        }

        // Build mapping: WindowFunctionKey -> column index
        // Use original_window_spec for key generation to match lookups using unresolved expressions
        let key =
            WindowFunctionKey::from_expression(&win_func.function_spec, &win_func.original_window_spec);
        let col_idx = base_column_count + idx;
        window_mapping.insert(key, col_idx);
    }

    // If we have row reordering (from PARTITION BY), reorder rows and values together
    if let Some(ref order) = row_reordering {
        // Reorder rows according to partition order
        let reordered_rows: Vec<Row> =
            order.iter().map(|&orig_idx| rows[orig_idx].clone()).collect();
        rows = reordered_rows;

        // Reorder window results to match the new row order
        // partition_order[i] = original_idx means position i in partition order came from original_idx
        // window_results are in original order, so we need: new[i] = old[partition_order[i]]
        for results in &mut window_results {
            let reordered: Vec<_> =
                order.iter().map(|&orig_idx| results[orig_idx].clone()).collect();
            *results = reordered;
        }
    }

    // Extend each row with window function results
    for (row_idx, row) in rows.iter_mut().enumerate() {
        for results in &window_results {
            row.values.push(results[row_idx].clone());
        }
    }

    Ok((rows, window_mapping))
}
