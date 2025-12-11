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
use vibesql_ast::SelectItem;
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
/// 1. Find all window functions in SELECT list
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
    evaluator: &CombinedExpressionEvaluator,
) -> Result<(Vec<Row>, HashMap<WindowFunctionKey, usize>), ExecutorError> {
    // Find all window functions in SELECT list
    let window_functions = collection::collect_window_functions(select_list)?;

    if window_functions.is_empty() {
        return Ok((rows, HashMap::new()));
    }

    // For each window function, compute values for all rows
    // We'll build a Vec<Vec<SqlValue>> where outer vec is window functions,
    // inner vec is values for each row
    let mut window_results: Vec<Vec<::vibesql_types::SqlValue>> = Vec::new();
    let mut window_mapping = HashMap::new();

    // Track the column index where window function results start
    let base_column_count = if rows.is_empty() { 0 } else { rows[0].values.len() };

    for (idx, win_func) in window_functions.iter().enumerate() {
        let values = evaluation::evaluate_single_window_function(&rows, win_func, evaluator)?;
        window_results.push(values);

        // Build mapping: WindowFunctionKey -> column index
        let key =
            WindowFunctionKey::from_expression(&win_func.function_spec, &win_func.window_spec);
        let col_idx = base_column_count + idx;
        window_mapping.insert(key, col_idx);
    }

    // Extend each row with window function results
    for (row_idx, row) in rows.iter_mut().enumerate() {
        for results in &window_results {
            row.values.push(results[row_idx].clone());
        }
    }

    Ok((rows, window_mapping))
}
