//! Aggregate function evaluation (COUNT, SUM, AVG, MIN, MAX)

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{compiled_case::CompiledCaseExpression, CombinedExpressionEvaluator},
    schema::CombinedSchema,
    select::grouping::{compare_sql_values, AggregateAccumulator},
};

/// Check if an expression references only outer columns (not inner FROM columns).
///
/// This is used to detect outer-correlated aggregates in scalar subqueries (issue #4930).
/// When an aggregate's argument references only outer columns, it should aggregate
/// over all outer rows, not the inner FROM rows.
///
/// Returns true if:
/// - The expression has column references
/// - NONE of those column references resolve to the inner schema
/// - (They must all resolve to outer context)
fn expression_refs_only_outer_columns(
    expr: &vibesql_ast::Expression,
    inner_schema: &CombinedSchema,
    has_outer_context: bool,
) -> bool {
    // If there's no outer context, this check doesn't apply
    if !has_outer_context {
        return false;
    }

    // Recursively check if all column refs are outer-only
    let mut has_column_refs = false;
    let mut all_outer = true;

    check_expr_columns(expr, inner_schema, &mut has_column_refs, &mut all_outer);

    // Return true only if there ARE column refs and they're ALL outer
    has_column_refs && all_outer
}

/// Recursive helper to check column references in an expression
fn check_expr_columns(
    expr: &vibesql_ast::Expression,
    inner_schema: &CombinedSchema,
    has_column_refs: &mut bool,
    all_outer: &mut bool,
) {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(col_id) => {
            *has_column_refs = true;
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();

            // Check if this column is in the inner schema
            let is_in_inner = if let Some(table_name) = table {
                inner_schema.get_column_index(Some(&table_name), &column).is_some()
            } else {
                // Unqualified column - check if it's in any inner table
                inner_schema.get_column_index(None, &column).is_some()
            };

            if is_in_inner {
                *all_outer = false;
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            check_expr_columns(left, inner_schema, has_column_refs, all_outer);
            check_expr_columns(right, inner_schema, has_column_refs, all_outer);
        }
        Expression::UnaryOp { expr: inner, .. } => {
            check_expr_columns(inner, inner_schema, has_column_refs, all_outer);
        }
        Expression::IsNull { expr: inner, .. } => {
            check_expr_columns(inner, inner_schema, has_column_refs, all_outer);
        }
        Expression::Cast { expr: inner, .. } => {
            check_expr_columns(inner, inner_schema, has_column_refs, all_outer);
        }
        Expression::Case { operand, when_clauses, else_result, .. } => {
            if let Some(op) = operand {
                check_expr_columns(op, inner_schema, has_column_refs, all_outer);
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    check_expr_columns(cond, inner_schema, has_column_refs, all_outer);
                }
                check_expr_columns(&case_when.result, inner_schema, has_column_refs, all_outer);
            }
            if let Some(else_expr) = else_result {
                check_expr_columns(else_expr, inner_schema, has_column_refs, all_outer);
            }
        }
        Expression::Function { args, .. } => {
            for arg in args {
                check_expr_columns(arg, inner_schema, has_column_refs, all_outer);
            }
        }
        Expression::Between { expr: inner, low, high, .. } => {
            check_expr_columns(inner, inner_schema, has_column_refs, all_outer);
            check_expr_columns(low, inner_schema, has_column_refs, all_outer);
            check_expr_columns(high, inner_schema, has_column_refs, all_outer);
        }
        Expression::InList { expr: inner, values, .. } => {
            check_expr_columns(inner, inner_schema, has_column_refs, all_outer);
            for item in values {
                check_expr_columns(item, inner_schema, has_column_refs, all_outer);
            }
        }
        Expression::In { expr: inner, .. } => {
            // IN with subquery - just check the expr
            check_expr_columns(inner, inner_schema, has_column_refs, all_outer);
        }
        Expression::Collate { expr: inner, .. } => {
            check_expr_columns(inner, inner_schema, has_column_refs, all_outer);
        }
        // Literals, wildcards, and other non-column expressions don't affect the result
        _ => {}
    }
}

/// Sort key type: (sort_value, is_descending, nulls_first)
type SortKey = (vibesql_types::SqlValue, bool, Option<bool>);

/// Evaluate an expression against an outer row using the outer schema.
/// This is used for outer-correlated aggregates (issue #4930).
///
/// Supports:
/// - Simple column references
/// - Literals
///
/// For more complex expressions, returns None to indicate fallback to normal evaluation.
fn evaluate_expr_against_outer_row(
    expr: &vibesql_ast::Expression,
    outer_row: &vibesql_storage::Row,
    outer_schema: &CombinedSchema,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    match expr {
        Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();

            // Look up column in outer schema
            let table_ref = table.as_deref();
            if let Some(idx) = outer_schema.get_column_index(table_ref, &column) {
                if idx < outer_row.values.len() {
                    return Ok(outer_row.values[idx].clone());
                }
            }

            // Column not found - return NULL (shouldn't happen if expression_refs_only_outer_columns is correct)
            Ok(SqlValue::Null)
        }
        Expression::Literal(val) => Ok(val.clone()),
        Expression::Cast { expr: inner, .. } => {
            // For casts, just evaluate the inner expression (simplified)
            evaluate_expr_against_outer_row(inner, outer_row, outer_schema)
        }
        _ => {
            // For unsupported expressions, return NULL
            // In practice, outer-only aggregate args are usually simple column refs
            Ok(SqlValue::Null)
        }
    }
}

/// Compare two sets of sort keys with collation and NULLS FIRST/LAST support.
/// This is a helper function to eliminate code duplication between GROUP_CONCAT and JSON_GROUP_ARRAY.
fn compare_sort_keys(
    a_keys: &[SortKey],
    b_keys: &[SortKey],
    collations: &[Option<String>],
) -> std::cmp::Ordering {
    for (idx, ((val_a, desc_a, nulls_first_a), (val_b, _, _))) in
        a_keys.iter().zip(b_keys.iter()).enumerate()
    {
        // Handle NULLs with explicit ordering
        let (a_null, b_null) = (val_a.is_null(), val_b.is_null());
        if a_null || b_null {
            if a_null && b_null {
                continue; // Both NULL, equal for this key
            }
            // Determine if NULLs should come first
            // Default: NULLS LAST for ASC, NULLS FIRST for DESC (SQLite behavior)
            let nulls_first = nulls_first_a.unwrap_or(*desc_a);
            if a_null {
                return if nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                };
            } else {
                return if nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                };
            }
        }

        // Use collation-aware comparison if collation specified
        let cmp = if let Some(Some(collation)) = collations.get(idx) {
            crate::select::grouping::compare_sql_values_with_collation(
                val_a,
                val_b,
                Some(collation.as_str()),
            )
        } else {
            compare_sql_values(val_a, val_b)
        };

        if cmp != std::cmp::Ordering::Equal {
            return if *desc_a { cmp.reverse() } else { cmp };
        }
    }
    std::cmp::Ordering::Equal
}

/// Extract collations from ORDER BY expressions (if wrapped in Collate).
fn extract_collations(order_items: &[vibesql_ast::OrderByItem]) -> Vec<Option<String>> {
    order_items
        .iter()
        .map(|item| {
            if let vibesql_ast::Expression::Collate { collation, .. } = &item.expr {
                Some(collation.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Validate aggregate function argument count
/// Returns error with SQLite-compatible message if validation fails
/// `name` is the FunctionIdentifier which provides both canonical (lowercase) and display forms
fn validate_aggregate_args(
    name: &vibesql_ast::FunctionIdentifier,
    args: &[vibesql_ast::Expression],
) -> Result<(), ExecutorError> {
    let name_upper = name.canonical().to_uppercase();
    let arg_count = args.len();

    // Check for wildcard in non-COUNT aggregates
    let has_wildcard = args.iter().any(|arg| {
        matches!(arg, vibesql_ast::Expression::Wildcard)
            || matches!(
                arg,
                vibesql_ast::Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
            )
    });

    match name_upper.as_str() {
        "COUNT" => {
            // COUNT allows 0 args (COUNT(*) is handled specially), 1 arg, or multiple with DISTINCT
            // COUNT() with no args and no * is still allowed
            Ok(())
        }
        "MIN" | "MAX" => {
            // min() and max() with * is wrong number of args
            if has_wildcard || arg_count == 0 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.display().to_string(),
                });
            }
            // min/max with > 1 arg becomes scalar function, handled elsewhere
            Ok(())
        }
        "SUM" | "AVG" | "TOTAL" => {
            // These require exactly 1 argument, no wildcard allowed
            if has_wildcard || arg_count == 0 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.display().to_string(),
                });
            }
            if arg_count > 1 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.display().to_string(),
                });
            }
            Ok(())
        }
        "GROUP_CONCAT" | "STRING_AGG" => {
            // GROUP_CONCAT/STRING_AGG requires 1 or 2 arguments
            if arg_count == 0 || arg_count > 2 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.display().to_string(),
                });
            }
            Ok(())
        }
        "MEDIAN" => {
            // median(Y) requires exactly 1 argument (percentile.c)
            if has_wildcard || arg_count != 1 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.display().to_string(),
                });
            }
            Ok(())
        }
        "PERCENTILE" | "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
            // percentile(Y,P) family requires exactly 2 arguments (percentile.c)
            if has_wildcard || arg_count != 2 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.display().to_string(),
                });
            }
            Ok(())
        }
        _ => {
            // Unknown aggregate functions require at least 1 argument
            if arg_count == 0 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.display().to_string(),
                });
            }
            Ok(())
        }
    }
}

/// Evaluate aggregate function expressions (COUNT, SUM, AVG, MIN, MAX)
/// Only handles AggregateFunction variant
pub(super) fn evaluate(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Extract name, distinct, args, order_by, and filter from AggregateFunction
    let (name, distinct, args, order_by, filter) = match expr {
        vibesql_ast::Expression::AggregateFunction { name, distinct, args, order_by, filter } => {
            (name, *distinct, args, order_by, filter)
        }
        _ => unreachable!("evaluate called with non-aggregate expression"),
    };

    // Helper closure to check if a row passes the FILTER condition
    // Returns true if there's no filter, or if the filter evaluates to truthy
    let passes_filter = |row: &vibesql_storage::Row,
                         evaluator: &CombinedExpressionEvaluator|
     -> Result<bool, ExecutorError> {
        if let Some(filter_expr) = filter {
            let filter_result = evaluator.eval(filter_expr, row)?;
            // SQLite uses general truthiness: any non-zero, non-NULL value is true
            Ok(executor.is_truthy(&filter_result)?)
        } else {
            Ok(true)
        }
    };

    // Validate argument count first
    validate_aggregate_args(name, args)?;

    // Generate cache key for this aggregate expression
    // Format: "{name}:{distinct}:{arg_debug}:{order_by_debug}:{filter_debug}"
    // Include order_by and filter to distinguish aggregates with different clauses
    let cache_key =
        format!("{}:{}:{:?}:{:?}:{:?}", name.to_uppercase(), distinct, args, order_by, filter);

    // Check cache first (lazily initialized)
    if let Some(cached_result) = executor.get_aggregate_cache().borrow().get(&cache_key) {
        return Ok(cached_result.clone());
    }

    let mut acc = AggregateAccumulator::new(name.canonical(), distinct)?;

    // Special handling for COUNT(*)
    if name.to_uppercase() == "COUNT" && args.len() == 1 {
        let is_count_star = matches!(args[0], vibesql_ast::Expression::Wildcard)
            || matches!(
                &args[0],
                vibesql_ast::Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
            );

        if is_count_star {
            // COUNT(*) - count all rows (DISTINCT not allowed with *)
            if distinct {
                return Err(ExecutorError::UnsupportedExpression(
                    "COUNT(DISTINCT *) is not valid SQL".to_string(),
                ));
            }
            // Fast path: COUNT(*) without DISTINCT and without FILTER is just row count (O(1) vs O(n))
            if filter.is_none() {
                let result = vibesql_types::SqlValue::Integer(group_rows.len() as i64);
                // Cache the result (lazily initialized)
                executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
                return Ok(result);
            }
            // COUNT(*) with FILTER: need to count rows that pass the filter
            let mut count = 0i64;
            for row in group_rows {
                evaluator.clear_cse_cache();
                if passes_filter(row, evaluator)? {
                    count += 1;
                }
            }
            let result = vibesql_types::SqlValue::Integer(count);
            executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }
    }

    // Handle multi-argument COUNT(DISTINCT a, b, ...) - SQLite extension
    // This counts distinct combinations of values
    if name.to_uppercase() == "COUNT" && args.len() > 1 {
        if !distinct {
            // SQLite-compatible error message - preserve original case
            return Err(ExecutorError::WrongNumberOfArguments {
                function_name: name.display().to_string(),
            });
        }

        // Evaluate all arguments for each row and accumulate as tuples
        for row in group_rows {
            evaluator.clear_cse_cache();

            // Skip rows that don't pass the FILTER condition
            if !passes_filter(row, evaluator)? {
                continue;
            }

            let mut tuple_values = Vec::with_capacity(args.len());
            for arg in args {
                let value = evaluator.eval(arg, row)?;
                tuple_values.push(value);
            }
            acc.accumulate_tuple(tuple_values);
        }

        let result = acc.finalize()?;
        executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
        return Ok(result);
    }

    // Handle GROUP_CONCAT/STRING_AGG with optional separator (2nd argument)
    // GROUP_CONCAT(expr) - uses comma separator
    // GROUP_CONCAT(expr, separator) - uses custom separator
    // GROUP_CONCAT(expr ORDER BY ...) - sorted concatenation
    // STRING_AGG is an alias for GROUP_CONCAT (SQLite 3.44+)
    let name_upper = name.to_uppercase();

    // Handle the PERCENTILE family (median/percentile/percentile_cont/
    // percentile_disc). The second argument (the fraction P/F) is evaluated
    // per row and validated inside the accumulator, mirroring SQLite's
    // percentile.c step function (same per-row threading as GROUP_CONCAT's
    // separator argument).
    if matches!(
        name_upper.as_str(),
        "MEDIAN" | "PERCENTILE" | "PERCENTILE_CONT" | "PERCENTILE_DISC"
    ) {
        let fraction_expr = args.get(1);
        for row in group_rows {
            evaluator.clear_cse_cache();
            // Skip rows that don't pass the FILTER condition
            if !passes_filter(row, evaluator)? {
                continue;
            }
            let fraction_value = match fraction_expr {
                Some(expr) => Some(evaluator.eval(expr, row)?),
                None => None,
            };
            let value = evaluator.eval(&args[0], row)?;
            acc.accumulate_percentile(&value, fraction_value.as_ref());
        }

        let result = acc.finalize()?;
        executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
        return Ok(result);
    }

    if name_upper == "GROUP_CONCAT" || name_upper == "STRING_AGG" {
        let separator = if args.len() == 2 {
            // Evaluate separator (second argument)
            // SQLite uses the LAST row's value when separator is an expression
            // (e.g., when separator is a column reference like b1 in group_concat(a1, b1))
            // Fix for aggnested-1.4 test
            if let Some(last_row) = group_rows.last() {
                evaluator.clear_cse_cache();
                let sep_value = evaluator.eval(&args[1], last_row)?;
                match sep_value {
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                        s.to_string()
                    }
                    vibesql_types::SqlValue::Null => String::new(), // NULL separator = empty string (SQLite behavior)
                    other => other.to_string(),                     // Convert other types to string
                }
            } else {
                ",".to_string() // Empty group, use default
            }
        } else if args.len() == 1 {
            ",".to_string() // Default separator
        } else {
            return Err(ExecutorError::UnsupportedExpression(format!(
                "{} expects 1 or 2 arguments, got {}",
                name,
                args.len()
            )));
        };

        // Handle ORDER BY clause within the aggregate
        if let Some(order_items) = order_by {
            let collations = extract_collations(order_items);

            // Collect (value, sort_keys, row_index) for each row
            // We need row_index to find the last row after sorting for separator evaluation
            let mut value_sort_pairs: Vec<(vibesql_types::SqlValue, Vec<SortKey>, usize)> =
                Vec::with_capacity(group_rows.len());

            for (row_idx, row) in group_rows.iter().enumerate() {
                evaluator.clear_cse_cache();

                // Skip rows that don't pass the FILTER condition
                if !passes_filter(row, evaluator)? {
                    continue;
                }

                let value = evaluator.eval(&args[0], row)?;

                // Skip NULL values for GROUP_CONCAT
                if matches!(value, vibesql_types::SqlValue::Null) {
                    continue;
                }

                // Evaluate sort keys
                let mut sort_keys = Vec::with_capacity(order_items.len());
                for item in order_items {
                    let sort_value = evaluator.eval(&item.expr, row)?;
                    let is_desc = item.direction == vibesql_ast::OrderDirection::Desc;
                    let nulls_first = item
                        .nulls_order
                        .as_ref()
                        .map(|no| matches!(no, vibesql_ast::NullsOrder::First));
                    sort_keys.push((sort_value, is_desc, nulls_first));
                }

                value_sort_pairs.push((value, sort_keys, row_idx));
            }

            // Sort using the helper function
            value_sort_pairs.sort_by(|a, b| compare_sort_keys(&a.1, &b.1, &collations));

            // For ORDER BY, get separator from the last row after sorting
            let final_separator = if args.len() == 2 && !value_sort_pairs.is_empty() {
                let last_row_idx = value_sort_pairs.last().map(|(_, _, idx)| *idx).unwrap_or(0);
                let last_row = &group_rows[last_row_idx];
                evaluator.clear_cse_cache();
                let sep_value = evaluator.eval(&args[1], last_row)?;
                match sep_value {
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                        s.to_string()
                    }
                    vibesql_types::SqlValue::Null => String::new(),
                    other => other.to_string(),
                }
            } else {
                separator.clone()
            };

            // Now accumulate in sorted order
            let mut acc = AggregateAccumulator::new_with_separator(
                name.canonical(),
                distinct,
                &final_separator,
            )?;
            for (value, _, _) in value_sort_pairs {
                acc.accumulate(&value);
            }

            let result = acc.finalize()?;
            executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }

        // No ORDER BY - use the original path
        let mut acc =
            AggregateAccumulator::new_with_separator(name.canonical(), distinct, &separator)?;

        // Issue #4930: Check if aggregate argument references only outer columns
        // If so, iterate over outer_rows instead of group_rows
        let has_outer_context = evaluator.get_outer_schema().is_some();
        let is_outer_only =
            expression_refs_only_outer_columns(&args[0], evaluator.get_schema(), has_outer_context);

        if is_outer_only {
            // Outer-correlated aggregate: iterate over all outer rows
            if let (Some(outer_rows), Some(outer_schema)) =
                (evaluator.get_outer_rows(), evaluator.get_outer_schema())
            {
                // Create a temporary evaluator with outer_schema as the main schema
                // This allows us to evaluate the expression against each outer row
                for outer_row in outer_rows.iter() {
                    evaluator.clear_cse_cache();
                    // For outer-only expressions, evaluate using outer context
                    // We need to look up the column in outer_schema and get value from outer_row
                    let value = evaluate_expr_against_outer_row(&args[0], outer_row, outer_schema)?;
                    acc.accumulate(&value);
                }

                let result = acc.finalize()?;
                executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
                return Ok(result);
            }
            // Fall through to normal path if outer_rows not available
        }

        for row in group_rows {
            evaluator.clear_cse_cache();
            // Skip rows that don't pass the FILTER condition
            if !passes_filter(row, evaluator)? {
                continue;
            }
            let value = evaluator.eval(&args[0], row)?;
            acc.accumulate(&value);
        }

        let result = acc.finalize()?;
        executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
        return Ok(result);
    }

    // Handle JSON_GROUP_ARRAY with optional ORDER BY
    if name_upper == "JSON_GROUP_ARRAY" {
        if args.len() != 1 {
            return Err(ExecutorError::WrongNumberOfArguments {
                function_name: name.display().to_string(),
            });
        }

        // Handle ORDER BY clause within the aggregate
        if let Some(order_items) = order_by {
            let collations = extract_collations(order_items);

            // Collect (value, sort_keys) pairs for each row
            let mut value_sort_pairs: Vec<(vibesql_types::SqlValue, Vec<SortKey>)> =
                Vec::with_capacity(group_rows.len());

            for row in group_rows {
                evaluator.clear_cse_cache();

                // Skip rows that don't pass the FILTER condition
                if !passes_filter(row, evaluator)? {
                    continue;
                }

                let value = evaluator.eval(&args[0], row)?;

                // JSON_GROUP_ARRAY includes NULL values (unlike GROUP_CONCAT)

                // Evaluate sort keys
                let mut sort_keys = Vec::with_capacity(order_items.len());
                for item in order_items {
                    let sort_value = evaluator.eval(&item.expr, row)?;
                    let is_desc = item.direction == vibesql_ast::OrderDirection::Desc;
                    let nulls_first = item
                        .nulls_order
                        .as_ref()
                        .map(|no| matches!(no, vibesql_ast::NullsOrder::First));
                    sort_keys.push((sort_value, is_desc, nulls_first));
                }

                value_sort_pairs.push((value, sort_keys));
            }

            // Sort using the helper function
            value_sort_pairs.sort_by(|a, b| compare_sort_keys(&a.1, &b.1, &collations));

            // Now accumulate in sorted order
            let mut acc = AggregateAccumulator::new(name.canonical(), distinct)?;
            for (value, _) in value_sort_pairs {
                acc.accumulate(&value);
            }

            let result = acc.finalize()?;
            executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }

        // No ORDER BY - use the standard path
        let mut acc = AggregateAccumulator::new(name.canonical(), distinct)?;

        for row in group_rows {
            evaluator.clear_cse_cache();
            // Skip rows that don't pass the FILTER condition
            if !passes_filter(row, evaluator)? {
                continue;
            }
            let value = evaluator.eval(&args[0], row)?;
            acc.accumulate(&value);
        }

        let result = acc.finalize()?;
        executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
        return Ok(result);
    }

    // Handle MD5SUM with multiple arguments
    // md5sum(col, lit1, lit2, ...) concatenates all arguments for each row,
    // then concatenates all rows, and computes MD5 of the final string
    if name_upper == "MD5SUM" && args.len() > 1 {
        let mut acc = AggregateAccumulator::new(name.canonical(), distinct)?;

        for row in group_rows {
            evaluator.clear_cse_cache();

            // Skip rows that don't pass the FILTER condition
            if !passes_filter(row, evaluator)? {
                continue;
            }

            // Evaluate all arguments and concatenate them for this row
            let mut row_value = String::new();
            for arg in args {
                let value = evaluator.eval(arg, row)?;
                // Convert value to string representation
                let str_val = match &value {
                    vibesql_types::SqlValue::Null => String::new(), // NULL becomes empty
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                        s.to_string()
                    }
                    other => other.to_string(),
                };
                row_value.push_str(&str_val);
            }

            // Accumulate the concatenated row value
            acc.accumulate(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(row_value)));
        }

        let result = acc.finalize()?;
        executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
        return Ok(result);
    }

    // Regular aggregate - evaluate single argument for each row
    // This should be caught by validate_aggregate_args above, but keep as safety net
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: name.display().to_string(),
        });
    }

    // Special handling for COUNT with any argument
    // For COUNT, we need to evaluate the expression and count non-NULL results
    // However, COUNT(*) should count ALL rows regardless of NULL values
    if name.to_uppercase() == "COUNT" {
        // Double-check for COUNT(*) with various representations
        // This handles cases where the wildcard might not be caught by the fast path above
        let is_count_star_fallback = matches!(&args[0], vibesql_ast::Expression::Wildcard)
            || matches!(
                &args[0],
                vibesql_ast::Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
            );

        if is_count_star_fallback {
            // COUNT(*) fallback: just count all rows
            let result = vibesql_types::SqlValue::Integer(group_rows.len() as i64);
            executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }
    }

    // Issue #4930 / #5104: outer-correlated aggregate. When the aggregate's
    // argument references only outer columns, iterate over all outer rows
    // instead of `group_rows`. This is the inner-evaluation path used by
    // SQLite's implicit-outer-aggregate-collapse: outer query collapses to
    // one row, and the inner aggregate runs over all outer rows.
    //
    // Previously only GROUP_CONCAT/STRING_AGG implemented this path; here we
    // generalize it to all aggregates (AVG, SUM, MIN, MAX, COUNT, ...) so
    // `SELECT (SELECT avg(a)) FROM t2` produces a single 2.0 row instead of
    // averaging one row at a time. (window4.test 12.2)
    let has_outer_context_for_collapse = evaluator.get_outer_schema().is_some();
    let is_outer_only = expression_refs_only_outer_columns(
        &args[0],
        evaluator.get_schema(),
        has_outer_context_for_collapse,
    );
    if is_outer_only {
        if let (Some(outer_rows), Some(outer_schema)) =
            (evaluator.get_outer_rows(), evaluator.get_outer_schema())
        {
            for outer_row in outer_rows.iter() {
                evaluator.clear_cse_cache();
                // FILTER inside the inner aggregate may reference the same
                // outer column. Evaluate it against the outer row using the
                // outer schema; on failure (e.g. references inner columns),
                // fall back to skipping the filter (no rows to filter from).
                if let Some(filter_expr) = filter {
                    let filter_val = evaluate_expr_against_outer_row(
                        filter_expr,
                        outer_row,
                        outer_schema,
                    )?;
                    if !executor.is_truthy(&filter_val)? {
                        continue;
                    }
                }
                let value =
                    evaluate_expr_against_outer_row(&args[0], outer_row, outer_schema)?;
                acc.accumulate(&value);
            }
            let result = acc.finalize()?;
            executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }
        // Fall through to normal path if outer_rows not available
    }

    // Try to compile CASE expression for fast-path evaluation (#3079)
    // This optimization helps TPC-DS Q2 which has 7 SUM(CASE...) aggregates
    // For ~14K rows × 7 aggregates = ~98K evaluations, compiled CASE avoids:
    // - CSE cache clearing overhead per row
    // - Expression tree traversal
    // - Dynamic dispatch through evaluator
    // Provides ~5-10% improvement for CASE-heavy GROUP BY queries
    let compiled_case = if matches!(&args[0], vibesql_ast::Expression::Case { .. }) {
        CompiledCaseExpression::try_compile(&args[0], evaluator.schema())
    } else {
        None
    };

    if let Some(ref compiled) = compiled_case {
        // Fast path: use compiled CASE expression (no CSE cache, no expression traversal)
        // Note: if there's a filter, we still need to evaluate it through the evaluator
        if filter.is_some() {
            for row in group_rows {
                evaluator.clear_cse_cache();
                // Skip rows that don't pass the FILTER condition
                if !passes_filter(row, evaluator)? {
                    continue;
                }
                let value = compiled.evaluate(row);
                acc.accumulate(&value);
            }
        } else {
            for row in group_rows {
                let value = compiled.evaluate(row);
                acc.accumulate(&value);
            }
        }
    } else {
        // Slow path: full expression evaluation
        for row in group_rows {
            // Clear CSE cache before evaluating each row to prevent column values
            // from being incorrectly cached across different rows
            evaluator.clear_cse_cache();

            // Skip rows that don't pass the FILTER condition
            if !passes_filter(row, evaluator)? {
                continue;
            }

            let value = evaluator.eval(&args[0], row)?;
            acc.accumulate(&value);
        }
    }

    let result = acc.finalize()?;
    // Cache the result for reuse within this group (lazily initialized)
    executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
    Ok(result)
}
