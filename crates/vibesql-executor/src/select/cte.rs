//! Common Table Expression (CTE) handling for SELECT queries

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::errors::ExecutorError;

/// CTE result: (schema, shared rows)
///
/// Uses `Arc<Vec<Row>>` to enable O(1) cloning when CTEs are:
/// - Propagated from outer queries to subqueries
/// - Referenced multiple times without filtering
///
/// This avoids deep-cloning all rows on every CTE reference.
pub type CteResult = (vibesql_catalog::TableSchema, Arc<Vec<vibesql_storage::Row>>);

/// Execute all CTEs and return their results
///
/// CTEs are executed in order, allowing later CTEs to reference earlier ones.
pub fn execute_ctes<F>(
    ctes: &[vibesql_ast::CommonTableExpr],
    executor: F,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
{
    // Use the memory-tracking version with a no-op memory check
    execute_ctes_with_memory_check(ctes, executor, |_| Ok(()))
}

/// Execute all CTEs with memory tracking
///
/// CTEs are executed in order, allowing later CTEs to reference earlier ones.
/// After each CTE is materialized, the memory_check callback is called with
/// the estimated size of the CTE result to enforce memory limits.
pub(super) fn execute_ctes_with_memory_check<F, M>(
    ctes: &[vibesql_ast::CommonTableExpr],
    executor: F,
    memory_check: M,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    let mut cte_results = HashMap::new();

    // Execute each CTE in order
    // CTEs can reference previously defined CTEs
    for cte in ctes {
        // Check if this is a recursive CTE
        // SQLite compatibility: auto-detect recursive CTEs even without RECURSIVE keyword
        // A CTE is recursive if it references itself in a UNION/UNION ALL set operation
        let is_recursive = cte.recursive || is_cte_self_referential(cte);
        let rows = if is_recursive {
            // Recursive CTE: execute base term, then iteratively execute recursive term
            execute_recursive_cte(cte, &cte_results, &executor, &memory_check)?
        } else {
            // Non-recursive CTE: execute query directly
            executor(&cte.query, &cte_results)?
        };

        // Track memory for this CTE result before storing
        let estimated_size = super::helpers::estimate_result_size(&rows);
        memory_check(estimated_size)?;

        //  Determine the schema for this CTE
        let schema = derive_cte_schema(cte, &rows)?;

        // Store the CTE result wrapped in Arc for efficient sharing
        cte_results.insert(cte.name.clone(), (schema, Arc::new(rows)));
    }

    Ok(cte_results)
}

/// Derive the schema for a CTE from its query and results
pub(super) fn derive_cte_schema(
    cte: &vibesql_ast::CommonTableExpr,
    rows: &[vibesql_storage::Row],
) -> Result<vibesql_catalog::TableSchema, ExecutorError> {
    // If column names are explicitly specified, use those
    if let Some(column_names) = &cte.columns {
        // Get data types from first row (if available)
        if let Some(first_row) = rows.first() {
            if first_row.values.len() != column_names.len() {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CTE column count mismatch: specified {} columns but query returned {}",
                    column_names.len(),
                    first_row.values.len()
                )));
            }

            let columns = column_names
                .iter()
                .zip(&first_row.values)
                .map(|(name, value)| {
                    let data_type = infer_type_from_value(value);
                    vibesql_catalog::ColumnSchema::new(name.clone(), data_type, true)
                    // nullable for
                    // simplicity
                })
                .collect();

            Ok(vibesql_catalog::TableSchema::new(cte.name.clone(), columns))
        } else {
            // Empty result set - create schema with VARCHAR columns
            let columns = column_names
                .iter()
                .map(|name| {
                    vibesql_catalog::ColumnSchema::new(
                        name.clone(),
                        vibesql_types::DataType::Varchar { max_length: Some(255) },
                        true,
                    )
                })
                .collect();

            Ok(vibesql_catalog::TableSchema::new(cte.name.clone(), columns))
        }
    } else {
        // No explicit column names - infer from query SELECT list
        // Extract column names from SELECT items
        let columns = cte
            .query
            .select_list
            .iter()
            .enumerate()
            .map(|(i, item)| {
                // Infer data type from first row if available, otherwise use default
                let data_type = if let Some(first_row) = rows.first() {
                    infer_type_from_value(&first_row.values[i])
                } else {
                    // No rows - use default type (VARCHAR)
                    vibesql_types::DataType::Varchar { max_length: Some(255) }
                };

                // Extract column name from SELECT item
                let col_name = match item {
                    vibesql_ast::SelectItem::Wildcard { .. }
                    | vibesql_ast::SelectItem::QualifiedWildcard { .. } => format!("col{}", i),
                    vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                        if let Some(a) = alias {
                            a.clone()
                        } else {
                            // Try to extract name from expression
                            match expr {
                                vibesql_ast::Expression::ColumnRef(col_id) => {
                                    col_id.column_canonical().to_string()
                                }
                                _ => format!("col{}", i),
                            }
                        }
                    }
                };

                vibesql_catalog::ColumnSchema::new(col_name, data_type, true) // nullable
            })
            .collect();

        Ok(vibesql_catalog::TableSchema::new(cte.name.clone(), columns))
    }
}

/// Execute a recursive CTE using iterative evaluation
///
/// Recursive CTEs in SQL:1999/SQLite are defined with UNION or UNION ALL:
/// ```sql
/// WITH RECURSIVE cte AS (
///   base_query          -- Executed once to get initial rows
///   UNION [ALL]
///   recursive_query     -- References 'cte', executed iteratively
/// )
/// ```
///
/// Algorithm:
/// 1. Split query into base and recursive terms (before/after UNION [ALL])
/// 2. Execute base term to get initial working table
/// 3. Repeat until no new rows or max depth reached:
///    - Make working table available as CTE
///    - Execute recursive term
///    - Add new rows to result (with deduplication for UNION)
///    - Update working table to new rows
fn execute_recursive_cte<F, M>(
    cte: &vibesql_ast::CommonTableExpr,
    cte_results: &HashMap<String, CteResult>,
    executor: &F,
    memory_check: &M,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    use crate::limits::MAX_RECURSIVE_CTE_ITERATIONS;

    // Validate that recursive CTE uses UNION ALL
    let set_op = cte.query.set_operation.as_ref().ok_or_else(|| {
        ExecutorError::UnsupportedFeature(format!(
            "Recursive CTE '{}' must use UNION ALL",
            cte.name
        ))
    })?;

    if set_op.op != vibesql_ast::SetOperator::Union {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "Recursive CTE '{}' must use UNION or UNION ALL (not INTERSECT or EXCEPT)",
            cte.name
        )));
    }

    // Extract base and recursive terms
    // Base term: the main SELECT (before UNION [ALL])
    // Recursive term: the right side of UNION [ALL]

    // Create base-only query without the UNION ALL set operation
    // This prevents the base term from trying to reference the CTE before it exists
    let base_query = vibesql_ast::SelectStmt {
        with_clause: cte.query.with_clause.clone(),
        distinct: cte.query.distinct,
        select_list: cte.query.select_list.clone(),
        into_table: cte.query.into_table.clone(),
        into_variables: cte.query.into_variables.clone(),
        from: cte.query.from.clone(),
        where_clause: cte.query.where_clause.clone(),
        group_by: cte.query.group_by.clone(),
        having: cte.query.having.clone(),
        window_definitions: cte.query.window_definitions.clone(),
        order_by: cte.query.order_by.clone(),
        limit: cte.query.limit.clone(),
        offset: cte.query.offset.clone(),
        set_operation: None, // Remove UNION ALL for base term execution
        values: cte.query.values.clone(),
    };
    let recursive_query = &set_op.right;

    // SQLite compatibility: window functions are not allowed in the recursive
    // part of a recursive CTE (window1.test 15.0). SQLite reports the exact
    // error "cannot use window functions in recursive queries".
    if crate::select::window::has_window_functions(&recursive_query.select_list) {
        return Err(ExecutorError::SqliteCompatError(
            "cannot use window functions in recursive queries".to_string(),
        ));
    }

    // Try static validation first (works for explicit column lists and VALUES)
    // This provides better SQLite compatibility by catching errors at prepare time
    // rather than waiting until runtime
    // Note: For VALUES statements, column count comes from the VALUES rows, not select_list
    if let (Some(base_count), Some(recursive_count)) =
        (count_stmt_columns(&base_query), count_stmt_columns(recursive_query))
    {
        if base_count != recursive_count {
            return Err(ExecutorError::UnsupportedFeature(
                "SELECTs to the left and right of UNION ALL do not have the same number of result columns".to_string()
            ));
        }
    }
    // Fall back to runtime validation for wildcards (existing code below at line 279-289)

    // Step 1: Execute base term to get initial rows
    let mut all_rows = executor(&base_query, cte_results)?;
    let mut working_table = all_rows.clone();

    // Derive schema from base term result
    let schema = derive_cte_schema(cte, &all_rows)?;

    // Track seen rows for UNION (deduplication)
    // For UNION ALL, we skip tracking to preserve all rows
    let mut seen_rows: Option<HashSet<vibesql_storage::RowValues>> = if !set_op.all {
        let mut seen = HashSet::with_capacity(all_rows.len());
        for row in &all_rows {
            seen.insert(row.values.clone());
        }
        Some(seen)
    } else {
        None
    };

    // Step 2: Iterative evaluation
    let mut depth = 0;
    while !working_table.is_empty() && depth < MAX_RECURSIVE_CTE_ITERATIONS {
        depth += 1;

        // Make working table available as this CTE for recursive reference
        let mut recursive_cte_results = cte_results.clone();
        recursive_cte_results
            .insert(cte.name.clone(), (schema.clone(), Arc::new(working_table.clone())));

        // Execute recursive term with working table as CTE
        let new_rows = executor(recursive_query, &recursive_cte_results)?;

        // If no new rows, we're done
        if new_rows.is_empty() {
            break;
        }

        // Validate that recursive term returns same number of columns as base term
        // This check is done on first iteration to catch schema mismatches early
        if depth == 1 && !new_rows.is_empty() && !all_rows.is_empty() {
            let base_col_count = all_rows[0].values.len();
            let recursive_col_count = new_rows[0].values.len();
            if base_col_count != recursive_col_count {
                return Err(ExecutorError::UnsupportedFeature(
                    "SELECTs to the left and right of UNION ALL do not have the same number of result columns".to_string()
                ));
            }
        }

        // Check memory before adding new rows
        let estimated_size = super::helpers::estimate_result_size(&new_rows);
        memory_check(estimated_size)?;

        // Filter out duplicates for UNION (keep all for UNION ALL)
        let rows_to_add: Vec<vibesql_storage::Row> = if let Some(ref mut seen) = seen_rows {
            // UNION: only add rows we haven't seen before
            new_rows.into_iter().filter(|row| seen.insert(row.values.clone())).collect()
        } else {
            // UNION ALL: keep all rows
            new_rows
        };

        // If no new unique rows (for UNION), we're done
        if rows_to_add.is_empty() {
            break;
        }

        // Add new rows to result
        all_rows.extend(rows_to_add.clone());

        // Update working table to be the new rows for next iteration
        working_table = rows_to_add;
    }

    // Check if we hit max recursion depth
    if depth >= MAX_RECURSIVE_CTE_ITERATIONS {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "Recursive CTE '{}' exceeded maximum iteration limit of {}",
            cte.name, MAX_RECURSIVE_CTE_ITERATIONS
        )));
    }

    Ok(all_rows)
}

/// Count columns if select list has only explicit expressions (no wildcards)
///
/// Returns Some(count) if all select items are explicit expressions.
/// Returns None if any wildcards are present (requires schema info to count).
fn count_explicit_columns(select_list: &[vibesql_ast::SelectItem]) -> Option<usize> {
    let mut count = 0;
    for item in select_list {
        match item {
            vibesql_ast::SelectItem::Expression { .. } => count += 1,
            // Can't count wildcards statically - need schema info
            vibesql_ast::SelectItem::Wildcard { .. }
            | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                return None;
            }
        }
    }
    Some(count)
}

/// Count columns in a SELECT statement, considering both select_list and VALUES.
/// For VALUES statements, the column count comes from the first row of values.
/// For SELECT statements, the column count comes from the select_list.
/// Returns None if any wildcards are present (requires schema info to count).
fn count_stmt_columns(stmt: &vibesql_ast::SelectStmt) -> Option<usize> {
    // If this is a VALUES statement, count columns from the first VALUES row
    if let Some(values_rows) = &stmt.values {
        return values_rows.first().map(|row| row.len());
    }

    // Otherwise, count columns from the select_list
    count_explicit_columns(&stmt.select_list)
}

/// Check if a CTE is self-referential (references itself in UNION/UNION ALL)
///
/// SQLite allows recursive CTEs without the RECURSIVE keyword if the CTE
/// references itself in a set operation. This function detects such cases
/// by checking if the right side of a UNION/UNION ALL references the CTE name.
fn is_cte_self_referential(cte: &vibesql_ast::CommonTableExpr) -> bool {
    // Check if the CTE has a UNION/UNION ALL set operation
    let set_op = match &cte.query.set_operation {
        Some(op) if op.op == vibesql_ast::SetOperator::Union => op,
        _ => return false,
    };

    // Check if the recursive term references this CTE
    stmt_references_table(&set_op.right, &cte.name)
}

/// Check if a SELECT statement references a table name
fn stmt_references_table(stmt: &vibesql_ast::SelectStmt, table_name: &str) -> bool {
    // Check FROM clause
    if let Some(from) = &stmt.from {
        if from_clause_references_table(from, table_name) {
            return true;
        }
    }

    // Check WHERE clause for subqueries
    if let Some(where_clause) = &stmt.where_clause {
        if expr_references_table(where_clause, table_name) {
            return true;
        }
    }

    // Check SELECT list for subqueries
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            if expr_references_table(expr, table_name) {
                return true;
            }
        }
    }

    false
}

/// Check if a FROM clause references a table name
fn from_clause_references_table(from: &vibesql_ast::FromClause, table_name: &str) -> bool {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => name.eq_ignore_ascii_case(table_name),
        vibesql_ast::FromClause::Subquery { query, .. } => stmt_references_table(query, table_name),
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            from_clause_references_table(left, table_name)
                || from_clause_references_table(right, table_name)
                || condition.as_ref().map_or(false, |c| expr_references_table(c, table_name))
        }
        vibesql_ast::FromClause::Values { .. } => false,
    }
}

/// Check if an expression references a table name (in subqueries)
fn expr_references_table(expr: &vibesql_ast::Expression, table_name: &str) -> bool {
    match expr {
        vibesql_ast::Expression::ScalarSubquery(subquery) => {
            stmt_references_table(subquery, table_name)
        }
        vibesql_ast::Expression::In { subquery, .. } => stmt_references_table(subquery, table_name),
        vibesql_ast::Expression::Exists { subquery, .. } => {
            stmt_references_table(subquery, table_name)
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            expr_references_table(left, table_name) || expr_references_table(right, table_name)
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => expr_references_table(expr, table_name),
        vibesql_ast::Expression::Function { args, .. } => {
            args.iter().any(|arg| expr_references_table(arg, table_name))
        }
        vibesql_ast::Expression::AggregateFunction { args, filter, .. } => {
            args.iter().any(|arg| expr_references_table(arg, table_name))
                || filter.as_ref().map_or(false, |f| expr_references_table(f, table_name))
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().map_or(false, |o| expr_references_table(o, table_name))
                || when_clauses.iter().any(|when| {
                    when.conditions.iter().any(|c| expr_references_table(c, table_name))
                        || expr_references_table(&when.result, table_name)
                })
                || else_result.as_ref().map_or(false, |e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            expr_references_table(expr, table_name)
                || expr_references_table(low, table_name)
                || expr_references_table(high, table_name)
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            expr_references_table(expr, table_name)
                || values.iter().any(|e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::Cast { expr, .. }
        | vibesql_ast::Expression::Collate { expr, .. } => expr_references_table(expr, table_name),
        vibesql_ast::Expression::Conjunction(exprs)
        | vibesql_ast::Expression::Disjunction(exprs) => {
            exprs.iter().any(|e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::QuantifiedComparison { expr, subquery, .. } => {
            expr_references_table(expr, table_name) || stmt_references_table(subquery, table_name)
        }
        _ => false,
    }
}

/// Infer data type from a SQL value
pub(super) fn infer_type_from_value(value: &vibesql_types::SqlValue) -> vibesql_types::DataType {
    match value {
        vibesql_types::SqlValue::Null => vibesql_types::DataType::Varchar { max_length: Some(255) }, /* default */
        vibesql_types::SqlValue::Integer(_) => vibesql_types::DataType::Integer,
        vibesql_types::SqlValue::Varchar(_) => {
            vibesql_types::DataType::Varchar { max_length: Some(255) }
        }
        vibesql_types::SqlValue::Character(_) => vibesql_types::DataType::Character { length: 1 },
        vibesql_types::SqlValue::Boolean(_) => vibesql_types::DataType::Boolean,
        vibesql_types::SqlValue::Float(_) => vibesql_types::DataType::Float { precision: 53 },
        vibesql_types::SqlValue::Double(_) => vibesql_types::DataType::DoublePrecision,
        vibesql_types::SqlValue::Numeric(_) => {
            vibesql_types::DataType::Numeric { precision: 10, scale: 2 }
        }
        vibesql_types::SqlValue::Real(_) => vibesql_types::DataType::Real,
        vibesql_types::SqlValue::Smallint(_) => vibesql_types::DataType::Smallint,
        vibesql_types::SqlValue::Bigint(_) => vibesql_types::DataType::Bigint,
        vibesql_types::SqlValue::Unsigned(_) => vibesql_types::DataType::Unsigned,
        vibesql_types::SqlValue::Date(_) => vibesql_types::DataType::Date,
        vibesql_types::SqlValue::Time(_) => vibesql_types::DataType::Time { with_timezone: false },
        vibesql_types::SqlValue::Timestamp(_) => {
            vibesql_types::DataType::Timestamp { with_timezone: false }
        }
        vibesql_types::SqlValue::Interval(_) => {
            // For now, return a simple INTERVAL type (can be enhanced to detect field types)
            vibesql_types::DataType::Interval {
                start_field: vibesql_types::IntervalField::Day,
                end_field: None,
            }
        }
        vibesql_types::SqlValue::Vector(v) => {
            vibesql_types::DataType::Vector { dimensions: v.len() as u32 }
        }
        vibesql_types::SqlValue::Blob(_) => vibesql_types::DataType::BinaryLargeObject,
    }
}
