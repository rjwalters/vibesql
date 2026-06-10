//! Schema validation utilities for subqueries
//!
//! This module provides utilities to compute and validate the schema of
//! subquery results, particularly for handling wildcards and column counts.

use std::collections::HashMap;

use crate::{errors::ExecutorError, select::cte::CteResult};

/// Build a linked outer schema chain for nested subquery column resolution
///
/// **NEW APPROACH (Issue #4493)**: Instead of flattening all outer scopes into a single
/// HashMap (which causes collisions when the same table exists at multiple levels),
/// we now preserve the outer schema chain using `outer_schema` links, similar to
/// SQLite's NameContext.pNext approach.
///
/// This enables proper column resolution in deeply nested subqueries where the same
/// table appears at multiple nesting levels.
///
/// # Example: Issue #4493 Test Case
///
/// ```sql
/// SELECT x FROM t2, t1 WHERE x IN (           -- Level 0: t2, t1
///     SELECT x FROM t2, t1 WHERE x IN (       -- Level 1: t2, t1
///         SELECT x FROM t1 WHERE x = c        -- Level 2: t1 only, but can see outer t2.x
///     )
/// )
/// ```
///
/// With chaining:
/// - Level 2: schema=[t1], outer_schema -> Level 1's schema
/// - Level 1: schema=[t2, t1], outer_schema -> Level 0's schema
/// - Level 0: schema=[t2, t1], outer_schema -> None
///
/// When Level 2 resolves `x`, it searches:
/// 1. Current level [t1] - not found
/// 2. Follow outer_schema chain to Level 1 [t2, t1] - FOUND in t2!
///
/// No HashMap collision because each level keeps its own tables!
///
/// # Index alignment with the merged row (window1.test 61.1)
///
/// [`build_merged_outer_row`] lays the row out as `current.values ++
/// outer.values` — the current level's values form the *prefix* and the
/// outer chain's values the *suffix*. This matches
/// [`CombinedSchema::get_column_index`](crate::schema::CombinedSchema::get_column_index)'s
/// index space: current-level tables keep their 0-based `start_index`
/// values (so the merged schema also stays aligned with *raw* current-level
/// rows, as used by the outer-correlated aggregate path over `outer_rows`,
/// issue #4930 / window1.test 53.0), while lookups that resolve in the
/// outer chain are offset by the current level's `total_columns` (and so
/// land in the outer suffix; by induction the outer row is itself merged
/// with this same convention at the previous nesting level).
///
/// Before this convention was made consistent, the merged row put the
/// outer values first while the schema kept current-level indices 0-based,
/// so a correlated reference that should resolve to the nearest enclosing
/// scope read the outermost scope's value instead — e.g.
/// `SELECT (SELECT sum(a IN (SELECT sum(a) OVER() FROM (SELECT 1 AS x))) FROM t1) FROM t1`
/// bound the inner `a` to the outermost t1 row (window1.test 61.1).
pub(super) fn build_merged_outer_schema<'a>(
    current_schema: &'a crate::schema::CombinedSchema,
    outer_schema: Option<&'a crate::schema::CombinedSchema>,
) -> std::borrow::Cow<'a, crate::schema::CombinedSchema> {
    if let Some(outer) = outer_schema {
        // Create a new schema with current_schema's tables but linked to outer_schema
        // This preserves the chain instead of flattening
        let mut new_schema = current_schema.clone();
        new_schema.outer_schema = Some(Box::new(outer.clone()));
        std::borrow::Cow::Owned(new_schema)
    } else {
        // No outer schema to link to
        std::borrow::Cow::Borrowed(current_schema)
    }
}

/// Build a merged outer row that matches the merged outer schema
///
/// When we merge schemas from multiple levels, we must also merge the corresponding
/// rows so that column indices align correctly.
///
/// Layout: `current.values ++ outer.values`. The current level's values form
/// the prefix (aligned with the merged schema's 0-based current-level
/// `start_index` values); the outer chain's values form the suffix (reached
/// via the `total_columns` offset applied by
/// [`CombinedSchema::get_column_index`](crate::schema::CombinedSchema::get_column_index)
/// when delegating to the outer chain). See [`build_merged_outer_schema`].
///
/// # Arguments
/// * `current_row` - Row from the current level
/// * `outer_row` - Optional row from outer level(s)
///
/// # Returns
/// A merged row with values from both rows, or just the current row if no outer row exists
pub(super) fn build_merged_outer_row<'a>(
    current_row: &'a vibesql_storage::Row,
    outer_row: Option<&'a vibesql_storage::Row>,
) -> std::borrow::Cow<'a, vibesql_storage::Row> {
    if let Some(outer) = outer_row {
        // Merge: current row values + outer row values (current is the prefix)
        let mut merged_values = current_row.values.clone();
        merged_values.extend(outer.values.iter().cloned());

        std::borrow::Cow::Owned(vibesql_storage::Row::new(merged_values))
    } else {
        // No outer row to merge, just use current row
        std::borrow::Cow::Borrowed(current_row)
    }
}

/// Compute the number of columns in a SELECT statement's result
/// Handles wildcards by expanding them using table schemas from the database
///
/// Issue #3562: Added CTE context so wildcards can be expanded for CTE references
/// Issue #4881: Validates set operation column count mismatches (UNION, INTERSECT, EXCEPT)
pub fn compute_select_list_column_count(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<usize, ExecutorError> {
    let left_count =
        compute_single_select_column_count(&stmt.select_list, &stmt.from, database, cte_results)?;

    // Issue #4881: Validate set operation column counts
    // If this SELECT has a set operation (UNION, INTERSECT, EXCEPT),
    // validate that left and right sides have the same number of columns.
    // This produces SQLite-compatible error messages for column count mismatches.
    if let Some(set_op) = &stmt.set_operation {
        validate_set_operation_column_counts(left_count, set_op, database, cte_results)?;
    }

    Ok(left_count)
}

/// Validate column counts for a chain of set operations
/// Returns SetOperationColumnMismatch error if any operation has mismatched column counts
fn validate_set_operation_column_counts(
    left_count: usize,
    set_op: &vibesql_ast::SetOperation,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<(), ExecutorError> {
    let right_stmt = &set_op.right;
    let right_count = compute_single_select_column_count(
        &right_stmt.select_list,
        &right_stmt.from,
        database,
        cte_results,
    )?;

    if left_count != right_count {
        let operator = match (&set_op.op, set_op.all) {
            (vibesql_ast::SetOperator::Union, true) => "UNION ALL",
            (vibesql_ast::SetOperator::Union, false) => "UNION",
            (vibesql_ast::SetOperator::Intersect, true) => "INTERSECT ALL",
            (vibesql_ast::SetOperator::Intersect, false) => "INTERSECT",
            (vibesql_ast::SetOperator::Except, true) => "EXCEPT ALL",
            (vibesql_ast::SetOperator::Except, false) => "EXCEPT",
        };
        return Err(ExecutorError::SetOperationColumnMismatch { operator: operator.to_string() });
    }

    // Recursively validate nested set operations
    if let Some(next_set_op) = &right_stmt.set_operation {
        validate_set_operation_column_counts(right_count, next_set_op, database, cte_results)?;
    }

    Ok(())
}

/// Compute column count for a single SELECT statement (without considering set operations)
fn compute_single_select_column_count(
    select_list: &[vibesql_ast::SelectItem],
    from: &Option<vibesql_ast::FromClause>,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<usize, ExecutorError> {
    let mut count = 0;

    for item in select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand * to count all columns from all tables in FROM clause
                if let Some(from_clause) = from {
                    count += count_columns_in_from_clause(from_clause, database, cte_results)?;
                } else {
                    // SELECT * without FROM is an error (should be caught earlier)
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * requires FROM clause".to_string(),
                    ));
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand table.* to count columns from that specific table
                // Issue #3562: Check CTEs first before database tables
                if let Some(cte_ctx) = cte_results {
                    if let Some((schema, _)) = cte_ctx.get(qualifier).or_else(|| {
                        cte_ctx
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(qualifier))
                            .map(|(_, v)| v)
                    }) {
                        count += schema.columns.len();
                        continue;
                    }
                }
                let tbl = database
                    .get_table(qualifier)
                    .ok_or_else(|| ExecutorError::TableNotFound(qualifier.clone()))?;
                count += tbl.schema.columns.len();
            }
            vibesql_ast::SelectItem::Expression { .. } => {
                // Each expression contributes one column
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Get the type affinity of a subquery's first SELECT expression
///
/// This is used for IN subquery comparisons where the affinity of the subquery's
/// result affects the comparison behavior. For example, if the subquery selects
/// from an INTEGER column, comparisons should use integer affinity rules.
///
/// # Returns
/// - Some(TypeAffinity) if the first expression is a column reference with known affinity
/// - None if the expression doesn't have a determinable affinity (e.g., function, literal, complex expr)
pub fn get_subquery_first_column_affinity(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Option<vibesql_types::TypeAffinity> {
    // Get the first item from the select list
    let first_item = stmt.select_list.first()?;

    // We only handle Expression items - wildcards expand to multiple columns
    let expr = match first_item {
        vibesql_ast::SelectItem::Expression { expr, .. } => expr,
        _ => return None, // Wildcards don't have a single affinity
    };

    // Get affinity based on expression type
    get_expression_affinity_from_from_clause(expr, &stmt.from, database)
}

/// Get the type affinity of an expression given a FROM clause context
///
/// This resolves column references by looking up the table schema.
fn get_expression_affinity_from_from_clause(
    expr: &vibesql_ast::Expression,
    from: &Option<vibesql_ast::FromClause>,
    database: &vibesql_storage::Database,
) -> Option<vibesql_types::TypeAffinity> {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) => {
            let table_name = col_id.table_canonical();
            let column_name = col_id.column_canonical();

            // If table qualifier is provided, use it directly
            if let Some(table) = table_name {
                if let Some(tbl) = database.get_table(&table) {
                    if let Some(col_idx) = tbl.schema.get_column_index(&column_name) {
                        return Some(tbl.schema.columns[col_idx].data_type.sqlite_affinity());
                    }
                }
                return None;
            }

            // No table qualifier - search tables in FROM clause
            if let Some(from_clause) = from {
                return find_column_affinity_in_from_clause(&column_name, from_clause, database);
            }
            None
        }
        // For COLLATE expressions, get affinity of the inner expression
        vibesql_ast::Expression::Collate { expr: inner, .. } => {
            get_expression_affinity_from_from_clause(inner, from, database)
        }
        // For CAST expressions, use the target type's affinity
        vibesql_ast::Expression::Cast { data_type, .. } => Some(data_type.sqlite_affinity()),
        // Literals, functions, and other expressions don't have column affinity
        _ => None,
    }
}

/// Search for a column in a FROM clause and return its affinity
fn find_column_affinity_in_from_clause(
    column_name: &str,
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
) -> Option<vibesql_types::TypeAffinity> {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            let table = database.get_table(name)?;
            let col_idx = table.schema.get_column_index(column_name)?;
            Some(table.schema.columns[col_idx].data_type.sqlite_affinity())
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            // Try left first, then right
            find_column_affinity_in_from_clause(column_name, left, database)
                .or_else(|| find_column_affinity_in_from_clause(column_name, right, database))
        }
        vibesql_ast::FromClause::Subquery { .. } | vibesql_ast::FromClause::Values { .. } => {
            // Subqueries and VALUES don't preserve affinity easily
            None
        }
    }
}

/// Count total columns in a FROM clause (handles joins and multiple tables)
///
/// Issue #3562: Added CTE context so CTEs can be resolved in FROM clause
fn count_columns_in_from_clause(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<usize, ExecutorError> {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            // Issue #3562: Check CTEs first before database tables
            if let Some(cte_ctx) = cte_results {
                if let Some((schema, _)) = cte_ctx.get(name).or_else(|| {
                    cte_ctx.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v)
                }) {
                    return Ok(schema.columns.len());
                }
            }
            let table = database
                .get_table(name)
                .ok_or_else(|| ExecutorError::TableNotFound(name.clone()))?;
            Ok(table.schema.columns.len())
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            let left_count = count_columns_in_from_clause(left, database, cte_results)?;
            let right_count = count_columns_in_from_clause(right, database, cte_results)?;
            Ok(left_count + right_count)
        }
        vibesql_ast::FromClause::Subquery { query, column_aliases, .. } => {
            // For subqueries in FROM, use column_aliases if provided,
            // otherwise recursively compute the column count of the inner query
            if let Some(aliases) = column_aliases {
                Ok(aliases.len())
            } else {
                compute_select_list_column_count(query, database, cte_results)
            }
        }
        vibesql_ast::FromClause::Values { rows, column_aliases, .. } => {
            // VALUES clause column count is determined by either:
            // 1. The column_aliases if provided, or
            // 2. The number of expressions in the first row
            if let Some(aliases) = column_aliases {
                Ok(aliases.len())
            } else if let Some(first_row) = rows.first() {
                Ok(first_row.len())
            } else {
                Ok(0) // Empty VALUES clause
            }
        }
    }
}
