//! Validation functions for SELECT execution
//!
//! This module provides upfront validation for SELECT statements:
//! - Column reference validation (ensuring columns exist in schema)
//! - IN subquery validation (ensuring correct column count)
//!
//! These validations happen before row iteration, ensuring proper error messages
//! even when there are no rows to process.

#![allow(clippy::needless_return, clippy::collapsible_if)]

use std::collections::HashMap;

use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::cte::CteResult,
    sqlite_schema::{
        get_sqlite_schema_table_schema, is_sqlite_schema_table, is_sqlite_temp_schema_table,
    },
};

/// Validate IN subqueries in WHERE clause before row iteration
/// This ensures schema validation happens even when there are no rows to process
///
/// Issue #3562: Added CTE context so CTEs can be resolved in IN subqueries
pub(super) fn validate_where_clause_subqueries(
    expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<(), ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::In { expr: lhs, subquery, .. } => {
            // For row-value LHS, the subquery must return the same number of columns
            // as the row value's arity (SQL:1999 Section 8.4). For scalar LHS, the
            // subquery must return exactly 1 column.
            // Issue #3562: Pass CTE context so CTEs can be resolved
            let expected = match lhs.as_ref() {
                Expression::RowValueConstructor(elements) => elements.len(),
                // Multi-column scalar-subquery LHS (`(SELECT a, b) IN (...)`)
                // expects its own column count.
                Expression::ScalarSubquery(left_sub) => {
                    match compute_select_list_column_count(left_sub, database, cte_results) {
                        Ok(n) => n,
                        // Cannot be determined statically — defer to runtime.
                        Err(_) => return Ok(()),
                    }
                }
                _ => 1,
            };
            let column_count = compute_select_list_column_count(subquery, database, cte_results)?;
            if column_count != expected {
                return Err(ExecutorError::SubqueryColumnCountMismatch {
                    expected,
                    actual: column_count,
                });
            }
            Ok(())
        }
        // Recurse into binary operations
        Expression::BinaryOp { left, right, .. } => {
            validate_where_clause_subqueries(left, database, cte_results)?;
            validate_where_clause_subqueries(right, database, cte_results)
        }
        // Recurse into unary operations
        Expression::UnaryOp { expr, .. } => {
            validate_where_clause_subqueries(expr, database, cte_results)
        }
        // Recurse into other composite expressions
        Expression::IsNull { expr, .. } => {
            validate_where_clause_subqueries(expr, database, cte_results)
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            validate_where_clause_subqueries(left, database, cte_results)?;
            validate_where_clause_subqueries(right, database, cte_results)
        }
        Expression::IsTruthValue { expr, .. } => {
            validate_where_clause_subqueries(expr, database, cte_results)
        }
        Expression::InList { expr, values, .. } => {
            validate_where_clause_subqueries(expr, database, cte_results)?;
            for val in values {
                validate_where_clause_subqueries(val, database, cte_results)?;
            }
            Ok(())
        }
        Expression::Between { expr, low, high, .. } => {
            validate_where_clause_subqueries(expr, database, cte_results)?;
            validate_where_clause_subqueries(low, database, cte_results)?;
            validate_where_clause_subqueries(high, database, cte_results)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_where_clause_subqueries(op, database, cte_results)?;
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    validate_where_clause_subqueries(cond, database, cte_results)?;
                }
                validate_where_clause_subqueries(&when_clause.result, database, cte_results)?;
            }
            if let Some(else_res) = else_result {
                validate_where_clause_subqueries(else_res, database, cte_results)?;
            }
            Ok(())
        }
        // For all other expressions, no validation needed
        _ => Ok(()),
    }
}

/// Compute the number of columns in a SELECT statement's result
/// Handles wildcards by expanding them using table schemas from the database
///
/// Issue #3562: Added CTE context so wildcards can be expanded for CTE references
/// Issue #4602: Made public for set operation column count validation
/// Issue #4881: Validates set operation column count mismatches (UNION, INTERSECT, EXCEPT)
pub(crate) fn compute_select_list_column_count(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<usize, ExecutorError> {
    // VALUES statements (e.g. `IN (VALUES(1, 2))`) carry no select list; the
    // column count is the arity of the value rows.
    if let Some(values) = &stmt.values {
        if stmt.select_list.is_empty() {
            return Ok(values.first().map_or(0, |row| row.len()));
        }
    }

    let left_count = compute_single_select_column_count(stmt, database, cte_results)?;

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
    let right_count = compute_single_select_column_count(right_stmt, database, cte_results)?;

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
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<usize, ExecutorError> {
    // Issue #4602: Handle VALUES clauses without SELECT list
    // For statements like "VALUES(1,2,3),(4,5,6)", select_list is empty
    // and we need to count columns from the VALUES rows
    if stmt.select_list.is_empty() {
        if let Some(values_rows) = &stmt.values {
            if let Some(first_row) = values_rows.first() {
                return Ok(first_row.len());
            }
        }
        // Empty select_list and no VALUES - can't determine column count
        return Err(ExecutorError::UnsupportedFeature(
            "Cannot determine column count for empty SELECT".to_string(),
        ));
    }

    let mut count = 0;

    for item in &stmt.select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand * to count all columns from all tables in FROM clause
                if let Some(from) = &stmt.from {
                    count += count_columns_in_from_clause(from, database, cte_results)?;
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
                // Issue #4577: Check for sqlite_schema/sqlite_master virtual tables.
                // #5513: sqlite_temp_master shares the same column shape.
                if is_sqlite_schema_table(qualifier) || is_sqlite_temp_schema_table(qualifier) {
                    count += get_sqlite_schema_table_schema().columns.len();
                    continue;
                }
                // Check for views before regular tables
                if let Some(view) = database.catalog.get_view(qualifier) {
                    count += compute_select_list_column_count(&view.query, database, cte_results)?;
                    continue;
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
            // Issue #4577: Check for sqlite_schema/sqlite_master virtual tables.
            // #5513: sqlite_temp_master shares the same column shape.
            if is_sqlite_schema_table(name) || is_sqlite_temp_schema_table(name) {
                return Ok(get_sqlite_schema_table_schema().columns.len());
            }
            // Check for views before regular tables
            if let Some(view) = database.catalog.get_view(name) {
                // Count columns from view definition
                return compute_select_list_column_count(&view.query, database, cte_results);
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

/// Collect aliases from SELECT list items
fn collect_select_aliases(
    select_list: &[vibesql_ast::SelectItem],
) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();
    for item in select_list {
        if let vibesql_ast::SelectItem::Expression { alias: Some(alias), .. } = item {
            // Store both the original case and lowercase for case-insensitive matching
            aliases.insert(alias.clone());
            aliases.insert(alias.to_lowercase());
        }
    }
    aliases
}

/// Validate column references with optional procedural context and outer schema
///
/// When a procedural context is provided, variable names from the context are
/// allowed as column references (they will be resolved at runtime).
///
/// When an outer_schema is provided (for correlated subqueries), column references
/// are also validated against the outer schema. This fixes issue #2694 where
/// correlated subqueries failed to resolve outer table references during validation.
pub(super) fn validate_select_column_references_with_context(
    stmt: &vibesql_ast::SelectStmt,
    schema: &CombinedSchema,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    outer_schema: Option<&CombinedSchema>,
) -> Result<(), ExecutorError> {
    // Collect SELECT aliases for ORDER BY validation
    let select_aliases = collect_select_aliases(&stmt.select_list);

    // Collect procedure variable names if in procedural context
    let proc_vars: std::collections::HashSet<String> = procedural_context
        .map(|ctx| {
            ctx.get_available_names()
                .into_iter()
                .flat_map(|name| vec![name.clone(), name.to_lowercase()])
                .collect()
        })
        .unwrap_or_default();

    // Validate SELECT list column references
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            validate_expression_column_refs(expr, schema, outer_schema, &proc_vars)?;
        }
        // Wildcards (*, table.*) don't need validation - they're handled separately
    }

    // Validate WHERE clause column references (allowing aliases and procedure variables)
    // SQLite extension: SELECT aliases can be referenced in WHERE clause
    if let Some(where_expr) = &stmt.where_clause {
        let combined: std::collections::HashSet<String> =
            select_aliases.union(&proc_vars).cloned().collect();
        log::debug!(
            "WHERE alias validation: select_aliases={:?}, combined={:?}",
            select_aliases,
            combined
        );
        validate_expression_column_refs(where_expr, schema, outer_schema, &combined)?;
    }

    // Validate ORDER BY column references (allowing aliases and procedure variables)
    // Skip validation for compound queries (UNION, INTERSECT, EXCEPT) because ORDER BY
    // applies to the compound result, not the individual SELECT
    if let Some(order_by) = &stmt.order_by {
        if stmt.set_operation.is_none() {
            let combined: std::collections::HashSet<String> =
                select_aliases.union(&proc_vars).cloned().collect();
            for order_item in order_by {
                validate_expression_column_refs(&order_item.expr, schema, outer_schema, &combined)?;
            }
        }
    }

    // Validate GROUP BY column references (allowing aliases and procedure variables)
    if let Some(group_by) = &stmt.group_by {
        let combined: std::collections::HashSet<String> =
            select_aliases.union(&proc_vars).cloned().collect();
        for group_expr in group_by.all_expressions() {
            validate_expression_column_refs(group_expr, schema, outer_schema, &combined)?;
        }
    }

    // Validate HAVING clause column references (allowing aliases and procedure variables)
    if let Some(having_expr) = &stmt.having {
        let combined: std::collections::HashSet<String> =
            select_aliases.union(&proc_vars).cloned().collect();
        validate_expression_column_refs(having_expr, schema, outer_schema, &combined)?;
    }

    Ok(())
}

/// Recursively validate column references in an expression against the schema.
///
/// The `outer_schema` parameter contains the outer query's schema for correlated subqueries.
/// This allows correlated subqueries to reference columns from the outer query (#2694).
///
/// The `allowed_aliases` parameter contains aliases that are valid to use in this context
/// (e.g., SELECT aliases when validating ORDER BY).
/// Check if a column name is a ROWID pseudo-column alias (SQLite compatibility)
/// Returns true for 'rowid', '_rowid_', and 'oid' (case-insensitive)
fn is_rowid_pseudo_column(column: &str) -> bool {
    let lower = column.to_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

fn validate_expression_column_refs(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
    outer_schema: Option<&CombinedSchema>,
    allowed_aliases: &std::collections::HashSet<String>,
) -> Result<(), ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();
            // Skip "*" - it's a wildcard used in COUNT(*) and is not a real column
            if column == "*" {
                return Ok(());
            }

            // Check if the column is an allowed alias (case-insensitive)
            // Only for unqualified column references (no table prefix)
            if table.is_none() {
                if allowed_aliases.contains(column)
                    || allowed_aliases.contains(&column.to_lowercase())
                {
                    return Ok(());
                }
            }

            // Try to resolve the column in the inner schema first
            if schema.get_column_index(table, column).is_some() {
                return Ok(());
            }

            // SQLite compatibility: Allow ROWID pseudo-column references
            // Only if there's no actual column with that name (real columns take precedence)
            if is_rowid_pseudo_column(column) {
                // Verify the qualifier matches a table in the schema (if qualified)
                if let Some(qualifier) = table {
                    let qualifier_lower = qualifier.to_lowercase();
                    let table_exists =
                        schema.table_schemas.keys().any(|k| k.canonical() == qualifier_lower);
                    let table_in_outer = outer_schema.is_some_and(|outer| {
                        outer.table_schemas.keys().any(|k| k.canonical() == qualifier_lower)
                    });
                    if table_exists || table_in_outer {
                        return Ok(());
                    }
                } else {
                    // Unqualified ROWID is valid if there's at least one table in scope
                    if !schema.table_schemas.is_empty() {
                        return Ok(());
                    }
                }
            }

            // For correlated subqueries, also check outer schema (#2694)
            if let Some(outer) = outer_schema {
                if outer.get_column_index(table, column).is_some() {
                    return Ok(());
                }
            }

            // Column not found - build error with context
            let mut searched_tables: Vec<String> = schema.table_names();
            let mut available_columns: Vec<String> = schema
                .table_schemas
                .values()
                .flat_map(|(_, tbl_schema)| tbl_schema.columns.iter().map(|c| c.name.clone()))
                .collect();

            // Include outer schema info in error message
            if let Some(outer) = outer_schema {
                searched_tables.extend(outer.table_names());
                available_columns.extend(
                    outer.table_schemas.values().flat_map(|(_, tbl_schema)| {
                        tbl_schema.columns.iter().map(|c| c.name.clone())
                    }),
                );
            }

            return Err(ExecutorError::ColumnNotFound {
                column_name: column.to_string(),
                table_name: table.map(|t| t.to_string()).unwrap_or_else(|| "unknown".to_string()),
                searched_tables,
                available_columns,
            });
        }

        // Recurse into binary operations
        Expression::BinaryOp { left, right, .. } => {
            validate_expression_column_refs(left, schema, outer_schema, allowed_aliases)?;
            validate_expression_column_refs(right, schema, outer_schema, allowed_aliases)
        }

        // Recurse into unary operations
        Expression::UnaryOp { expr, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)
        }

        // Function calls
        Expression::Function { args, .. } => {
            for arg in args {
                validate_expression_column_refs(arg, schema, outer_schema, allowed_aliases)?;
            }
            Ok(())
        }

        // Aggregate functions
        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                validate_expression_column_refs(arg, schema, outer_schema, allowed_aliases)?;
            }
            Ok(())
        }

        // Window functions
        Expression::WindowFunction { function, over } => {
            // Validate function arguments
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. } => args,
                vibesql_ast::WindowFunctionSpec::Ranking { args, .. } => args,
                vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            for arg in args {
                validate_expression_column_refs(arg, schema, outer_schema, allowed_aliases)?;
            }
            // Validate PARTITION BY expressions
            if let Some(partition_exprs) = &over.partition_by {
                for partition_expr in partition_exprs {
                    validate_expression_column_refs(
                        partition_expr,
                        schema,
                        outer_schema,
                        allowed_aliases,
                    )?;
                }
            }
            // Validate ORDER BY expressions
            if let Some(order_items) = &over.order_by {
                for order_item in order_items {
                    validate_expression_column_refs(
                        &order_item.expr,
                        schema,
                        outer_schema,
                        allowed_aliases,
                    )?;
                }
            }
            Ok(())
        }

        // CASE expressions
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_expression_column_refs(op, schema, outer_schema, allowed_aliases)?;
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    validate_expression_column_refs(cond, schema, outer_schema, allowed_aliases)?;
                }
                validate_expression_column_refs(
                    &when_clause.result,
                    schema,
                    outer_schema,
                    allowed_aliases,
                )?;
            }
            if let Some(else_res) = else_result {
                validate_expression_column_refs(else_res, schema, outer_schema, allowed_aliases)?;
            }
            Ok(())
        }

        // IS NULL / IS NOT NULL
        Expression::IsNull { expr, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)
        }

        // IS DISTINCT FROM / IS NOT DISTINCT FROM
        Expression::IsDistinctFrom { left, right, .. } => {
            validate_expression_column_refs(left, schema, outer_schema, allowed_aliases)?;
            validate_expression_column_refs(right, schema, outer_schema, allowed_aliases)
        }

        // IS TRUE / IS FALSE / IS UNKNOWN
        Expression::IsTruthValue { expr, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)
        }

        // IN list
        Expression::InList { expr, values, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)?;
            for val in values {
                validate_expression_column_refs(val, schema, outer_schema, allowed_aliases)?;
            }
            Ok(())
        }

        // IN subquery - don't validate inside subquery (it has its own schema)
        Expression::In { expr, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)
        }

        // EXISTS subquery - no column refs to validate at this level
        Expression::Exists { .. } => Ok(()),

        // BETWEEN
        Expression::Between { expr, low, high, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)?;
            validate_expression_column_refs(low, schema, outer_schema, allowed_aliases)?;
            validate_expression_column_refs(high, schema, outer_schema, allowed_aliases)
        }

        // LIKE pattern matching
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)?;
            validate_expression_column_refs(pattern, schema, outer_schema, allowed_aliases)
        }

        // CAST
        Expression::Cast { expr, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)
        }

        // Literals and other simple expressions - no column refs to validate
        Expression::Literal(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::NextValue { .. }
        | Expression::SessionVariable { .. } => Ok(()),

        // Scalar subquery - has its own schema, don't validate here
        Expression::ScalarSubquery(_) => Ok(()),

        // Quantified comparison - validate left expression, subquery has its own schema
        Expression::QuantifiedComparison { expr, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)
        }

        // INTERVAL expression
        Expression::Interval { value, .. } => {
            validate_expression_column_refs(value, schema, outer_schema, allowed_aliases)
        }

        // POSITION expression
        Expression::Position { substring, string, .. } => {
            validate_expression_column_refs(substring, schema, outer_schema, allowed_aliases)?;
            validate_expression_column_refs(string, schema, outer_schema, allowed_aliases)
        }

        // TRIM expression
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                validate_expression_column_refs(char_expr, schema, outer_schema, allowed_aliases)?;
            }
            validate_expression_column_refs(string, schema, outer_schema, allowed_aliases)
        }

        // EXTRACT - extract field from expression
        Expression::Extract { expr, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)
        }

        // MATCH AGAINST - column names are strings, not expressions
        Expression::MatchAgainst { search_modifier, .. } => {
            validate_expression_column_refs(search_modifier, schema, outer_schema, allowed_aliases)
        }

        // Pseudo variables (OLD.col, NEW.col) - used in triggers, not validated against schema
        Expression::PseudoVariable { .. } => Ok(()),

        // VALUES() in ON DUPLICATE KEY UPDATE - not validated against regular schema
        Expression::DuplicateKeyValue { .. } => Ok(()),

        // Placeholders - no column references to validate
        Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => Ok(()),

        // Conjunction, Disjunction, and RowValueConstructor - validate all children
        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for child in children {
                validate_expression_column_refs(child, schema, outer_schema, allowed_aliases)?;
            }
            Ok(())
        }

        Expression::Collate { expr, .. } => {
            validate_expression_column_refs(expr, schema, outer_schema, allowed_aliases)
        }

        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                validate_expression_column_refs(msg, schema, outer_schema, allowed_aliases)?;
            }
            Ok(())
        }
    }
}
