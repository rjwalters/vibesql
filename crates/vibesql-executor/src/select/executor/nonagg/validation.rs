//! Validation functions for SELECT execution
//!
//! This module provides upfront validation for SELECT statements:
//! - Column reference validation (ensuring columns exist in schema)
//! - IN subquery validation (ensuring correct column count)
//!
//! These validations happen before row iteration, ensuring proper error messages
//! even when there are no rows to process.

#![allow(clippy::needless_return, clippy::collapsible_if)]

use crate::{errors::ExecutorError, schema::CombinedSchema, select::cte::CteResult};
use std::collections::HashMap;

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
        Expression::In { subquery, .. } => {
            // Validate that the subquery returns exactly 1 column (scalar subquery requirement)
            // Issue #3562: Pass CTE context so CTEs can be resolved
            let column_count = compute_select_list_column_count(subquery, database, cte_results)?;
            if column_count != 1 {
                return Err(ExecutorError::SubqueryColumnCountMismatch {
                    expected: 1,
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
fn compute_select_list_column_count(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<usize, ExecutorError> {
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
        vibesql_ast::FromClause::Subquery { .. } => {
            // For subqueries in FROM, we'd need to execute them to know column count
            // This is complex, so for now we'll return an error
            // In practice, this case is rare in IN subqueries
            Err(ExecutorError::UnsupportedFeature(
                "Subqueries in FROM clause within IN predicates are not yet supported for schema validation".to_string(),
            ))
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

    // Validate WHERE clause column references (allowing procedure variables)
    if let Some(where_expr) = &stmt.where_clause {
        validate_expression_column_refs(where_expr, schema, outer_schema, &proc_vars)?;
    }

    // Validate ORDER BY column references (allowing aliases and procedure variables)
    if let Some(order_by) = &stmt.order_by {
        let combined: std::collections::HashSet<String> =
            select_aliases.union(&proc_vars).cloned().collect();
        for order_item in order_by {
            validate_expression_column_refs(&order_item.expr, schema, outer_schema, &combined)?;
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
fn validate_expression_column_refs(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
    outer_schema: Option<&CombinedSchema>,
    allowed_aliases: &std::collections::HashSet<String>,
) -> Result<(), ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef { table, column } => {
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
            if schema.get_column_index(table.as_deref(), column).is_some() {
                return Ok(());
            }

            // For correlated subqueries, also check outer schema (#2694)
            if let Some(outer) = outer_schema {
                if outer.get_column_index(table.as_deref(), column).is_some() {
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
                column_name: column.clone(),
                table_name: table.clone().unwrap_or_else(|| "unknown".to_string()),
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
        Expression::Like { expr, pattern, .. } => {
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

        // Conjunction and Disjunction - validate all children
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                validate_expression_column_refs(child, schema, outer_schema, allowed_aliases)?;
            }
            Ok(())
        }
    }
}
