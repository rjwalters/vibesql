//! Validation functions for SELECT execution
//!
//! This module provides upfront validation for SELECT statements:
//! - Column reference validation (ensuring columns exist in schema)
//! - IN subquery validation (ensuring correct column count)
//!
//! These validations happen before row iteration, ensuring proper error messages
//! even when there are no rows to process.

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Validate IN subqueries in WHERE clause before row iteration
/// This ensures schema validation happens even when there are no rows to process
pub(super) fn validate_where_clause_subqueries(
    expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::In { subquery, .. } => {
            // Validate that the subquery returns exactly 1 column (scalar subquery requirement)
            let column_count = compute_select_list_column_count(subquery, database)?;
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
            validate_where_clause_subqueries(left, database)?;
            validate_where_clause_subqueries(right, database)
        }
        // Recurse into unary operations
        Expression::UnaryOp { expr, .. } => validate_where_clause_subqueries(expr, database),
        // Recurse into other composite expressions
        Expression::IsNull { expr, .. } => validate_where_clause_subqueries(expr, database),
        Expression::InList { expr, values, .. } => {
            validate_where_clause_subqueries(expr, database)?;
            for val in values {
                validate_where_clause_subqueries(val, database)?;
            }
            Ok(())
        }
        Expression::Between { expr, low, high, .. } => {
            validate_where_clause_subqueries(expr, database)?;
            validate_where_clause_subqueries(low, database)?;
            validate_where_clause_subqueries(high, database)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_where_clause_subqueries(op, database)?;
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    validate_where_clause_subqueries(cond, database)?;
                }
                validate_where_clause_subqueries(&when_clause.result, database)?;
            }
            if let Some(else_res) = else_result {
                validate_where_clause_subqueries(else_res, database)?;
            }
            Ok(())
        }
        // For all other expressions, no validation needed
        _ => Ok(()),
    }
}

/// Compute the number of columns in a SELECT statement's result
/// Handles wildcards by expanding them using table schemas from the database
fn compute_select_list_column_count(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    let mut count = 0;

    for item in &stmt.select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand * to count all columns from all tables in FROM clause
                if let Some(from) = &stmt.from {
                    count += count_columns_in_from_clause(from, database)?;
                } else {
                    // SELECT * without FROM is an error (should be caught earlier)
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * requires FROM clause".to_string(),
                    ));
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand table.* to count columns from that specific table
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
fn count_columns_in_from_clause(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            let table = database
                .get_table(name)
                .ok_or_else(|| ExecutorError::TableNotFound(name.clone()))?;
            Ok(table.schema.columns.len())
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            let left_count = count_columns_in_from_clause(left, database)?;
            let right_count = count_columns_in_from_clause(right, database)?;
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

/// Validate that all column references in a SELECT statement resolve to columns in the schema.
///
/// This validation happens before row iteration, ensuring proper error messages
/// even when the table is empty (has no rows to process).
///
/// Returns `Ok(())` if all column references are valid, or `Err(ColumnNotFound)` with
/// context about available columns if a reference cannot be resolved.
pub(super) fn validate_select_column_references(
    stmt: &vibesql_ast::SelectStmt,
    schema: &CombinedSchema,
) -> Result<(), ExecutorError> {
    // Validate SELECT list column references
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            validate_expression_column_refs(expr, schema)?;
        }
        // Wildcards (*, table.*) don't need validation - they're handled separately
    }

    // Validate WHERE clause column references
    if let Some(where_expr) = &stmt.where_clause {
        validate_expression_column_refs(where_expr, schema)?;
    }

    // Validate ORDER BY column references
    if let Some(order_by) = &stmt.order_by {
        for order_item in order_by {
            validate_expression_column_refs(&order_item.expr, schema)?;
        }
    }

    // Validate GROUP BY column references
    if let Some(group_by) = &stmt.group_by {
        for group_expr in group_by {
            validate_expression_column_refs(group_expr, schema)?;
        }
    }

    // Validate HAVING clause column references
    if let Some(having_expr) = &stmt.having {
        validate_expression_column_refs(having_expr, schema)?;
    }

    Ok(())
}

/// Recursively validate column references in an expression against the schema.
fn validate_expression_column_refs(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Result<(), ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef { table, column } => {
            // Skip "*" - it's a wildcard used in COUNT(*) and is not a real column
            if column == "*" {
                return Ok(());
            }

            // Try to resolve the column in the schema
            if schema.get_column_index(table.as_deref(), column).is_none() {
                // Column not found - build error with context
                let searched_tables: Vec<String> =
                    schema.table_schemas.keys().cloned().collect();
                let available_columns: Vec<String> = schema
                    .table_schemas
                    .values()
                    .flat_map(|(_, tbl_schema)| {
                        tbl_schema.columns.iter().map(|c| c.name.clone())
                    })
                    .collect();

                return Err(ExecutorError::ColumnNotFound {
                    column_name: column.clone(),
                    table_name: table.clone().unwrap_or_else(|| "unknown".to_string()),
                    searched_tables,
                    available_columns,
                });
            }
            Ok(())
        }

        // Recurse into binary operations
        Expression::BinaryOp { left, right, .. } => {
            validate_expression_column_refs(left, schema)?;
            validate_expression_column_refs(right, schema)
        }

        // Recurse into unary operations
        Expression::UnaryOp { expr, .. } => validate_expression_column_refs(expr, schema),

        // Function calls
        Expression::Function { args, .. } => {
            for arg in args {
                validate_expression_column_refs(arg, schema)?;
            }
            Ok(())
        }

        // Aggregate functions
        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                validate_expression_column_refs(arg, schema)?;
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
                validate_expression_column_refs(arg, schema)?;
            }
            // Validate PARTITION BY expressions
            if let Some(partition_exprs) = &over.partition_by {
                for partition_expr in partition_exprs {
                    validate_expression_column_refs(partition_expr, schema)?;
                }
            }
            // Validate ORDER BY expressions
            if let Some(order_items) = &over.order_by {
                for order_item in order_items {
                    validate_expression_column_refs(&order_item.expr, schema)?;
                }
            }
            Ok(())
        }

        // CASE expressions
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_expression_column_refs(op, schema)?;
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    validate_expression_column_refs(cond, schema)?;
                }
                validate_expression_column_refs(&when_clause.result, schema)?;
            }
            if let Some(else_res) = else_result {
                validate_expression_column_refs(else_res, schema)?;
            }
            Ok(())
        }

        // IS NULL / IS NOT NULL
        Expression::IsNull { expr, .. } => validate_expression_column_refs(expr, schema),

        // IN list
        Expression::InList { expr, values, .. } => {
            validate_expression_column_refs(expr, schema)?;
            for val in values {
                validate_expression_column_refs(val, schema)?;
            }
            Ok(())
        }

        // IN subquery - don't validate inside subquery (it has its own schema)
        Expression::In { expr, .. } => {
            validate_expression_column_refs(expr, schema)
        }

        // EXISTS subquery - no column refs to validate at this level
        Expression::Exists { .. } => Ok(()),

        // BETWEEN
        Expression::Between { expr, low, high, .. } => {
            validate_expression_column_refs(expr, schema)?;
            validate_expression_column_refs(low, schema)?;
            validate_expression_column_refs(high, schema)
        }

        // LIKE pattern matching
        Expression::Like { expr, pattern, .. } => {
            validate_expression_column_refs(expr, schema)?;
            validate_expression_column_refs(pattern, schema)
        }

        // CAST
        Expression::Cast { expr, .. } => validate_expression_column_refs(expr, schema),

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
            validate_expression_column_refs(expr, schema)
        }

        // INTERVAL expression
        Expression::Interval { value, .. } => validate_expression_column_refs(value, schema),

        // POSITION expression
        Expression::Position { substring, string, .. } => {
            validate_expression_column_refs(substring, schema)?;
            validate_expression_column_refs(string, schema)
        }

        // TRIM expression
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                validate_expression_column_refs(char_expr, schema)?;
            }
            validate_expression_column_refs(string, schema)
        }

        // MATCH AGAINST - column names are strings, not expressions
        Expression::MatchAgainst { search_modifier, .. } => {
            validate_expression_column_refs(search_modifier, schema)
        }

        // Pseudo variables (OLD.col, NEW.col) - used in triggers, not validated against schema
        Expression::PseudoVariable { .. } => Ok(()),

        // VALUES() in ON DUPLICATE KEY UPDATE - not validated against regular schema
        Expression::DuplicateKeyValue { .. } => Ok(()),
    }
}
