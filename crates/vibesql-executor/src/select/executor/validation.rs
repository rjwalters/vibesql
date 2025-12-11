//! Column validation for SELECT statements
//!
//! Validates column references in SELECT list and WHERE clause expressions
//! against the available schema BEFORE row processing begins. This ensures
//! that column errors are caught even when tables are empty.

#![allow(clippy::collapsible_if)]

use vibesql_ast::{Expression, SelectItem};

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Represents a column reference extracted from an expression
#[derive(Debug)]
struct ColumnReference {
    /// Optional table qualifier
    table: Option<String>,
    /// Column name
    column: String,
}

/// Extract all column references from an expression recursively
fn extract_column_refs(expr: &Expression, refs: &mut Vec<ColumnReference>) {
    match expr {
        Expression::ColumnRef { table, column } => {
            // Skip "*" - it's a wildcard, not an actual column reference
            // This handles cases like COUNT(*) parsed as ColumnRef { column: "*" }
            if column != "*" {
                refs.push(ColumnReference { table: table.clone(), column: column.clone() });
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            extract_column_refs(left, refs);
            extract_column_refs(right, refs);
        }
        Expression::UnaryOp { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                extract_column_refs(arg, refs);
            }
        }
        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                extract_column_refs(arg, refs);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                extract_column_refs(op, refs);
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    extract_column_refs(cond, refs);
                }
                extract_column_refs(&case_when.result, refs);
            }
            if let Some(else_expr) = else_result {
                extract_column_refs(else_expr, refs);
            }
        }
        Expression::IsNull { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            extract_column_refs(left, refs);
            extract_column_refs(right, refs);
        }
        Expression::IsTruthValue { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::Between { expr, low, high, .. } => {
            extract_column_refs(expr, refs);
            extract_column_refs(low, refs);
            extract_column_refs(high, refs);
        }
        Expression::InList { expr, values, .. } => {
            extract_column_refs(expr, refs);
            for val in values {
                extract_column_refs(val, refs);
            }
        }
        Expression::In { expr, .. } => {
            // Only validate the left-hand expression
            // Subquery columns are validated separately when executing the subquery
            extract_column_refs(expr, refs);
        }
        Expression::Exists { .. } => {
            // EXISTS subqueries are validated separately
        }
        Expression::Cast { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::Like { expr, pattern, .. } => {
            extract_column_refs(expr, refs);
            extract_column_refs(pattern, refs);
        }
        Expression::Position { substring, string, .. } => {
            extract_column_refs(substring, refs);
            extract_column_refs(string, refs);
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                extract_column_refs(char_expr, refs);
            }
            extract_column_refs(string, refs);
        }
        Expression::Extract { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::ScalarSubquery(_) => {
            // Scalar subquery columns are validated when executing the subquery
        }
        Expression::QuantifiedComparison { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::Interval { value, .. } => {
            extract_column_refs(value, refs);
        }
        Expression::WindowFunction { function, over } => {
            // Extract from window function arguments
            match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                    for arg in args {
                        extract_column_refs(arg, refs);
                    }
                }
            }
            // Extract from PARTITION BY
            if let Some(partition) = &over.partition_by {
                for expr in partition {
                    extract_column_refs(expr, refs);
                }
            }
            // Extract from ORDER BY
            if let Some(order) = &over.order_by {
                for item in order {
                    extract_column_refs(&item.expr, refs);
                }
            }
        }
        Expression::MatchAgainst { search_modifier, .. } => {
            extract_column_refs(search_modifier, refs);
        }
        Expression::PseudoVariable { .. } => {
            // OLD/NEW pseudo-variables in triggers - skip validation
        }
        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for child in children {
                extract_column_refs(child, refs);
            }
        }

        // Terminals with no column references
        Expression::Literal(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::NextValue { .. }
        | Expression::SessionVariable { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => {}
    }
}

/// Check if a column name is a ROWID pseudo-column alias (SQLite compatibility)
/// Returns true for 'rowid', '_rowid_', and 'oid' (case-insensitive)
fn is_rowid_pseudo_column(column: &str) -> bool {
    let lower = column.to_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

/// Check if a function name is an aggregate function
fn is_aggregate_function(name: &str) -> bool {
    let upper = name.to_uppercase();
    matches!(
        upper.as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "TOTAL" | "GROUP_CONCAT"
    )
}

/// Check if an aggregate function has wrong number of arguments
/// Returns Some((function_name, arg_count)) if there's an error, None otherwise
fn check_aggregate_arg_count(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, args, .. } => {
            let upper = name.to_uppercase();
            let arg_count = args.len();

            // Check for wildcard in non-COUNT aggregates
            let has_wildcard = args.iter().any(|arg| {
                matches!(arg, Expression::Wildcard)
                    || matches!(
                        arg,
                        Expression::ColumnRef { table: None, column } if column == "*"
                    )
            });

            match upper.as_str() {
                "COUNT" => {
                    // Multi-arg count without DISTINCT is an error
                    // But this is checked elsewhere, so skip here
                    None
                }
                "MIN" | "MAX" => {
                    if has_wildcard || arg_count == 0 {
                        Some(name.clone())
                    } else {
                        None
                    }
                }
                "SUM" | "AVG" | "TOTAL" => {
                    if has_wildcard || arg_count == 0 || arg_count > 1 {
                        Some(name.clone())
                    } else {
                        None
                    }
                }
                "GROUP_CONCAT" => {
                    if arg_count == 0 || arg_count > 2 {
                        Some(name.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        Expression::Function { name, args, .. } => {
            // Check if this is an aggregate function with wrong args
            if is_aggregate_function(name) {
                let upper = name.to_uppercase();
                let arg_count = args.len();

                // Check for wildcard
                let has_wildcard = args.iter().any(|arg| {
                    matches!(arg, Expression::Wildcard)
                        || matches!(
                            arg,
                            Expression::ColumnRef { table: None, column } if column == "*"
                        )
                });

                match upper.as_str() {
                    "COUNT" => {
                        // count(a, b) without DISTINCT is wrong
                        // Regular count without DISTINCT can only have 0-1 args
                        if arg_count > 1 {
                            Some(name.clone())
                        } else {
                            None
                        }
                    }
                    "MIN" | "MAX" => {
                        // Multi-arg min/max are scalar, so only check single arg case
                        if arg_count <= 1 && (has_wildcard || arg_count == 0) {
                            Some(name.clone())
                        } else {
                            None
                        }
                    }
                    "SUM" | "AVG" | "TOTAL" => {
                        if has_wildcard || arg_count == 0 || arg_count > 1 {
                            Some(name.clone())
                        } else {
                            None
                        }
                    }
                    "GROUP_CONCAT" => {
                        if arg_count == 0 || arg_count > 2 {
                            Some(name.clone())
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            } else {
                // Check function arguments recursively
                for arg in args {
                    if let Some(found) = check_aggregate_arg_count(arg) {
                        return Some(found);
                    }
                }
                None
            }
        }
        Expression::BinaryOp { left, right, .. } => check_aggregate_arg_count(left)
            .or_else(|| check_aggregate_arg_count(right)),
        Expression::UnaryOp { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = check_aggregate_arg_count(op) {
                    return Some(found);
                }
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    if let Some(found) = check_aggregate_arg_count(cond) {
                        return Some(found);
                    }
                }
                if let Some(found) = check_aggregate_arg_count(&case_when.result) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                check_aggregate_arg_count(else_expr)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Cast { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = check_aggregate_arg_count(child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Find the first aggregate function in an expression
/// Returns the function name if found, None otherwise
fn find_aggregate_in_expression(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, .. } => Some(name.clone()),
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate
            // Note: MIN/MAX with multiple args are scalar functions in SQLite
            if is_aggregate_function(name) {
                let upper = name.to_uppercase();
                if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                    // Multi-arg min/max are scalar, not aggregate
                    None
                } else {
                    Some(name.clone())
                }
            } else {
                // Check function arguments recursively
                for arg in args {
                    if let Some(found) = find_aggregate_in_expression(arg) {
                        return Some(found);
                    }
                }
                None
            }
        }
        Expression::BinaryOp { left, right, .. } => find_aggregate_in_expression(left)
            .or_else(|| find_aggregate_in_expression(right)),
        Expression::UnaryOp { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = find_aggregate_in_expression(op) {
                    return Some(found);
                }
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    if let Some(found) = find_aggregate_in_expression(cond) {
                        return Some(found);
                    }
                }
                if let Some(found) = find_aggregate_in_expression(&case_when.result) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                find_aggregate_in_expression(else_expr)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => find_aggregate_in_expression(expr),
        Expression::IsDistinctFrom { left, right, .. } => find_aggregate_in_expression(left)
            .or_else(|| find_aggregate_in_expression(right)),
        Expression::IsTruthValue { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Between { expr, low, high, .. } => find_aggregate_in_expression(expr)
            .or_else(|| find_aggregate_in_expression(low))
            .or_else(|| find_aggregate_in_expression(high)),
        Expression::InList { expr, values, .. } => {
            if let Some(found) = find_aggregate_in_expression(expr) {
                return Some(found);
            }
            for val in values {
                if let Some(found) = find_aggregate_in_expression(val) {
                    return Some(found);
                }
            }
            None
        }
        Expression::In { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Exists { .. } => None, // EXISTS subqueries have their own scope
        Expression::Cast { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Like { expr, pattern, .. } => find_aggregate_in_expression(expr)
            .or_else(|| find_aggregate_in_expression(pattern)),
        Expression::Position { substring, string, .. } => find_aggregate_in_expression(substring)
            .or_else(|| find_aggregate_in_expression(string)),
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                if let Some(found) = find_aggregate_in_expression(char_expr) {
                    return Some(found);
                }
            }
            find_aggregate_in_expression(string)
        }
        Expression::Extract { expr, .. } => find_aggregate_in_expression(expr),
        Expression::ScalarSubquery(_) => None, // Subqueries have their own scope
        Expression::QuantifiedComparison { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Interval { value, .. } => find_aggregate_in_expression(value),
        Expression::WindowFunction { .. } => None, // Window functions are not regular aggregates
        Expression::MatchAgainst { search_modifier, .. } => {
            find_aggregate_in_expression(search_modifier)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = find_aggregate_in_expression(child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Validate a single column reference against the schema (and optionally outer schema)
fn validate_column_ref(
    col_ref: &ColumnReference,
    schema: &CombinedSchema,
    outer_schema: Option<&CombinedSchema>,
) -> Result<(), ExecutorError> {
    // Check if column exists in inner schema
    if schema.get_column_index(col_ref.table.as_deref(), &col_ref.column).is_some() {
        return Ok(());
    }

    // SQLite compatibility: Allow ROWID pseudo-column references
    // Only if there's no actual column with that name (real columns take precedence)
    if is_rowid_pseudo_column(&col_ref.column) {
        // Verify the qualifier matches a table in the schema (if qualified)
        if let Some(ref qualifier) = col_ref.table {
            let qualifier_lower = qualifier.to_lowercase();
            let table_exists =
                schema.table_schemas.keys().any(|k| k.to_lowercase() == qualifier_lower);
            let table_in_outer = outer_schema.is_some_and(|outer| {
                outer.table_schemas.keys().any(|k| k.to_lowercase() == qualifier_lower)
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
        if outer.get_column_index(col_ref.table.as_deref(), &col_ref.column).is_some() {
            return Ok(());
        }
    }

    // Column not found - build error with context
    let mut searched_tables: Vec<String> = if let Some(ref table) = col_ref.table {
        // If qualified, only report that table
        vec![table.clone()]
    } else {
        // If unqualified, list all tables that were searched
        schema.table_names()
    };

    // Collect available columns for suggestions (from both schemas)
    let mut available_columns: Vec<String> = schema
        .table_schemas
        .values()
        .flat_map(|(_, table_schema)| table_schema.columns.iter().map(|c| c.name.clone()))
        .collect();

    // Include outer schema tables and columns in error message
    if let Some(outer) = outer_schema {
        if col_ref.table.is_none() {
            searched_tables.extend(outer.table_names());
        }
        available_columns.extend(
            outer
                .table_schemas
                .values()
                .flat_map(|(_, table_schema)| table_schema.columns.iter().map(|c| c.name.clone())),
        );
    }

    Err(ExecutorError::ColumnNotFound {
        column_name: col_ref.column.clone(),
        table_name: col_ref.table.clone().unwrap_or_else(|| {
            // Use the first table name if no qualifier was provided
            searched_tables.first().cloned().unwrap_or_else(|| "unknown".to_string())
        }),
        searched_tables,
        available_columns,
    })
}

/// Validate all column references with optional procedural context and outer schema
///
/// When a procedural context is provided, variable names from the context are
/// allowed as column references (they will be resolved at runtime).
///
/// When an outer_schema is provided (for correlated subqueries), column references
/// are also validated against the outer schema. This fixes issue #2694 where
/// correlated subqueries failed to resolve outer table references during validation.
pub fn validate_select_columns_with_context(
    select_list: &[SelectItem],
    where_clause: Option<&Expression>,
    schema: &CombinedSchema,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    outer_schema: Option<&CombinedSchema>,
) -> Result<(), ExecutorError> {
    // Collect procedure variable names if in procedural context
    let proc_vars: std::collections::HashSet<String> = procedural_context
        .map(|ctx| {
            ctx.get_available_names()
                .into_iter()
                .flat_map(|name| vec![name.clone(), name.to_lowercase()])
                .collect()
        })
        .unwrap_or_default();

    let mut column_refs = Vec::new();

    // Extract column references from SELECT list
    for item in select_list {
        match item {
            SelectItem::Expression { expr, .. } => {
                extract_column_refs(expr, &mut column_refs);
            }
            SelectItem::Wildcard { .. } => {
                // Wildcard doesn't reference specific columns
            }
            SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Validate that the qualifier matches a known table (check both schemas)
                let qualifier_lower = qualifier.to_lowercase();
                let table_in_inner =
                    schema.table_schemas.keys().any(|k| k.to_lowercase() == qualifier_lower);
                let table_in_outer = outer_schema.is_some_and(|outer| {
                    outer.table_schemas.keys().any(|k| k.to_lowercase() == qualifier_lower)
                });

                if !table_in_inner && !table_in_outer {
                    let mut available_tables: Vec<String> = schema.table_names();
                    if let Some(outer) = outer_schema {
                        available_tables.extend(outer.table_names());
                    }
                    return Err(ExecutorError::InvalidTableQualifier {
                        qualifier: qualifier.clone(),
                        column: "*".to_string(),
                        available_tables,
                    });
                }
            }
        }
    }

    // Extract column references from WHERE clause
    if let Some(where_expr) = where_clause {
        extract_column_refs(where_expr, &mut column_refs);

        // Check for wrong argument count FIRST (takes priority over misuse)
        // SQLite reports arg count errors before context errors
        if let Some(agg_name) = check_aggregate_arg_count(where_expr) {
            return Err(ExecutorError::WrongNumberOfArguments {
                function_name: agg_name,
            });
        }

        // Check for aggregate functions in WHERE clause (misuse of aggregate)
        if let Some(agg_name) = find_aggregate_in_expression(where_expr) {
            return Err(ExecutorError::MisuseOfAggregate {
                function_name: agg_name,
            });
        }
    }

    // Validate each column reference
    for col_ref in &column_refs {
        // Skip validation for procedure variables (unqualified only)
        if col_ref.table.is_none() {
            if proc_vars.contains(&col_ref.column)
                || proc_vars.contains(&col_ref.column.to_lowercase())
            {
                continue;
            }
        }
        validate_column_ref(col_ref, schema, outer_schema)?;
    }

    Ok(())
}

/// Validate column references in SELECT list and WHERE clause against schema
///
/// Simple validation without procedural context - used for standard SELECT queries.
#[cfg(test)]
pub fn validate_select_columns(
    select_list: &[SelectItem],
    where_clause: Option<&Expression>,
    schema: &CombinedSchema,
) -> Result<(), ExecutorError> {
    validate_select_columns_with_context(select_list, where_clause, schema, None, None)
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;

    fn make_test_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
        ];
        let table_schema = TableSchema::new("PRODUCTS".to_string(), columns);
        CombinedSchema::from_table("PRODUCTS".to_string(), table_schema)
    }

    #[test]
    fn test_valid_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "invalid_column".to_string() },
            alias: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::ColumnNotFound { column_name, available_columns, .. }) => {
                assert_eq!(column_name, "invalid_column");
                assert!(available_columns.contains(&"ID".to_string()));
                assert!(available_columns.contains(&"NAME".to_string()));
            }
            _ => panic!("Expected ColumnNotFound error"),
        }
    }

    #[test]
    fn test_qualified_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef {
                table: Some("products".to_string()),
                column: "id".to_string(),
            },
            alias: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_qualified_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef {
                table: Some("products".to_string()),
                column: "invalid_column".to_string(),
            },
            alias: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::ColumnNotFound { column_name, .. }) => {
                assert_eq!(column_name, "invalid_column");
            }
            _ => panic!("Expected ColumnNotFound error"),
        }
    }

    #[test]
    fn test_column_in_expression() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Plus,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "invalid_col".to_string(),
                }),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1))),
            },
            alias: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_where_clause_validation() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Wildcard { alias: None }];
        let where_clause = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "nonexistent".to_string(),
            }),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        };

        let result = validate_select_columns(&select_list, Some(&where_clause), &schema);
        assert!(result.is_err());
    }
}
