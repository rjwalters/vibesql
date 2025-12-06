//! Column validation for SELECT statements
//!
//! Validates column references in SELECT list and WHERE clause expressions
//! against the available schema BEFORE row processing begins. This ensures
//! that column errors are caught even when tables are empty.

#![allow(clippy::collapsible_if)]

use crate::{errors::ExecutorError, schema::CombinedSchema};
use vibesql_ast::{Expression, SelectItem};

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
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
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
                    let mut available_tables: Vec<String> =
                        schema.table_names();
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
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

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
