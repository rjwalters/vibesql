//! Column validation for SELECT statements
//!
//! Validates column references in SELECT list and WHERE clause expressions
//! against the available schema BEFORE row processing begins. This ensures
//! that column errors are caught even when tables are empty.
//!
//! This module is organized into focused submodules:
//! - `join_limits` - Join table limit validation (max 64 tables)
//! - `column_refs` - Column reference extraction and validation
//! - `aggregates` - Aggregate function validation (arg counts, nesting, aliased misuse)
//! - `schema` - Schema-level validation for subqueries

#![allow(clippy::collapsible_if)]

mod aggregates;
mod collation_names;
mod column_refs;
mod in_subquery_columns;
mod index_hints;
mod join_limits;
mod row_values;
mod scalar_subquery_arity;
mod schema;

use std::collections::HashSet;

// Re-export public validation functions
pub use aggregates::{
    check_aggregate_arg_count, find_aggregate_in_expression, find_window_function_in_expression,
    validate_aggregate_arguments, validate_group_by_window_misuse,
    validate_having_aliased_aggregates, validate_no_nested_aggregates,
    validate_order_by_aliased_window_functions, validate_subquery_context_misuse,
    validate_window_query_order_by_aggregates, SubqueryContext,
};
pub use collation_names::validate_collation_names;
pub use column_refs::{extract_column_refs, validate_column_ref};
pub use in_subquery_columns::validate_in_subquery_column_counts;
pub use index_hints::validate_index_hints;
pub use join_limits::validate_join_table_limit;
pub use row_values::validate_row_value_usage;
pub use scalar_subquery_arity::{
    validate_predicate_expr as validate_predicate_subquery_arity,
    validate_select_where_expr as validate_select_where_subquery_arity,
    validate_value_expr as validate_value_subquery_arity,
};
pub use schema::validate_aggregate_subquery_outer_refs;
use vibesql_ast::{Expression, SelectItem};

use crate::{errors::ExecutorError, schema::CombinedSchema};

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
    let proc_vars: HashSet<String> = procedural_context
        .map(|ctx| {
            ctx.get_available_names()
                .into_iter()
                .flat_map(|name| vec![name.clone(), name.to_lowercase()])
                .collect()
        })
        .unwrap_or_default();

    // Collect SELECT aliases (SQLite extension: allow aliases in WHERE clause)
    let select_aliases: HashSet<String> = select_list
        .iter()
        .filter_map(|item| {
            if let SelectItem::Expression { alias: Some(alias), .. } = item {
                Some(vec![alias.clone(), alias.to_lowercase()])
            } else {
                None
            }
        })
        .flatten()
        .collect();

    // Combine procedure variables and SELECT aliases for WHERE clause validation
    let allowed_in_where: HashSet<String> = proc_vars.union(&select_aliases).cloned().collect();

    let mut column_refs = Vec::new();
    let mut where_column_refs = Vec::new();

    // Extract column references from SELECT list
    for item in select_list {
        match item {
            SelectItem::Expression { expr, .. } => {
                // Validate COLLATE names at prepare time (issue #6089): an
                // unknown collating sequence must error even on an empty table.
                validate_collation_names(expr)?;
                extract_column_refs(expr, &mut column_refs);
            }
            SelectItem::Wildcard { .. } => {
                // Wildcard doesn't reference specific columns
            }
            SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Validate that the qualifier matches a known table (check both schemas)
                let qualifier_lower = qualifier.to_lowercase();
                let table_in_inner =
                    schema.table_schemas.keys().any(|k| k.canonical() == qualifier_lower);
                let table_in_outer = outer_schema.is_some_and(|outer| {
                    outer.table_schemas.keys().any(|k| k.canonical() == qualifier_lower)
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

    // Extract column references from WHERE clause (tracked separately for alias resolution)
    if let Some(where_expr) = where_clause {
        // Validate COLLATE names at prepare time (issue #6089): rowvalue4 §7.4
        // `... WHERE (?, ? COLLATE nose) > (a, b)` must error at prepare time.
        validate_collation_names(where_expr)?;
        extract_column_refs(where_expr, &mut where_column_refs);

        // Check for wrong argument count FIRST (takes priority over misuse)
        // SQLite reports arg count errors before context errors
        if let Some(agg_name) = check_aggregate_arg_count(where_expr) {
            return Err(ExecutorError::WrongNumberOfArguments { function_name: agg_name });
        }

        // Check for aggregate functions in WHERE clause (misuse of aggregate)
        if let Some(agg_name) = find_aggregate_in_expression(where_expr) {
            return Err(ExecutorError::MisuseOfAggregate { function_name: agg_name });
        }

        // Check for window functions in WHERE clause (misuse of window function)
        if let Some(window_name) = find_window_function_in_expression(where_expr) {
            return Err(ExecutorError::MisuseOfWindowFunction { function_name: window_name });
        }

        // Check for aggregates/windows inside bare scalar subqueries in WHERE
        // (e.g. `WHERE (SELECT AVG(0) FILTER(WHERE outer.c))`). The bare
        // subquery has no FROM clause so its aggregate borrows the outer
        // aggregation context, which SQLite reports as a misuse.
        validate_subquery_context_misuse(where_expr, SubqueryContext::WhereOrEqual)?;
    }

    // Validate SELECT list column references (only procedure variables allowed)
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

    // Validate WHERE clause column references (aliases + procedure variables allowed)
    log::debug!("Validating WHERE: allowed_in_where={:?}", allowed_in_where);
    for col_ref in &where_column_refs {
        // Skip validation for allowed names (procedure variables and SELECT aliases)
        if col_ref.table.is_none() {
            if allowed_in_where.contains(&col_ref.column)
                || allowed_in_where.contains(&col_ref.column.to_lowercase())
            {
                log::debug!("WHERE validation: allowing alias '{}'", col_ref.column);
                continue;
            }
        }
        log::debug!("WHERE validation: checking column '{}' against schema", col_ref.column);
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
    use vibesql_ast::{BinaryOperator, ColumnIdentifier};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    use super::*;

    fn make_test_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ];
        let table_schema = TableSchema::new("PRODUCTS".to_string(), columns);
        CombinedSchema::from_table("PRODUCTS".to_string(), table_schema)
    }

    #[test]
    fn test_valid_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef(ColumnIdentifier::simple("id", false)),
            alias: None,
            source_text: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef(ColumnIdentifier::simple("invalid_column", false)),
            alias: None,
            source_text: None,
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
            expr: Expression::ColumnRef(ColumnIdentifier::qualified(
                "products", false, "id", false,
            )),
            alias: None,
            source_text: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_qualified_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef(ColumnIdentifier::qualified(
                "products",
                false,
                "invalid_column",
                false,
            )),
            alias: None,
            source_text: None,
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
                op: BinaryOperator::Plus,
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple(
                    "invalid_col",
                    false,
                ))),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            },
            alias: None,
            source_text: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_where_clause_validation() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Wildcard { alias: None }];
        let where_clause = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("nonexistent", false))),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };

        let result = validate_select_columns(&select_list, Some(&where_clause), &schema);
        assert!(result.is_err());
    }
}
