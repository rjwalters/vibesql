//! JOIN execution logic for FROM clause scanning
//!
//! Handles execution of JOIN operations within FROM clauses by:
//! - Recursively executing left and right sides
//! - Extracting equijoin predicates from WHERE clause
//! - Delegating to nested loop join implementation

use std::collections::HashMap;

use crate::{
    errors::ExecutorError, optimizer::PredicatePlan, select::cte::CteResult,
    timeout::TimeoutContext,
};

/// Execute a JOIN operation
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_join<F>(
    left: &vibesql_ast::FromClause,
    right: &vibesql_ast::FromClause,
    join_type: &vibesql_ast::JoinType,
    condition: &Option<vibesql_ast::Expression>,
    natural: bool,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&crate::schema::CombinedSchema>,
    execute_subquery: F,
) -> Result<super::FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<crate::select::SelectResult, ExecutorError> + Copy,
{
    // Execute left and right sides with WHERE clause for predicate pushdown
    // Note: ORDER BY is not optimized at JOIN level, so we pass None
    let left_result = super::execute_from_clause(left, cte_results, database, where_clause, None, outer_row, outer_schema, execute_subquery)?;

    // For SEMI and ANTI joins (from IN/EXISTS subquery transformations), we must NOT pass
    // the outer WHERE clause to the right side. The right side represents the subquery
    // table, and the outer query's WHERE conditions should not be pushed down to it.
    // The subquery's own WHERE conditions are already included in the JOIN condition
    // (see subquery_to_join.rs), so they will be applied during the join.
    //
    // Bug fix for #2599: Passing outer WHERE clause to the right side caused incorrect
    // index scans that filtered out rows that should have been in the subquery result.
    let right_where_clause = match join_type {
        vibesql_ast::JoinType::Semi | vibesql_ast::JoinType::Anti => None,
        _ => where_clause,
    };
    let right_result = super::execute_from_clause(right, cte_results, database, right_where_clause, None, outer_row, outer_schema, execute_subquery)?;

    // For NATURAL JOIN, generate the implicit join condition based on common column names
    let natural_join_condition = if natural {
        generate_natural_join_condition(&left_result.schema, &right_result.schema)?
    } else {
        None
    };

    // Use the natural join condition if present, otherwise use the explicit condition
    let effective_condition = natural_join_condition.or_else(|| condition.clone());

    // If we have a WHERE clause, use predicate plan to extract equijoin conditions (Phase 1)
    let equijoin_predicates = if let Some(where_expr) = where_clause {
        // Build combined schema for WHERE clause analysis using SchemaBuilder for O(n) performance
        let mut schema_builder = crate::schema::SchemaBuilder::from_schema(left_result.schema.clone());
        for (table_name, (_start_idx, table_schema)) in &right_result.schema.table_schemas {
            schema_builder.add_table(table_name.clone(), table_schema.clone());
        }
        let combined_schema = schema_builder.build();

        // Build predicate plan once for this join (Phase 1 optimization)
        let predicate_plan = PredicatePlan::from_where_clause(Some(where_expr), &combined_schema)
            .map_err(ExecutorError::InvalidWhereClause)?;

        // Extract equijoin conditions that apply to this join
        let left_schema_tables: std::collections::HashSet<_> =
            left_result.schema.table_schemas.keys().cloned().collect();
        let right_schema_tables: std::collections::HashSet<_> =
            right_result.schema.table_schemas.keys().cloned().collect();

        predicate_plan
            .get_equijoin_conditions()
            .iter()
            .filter_map(|(left_table, _left_col, right_table, _right_col, expr)| {
                // Check if this equijoin connects tables from left and right
                let left_in_left = left_schema_tables.contains(left_table);
                let right_in_right = right_schema_tables.contains(right_table);
                let right_in_left = left_schema_tables.contains(right_table);
                let left_in_right = right_schema_tables.contains(left_table);

                if (left_in_left && right_in_right) || (right_in_left && left_in_right) {
                    Some(expr.clone())
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    // Perform nested loop join with equijoin predicates from WHERE clause
    use crate::select::join::nested_loop_join;
    // Note: Using default timeout context - proper timeout propagation from SelectExecutor
    // is a future improvement (see issue #2631 for context)
    let timeout_ctx = TimeoutContext::new_default();
    let result = nested_loop_join(
        left_result,
        right_result,
        join_type,
        &effective_condition,
        natural,
        database,
        &equijoin_predicates,
        &timeout_ctx,
    )?;
    Ok(result)
}

/// Generate the implicit join condition for a NATURAL JOIN
///
/// Finds all common column names between the left and right schemas (case-insensitive)
/// and creates an AND chain of equality conditions.
///
/// Returns None if there are no common columns (which means NATURAL JOIN should behave like CROSS JOIN)
fn generate_natural_join_condition(
    left_schema: &crate::schema::CombinedSchema,
    right_schema: &crate::schema::CombinedSchema,
) -> Result<Option<vibesql_ast::Expression>, ExecutorError> {
    use std::collections::HashMap;

    // Get all column names from left schema (normalized to lowercase for case-insensitive comparison)
    let mut left_columns: HashMap<String, Vec<(String, String)>> = HashMap::new(); // lowercase_name -> [(table, actual_name)]
    for (table_name, (_table_idx, table_schema)) in &left_schema.table_schemas {
        for col in &table_schema.columns {
            let lowercase_name = col.name.to_lowercase();
            left_columns
                .entry(lowercase_name)
                .or_default()
                .push((table_name.clone(), col.name.clone()));
        }
    }

    // Find common column names from right schema
    let mut common_columns: Vec<(String, String, String, String)> = Vec::new(); // (left_table, left_col, right_table, right_col)
    for (table_name, (_table_idx, table_schema)) in &right_schema.table_schemas {
        for col in &table_schema.columns {
            let lowercase_name = col.name.to_lowercase();
            if let Some(left_occurrences) = left_columns.get(&lowercase_name) {
                // Found a common column
                for (left_table, left_col) in left_occurrences {
                    common_columns.push((
                        left_table.clone(),
                        left_col.clone(),
                        table_name.clone(),
                        col.name.clone(),
                    ));
                }
            }
        }
    }

    // If no common columns, return None (NATURAL JOIN behaves like CROSS JOIN)
    if common_columns.is_empty() {
        return Ok(None);
    }

    // Build the join condition as an AND chain of equalities
    let mut condition: Option<vibesql_ast::Expression> = None;
    for (left_table, left_col, right_table, right_col) in common_columns {
        let equality = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some(left_table),
                column: left_col,
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some(right_table),
                column: right_col,
            }),
        };

        condition = Some(match condition {
            None => equality,
            Some(existing) => vibesql_ast::Expression::BinaryOp {
                left: Box::new(existing),
                op: vibesql_ast::BinaryOperator::And,
                right: Box::new(equality),
            },
        });
    }

    Ok(condition)
}
