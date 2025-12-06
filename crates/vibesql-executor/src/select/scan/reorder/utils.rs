//! Utility functions for join reordering

use crate::schema::CombinedSchema;
use crate::select::cte::CteResult;
use std::collections::HashMap;
use vibesql_ast::{Expression, FromClause, SelectItem};

/// Extract column names from a SELECT list for subquery schema inference
///
/// This enables join optimizer to resolve column indices for derived tables,
/// which is required for hash join optimization with arithmetic equijoins.
fn extract_column_names_from_select_list(select_list: &[SelectItem]) -> Vec<String> {
    let mut columns = Vec::new();
    for item in select_list {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                // For SELECT *, we can't know column names without executing the query
                // Return empty to fall back to CTE-fallback path
                return Vec::new();
            }
            SelectItem::Expression { expr, alias } => {
                let col_name = if let Some(a) = alias {
                    a.to_lowercase()
                } else {
                    // Extract column name from expression
                    extract_column_name_from_expr(expr).to_lowercase()
                };
                columns.push(col_name);
            }
        }
    }
    columns
}

/// Extract a column name from an expression (for schema inference)
fn extract_column_name_from_expr(expr: &Expression) -> String {
    match expr {
        Expression::ColumnRef { column, .. } => column.clone(),
        Expression::AggregateFunction { name, args, .. } => {
            // For aggregates like SUM(col), use the column name if simple
            if args.len() == 1 {
                if let Expression::ColumnRef { column, .. } = &args[0] {
                    return column.clone();
                }
            }
            name.clone()
        }
        Expression::Function { name, .. } => name.clone(),
        Expression::BinaryOp { left, .. } => extract_column_name_from_expr(left),
        _ => "?column?".to_string(),
    }
}

/// Check if join reordering optimization should be applied
///
/// ## Time-Bounded Search (Default)
///
/// The optimizer uses time-bounded anytime search with a configurable budget
/// (default: 1000ms). This enables optimization for queries of all sizes:
/// - Small queries (2-6 tables): Complete exhaustively in <1ms
/// - Medium queries (7-8 tables): Usually complete within budget
/// - Large queries (9+ tables): Get partial optimization (better than none!)
///
/// The time budget can be configured via JOIN_REORDER_TIME_BUDGET_MS environment variable.
///
/// ## Benefits vs Table-Count Limits
///
/// Previous approach (hard 8-table limit):
/// - 3-8 table joins: Find optimal ordering via exhaustive search with pruning
/// - 9+ table joins: Previously received NO optimization, now get partial
///   optimization within time budget
///
/// The time budget prevents pathological cases while enabling better plans
/// for complex queries that need optimization most.
pub(crate) fn should_apply_join_reordering(table_count: usize) -> bool {
    // Must have at least 2 tables for reordering to be beneficial
    if table_count < 2 {
        return false;
    }

    // Allow opt-out via environment variable if needed
    std::env::var("JOIN_REORDER_DISABLED").is_err()
}

/// Count the number of tables in a FROM clause (including nested joins)
pub(crate) fn count_tables_in_from(from: &FromClause) -> usize {
    match from {
        FromClause::Table { .. } => 1,
        FromClause::Subquery { .. } => 1,
        FromClause::Join { left, right, .. } => {
            count_tables_in_from(left) + count_tables_in_from(right)
        }
    }
}

/// Check if all joins in the tree are CROSS joins (comma-list syntax)
///
/// Join reordering changes column ordering, so we only apply it to implicit CROSS joins
/// from comma-list syntax (FROM t1, t2, t3). Explicit INNER/LEFT/RIGHT joins must
/// preserve their declared ordering.
///
/// Note: CROSS JOINs with ON conditions are NOT valid comma-list syntax and should
/// not be reordered. This ensures `CROSS JOIN ... ON` goes through the normal path
/// where the appropriate error is raised (CROSS JOIN does not support ON clause).
pub(crate) fn all_joins_are_cross(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => true,
        FromClause::Join { left, right, join_type, condition, .. } => {
            // Must be CROSS join type AND have no ON condition
            // CROSS JOIN with ON clause is invalid and should not be reordered
            matches!(join_type, vibesql_ast::JoinType::Cross)
                && condition.is_none()
                && all_joins_are_cross(left)
                && all_joins_are_cross(right)
        }
    }
}

/// Build a reordered combined schema with tables in original order
///
/// Takes the current schema (with tables in optimal order) and reconstructs it
/// with tables in the original FROM clause order.
pub(super) fn build_reordered_schema(
    current_schema: &CombinedSchema,
    original_order: &[String],
    _optimal_order: &[String],
) -> CombinedSchema {
    let mut new_table_schemas = HashMap::new();
    let mut current_position = 0;

    // Walk through original order and rebuild schema with correct positions
    for table_name in original_order {
        // Find this table's schema in the current (optimally ordered) schema
        // get_table handles case-insensitive lookups via TableKey
        let table_schema = current_schema
            .get_table(table_name)
            .map(|(_, schema)| schema.clone());

        if let Some(schema) = table_schema {
            let col_count = schema.columns.len();
            // TableKey handles case normalization automatically
            new_table_schemas.insert(crate::schema::TableKey::new(table_name), (current_position, schema));
            current_position += col_count;
        }
    }

    CombinedSchema { table_schemas: new_table_schemas, total_columns: current_position }
}

/// Build a column permutation to restore original table ordering
///
/// Given:
/// - Original table order: [tab0, tab2, tab1]
/// - Optimal execution order: [tab1, tab0, tab2]
/// - Column counts: {tab0: 3, tab1: 3, tab2: 3}
///
/// Returns permutation mapping current positions to original positions:
/// - Current: [tab1.col0, tab1.col1, tab1.col2, tab0.col0, tab0.col1, tab0.col2, tab2.col0, tab2.col1, tab2.col2]
/// - Target:  [tab0.col0, tab0.col1, tab0.col2, tab2.col0, tab2.col1, tab2.col2, tab1.col0, tab1.col1, tab1.col2]
/// - Permutation: [3, 4, 5, 6, 7, 8, 0, 1, 2]
pub(super) fn build_column_permutation(
    original_order: &[String],
    optimal_order: &[String],
    column_counts: &HashMap<String, usize>,
) -> Vec<usize> {
    // Build position map: table name -> starting column index in optimal order
    let mut optimal_positions: HashMap<String, usize> = HashMap::new();
    let mut current_position = 0;
    for table in optimal_order {
        optimal_positions.insert(table.clone(), current_position);
        current_position += column_counts.get(table).unwrap_or(&0);
    }

    // Build permutation by walking through original order
    let mut permutation = Vec::new();
    for table in original_order {
        let table_lower = table.to_lowercase();
        let start_pos = optimal_positions.get(&table_lower).unwrap_or(&0);
        let col_count = column_counts.get(&table_lower).unwrap_or(&0);

        // Add all column indices for this table
        for i in 0..*col_count {
            permutation.push(start_pos + i);
        }
    }

    permutation
}

/// Build a column-to-table mapping using actual database schema
///
/// This is the proper schema-based column resolution that replaces heuristic
/// pattern matching. For each unqualified column reference, we look up which
/// table(s) contain that column using the actual database schema.
///
/// # Parameters
/// - `database`: The database to query for table schemas
/// - `table_names`: The tables in the FROM clause (may be aliases)
/// - `table_refs`: Table references with name/alias info for resolving CTEs and subqueries
/// - `cte_results`: CTE results for looking up CTE schemas
///
/// # Returns
/// A HashMap mapping column names (lowercase) to the table name that contains them.
/// If a column exists in multiple tables, it's ambiguous and not included (user must qualify it).
pub(super) fn build_column_to_table_map(
    database: &vibesql_storage::Database,
    table_names: &[String],
    table_refs: &[super::graph::TableRef],
    cte_results: &HashMap<String, CteResult>,
) -> HashMap<String, String> {
    let mut column_to_table: HashMap<String, Vec<String>> = HashMap::new();

    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
        eprintln!(
            "[JOIN_REORDER] build_column_to_table_map: database.tables.keys(): {:?}",
            database.tables.keys().collect::<Vec<_>>()
        );
        eprintln!(
            "[JOIN_REORDER] build_column_to_table_map: table_refs: {:?}",
            table_refs.iter().map(|r| (&r.name, &r.alias)).collect::<Vec<_>>()
        );
    }

    for table_name in table_names {
        let table_lower = table_name.to_lowercase();

        // Find the corresponding TableRef to check if it's a subquery/CTE
        let table_ref = table_refs.iter().find(|r| {
            r.alias.as_ref().map(|a| a.to_lowercase()) == Some(table_lower.clone())
                || r.name.to_lowercase() == table_lower
        });

        // Get column names for this table
        let column_names: Vec<String> = if let Some(tr) = table_ref {
            if tr.is_subquery {
                // For subqueries (derived tables), infer column names from SELECT list
                // This enables hash join optimization for queries with derived tables
                if let Some(subquery) = &tr.subquery {
                    extract_column_names_from_select_list(&subquery.select_list)
                } else {
                    continue;
                }
            } else if let Some(cte_result) = cte_results
                .get(&tr.name)
                .or_else(|| cte_results.get(&tr.name.to_lowercase()))
                .or_else(|| cte_results.get(&tr.name.to_uppercase()))
            {
                // CTE: get columns from CTE result schema (CteResult is a tuple: (TableSchema, Vec<Row>))
                cte_result.0.columns.iter().map(|c| c.name.to_lowercase()).collect()
            } else {
                // Regular table: get columns from database
                // Try multiple case variations and schema prefixes since database keys may vary
                let actual_table_name = &tr.name;
                let public_prefixed = format!("public.{}", actual_table_name);
                let table = database
                    .tables
                    .get(actual_table_name)
                    .or_else(|| database.tables.get(&actual_table_name.to_lowercase()))
                    .or_else(|| database.tables.get(&actual_table_name.to_uppercase()))
                    .or_else(|| database.tables.get(&public_prefixed))
                    .or_else(|| database.tables.get(&public_prefixed.to_lowercase()))
                    .or_else(|| database.tables.get(&public_prefixed.to_uppercase()))
                    .or_else(|| {
                        // Case-insensitive search through all tables (handles schema.table format)
                        let target = actual_table_name.to_lowercase();
                        database
                            .tables
                            .iter()
                            .find(|(k, _)| {
                                let key_lower = k.to_lowercase();
                                key_lower == target || key_lower.ends_with(&format!(".{}", target))
                            })
                            .map(|(_, v)| v)
                    });

                if let Some(t) = table {
                    t.schema.columns.iter().map(|c| c.name.to_lowercase()).collect()
                } else {
                    // Table not found in database - might be an alias or CTE
                    continue;
                }
            }
        } else {
            // Direct table lookup without TableRef
            let public_prefixed = format!("public.{}", table_name);
            let table = database
                .tables
                .get(table_name)
                .or_else(|| database.tables.get(&table_lower))
                .or_else(|| database.tables.get(&table_name.to_uppercase()))
                .or_else(|| database.tables.get(&public_prefixed))
                .or_else(|| database.tables.get(&public_prefixed.to_lowercase()))
                .or_else(|| database.tables.get(&public_prefixed.to_uppercase()))
                .or_else(|| {
                    // Case-insensitive search through all tables (handles schema.table format)
                    database
                        .tables
                        .iter()
                        .find(|(k, _)| {
                            let key_lower = k.to_lowercase();
                            key_lower == table_lower
                                || key_lower.ends_with(&format!(".{}", table_lower))
                        })
                        .map(|(_, v)| v)
                });

            if let Some(t) = table {
                t.schema.columns.iter().map(|c| c.name.to_lowercase()).collect()
            } else {
                continue;
            }
        };

        // Add columns to the mapping
        for col_name in column_names {
            column_to_table.entry(col_name).or_default().push(table_lower.clone());
        }
    }

    // Convert to single-table mapping, excluding ambiguous columns
    let mut result: HashMap<String, String> = HashMap::new();
    for (col, tables) in column_to_table {
        if tables.len() == 1 {
            result.insert(col, tables.into_iter().next().unwrap());
        }
        // Ambiguous columns (in multiple tables) are not included
        // This forces the user to qualify them explicitly
    }

    result
}

/// Combine a list of predicates into a single AND expression, qualifying unqualified columns
///
/// If the list is empty, returns None.
/// If the list has one element, returns that element (with column qualification).
/// Otherwise, combines all predicates with AND, adding table qualifiers to any
/// unqualified column references. This is necessary for predicates extracted from
/// OR branches where columns may not have table qualifiers.
pub(super) fn combine_predicates_with_qualification(
    predicates: &[vibesql_ast::Expression],
    table_name: &str,
) -> Option<vibesql_ast::Expression> {
    match predicates.len() {
        0 => None,
        1 => Some(qualify_columns(&predicates[0], table_name)),
        _ => {
            let mut result = qualify_columns(&predicates[0], table_name);
            for pred in &predicates[1..] {
                result = vibesql_ast::Expression::BinaryOp {
                    op: vibesql_ast::BinaryOperator::And,
                    left: Box::new(result),
                    right: Box::new(qualify_columns(pred, table_name)),
                };
            }
            Some(result)
        }
    }
}

/// Add table qualifiers to unqualified column references in an expression
fn qualify_columns(expr: &vibesql_ast::Expression, table_name: &str) -> vibesql_ast::Expression {
    use vibesql_ast::Expression;

    // Use lowercase table name for consistency with schema lookups
    let table_name_lower = table_name.to_lowercase();

    match expr {
        Expression::ColumnRef { table: None, column } => {
            // Add table qualifier to unqualified column
            Expression::ColumnRef { table: Some(table_name_lower.clone()), column: column.clone() }
        }
        Expression::ColumnRef { table: Some(t), column } => {
            // Already qualified, keep as is
            Expression::ColumnRef { table: Some(t.clone()), column: column.clone() }
        }
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: *op,
            left: Box::new(qualify_columns(left, table_name)),
            right: Box::new(qualify_columns(right, table_name)),
        },
        Expression::UnaryOp { op, expr: inner } => {
            Expression::UnaryOp { op: *op, expr: Box::new(qualify_columns(inner, table_name)) }
        }
        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(qualify_columns(inner, table_name)),
            values: values.iter().map(|v| qualify_columns(v, table_name)).collect(),
            negated: *negated,
        },
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(qualify_columns(inner, table_name)),
            low: Box::new(qualify_columns(low, table_name)),
            high: Box::new(qualify_columns(high, table_name)),
            negated: *negated,
            symmetric: *symmetric,
        },
        // For other expressions (literals, etc.), return as-is
        _ => expr.clone(),
    }
}

/// Resolve an unqualified column to its table using schema-based lookup
///
/// Returns the table name if the column is found in exactly one table,
/// None otherwise (column not found or ambiguous).
pub(super) fn resolve_column_to_table(
    column: &str,
    column_to_table: &HashMap<String, String>,
) -> Option<String> {
    column_to_table.get(&column.to_lowercase()).cloned()
}

/// Resolve an unqualified column using schema-based lookup
///
/// Uses the schema-based column-to-table map to resolve column references.
/// All column resolution relies solely on actual database schema metadata.
///
/// # Parameters
/// - `column`: The unqualified column name
/// - `column_to_table`: Schema-based column-to-table mapping
pub(super) fn resolve_column_with_fallback(
    column: &str,
    column_to_table: &HashMap<String, String>,
) -> Option<String> {
    resolve_column_to_table(column, column_to_table)
}
