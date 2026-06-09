//! DISTINCT elimination optimization
//!
//! This module implements the optimization that eliminates redundant DISTINCT
//! when the SELECT columns are guaranteed to be unique due to:
//! 1. A UNIQUE index covering all SELECT columns
//! 2. All columns in that unique index being NOT NULL
//!
//! Without the NOT NULL guarantee, multiple NULL values could exist in the result
//! set (since NULL != NULL in SQL), making DISTINCT necessary.
//!
//! References:
//! - SQLite's distinct.test (tests 1.1 vs 1.2, etc.)
//! - SQL standard semantics for UNIQUE constraints with NULLs

use std::collections::HashSet;

use vibesql_ast::{Expression, SelectItem, SelectStmt};
use vibesql_storage::Database;

/// Check if DISTINCT can be eliminated for a SELECT statement.
///
/// DISTINCT can be eliminated when:
/// 1. The query is a simple single-table query (no joins, subqueries)
/// 2. There exists a UNIQUE index (or PRIMARY KEY, or rowid) that covers the SELECT columns
/// 3. All columns in that unique index are NOT NULL
///
/// This follows SQLite's behavior as documented in distinct.test.
///
/// # Arguments
/// * `stmt` - The SELECT statement to analyze
/// * `database` - Database for looking up table schemas and indexes
///
/// # Returns
/// * `true` if DISTINCT is provably redundant and can be eliminated
/// * `false` if DISTINCT must be preserved
pub fn can_eliminate_distinct(stmt: &SelectStmt, database: &Database) -> bool {
    // Only handle simple single-table queries
    let table_name = match &stmt.from {
        Some(vibesql_ast::FromClause::Table { name, .. }) => name.as_str(),
        // Joins, subqueries, etc. - don't eliminate DISTINCT
        _ => return false,
    };

    // Get the table schema
    let Some(table) = database.get_table(table_name) else {
        return false;
    };
    let schema = &table.schema;

    // Extract columns from SELECT list
    let select_columns = match extract_select_columns(&stmt.select_list, schema) {
        Some(cols) => cols,
        None => return false, // Wildcard or expression - can't determine columns
    };

    if select_columns.is_empty() {
        return false;
    }

    // Check if rowid is in the select list - rowid is always unique
    if select_columns.iter().any(|c| c.eq_ignore_ascii_case("rowid")) {
        return true;
    }

    // Check PRIMARY KEY (rowid alias)
    // If we're selecting the INTEGER PRIMARY KEY column, it's always unique
    if let Some(rowid_idx) = schema.rowid_alias_column {
        let rowid_col = &schema.columns[rowid_idx].name;
        if select_columns.iter().any(|c| c.eq_ignore_ascii_case(rowid_col)) {
            return true;
        }
    }

    // Check if PRIMARY KEY covers select columns AND all PK columns are NOT NULL
    if let Some(ref pk_cols) = schema.primary_key {
        if covers_columns(pk_cols, &select_columns, &stmt.where_clause, schema) {
            // Check if all PK columns are NOT NULL
            if all_columns_not_null(pk_cols, schema) {
                return true;
            }
        }
    }

    // Check UNIQUE constraints from CREATE TABLE
    for unique_cols in &schema.unique_constraints {
        if covers_columns(unique_cols, &select_columns, &stmt.where_clause, schema)
            && all_columns_not_null(unique_cols, schema)
        {
            return true;
        }
    }

    // Check user-defined UNIQUE indexes
    for index_name in database.list_indexes() {
        let Some(index) = database.get_index(&index_name) else {
            continue;
        };

        // Skip if not for this table or not unique
        if !index.table_name.eq_ignore_ascii_case(table_name) || !index.unique {
            continue;
        }

        // Skip partial UNIQUE indexes: they only enforce uniqueness over the
        // subset of rows matching the WHERE predicate, so they do not prove
        // distinctness of the full table. The `is_partial` flag lives on the
        // catalog-side `IndexMetadata` (mirroring the conservative exclusion
        // in `index_planner` and `index_scan::selection`).
        //
        // Follow-up: partial-index bodies still index every row today, so
        // this is correctness-preserving once the build path is fixed to
        // honour the predicate.
        if database.catalog.find_index_by_name(&index_name).map(|m| m.is_partial()).unwrap_or(false)
        {
            continue;
        }

        // Get column names from index (skip expression indexes)
        let index_cols: Vec<String> =
            index.columns.iter().filter_map(|c| c.column_name().map(|s| s.to_string())).collect();

        if index_cols.len() != index.columns.len() {
            // Has expression columns - skip for now
            continue;
        }

        if covers_columns(&index_cols, &select_columns, &stmt.where_clause, schema)
            && all_columns_not_null(&index_cols, schema)
        {
            return true;
        }
    }

    false
}

/// Extract column names from SELECT list.
/// Returns None if the select list contains wildcards or complex expressions.
fn extract_select_columns(
    select_list: &[SelectItem],
    schema: &vibesql_catalog::TableSchema,
) -> Option<Vec<String>> {
    let mut columns = Vec::new();

    for item in select_list {
        match item {
            SelectItem::Wildcard { .. } => {
                // SELECT * - return all columns
                for col in &schema.columns {
                    columns.push(col.name.clone());
                }
            }
            SelectItem::QualifiedWildcard { .. } => {
                // table.* - for single table, same as *
                for col in &schema.columns {
                    columns.push(col.name.clone());
                }
            }
            SelectItem::Expression { expr, .. } => {
                // Try to extract column reference
                if let Some(col_name) = extract_column_from_expr(expr) {
                    columns.push(col_name);
                } else {
                    // Complex expression - we can still continue
                    // The column might still be covered by an index
                    // But for safety, skip expressions that aren't simple column refs
                    return None;
                }
            }
        }
    }

    Some(columns)
}

/// Extract a column name from a simple column reference expression.
fn extract_column_from_expr(expr: &Expression) -> Option<String> {
    match expr {
        Expression::ColumnRef(col_id) => Some(col_id.column_canonical().to_string()),
        _ => None,
    }
}

/// Check if a set of unique columns covers the select columns.
///
/// A unique index "covers" the select columns if:
/// 1. All unique index columns are in the select columns, OR
/// 2. Some unique index columns are fixed by WHERE clause equality conditions
///
/// For example, with UNIQUE INDEX ON (a, b):
/// - SELECT a, b FROM t - covered (both columns in select)
/// - SELECT b FROM t WHERE a = 5 - covered (a is fixed, b in select)
fn covers_columns(
    unique_cols: &[String],
    select_cols: &[String],
    where_clause: &Option<Expression>,
    schema: &vibesql_catalog::TableSchema,
) -> bool {
    let select_set: HashSet<String> = select_cols.iter().map(|c| c.to_lowercase()).collect();

    // Get columns fixed by WHERE clause (col = const)
    let fixed_cols = get_fixed_columns(where_clause, schema);

    // Check if every unique column is either in select list or fixed by WHERE
    for unique_col in unique_cols {
        let unique_col_lower = unique_col.to_lowercase();
        if !select_set.contains(&unique_col_lower) && !fixed_cols.contains(&unique_col_lower) {
            return false;
        }
    }

    true
}

/// Get columns that are fixed to constant values by WHERE clause.
/// Only considers simple equality conditions (col = const).
fn get_fixed_columns(
    where_clause: &Option<Expression>,
    _schema: &vibesql_catalog::TableSchema,
) -> HashSet<String> {
    let mut fixed = HashSet::new();

    if let Some(expr) = where_clause {
        collect_fixed_columns(expr, &mut fixed);
    }

    fixed
}

/// Recursively collect columns that are fixed to constants.
fn collect_fixed_columns(expr: &Expression, fixed: &mut HashSet<String>) {
    use vibesql_ast::BinaryOperator;

    match expr {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Check if one side is a column and the other is a constant
            if let Expression::ColumnRef(col_id) = left.as_ref() {
                if is_constant(right) {
                    fixed.insert(col_id.column_canonical().to_lowercase());
                }
            }
            if let Expression::ColumnRef(col_id) = right.as_ref() {
                if is_constant(left) {
                    fixed.insert(col_id.column_canonical().to_lowercase());
                }
            }
        }
        Expression::Conjunction(exprs) => {
            for e in exprs {
                collect_fixed_columns(e, fixed);
            }
        }
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            collect_fixed_columns(left, fixed);
            collect_fixed_columns(right, fixed);
        }
        _ => {}
    }
}

/// Check if an expression is a constant (literal, placeholder, etc.).
fn is_constant(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Literal(_)
            | Expression::Placeholder(_)
            | Expression::NumberedPlaceholder(_)
            | Expression::NamedPlaceholder(_)
    )
}

/// Check if all columns in a list are NOT NULL.
fn all_columns_not_null(columns: &[String], schema: &vibesql_catalog::TableSchema) -> bool {
    for col_name in columns {
        let col_lower = col_name.to_lowercase();
        let col = schema.columns.iter().find(|c| c.name.to_lowercase() == col_lower);
        match col {
            Some(c) => {
                if c.nullable {
                    return false;
                }
            }
            None => {
                // Column not found - shouldn't happen, but be conservative
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{ColumnIdentifier, OrderDirection, SelectStmt};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Database;
    use vibesql_types::DataType;

    use super::*;

    fn create_test_database() -> Database {
        let mut db = Database::new();

        // Create t1: nullable columns with unique index
        let t1_cols = vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
            ColumnSchema::new("c".to_string(), DataType::Integer, true),
            ColumnSchema::new("d".to_string(), DataType::Integer, true),
        ];
        let t1_schema = TableSchema::new("t1".to_string(), t1_cols);
        db.create_table(t1_schema).unwrap();

        // Create t4: NOT NULL columns with unique index
        let t4_cols = vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true), // nullable
            ColumnSchema::new("b".to_string(), DataType::Integer, false), // NOT NULL
            ColumnSchema::new("c".to_string(), DataType::Integer, false), // NOT NULL
            ColumnSchema::new("d".to_string(), DataType::Integer, false), // NOT NULL
        ];
        let t4_schema = TableSchema::new("t4".to_string(), t4_cols);
        db.create_table(t4_schema).unwrap();

        // Create unique index on (b, c) for both tables
        db.create_index(
            "i1".to_string(),
            "t1".to_string(),
            true, // unique
            vec![
                vibesql_ast::IndexColumn::new_column("b".to_string(), OrderDirection::Asc),
                vibesql_ast::IndexColumn::new_column("c".to_string(), OrderDirection::Asc),
            ],
        )
        .unwrap();

        db.create_index(
            "t4i1".to_string(),
            "t4".to_string(),
            true, // unique
            vec![
                vibesql_ast::IndexColumn::new_column("b".to_string(), OrderDirection::Asc),
                vibesql_ast::IndexColumn::new_column("c".to_string(), OrderDirection::Asc),
            ],
        )
        .unwrap();

        db
    }

    fn make_select_stmt(table: &str, columns: &[&str]) -> SelectStmt {
        let select_list: Vec<SelectItem> = columns
            .iter()
            .map(|c| SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::simple(c, false)),
                alias: None,
                source_text: None,
            })
            .collect();

        SelectStmt {
            with_clause: None,
            distinct: true,
            select_list,
            into_table: None,
            into_variables: None,
            from: Some(vibesql_ast::FromClause::Table {
                index_hint: None,
                name: table.to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    #[test]
    fn test_distinct_not_eliminated_nullable_columns() {
        let db = create_test_database();

        // SELECT DISTINCT b, c FROM t1 - should NOT be eliminated (nullable columns)
        let stmt = make_select_stmt("t1", &["b", "c"]);
        assert!(!can_eliminate_distinct(&stmt, &db));
    }

    #[test]
    fn test_distinct_eliminated_not_null_columns() {
        let db = create_test_database();

        // SELECT DISTINCT b, c FROM t4 - SHOULD be eliminated (NOT NULL columns)
        let stmt = make_select_stmt("t4", &["b", "c"]);
        assert!(can_eliminate_distinct(&stmt, &db));
    }

    #[test]
    fn test_distinct_not_eliminated_partial_unique() {
        let db = create_test_database();

        // SELECT DISTINCT a, b FROM t4 - should NOT be eliminated (a not in unique index)
        let stmt = make_select_stmt("t4", &["a", "b"]);
        assert!(!can_eliminate_distinct(&stmt, &db));
    }
}
