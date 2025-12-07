//! Primary Key Column Detection for Subscription Updates
//!
//! This module provides functionality to detect which columns in a query's result set
//! are primary key columns. This information is used for selective column updates,
//! where we need to always include PK columns to identify rows.
//!
//! # Design
//!
//! For simple single-table queries (SELECT ... FROM table), we:
//! 1. Parse the query to extract the table name and SELECT list
//! 2. Query the database schema to find which columns are PKs
//! 3. Map those PK column names to positions in the result set
//!
//! For complex queries (joins, subqueries, etc.), we fall back to the default
//! behavior of assuming column 0 is the PK, as correctly mapping PKs across
//! joins requires more sophisticated analysis.

use std::collections::HashSet;

use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt, Statement};
use vibesql_storage::Database;

/// Reasons why PK detection was not confident
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkDetectionFailureReason {
    /// Query could not be parsed
    ParseError,
    /// Query has a set operation (UNION, INTERSECT, etc.)
    SetOperation,
    /// Query has no FROM clause
    NoFromClause,
    /// Query involves multiple tables (join)
    MultipleTablesInQuery,
    /// The referenced table was not found in the database
    TableNotFound,
    /// The table has no primary key defined
    NoPrimaryKeyOnTable,
    /// PK columns are not in the query's result set
    PkColumnsNotInResultSet,
    /// Query has a subquery in FROM clause
    SubqueryInFrom,
}

impl std::fmt::Display for PkDetectionFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError => write!(f, "query parse error"),
            Self::SetOperation => write!(f, "query contains set operation (UNION/INTERSECT/EXCEPT)"),
            Self::NoFromClause => write!(f, "query has no FROM clause"),
            Self::MultipleTablesInQuery => write!(f, "query involves multiple tables (join)"),
            Self::TableNotFound => write!(f, "table not found in database"),
            Self::NoPrimaryKeyOnTable => write!(f, "table has no primary key defined"),
            Self::PkColumnsNotInResultSet => write!(f, "PK columns not present in SELECT list"),
            Self::SubqueryInFrom => write!(f, "FROM clause contains subquery"),
        }
    }
}

/// Result of PK column detection
#[derive(Debug, Clone)]
pub struct PkDetectionResult {
    /// Indices of columns that are primary keys in the result set
    pub pk_column_indices: Vec<usize>,
    /// Whether detection was confident (single table, no complex joins)
    pub confident: bool,
    /// Tables involved in the query
    pub tables: HashSet<String>,
    /// Reason for lack of confidence (set when confident is false)
    pub reason: Option<PkDetectionFailureReason>,
}

impl Default for PkDetectionResult {
    fn default() -> Self {
        Self {
            pk_column_indices: vec![0], // Default: assume first column is PK
            confident: false,
            tables: HashSet::new(),
            reason: None,
        }
    }
}

impl PkDetectionResult {
    /// Create a default result with a specific failure reason
    fn with_reason(reason: PkDetectionFailureReason) -> Self {
        Self {
            pk_column_indices: vec![0],
            confident: false,
            tables: HashSet::new(),
            reason: Some(reason),
        }
    }

    /// Create a default result with a specific failure reason and tables
    fn with_reason_and_tables(reason: PkDetectionFailureReason, tables: HashSet<String>) -> Self {
        Self {
            pk_column_indices: vec![0],
            confident: false,
            tables,
            reason: Some(reason),
        }
    }
}

/// Detect primary key columns in a query result set
///
/// # Arguments
/// * `sql` - The SQL query string
/// * `db` - Reference to the database for schema lookup
///
/// # Returns
/// * `PkDetectionResult` with the detected PK column indices
pub fn detect_pk_columns(sql: &str, db: &Database) -> PkDetectionResult {
    // Parse the query
    let stmt = match vibesql_parser::Parser::parse_sql(sql) {
        Ok(stmt) => stmt,
        Err(_) => return PkDetectionResult::with_reason(PkDetectionFailureReason::ParseError),
    };

    detect_pk_columns_from_stmt(&stmt, db)
}

/// Detect primary key columns from a parsed statement
pub fn detect_pk_columns_from_stmt(stmt: &Statement, db: &Database) -> PkDetectionResult {
    match stmt {
        Statement::Select(select) => detect_pk_from_select(select, db),
        _ => PkDetectionResult::default(),
    }
}

/// Detect PK columns from a SELECT statement
fn detect_pk_from_select(select: &SelectStmt, db: &Database) -> PkDetectionResult {
    // Don't handle set operations (UNION, etc.) - too complex
    if select.set_operation.is_some() {
        return PkDetectionResult::with_reason(PkDetectionFailureReason::SetOperation);
    }

    // Extract the FROM clause
    let from = match &select.from {
        Some(from) => from,
        None => return PkDetectionResult::with_reason(PkDetectionFailureReason::NoFromClause),
    };

    // Check if this is a simple single-table query
    let (table_name, table_alias) = match extract_single_table(from) {
        Some(info) => info,
        None => {
            // Multi-table query (join) - try to detect PKs from all tables
            return detect_pk_from_join(select, from, db);
        }
    };

    let mut tables = HashSet::new();
    tables.insert(table_name.clone());

    // Get the table schema
    let table = match db.get_table(&table_name) {
        Some(t) => t,
        None => {
            return PkDetectionResult::with_reason_and_tables(
                PkDetectionFailureReason::TableNotFound,
                tables,
            );
        }
    };

    // Get PK column indices from schema
    let pk_indices = match table.schema.get_primary_key_indices() {
        Some(indices) => indices,
        None => {
            return PkDetectionResult::with_reason_and_tables(
                PkDetectionFailureReason::NoPrimaryKeyOnTable,
                tables,
            );
        }
    };

    // Get PK column names
    let pk_column_names: Vec<String> = pk_indices
        .iter()
        .filter_map(|&idx| table.schema.columns.get(idx).map(|c| c.name.to_lowercase()))
        .collect();

    if pk_column_names.is_empty() {
        return PkDetectionResult::with_reason_and_tables(
            PkDetectionFailureReason::NoPrimaryKeyOnTable,
            tables,
        );
    }

    // Map PK column names to result set positions
    let result_pk_indices = map_columns_to_result_positions(
        &select.select_list,
        &pk_column_names,
        table.schema.columns.iter().map(|c| c.name.to_string()).collect(),
        table_alias.as_deref(),
    );

    if result_pk_indices.is_empty() {
        // PKs not in result set - use default
        PkDetectionResult {
            pk_column_indices: vec![0],
            confident: false,
            tables,
            reason: Some(PkDetectionFailureReason::PkColumnsNotInResultSet),
        }
    } else {
        PkDetectionResult {
            pk_column_indices: result_pk_indices,
            confident: true,
            tables,
            reason: None,
        }
    }
}

/// Extract table name and alias from a simple single-table FROM clause
fn extract_single_table(from: &FromClause) -> Option<(String, Option<String>)> {
    match from {
        FromClause::Table { name, alias, .. } => {
            Some((name.to_lowercase(), alias.clone()))
        }
        FromClause::Join { .. } => None, // Join means multiple tables
        FromClause::Subquery { .. } => None, // Subquery is complex
    }
}

/// Detect PK columns from a JOIN query
///
/// For joins, we try to find the PK of the "primary" table (first table in FROM).
/// This is a best-effort approach - for complex queries, we fall back to defaults.
fn detect_pk_from_join(
    select: &SelectStmt,
    from: &FromClause,
    db: &Database,
) -> PkDetectionResult {
    // Collect all tables and their aliases
    let (tables_info, has_subquery) = collect_join_tables(from);

    if tables_info.is_empty() {
        let reason = if has_subquery {
            PkDetectionFailureReason::SubqueryInFrom
        } else {
            PkDetectionFailureReason::MultipleTablesInQuery
        };
        return PkDetectionResult::with_reason(reason);
    }

    let mut tables = HashSet::new();
    for (table_name, _) in &tables_info {
        tables.insert(table_name.clone());
    }

    // For now, try to use the first table's PK
    // More sophisticated logic could be added later
    let (first_table, first_alias) = &tables_info[0];

    let table = match db.get_table(first_table) {
        Some(t) => t,
        None => {
            return PkDetectionResult {
                pk_column_indices: vec![0],
                confident: false,
                tables,
                reason: Some(PkDetectionFailureReason::TableNotFound),
            };
        }
    };

    let pk_indices = match table.schema.get_primary_key_indices() {
        Some(indices) => indices,
        None => {
            return PkDetectionResult {
                pk_column_indices: vec![0],
                confident: false,
                tables,
                reason: Some(PkDetectionFailureReason::NoPrimaryKeyOnTable),
            };
        }
    };

    let pk_column_names: Vec<String> = pk_indices
        .iter()
        .filter_map(|&idx| table.schema.columns.get(idx).map(|c| c.name.to_lowercase()))
        .collect();

    if pk_column_names.is_empty() {
        return PkDetectionResult {
            pk_column_indices: vec![0],
            confident: false,
            tables,
            reason: Some(PkDetectionFailureReason::NoPrimaryKeyOnTable),
        };
    }

    // Try to map with table alias for qualified references
    let result_pk_indices = map_columns_to_result_positions(
        &select.select_list,
        &pk_column_names,
        table.schema.columns.iter().map(|c| c.name.to_string()).collect(),
        first_alias.as_deref(),
    );

    if result_pk_indices.is_empty() {
        PkDetectionResult {
            pk_column_indices: vec![0],
            confident: false,
            tables,
            reason: Some(PkDetectionFailureReason::PkColumnsNotInResultSet),
        }
    } else {
        // Not fully confident for joins since we only handle first table
        PkDetectionResult {
            pk_column_indices: result_pk_indices,
            confident: false,
            tables,
            reason: Some(PkDetectionFailureReason::MultipleTablesInQuery),
        }
    }
}

/// Collect all tables from a JOIN clause
/// Returns (tables, has_subquery) tuple
fn collect_join_tables(from: &FromClause) -> (Vec<(String, Option<String>)>, bool) {
    let mut tables = Vec::new();
    let mut has_subquery = false;
    collect_join_tables_recursive(from, &mut tables, &mut has_subquery);
    (tables, has_subquery)
}

fn collect_join_tables_recursive(
    from: &FromClause,
    tables: &mut Vec<(String, Option<String>)>,
    has_subquery: &mut bool,
) {
    match from {
        FromClause::Table { name, alias, .. } => {
            tables.push((name.to_lowercase(), alias.clone()));
        }
        FromClause::Join { left, right, .. } => {
            collect_join_tables_recursive(left, tables, has_subquery);
            collect_join_tables_recursive(right, tables, has_subquery);
        }
        FromClause::Subquery { .. } => {
            // Can't easily extract table info from subqueries
            *has_subquery = true;
        }
    }
}

/// Map column names to their positions in a SELECT list
///
/// Handles:
/// - SELECT * (expands to all columns)
/// - SELECT col1, col2 (specific columns)
/// - SELECT t.col1 (qualified column references)
/// - SELECT col1 AS alias (aliases - we match on original name)
fn map_columns_to_result_positions(
    select_list: &[SelectItem],
    pk_column_names: &[String],
    all_table_columns: Vec<String>,
    table_alias: Option<&str>,
) -> Vec<usize> {
    let mut result_indices = Vec::new();
    let mut current_pos = 0;

    for item in select_list {
        match item {
            SelectItem::Wildcard { alias: _ } => {
                // SELECT * - all columns from all tables in order
                for (idx, col_name) in all_table_columns.iter().enumerate() {
                    if pk_column_names.contains(&col_name.to_lowercase()) {
                        result_indices.push(current_pos + idx);
                    }
                }
                current_pos += all_table_columns.len();
            }
            SelectItem::QualifiedWildcard { qualifier, alias: _ } => {
                // SELECT t.* - all columns from specific table
                // Only expand if qualifier matches our table alias
                let qualifier_matches = table_alias
                    .map(|alias| alias.eq_ignore_ascii_case(qualifier))
                    .unwrap_or(false);

                if qualifier_matches {
                    for (idx, col_name) in all_table_columns.iter().enumerate() {
                        if pk_column_names.contains(&col_name.to_lowercase()) {
                            result_indices.push(current_pos + idx);
                        }
                    }
                    current_pos += all_table_columns.len();
                } else {
                    // Unknown qualifier - skip (can't determine column count)
                    // This could happen with joins - we'd need to know all table schemas
                    current_pos += 1; // Assume at least one column
                }
            }
            SelectItem::Expression { expr, alias: _ } => {
                // Check if this expression is a column reference
                if let Some(col_name) = extract_column_name(expr, table_alias) {
                    if pk_column_names.contains(&col_name.to_lowercase()) {
                        result_indices.push(current_pos);
                    }
                }
                current_pos += 1;
            }
        }
    }

    result_indices
}

/// Extract column name from an expression (if it's a simple column reference)
fn extract_column_name(expr: &Expression, table_alias: Option<&str>) -> Option<String> {
    match expr {
        Expression::ColumnRef { table, column } => {
            // For qualified references (t.col), check if table matches our alias
            if let Some(tbl) = table {
                if let Some(alias) = table_alias {
                    if !tbl.eq_ignore_ascii_case(alias) {
                        return None; // Different table
                    }
                }
            }
            Some(column.clone())
        }
        _ => None, // Not a simple column reference
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    fn create_test_db() -> Database {
        let mut db = Database::new();

        // Create users table with id as PK
        let user_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
            ColumnSchema::new("email".to_string(), DataType::Varchar { max_length: Some(255) }, true),
        ];
        let user_schema = TableSchema::with_primary_key(
            "users".to_string(),
            user_columns,
            vec!["id".to_string()],
        );
        db.create_table(user_schema).unwrap();

        // Create orders table with composite PK (order_id, user_id)
        let order_columns = vec![
            ColumnSchema::new("order_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("user_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("amount".to_string(), DataType::Integer, true),
            ColumnSchema::new("status".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ];
        let order_schema = TableSchema::with_primary_key(
            "orders".to_string(),
            order_columns,
            vec!["order_id".to_string(), "user_id".to_string()],
        );
        db.create_table(order_schema).unwrap();

        db
    }

    #[test]
    fn test_simple_select_star() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT * FROM users", &db);

        assert!(result.confident);
        assert_eq!(result.pk_column_indices, vec![0]); // id is first column
        assert!(result.tables.contains("users"));
    }

    #[test]
    fn test_select_specific_columns_with_pk() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT name, id, email FROM users", &db);

        assert!(result.confident);
        assert_eq!(result.pk_column_indices, vec![1]); // id is second in select list
    }

    #[test]
    fn test_select_without_pk() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT name, email FROM users", &db);

        // PK not in result set - should fall back to column 0
        assert!(!result.confident);
        assert_eq!(result.pk_column_indices, vec![0]);
    }

    #[test]
    fn test_composite_pk() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT * FROM orders", &db);

        assert!(result.confident);
        // order_id (0) and user_id (1) are composite PK
        assert_eq!(result.pk_column_indices, vec![0, 1]);
    }

    #[test]
    fn test_composite_pk_partial_select() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT order_id, amount, status FROM orders", &db);

        // Only order_id is in select list, user_id is missing
        assert!(result.confident);
        assert_eq!(result.pk_column_indices, vec![0]); // only order_id at position 0
    }

    #[test]
    fn test_nonexistent_table() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT * FROM nonexistent", &db);

        assert!(!result.confident);
        assert_eq!(result.pk_column_indices, vec![0]); // default
    }

    #[test]
    fn test_join_query() {
        let db = create_test_db();
        let result = detect_pk_columns(
            "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        );

        // Join queries are not fully confident, but should try to detect first table's PK
        assert!(!result.confident);
        assert!(result.tables.contains("users"));
        assert!(result.tables.contains("orders"));
    }

    #[test]
    fn test_aliased_table() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT u.id, u.name FROM users u", &db);

        assert!(result.confident);
        assert_eq!(result.pk_column_indices, vec![0]); // u.id is first
    }

    #[test]
    fn test_invalid_sql() {
        let db = create_test_db();
        let result = detect_pk_columns("INVALID SQL", &db);

        assert!(!result.confident);
        assert_eq!(result.pk_column_indices, vec![0]); // default
    }

    #[test]
    fn test_case_insensitive_table_name() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT * FROM USERS", &db);

        assert!(result.confident);
        assert_eq!(result.pk_column_indices, vec![0]);
    }

    #[test]
    fn test_select_with_alias() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT id AS user_id, name FROM users", &db);

        assert!(result.confident);
        assert_eq!(result.pk_column_indices, vec![0]); // id (aliased as user_id) is first
    }

    // Tests for PkDetectionFailureReason
    #[test]
    fn test_reason_parse_error() {
        let db = create_test_db();
        let result = detect_pk_columns("INVALID SQL", &db);

        assert!(!result.confident);
        assert_eq!(result.reason, Some(PkDetectionFailureReason::ParseError));
    }

    #[test]
    fn test_reason_table_not_found() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT * FROM nonexistent_table", &db);

        assert!(!result.confident);
        assert_eq!(result.reason, Some(PkDetectionFailureReason::TableNotFound));
    }

    #[test]
    fn test_reason_pk_columns_not_in_result_set() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT name, email FROM users", &db);

        assert!(!result.confident);
        assert_eq!(result.reason, Some(PkDetectionFailureReason::PkColumnsNotInResultSet));
    }

    #[test]
    fn test_reason_multiple_tables_in_query() {
        let db = create_test_db();
        let result = detect_pk_columns(
            "SELECT u.id, o.order_id FROM users u JOIN orders o ON u.id = o.user_id",
            &db,
        );

        assert!(!result.confident);
        assert_eq!(result.reason, Some(PkDetectionFailureReason::MultipleTablesInQuery));
    }

    #[test]
    fn test_reason_no_from_clause() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT 1 + 1", &db);

        assert!(!result.confident);
        assert_eq!(result.reason, Some(PkDetectionFailureReason::NoFromClause));
    }

    #[test]
    fn test_reason_set_operation() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT id FROM users UNION SELECT order_id FROM orders", &db);

        assert!(!result.confident);
        assert_eq!(result.reason, Some(PkDetectionFailureReason::SetOperation));
    }

    #[test]
    fn test_confident_query_has_no_reason() {
        let db = create_test_db();
        let result = detect_pk_columns("SELECT * FROM users", &db);

        assert!(result.confident);
        assert_eq!(result.reason, None);
    }

    #[test]
    fn test_failure_reason_display() {
        // Test Display implementation
        assert_eq!(
            PkDetectionFailureReason::ParseError.to_string(),
            "query parse error"
        );
        assert_eq!(
            PkDetectionFailureReason::TableNotFound.to_string(),
            "table not found in database"
        );
        assert_eq!(
            PkDetectionFailureReason::MultipleTablesInQuery.to_string(),
            "query involves multiple tables (join)"
        );
    }
}
