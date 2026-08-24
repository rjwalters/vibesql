//! Join table limit validation
//!
//! SQLite enforces a limit of 64 tables in a single join.

use vibesql_ast::{FromClause, SelectStmt};

use crate::{errors::ExecutorError, limits::MAX_TABLES_IN_JOIN};

/// Count the number of tables in a FROM clause
///
/// This recursively counts all table references in the FROM clause,
/// including tables in JOIN operations and subqueries.
///
/// SQLite enforces a limit of 64 tables in a single join.
fn count_tables_in_from_clause(from: &FromClause) -> usize {
    match from {
        FromClause::Table { .. } => 1,
        FromClause::Join { left, right, .. } => {
            count_tables_in_from_clause(left) + count_tables_in_from_clause(right)
        }
        FromClause::Subquery { .. } => 1, // Subquery counts as 1 table
        FromClause::Values { .. } => 1,   // VALUES clause counts as 1 table
        FromClause::TableFunction { .. } => 1, // Table function counts as 1 table
    }
}

/// Validate that the number of tables in a join doesn't exceed SQLite's limit
///
/// SQLite enforces a limit of 64 tables in a single join operation.
/// This validation catches queries like `SELECT * FROM t, t, t, ... (65+ times)`
/// before execution begins.
///
/// Returns an error if the table count exceeds MAX_TABLES_IN_JOIN (64).
pub fn validate_join_table_limit(stmt: &SelectStmt) -> Result<(), ExecutorError> {
    if let Some(from_clause) = &stmt.from {
        let table_count = count_tables_in_from_clause(from_clause);
        if table_count > MAX_TABLES_IN_JOIN {
            return Err(ExecutorError::JoinTableLimitExceeded {
                table_count,
                max_tables: MAX_TABLES_IN_JOIN,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{Expression, SelectItem};
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_join_table_limit_under_limit() {
        // Create a SELECT with 64 tables (exactly at limit) - should pass
        // SELECT * FROM t1, t2, t3, ... (64 times)
        let mut from: FromClause = FromClause::Table {
            index_hint: None,
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        };
        for i in 2..=64 {
            from = FromClause::Join {
                left: Box::new(from),
                right: Box::new(FromClause::Table {
                    index_hint: None,
                    name: format!("t{}", i),
                    alias: None,
                    column_aliases: None,
                    quoted: false,
                }),
                join_type: vibesql_ast::JoinType::Cross,
                condition: None,
                using_columns: None,
                natural: false,
                alias: None,
            };
        }
        let stmt = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(from),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        // Should pass - exactly 64 tables
        assert!(validate_join_table_limit(&stmt).is_ok());
    }

    #[test]
    fn test_join_table_limit_exceeds_limit() {
        // Create a SELECT with 65 tables (exceeds limit) - should fail
        let mut from: FromClause = FromClause::Table {
            index_hint: None,
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        };
        for i in 2..=65 {
            from = FromClause::Join {
                left: Box::new(from),
                right: Box::new(FromClause::Table {
                    index_hint: None,
                    name: format!("t{}", i),
                    alias: None,
                    column_aliases: None,
                    quoted: false,
                }),
                join_type: vibesql_ast::JoinType::Cross,
                condition: None,
                using_columns: None,
                natural: false,
                alias: None,
            };
        }
        let stmt = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(from),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        // Should fail - 65 tables exceeds limit
        let result = validate_join_table_limit(&stmt);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::JoinTableLimitExceeded { table_count, max_tables }) => {
                assert_eq!(table_count, 65);
                assert_eq!(max_tables, 64);
            }
            _ => panic!("Expected JoinTableLimitExceeded error"),
        }
    }

    #[test]
    fn test_join_table_limit_no_from_clause() {
        // SELECT 1 (no FROM clause) - should pass
        let stmt = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(1)),
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        assert!(validate_join_table_limit(&stmt).is_ok());
    }
}
