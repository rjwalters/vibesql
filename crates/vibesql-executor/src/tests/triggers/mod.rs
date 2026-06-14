//! Organized test suite for TRIGGER functionality
//!
//! This module contains comprehensive tests for triggers across different feature areas:
//! - DDL operations (CREATE/DROP)
//! - Event-specific firing (INSERT/UPDATE/DELETE)
//! - Multiple trigger coordination and ordering
//! - Conditional execution (WHEN clauses)
//! - Error handling, rollback, and recursion prevention
//! - Pseudo-variables (OLD/NEW row references)

use vibesql_storage::Database;

use crate::{CreateTableExecutor, SelectExecutor};

// Re-export test modules
mod conditions;
mod coordination;
mod ddl;
mod delete;
mod error_handling;
mod insert;
mod pseudo_vars;
mod update;

// ============================================================================
// Shared Test Helpers
// ============================================================================

/// Helper to create audit log table for tracking trigger executions
pub(super) fn create_audit_table(db: &mut Database) {
    let stmt = vibesql_ast::CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "AUDIT_LOG".to_string(),
        columns: vec![vibesql_ast::ColumnDef {
            name: "event".to_string(),
            data_type: vibesql_types::DataType::Varchar { max_length: Some(255) },
            nullable: true,
            constraints: vec![],
            default_value: None,
            comment: None,
            generated_expr: None,
            is_exact_integer_type: false,
        }],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };
    CreateTableExecutor::execute(&stmt, db).expect("Failed to create audit_log table");
}

/// Helper to create users table for testing trigger operations
pub(super) fn create_users_table(db: &mut Database) {
    let stmt = vibesql_ast::CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "USERS".to_string(),
        columns: vec![
            vibesql_ast::ColumnDef {
                name: "id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            vibesql_ast::ColumnDef {
                name: "username".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };
    CreateTableExecutor::execute(&stmt, db).expect("Failed to create users table");
}

/// Helper to count rows in audit log
pub(super) fn count_audit_rows(db: &Database) -> usize {
    let select = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            index_hint: None,
            quoted: false,
            name: "AUDIT_LOG".to_string(),
            alias: None,
            column_aliases: None,
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
    };
    let executor = SelectExecutor::new(db);
    let result = executor.execute(&select).expect("Failed to select from audit_log");
    result.len()
}
