//! Data Manipulation Language (DML) statements
//!
//! This module contains INSERT, UPDATE, and DELETE statement types.

use crate::{Expression, SelectStmt};

// ============================================================================
// INSERT Statement
// ============================================================================

/// Source of data for INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    /// INSERT ... VALUES (...)
    Values(Vec<Vec<Expression>>),
    /// INSERT ... SELECT ...
    Select(Box<SelectStmt>),
}

/// Conflict resolution strategy for INSERT and UPDATE statements (SQLite extension)
///
/// SQLite supports conflict resolution clauses in both INSERT and UPDATE statements:
/// - `INSERT OR REPLACE INTO ...`
/// - `UPDATE OR REPLACE ... SET ...`
///
/// See: <https://www.sqlite.org/lang_conflict.html>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictClause {
    /// ABORT - Abort current statement, rollback changes from this statement (default)
    /// When a constraint violation occurs, the statement is aborted and all changes
    /// made by the statement are rolled back, but changes from prior statements
    /// in the same transaction are preserved.
    Abort,
    /// FAIL - Abort statement but keep prior changes within the statement
    /// When a constraint violation occurs, the statement is aborted but changes
    /// made by the statement prior to the violation are preserved.
    Fail,
    /// IGNORE - Skip the row causing violation, continue with next row
    /// When a constraint violation occurs, the row is simply skipped and processing
    /// continues with the next row.
    Ignore,
    /// REPLACE - Delete conflicting rows, then insert/update the new row
    /// When a UNIQUE or PRIMARY KEY constraint violation occurs, the conflicting
    /// row is deleted before inserting/updating the new row.
    Replace,
    /// ROLLBACK - Abort and rollback entire transaction
    /// When a constraint violation occurs, the entire transaction is rolled back
    /// and the statement returns an error.
    Rollback,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    /// Optional schema name for schema-qualified table references (e.g., schema.table)
    pub schema_name: Option<String>,
    /// Whether the schema name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub schema_quoted: bool,
    pub table_name: String,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub table_quoted: bool,
    pub columns: Vec<String>,
    pub source: InsertSource,
    /// Conflict resolution strategy (None = fail on conflict)
    pub conflict_clause: Option<ConflictClause>,
    /// ON DUPLICATE KEY UPDATE clause (MySQL-style upsert)
    pub on_duplicate_key_update: Option<Vec<Assignment>>,
}

// ============================================================================
// UPDATE Statement
// ============================================================================

/// WHERE clause for positioned UPDATE/DELETE
#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause {
    /// Normal WHERE condition
    Condition(Expression),
    /// WHERE CURRENT OF cursor_name (positioned update/delete)
    CurrentOf(String),
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table_name: String,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub quoted: bool,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<WhereClause>,
    /// Optional conflict resolution clause (SQLite extension)
    /// Syntax: UPDATE OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL table SET ...
    pub conflict_clause: Option<ConflictClause>,
}

/// Column assignment (column = value)
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

// ============================================================================
// DELETE Statement
// ============================================================================

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    /// If true, DELETE FROM ONLY (excludes derived tables in table inheritance)
    pub only: bool,
    pub table_name: String,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub quoted: bool,
    pub where_clause: Option<WhereClause>,
}
