//! Data Manipulation Language (DML) statements
//!
//! This module contains INSERT, UPDATE, and DELETE statement types.

use crate::{CommonTableExpr, Expression, FromClause, OrderByItem, SelectStmt};

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
    /// INSERT ... DEFAULT VALUES
    /// Inserts a single row using default values for all columns.
    DefaultValues,
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

// ============================================================================
// ON CONFLICT (Upsert) Clause
// ============================================================================

/// Action to take when a conflict occurs (SQLite upsert clause)
///
/// Part of the `ON CONFLICT` clause syntax:
/// ```sql
/// INSERT INTO t VALUES (...) ON CONFLICT (cols) DO NOTHING;
/// INSERT INTO t VALUES (...) ON CONFLICT (cols) DO UPDATE SET col = val WHERE ...;
/// ```
///
/// See: <https://www.sqlite.org/lang_upsert.html>
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
    /// DO NOTHING - Skip the conflicting row
    DoNothing,
    /// DO UPDATE SET ... [WHERE ...] - Update the conflicting row
    DoUpdate {
        /// Assignments for the update (SET col = val, ...)
        assignments: Vec<Assignment>,
        /// Optional WHERE clause for the update
        where_clause: Option<Expression>,
    },
}

/// ON CONFLICT clause for INSERT statements (SQLite upsert)
///
/// Syntax:
/// ```sql
/// INSERT INTO t VALUES (...) ON CONFLICT [(column_list)] DO {NOTHING | UPDATE SET ...};
/// ```
///
/// The conflict target (column list) is optional. When omitted, the conflict
/// clause applies to any unique constraint violation.
///
/// See: <https://www.sqlite.org/lang_upsert.html>
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflictClause {
    /// Optional list of indexed columns to match for conflict detection.
    /// When None, matches any unique constraint violation.
    pub conflict_target: Option<Vec<String>>,
    /// The action to take when a conflict occurs.
    pub action: OnConflictAction,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    /// Optional WITH clause (CTEs) for INSERT statements
    /// SQLite supports: WITH cte AS (...) INSERT INTO table SELECT * FROM cte
    pub with_clause: Option<Vec<CommonTableExpr>>,
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
    /// Used for INSERT OR REPLACE/IGNORE/etc. syntax
    pub conflict_clause: Option<ConflictClause>,
    /// ON CONFLICT clause (SQLite upsert)
    /// Used for INSERT ... ON CONFLICT (cols) DO NOTHING/UPDATE syntax
    pub on_conflict: Option<OnConflictClause>,
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
    /// Optional WITH clause (CTEs) for UPDATE statements
    /// SQLite supports: WITH cte AS (...) UPDATE table SET ... WHERE ...
    pub with_clause: Option<Vec<CommonTableExpr>>,
    pub table_name: String,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub quoted: bool,
    /// Optional table alias (SQLite extension: UPDATE t1 AS xyz SET ...)
    pub alias: Option<String>,
    pub assignments: Vec<Assignment>,
    /// Optional FROM clause for multi-table UPDATE (SQLite 3.33.0+ / SQL:1999)
    /// Syntax: UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id
    /// See: <https://www.sqlite.org/lang_update.html#update_from>
    pub from_clause: Option<Vec<FromClause>>,
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
    /// Optional WITH clause (CTEs) for DELETE statements
    /// SQLite supports: WITH cte AS (...) DELETE FROM table WHERE ...
    pub with_clause: Option<Vec<CommonTableExpr>>,
    /// If true, DELETE FROM ONLY (excludes derived tables in table inheritance)
    pub only: bool,
    pub table_name: String,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub quoted: bool,
    pub where_clause: Option<WhereClause>,
    /// Optional ORDER BY clause (SQLite extension for DELETE)
    /// When present with LIMIT, deletes rows in the specified order
    pub order_by: Option<Vec<OrderByItem>>,
    /// Optional LIMIT clause (SQLite extension for DELETE)
    /// When used with ORDER BY, limits the number of rows deleted
    pub limit: Option<Expression>,
    /// Optional OFFSET clause (SQLite extension for DELETE)
    /// When used with ORDER BY and LIMIT, skips rows before deleting
    pub offset: Option<Expression>,
}
