//! Arena-allocated DML (Data Manipulation Language) statement types.
//!
//! This module provides arena-based versions of INSERT, UPDATE, and DELETE statements.

use bumpalo::collections::Vec as BumpVec;

use super::{
    expression::{Expression, OrderByItem},
    interner::Symbol,
    select::{CommonTableExpr, FromClause, SelectItem, SelectStmt},
};

// ============================================================================
// INSERT Statement
// ============================================================================

/// Source of data for INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource<'arena> {
    /// INSERT ... VALUES (...)
    Values(BumpVec<'arena, BumpVec<'arena, Expression<'arena>>>),
    /// INSERT ... SELECT ...
    Select(&'arena SelectStmt<'arena>),
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
    Abort,
    /// FAIL - Abort statement but keep prior changes within the statement
    Fail,
    /// IGNORE - Skip the row causing violation, continue with next row
    Ignore,
    /// REPLACE - Delete conflicting rows, then insert/update the new row
    Replace,
    /// ROLLBACK - Abort and rollback entire transaction
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
pub enum OnConflictAction<'arena> {
    /// DO NOTHING - Skip the conflicting row
    DoNothing,
    /// DO UPDATE SET ... [WHERE ...] - Update the conflicting row
    DoUpdate {
        /// Assignments for the update (SET col = val, ...)
        assignments: BumpVec<'arena, Assignment<'arena>>,
        /// Optional WHERE clause for the update
        where_clause: Option<Expression<'arena>>,
    },
}

/// One entry of an `ON CONFLICT (...)` conflict target.
/// Arena mirror of the owned-AST `ConflictTargetItem`.
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictTargetItem<'arena> {
    /// Plain column name with the default BINARY collation.
    Column(Symbol),
    /// Expression entry (e.g. `ON CONFLICT(a+b)`).
    Expression(Expression<'arena>),
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
pub struct OnConflictClause<'arena> {
    /// Optional list of indexed columns/expressions to match for conflict
    /// detection. When None, matches any unique constraint violation.
    pub conflict_target: Option<BumpVec<'arena, ConflictTargetItem<'arena>>>,
    /// Optional target-level WHERE predicate (partial-index upsert). See the
    /// owned-AST `OnConflictClause::target_where`.
    pub target_where: Option<Expression<'arena>>,
    /// True when the conflict target contains components the AST cannot
    /// represent exactly (currently non-BINARY COLLATE). See the owned-AST
    /// `OnConflictClause::target_inexact` for details (issue #5269).
    pub target_inexact: bool,
    /// The action to take when a conflict occurs.
    pub action: OnConflictAction<'arena>,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt<'arena> {
    /// Optional WITH clause (CTEs) for INSERT statements
    /// SQLite supports: WITH cte AS (...) INSERT INTO table SELECT * FROM cte
    pub with_clause: Option<BumpVec<'arena, CommonTableExpr<'arena>>>,
    /// Optional schema name for schema-qualified table references (e.g., schema.table)
    pub schema_name: Option<Symbol>,
    /// Whether the schema name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub schema_quoted: bool,
    pub table_name: Symbol,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub table_quoted: bool,
    pub columns: BumpVec<'arena, Symbol>,
    pub source: InsertSource<'arena>,
    /// Conflict resolution strategy (None = fail on conflict)
    /// Used for INSERT OR REPLACE/IGNORE/etc. syntax
    pub conflict_clause: Option<ConflictClause>,
    /// ON CONFLICT clause (SQLite upsert)
    /// Used for INSERT ... ON CONFLICT (cols) DO NOTHING/UPDATE syntax
    pub on_conflict: Option<OnConflictClause<'arena>>,
    /// ON DUPLICATE KEY UPDATE clause (MySQL-style upsert)
    pub on_duplicate_key_update: Option<BumpVec<'arena, Assignment<'arena>>>,
    /// Optional RETURNING clause (SQLite 3.35.0+)
    /// Syntax: INSERT INTO table ... [ON CONFLICT ...] RETURNING expr [, expr ...]
    /// Returns the NEW row values (as actually inserted) for each inserted row.
    pub returning: Option<BumpVec<'arena, SelectItem<'arena>>>,
}

// ============================================================================
// UPDATE Statement
// ============================================================================

/// WHERE clause for positioned UPDATE/DELETE
#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause<'arena> {
    /// Normal WHERE condition
    Condition(Expression<'arena>),
    /// WHERE CURRENT OF cursor_name (positioned update/delete)
    CurrentOf(Symbol),
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt<'arena> {
    /// Optional WITH clause (CTEs) for UPDATE statements
    /// SQLite supports: WITH cte AS (...) UPDATE table SET ... WHERE ...
    pub with_clause: Option<BumpVec<'arena, CommonTableExpr<'arena>>>,
    pub table_name: Symbol,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub quoted: bool,
    /// Optional table alias (SQLite extension: UPDATE t1 AS xyz SET ...)
    pub alias: Option<Symbol>,
    pub assignments: BumpVec<'arena, Assignment<'arena>>,
    /// Optional FROM clause for multi-table UPDATE (SQLite 3.33.0+ / SQL:1999)
    /// Syntax: UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id
    /// See: <https://www.sqlite.org/lang_update.html#update_from>
    pub from_clause: Option<BumpVec<'arena, FromClause<'arena>>>,
    pub where_clause: Option<WhereClause<'arena>>,
    /// Optional conflict resolution clause (SQLite extension)
    /// Syntax: UPDATE OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL table SET ...
    pub conflict_clause: Option<ConflictClause>,
    /// Optional RETURNING clause (SQLite 3.35.0+)
    /// Syntax: UPDATE table SET ... [WHERE ...] RETURNING expr [, expr ...]
    /// Returns the NEW row values (after assignments) for each updated row.
    pub returning: Option<BumpVec<'arena, SelectItem<'arena>>>,
}

/// Column assignment (column = value)
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment<'arena> {
    pub column: Symbol,
    pub value: Expression<'arena>,
}

// ============================================================================
// DELETE Statement
// ============================================================================

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt<'arena> {
    /// Optional WITH clause (CTEs) for DELETE statements
    /// SQLite supports: WITH cte AS (...) DELETE FROM table WHERE ...
    pub with_clause: Option<BumpVec<'arena, CommonTableExpr<'arena>>>,
    /// If true, DELETE FROM ONLY (excludes derived tables in table inheritance)
    pub only: bool,
    pub table_name: Symbol,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub quoted: bool,
    pub where_clause: Option<WhereClause<'arena>>,
    /// Optional ORDER BY clause (SQLite extension for DELETE)
    /// When present with LIMIT, deletes rows in the specified order
    pub order_by: Option<BumpVec<'arena, OrderByItem<'arena>>>,
    /// Optional LIMIT clause (SQLite extension for DELETE)
    /// When used with ORDER BY, limits the number of rows deleted
    pub limit: Option<Expression<'arena>>,
    /// Optional OFFSET clause (SQLite extension for DELETE)
    /// When used with ORDER BY and LIMIT, skips rows before deleting
    pub offset: Option<Expression<'arena>>,
    /// Optional RETURNING clause (SQLite 3.35.0+)
    /// Syntax: DELETE FROM table [WHERE ...] RETURNING expr [, expr ...]
    /// Returns the OLD row values (before deletion) for each deleted row.
    pub returning: Option<BumpVec<'arena, SelectItem<'arena>>>,
}
