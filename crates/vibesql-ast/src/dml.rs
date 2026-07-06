//! Data Manipulation Language (DML) statements
//!
//! This module contains INSERT, UPDATE, and DELETE statement types.

use crate::{CommonTableExpr, Expression, FromClause, OrderByItem, SelectItem, SelectStmt};

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

/// One entry of an `ON CONFLICT (...)` conflict target.
///
/// SQLite allows each entry to be an arbitrary indexed expression: a plain
/// column name matches simple-column indexes/constraints, while an
/// expression entry (`ON CONFLICT(a+b)`) matches expression indexes with a
/// structurally identical component (upsert1-200).
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictTargetItem {
    /// Plain column name with the default BINARY collation.
    Column(String),
    /// Expression entry (e.g. `ON CONFLICT(a+b)`); matched structurally
    /// against expression-index components.
    Expression(Expression),
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
    /// Optional list of indexed columns/expressions to match for conflict
    /// detection. When None, matches any unique constraint violation.
    pub conflict_target: Option<Vec<ConflictTargetItem>>,
    /// Optional target-level WHERE predicate (`ON CONFLICT(b) WHERE b>10`),
    /// matched structurally against a partial unique index's predicate
    /// (upsert1-320).
    pub target_where: Option<Expression>,
    /// True when the conflict target contains components that the AST cannot
    /// represent exactly: currently an explicit non-BINARY COLLATE
    /// (`ON CONFLICT(b COLLATE nocase)`).
    ///
    /// The executor conservatively reports inexact targets with SQLite's
    /// canonical "ON CONFLICT clause does not match any PRIMARY KEY or
    /// UNIQUE constraint" error (upsert1-130; issue #5269).
    pub target_inexact: bool,
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
    /// ON CONFLICT clauses (SQLite upsert). Empty = no upsert.
    ///
    /// SQLite's generalized UPSERT allows multiple ON CONFLICT clauses
    /// (upsert5.test): the leftmost clause whose conflict target matches the
    /// actual conflict fires; a target-less clause matches any conflict and
    /// must be the last clause (enforced by the parser).
    pub on_conflict: Vec<OnConflictClause>,
    /// ON DUPLICATE KEY UPDATE clause (MySQL-style upsert)
    pub on_duplicate_key_update: Option<Vec<Assignment>>,
    /// Optional RETURNING clause (SQLite 3.35.0+)
    /// Syntax: INSERT INTO table ... [ON CONFLICT ...] RETURNING expr [, expr ...]
    /// Returns the NEW row values (as actually inserted) for each inserted row.
    pub returning: Option<Vec<SelectItem>>,
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
    /// Optional index hint (SQLite extension: UPDATE t1 INDEXED BY idx ... / NOT INDEXED).
    /// Advisory only; VibeSQL's planner chooses indexes independently, so the hint is
    /// parsed for SQLite compatibility but has no effect on plan selection.
    pub index_hint: Option<crate::IndexHint>,
    pub assignments: Vec<Assignment>,
    /// Optional FROM clause for multi-table UPDATE (SQLite 3.33.0+ / SQL:1999)
    /// Syntax: UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id
    /// See: <https://www.sqlite.org/lang_update.html#update_from>
    pub from_clause: Option<Vec<FromClause>>,
    pub where_clause: Option<WhereClause>,
    /// Optional ORDER BY clause (SQLite extension for UPDATE)
    /// When present with LIMIT, updates rows in the specified order.
    /// Requires SQLite to be compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT;
    /// VibeSQL supports it unconditionally to match the conformance suite.
    pub order_by: Option<Vec<OrderByItem>>,
    /// Optional LIMIT clause (SQLite extension for UPDATE)
    /// Limits the number of rows updated. May appear with or without ORDER BY.
    pub limit: Option<Expression>,
    /// Optional OFFSET clause (SQLite extension for UPDATE)
    /// When used with LIMIT, skips rows before updating.
    pub offset: Option<Expression>,
    /// Optional conflict resolution clause (SQLite extension)
    /// Syntax: UPDATE OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL table SET ...
    pub conflict_clause: Option<ConflictClause>,
    /// Optional RETURNING clause (SQLite 3.35.0+)
    /// Syntax: UPDATE table SET ... [WHERE ...] RETURNING expr [, expr ...]
    /// Returns the NEW row values (after assignments) for each updated row.
    pub returning: Option<Vec<SelectItem>>,
}

/// Column assignment (`column = value`, or the tuple form
/// `(col-a, col-b, ...) = (row-value | scalar-subquery)`).
///
/// SQLite allows the left-hand side of an `UPDATE`/`DO UPDATE` assignment to be
/// a parenthesized column list, e.g.
/// `SET (b, c) = (SELECT 'x', 'y')` or `SET (c, a) = ('four', 4)`.
/// For an ordinary single-column assignment `columns` is empty and `column`
/// is authoritative. For a tuple assignment `columns` holds the full ordered
/// column list (length >= 2), `column` mirrors `columns[0]` so naive
/// single-column readers still see a real target, and the single `value` is a
/// row-valued expression (a `RowValueConstructor` or a multi-column
/// `ScalarSubquery`) whose elements map positionally to `columns`.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    /// Tuple-assignment column list; empty for a single-column assignment.
    pub columns: Vec<String>,
    pub value: Expression,
}

impl Assignment {
    /// Construct an ordinary single-column assignment (`column = value`).
    pub fn single(column: String, value: Expression) -> Self {
        Assignment { column, columns: Vec::new(), value }
    }

    /// Construct a tuple assignment (`(cols) = value`). `columns` must contain
    /// at least two names; `column` mirrors the first for single-column readers.
    pub fn tuple(columns: Vec<String>, value: Expression) -> Self {
        let column = columns.first().cloned().unwrap_or_default();
        Assignment { column, columns, value }
    }

    /// True when this is a tuple/row assignment (`(a, b) = ...`).
    pub fn is_tuple(&self) -> bool {
        self.columns.len() >= 2
    }
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
    /// Optional table alias (SQLite 3.24+ extension: DELETE FROM t1 AS xyz WHERE ...)
    pub alias: Option<String>,
    /// Optional index hint (SQLite extension: DELETE FROM t1 INDEXED BY idx ... / NOT INDEXED).
    /// Advisory only; VibeSQL's planner chooses indexes independently, so the hint is
    /// parsed for SQLite compatibility but has no effect on plan selection.
    pub index_hint: Option<crate::IndexHint>,
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
    /// Optional RETURNING clause (SQLite 3.35.0+)
    /// Syntax: DELETE FROM table [WHERE ...] RETURNING expr [, expr ...]
    /// Returns the OLD row values (before deletion) for each deleted row.
    pub returning: Option<Vec<SelectItem>>,
}
