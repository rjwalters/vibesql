//! Arena-allocated DML (Data Manipulation Language) statement types.
//!
//! This module provides arena-based versions of INSERT, UPDATE, and DELETE statements.

use bumpalo::collections::Vec as BumpVec;

use super::{expression::Expression, interner::Symbol, select::SelectStmt};

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
}

/// Conflict resolution strategy for INSERT statements
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConflictClause {
    /// INSERT OR REPLACE / REPLACE INTO - delete conflicting row and insert new one
    Replace,
    /// INSERT OR IGNORE - silently ignore constraint violations
    Ignore,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt<'arena> {
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
    pub conflict_clause: Option<ConflictClause>,
    /// ON DUPLICATE KEY UPDATE clause (MySQL-style upsert)
    pub on_duplicate_key_update: Option<BumpVec<'arena, Assignment<'arena>>>,
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
    pub table_name: Symbol,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub quoted: bool,
    pub assignments: BumpVec<'arena, Assignment<'arena>>,
    pub where_clause: Option<WhereClause<'arena>>,
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
    /// If true, DELETE FROM ONLY (excludes derived tables in table inheritance)
    pub only: bool,
    pub table_name: Symbol,
    /// Whether the table name was quoted (delimited) in the original SQL.
    /// Per SQL:1999, quoted identifiers are case-sensitive.
    pub quoted: bool,
    pub where_clause: Option<WhereClause<'arena>>,
}
