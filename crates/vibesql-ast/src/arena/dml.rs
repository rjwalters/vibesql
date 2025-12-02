//! Arena-allocated DML (Data Manipulation Language) statement types.
//!
//! This module provides arena-based versions of INSERT, UPDATE, and DELETE statements.

use bumpalo::collections::Vec as BumpVec;

use super::expression::Expression;
use super::interner::Symbol;
use super::select::SelectStmt;

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
    pub table_name: Symbol,
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
    pub where_clause: Option<WhereClause<'arena>>,
}
