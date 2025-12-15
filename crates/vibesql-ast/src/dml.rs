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

/// Conflict resolution strategy for INSERT statements
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictClause {
    /// INSERT OR REPLACE / REPLACE INTO - delete conflicting row and insert new one
    Replace,
    /// INSERT OR IGNORE - silently ignore constraint violations (future)
    Ignore,
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
