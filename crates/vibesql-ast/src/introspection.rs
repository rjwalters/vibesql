//! Database introspection statements (SHOW, DESCRIBE)
//!
//! This module defines MySQL-style introspection statements for querying metadata.

use crate::Expression;

// ============================================================================
// SHOW Statements
// ============================================================================

/// SHOW TABLES statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTablesStmt {
    /// Optional database name (FROM database_name)
    pub database: Option<String>,
    /// Optional LIKE pattern
    pub like_pattern: Option<String>,
    /// Optional WHERE expression
    pub where_clause: Option<Expression>,
}

/// SHOW DATABASES statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowDatabasesStmt {
    /// Optional LIKE pattern
    pub like_pattern: Option<String>,
    /// Optional WHERE expression
    pub where_clause: Option<Expression>,
}

/// SHOW COLUMNS statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowColumnsStmt {
    /// Table name
    pub table_name: String,
    /// Optional database name (FROM database_name)
    pub database: Option<String>,
    /// Show full column information
    pub full: bool,
    /// Optional LIKE pattern
    pub like_pattern: Option<String>,
    /// Optional WHERE expression
    pub where_clause: Option<Expression>,
}

/// SHOW INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowIndexStmt {
    /// Table name
    pub table_name: String,
    /// Optional database name (FROM database_name)
    pub database: Option<String>,
}

/// SHOW CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowCreateTableStmt {
    /// Table name
    pub table_name: String,
}

// ============================================================================
// DESCRIBE Statement
// ============================================================================

/// DESCRIBE statement (synonym for SHOW COLUMNS)
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeStmt {
    /// Table name
    pub table_name: String,
    /// Optional column name or pattern
    pub column_pattern: Option<String>,
}

// ============================================================================
// EXPLAIN Statement
// ============================================================================

/// EXPLAIN statement output format
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ExplainFormat {
    /// Plain text output (default)
    #[default]
    Text,
    /// JSON output
    Json,
}

/// EXPLAIN statement
///
/// Shows the query execution plan for a SELECT, INSERT, UPDATE, or DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStmt {
    /// The statement to explain
    pub statement: Box<crate::Statement>,
    /// Output format
    pub format: ExplainFormat,
    /// Whether to analyze the query (run it and show actual timing)
    pub analyze: bool,
}
