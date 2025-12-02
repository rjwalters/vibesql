//! Arena-allocated prepared statements for zero-copy query execution.
//!
//! This module provides `ArenaPreparedStatement`, a self-referential struct that
//! keeps the bump arena alive alongside the arena-allocated AST. This enables
//! prepared statement execution without any heap allocations for simple queries.
//!
//! # Performance Benefits
//!
//! - **Zero allocation**: No heap allocations during parameter binding
//! - **Cache locality**: Contiguous memory layout in bump arena
//! - **Arena reuse**: Single deallocation when statement is dropped
//!
//! # Usage
//!
//! ```ignore
//! use vibesql_executor::cache::ArenaPreparedStatement;
//!
//! // Create arena-based prepared statement
//! let stmt = ArenaPreparedStatement::try_new("SELECT * FROM users WHERE id = ?")?;
//!
//! // Access the parsed statement
//! stmt.with_statement(|select_stmt| {
//!     // Use select_stmt directly - no conversion needed
//! });
//! ```

use std::collections::HashSet;

use bumpalo::Bump;
use ouroboros::self_referencing;
use vibesql_ast::arena::{FromClause as ArenaFromClause, SelectStmt as ArenaSelectStmt};
use vibesql_parser::arena_parser::ArenaParser;

use crate::cache::QuerySignature;

/// Arena-allocated prepared statement with self-referential lifetime.
///
/// This struct owns the bump arena and contains a reference to the parsed
/// `SelectStmt` that lives within that arena. The `ouroboros` crate handles
/// the lifetime management safely.
#[self_referencing]
pub struct ArenaPreparedStatement {
    /// Original SQL with `?` placeholders
    sql: String,

    /// Number of parameters expected
    param_count: usize,

    /// Tables referenced by this statement (for invalidation)
    tables: HashSet<String>,

    /// Query signature for cache lookup (ignores literal values)
    signature: QuerySignature,

    /// The bump arena that owns all allocations
    arena: Bump,

    /// Parsed SelectStmt allocated in the arena
    #[borrows(arena)]
    #[covariant]
    statement: &'this ArenaSelectStmt<'this>,
}

impl ArenaPreparedStatement {
    /// Create a new arena-allocated prepared statement from SQL.
    ///
    /// Returns an error if the SQL is not a valid SELECT statement.
    /// For non-SELECT statements, use the standard `PreparedStatement` instead.
    pub fn create(sql: &str) -> Result<Self, ArenaParseError> {
        // First parse to compute metadata (tables, signature, param_count)
        // We do this in a temporary arena since we can't access the main arena's
        // parsed statement during construction
        let temp_arena = Bump::new();
        let temp_stmt = ArenaParser::parse_sql(sql, &temp_arena)
            .map_err(|e| ArenaParseError::ParseError(e.to_string()))?;

        let tables = extract_tables_from_arena_select(temp_stmt);
        let signature = QuerySignature::from_arena_select(temp_stmt);
        let param_count = count_placeholders_in_sql(sql);

        // Now build the self-referential struct with the real arena
        ArenaPreparedStatementTryBuilder {
            sql: sql.to_string(),
            param_count,
            tables,
            signature,
            arena: Bump::new(),
            statement_builder: |arena: &Bump| {
                ArenaParser::parse_sql(sql, arena)
                    .map_err(|e| ArenaParseError::ParseError(e.to_string()))
            },
        }
        .try_build()
    }

    /// Get the original SQL string.
    pub fn sql(&self) -> &str {
        self.borrow_sql()
    }

    /// Get the number of parameters expected.
    pub fn param_count(&self) -> usize {
        *self.borrow_param_count()
    }

    /// Get the tables referenced by this statement.
    pub fn tables(&self) -> &HashSet<String> {
        self.borrow_tables()
    }

    /// Get the query signature.
    pub fn signature(&self) -> &QuerySignature {
        self.borrow_signature()
    }
}

/// Errors that can occur during arena-based prepared statement operations.
#[derive(Debug, Clone)]
pub enum ArenaParseError {
    /// Failed to parse SQL
    ParseError(String),
    /// Statement type not supported for arena path
    UnsupportedStatement(String),
}

impl std::fmt::Display for ArenaParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArenaParseError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            ArenaParseError::UnsupportedStatement(msg) => {
                write!(f, "Unsupported statement for arena path: {}", msg)
            }
        }
    }
}

impl std::error::Error for ArenaParseError {}

/// Count the number of `?` placeholder parameters in SQL.
fn count_placeholders_in_sql(sql: &str) -> usize {
    // Simple count - doesn't handle ? inside string literals, but for
    // prepared statements we expect ? to only appear as placeholders
    sql.chars().filter(|&c| c == '?').count()
}

/// Extract table names from an arena-allocated SelectStmt.
fn extract_tables_from_arena_select(select: &ArenaSelectStmt<'_>) -> HashSet<String> {
    let mut tables = HashSet::new();

    if let Some(from) = &select.from {
        collect_tables_from_clause(from, &mut tables);
    }

    tables
}

/// Collect table names from FROM clause.
fn collect_tables_from_clause(from: &ArenaFromClause<'_>, tables: &mut HashSet<String>) {
    match from {
        ArenaFromClause::Table { name, .. } => {
            // Normalize to lowercase for case-insensitive matching
            tables.insert(name.to_lowercase());
        }
        ArenaFromClause::Join { left, right, .. } => {
            collect_tables_from_clause(left, tables);
            collect_tables_from_clause(right, tables);
        }
        ArenaFromClause::Subquery { query, .. } => {
            tables.extend(extract_tables_from_arena_select(query));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_prepared_statement_basic() {
        let stmt = ArenaPreparedStatement::create("SELECT * FROM users WHERE id = 1")
            .expect("should parse simple SELECT");

        assert_eq!(stmt.param_count(), 0);
        assert!(stmt.sql().contains("SELECT"));
        assert!(stmt.tables().contains("users"));
    }

    #[test]
    fn test_arena_prepared_statement_with_placeholder() {
        let stmt = ArenaPreparedStatement::create("SELECT * FROM users WHERE id = ?")
            .expect("should parse SELECT with placeholder");

        assert_eq!(stmt.param_count(), 1);
    }

    #[test]
    fn test_arena_prepared_statement_multiple_placeholders() {
        let stmt = ArenaPreparedStatement::create(
            "SELECT * FROM users WHERE id = ? AND name = ?",
        )
        .expect("should parse SELECT with multiple placeholders");

        assert_eq!(stmt.param_count(), 2);
    }

    #[test]
    fn test_arena_prepared_statement_parse_error() {
        let result = ArenaPreparedStatement::create("INVALID SQL SYNTAX HERE");
        assert!(result.is_err());
    }

    #[test]
    fn test_arena_prepared_statement_join() {
        let stmt = ArenaPreparedStatement::create(
            "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = ?",
        )
        .expect("should parse SELECT with JOIN");

        assert_eq!(stmt.param_count(), 1);
        assert!(stmt.tables().contains("users"));
        assert!(stmt.tables().contains("orders"));
    }

    #[test]
    fn test_arena_prepared_statement_signature() {
        // Two queries with same structure but different literals should have same signature
        let stmt1 = ArenaPreparedStatement::create("SELECT * FROM users WHERE id = 1")
            .expect("should parse");
        let stmt2 = ArenaPreparedStatement::create("SELECT * FROM users WHERE id = 2")
            .expect("should parse");

        assert_eq!(stmt1.signature(), stmt2.signature());
    }
}
