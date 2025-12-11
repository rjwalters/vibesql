//! vibesql - SQL database engine with SQL:1999 compliance
//!
//! This is the root crate that re-exports all components.
//!
//! # Overview
//!
//! vibesql is an in-memory SQL database engine with a focus on SQL:1999 compliance.
//! It provides a complete SQL implementation including:
//!
//! - Full SQL query support (SELECT, INSERT, UPDATE, DELETE)
//! - Complex joins and subqueries
//! - Window functions and CTEs
//! - Aggregate functions
//! - Transaction support
//! - Type system with NULL handling
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use vibesql::{executor::SelectExecutor, parser::Parser, storage::Database};
//!
//! let mut db = Database::new();
//!
//! // Parse and execute SQL
//! let stmt = Parser::parse_sql("SELECT 1 + 1;").unwrap();
//! ```
//!
//! # Modules
//!
//! - [`types`] - SQL type system
//! - [`ast`] - Abstract Syntax Tree definitions
//! - [`parser`] - SQL parser
//! - [`storage`] - In-memory storage engine
//! - [`catalog`] - Schema and metadata management
//! - [`executor`] - Query execution engine

pub use vibesql_ast as ast;
pub use vibesql_catalog as catalog;
pub use vibesql_executor as executor;
pub use vibesql_parser as parser;
pub use vibesql_storage as storage;
pub use vibesql_types as types;
