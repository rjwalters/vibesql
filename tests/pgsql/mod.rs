//! PostgreSQL-inspired regression test suite for VibeSQL.
//!
//! This module provides a test framework inspired by PostgreSQL's regression test suite,
//! adapted for VibeSQL's SQLite-compatible SQL dialect. Tests are organized by category
//! and run against VibeSQL to verify SQL conformance.
//!
//! ## Test Format
//!
//! Test files are stored in `tests/pgsql/sql/` with a `.sql` extension. Each file
//! contains SQL statements with embedded expected results in comments:
//!
//! ```sql
//! -- Create test table
//! CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT);
//!
//! -- Insert data
//! INSERT INTO test_table VALUES (1, 'hello');
//!
//! -- Verify: expect 1 row
//! -- EXPECT: 1|hello
//! SELECT * FROM test_table;
//! ```
//!
//! ## Categories
//!
//! Tests are organized by PostgreSQL category for familiarity:
//! - `triggers.sql` - Trigger functionality (BEFORE/AFTER/INSTEAD OF)
//! - `select.sql` - Core SELECT operations
//! - `insert.sql` - INSERT operations
//! - `update.sql` - UPDATE operations
//! - `delete.sql` - DELETE operations
//! - `join.sql` - All join types
//! - `constraints.sql` - CHECK, UNIQUE, FK constraints

pub mod runner;
pub mod stats;
