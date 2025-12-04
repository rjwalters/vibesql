//! Data Definition Language (DDL) AST nodes
//!
//! This module contains AST nodes for all DDL statements organized by category:
//!
//! - **table**: Table operations (CREATE/DROP/ALTER TABLE)
//! - **transaction**: Transaction control (BEGIN/COMMIT/ROLLBACK/SAVEPOINT)
//! - **schema**: Schema, view, index, role, and catalog operations
//! - **cursor**: Cursor operations (DECLARE/OPEN/FETCH/CLOSE)
//! - **prepared**: Prepared statement operations (PREPARE/EXECUTE/DEALLOCATE)
//! - **advanced**: Advanced SQL:1999 objects (SEQUENCE, TYPE, DOMAIN, COLLATION, etc.)
//! - **schedule**: Scheduled functions and cron jobs

// Declare modules
pub mod advanced;
pub mod cursor;
pub mod prepared;
pub mod schedule;
pub mod schema;
pub mod table;
pub mod transaction;

// Re-export all types to maintain backward compatibility
pub use advanced::*;
pub use cursor::*;
pub use prepared::*;
pub use schedule::*;
pub use schema::*;
pub use table::*;
pub use transaction::*;
