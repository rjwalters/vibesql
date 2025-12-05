//! CREATE INDEX, DROP INDEX, REINDEX, and ANALYZE statement execution
//!
//! This module provides executors for index DDL and statistics operations.
//!
//! # Structure
//!
//! - `create_index.rs` - CREATE INDEX executor
//! - `drop_index.rs` - DROP INDEX executor
//! - `reindex.rs` - REINDEX executor
//! - `analyze.rs` - ANALYZE executor

pub mod analyze;
pub mod create_index;
pub mod drop_index;
pub mod reindex;

pub use analyze::AnalyzeExecutor;
pub use create_index::CreateIndexExecutor;
pub use drop_index::DropIndexExecutor;
pub use reindex::ReindexExecutor;
use vibesql_ast::{CreateIndexStmt, DropIndexStmt, ReindexStmt};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Unified executor for index operations (CREATE, DROP, and REINDEX INDEX)
///
/// This struct provides backward compatibility with the original API,
/// delegating to specialized executors for each operation.
pub struct IndexExecutor;

impl IndexExecutor {
    /// Execute a CREATE INDEX statement (delegates to CreateIndexExecutor)
    pub fn execute(
        stmt: &CreateIndexStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        CreateIndexExecutor::execute(stmt, database)
    }

    /// Execute a DROP INDEX statement (delegates to DropIndexExecutor)
    pub fn execute_drop(
        stmt: &DropIndexStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        DropIndexExecutor::execute(stmt, database)
    }

    /// Execute a REINDEX statement (delegates to ReindexExecutor)
    pub fn execute_reindex(
        stmt: &ReindexStmt,
        database: &Database,
    ) -> Result<String, ExecutorError> {
        ReindexExecutor::execute(stmt, database)
    }
}
