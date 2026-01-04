//! CREATE INDEX, DROP INDEX, REINDEX, and ANALYZE statement execution
//!
//! This module provides executors for index DDL and statistics operations.
//!
//! # Structure
//!
//! ## CREATE INDEX submodules
//! - `create_index.rs` - Main CREATE INDEX executor and dispatch logic
//! - `validation.rs` - Pre-creation validation (column checks, privileges, etc.)
//! - `btree_index.rs` - B-tree index creation (standard and unique)
//! - `spatial_index.rs` - Spatial/R-tree index creation
//! - `vector_index.rs` - IVFFlat and HNSW vector index creation
//! - `expression_index.rs` - Expression-based functional index creation
//!
//! ## Other DDL operations
//! - `drop_index.rs` - DROP INDEX executor
//! - `reindex.rs` - REINDEX executor
//! - `analyze.rs` - ANALYZE executor

pub mod analyze;
mod btree_index;
pub mod create_index;
pub mod drop_index;
mod expression_index;
pub mod reindex;
mod spatial_index;
mod validation;
mod vector_index;

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
