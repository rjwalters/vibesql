//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

pub mod advanced_objects;
mod alter;
pub mod arena;
pub mod cache;
mod constraint_validator;
pub mod correlation;
mod create_table;
pub mod cursor;
pub mod debug_output;
mod delete;
pub mod dml_cost;
mod domain_ddl;
mod drop_table;
pub mod errors;
pub mod evaluator;
mod explain;
mod grant;
pub mod index_ddl;
pub mod information_schema;
mod insert;
mod introspection;
pub mod limits;
pub mod memory;
mod optimizer;
pub mod persistence;
pub mod pipeline;
mod privilege_checker;
pub mod procedural;
pub mod profiling;
mod revoke;
mod role_ddl;
pub mod schema;
mod schema_ddl;
pub mod select;
mod select_into;
pub mod session;
pub mod timeout;
mod transaction;
mod trigger_ddl;
mod trigger_execution;
pub mod truncate;
mod truncate_table;
mod truncate_validation;
mod type_ddl;
mod update;
mod view_ddl;

// SIMD-accelerated operations for columnar execution
#[cfg(feature = "simd")]
pub mod simd;

pub use alter::AlterTableExecutor;
pub use cache::{
    CacheManager, CacheStats, CachedQueryContext, PreparedStatement, PreparedStatementCache,
    PreparedStatementCacheStats, PreparedStatementError, QueryPlanCache, QuerySignature,
};
pub use constraint_validator::ConstraintValidator;
pub use create_table::CreateTableExecutor;
pub use cursor::{Cursor, CursorExecutor, CursorResult, CursorStore, FetchResult};
pub use delete::DeleteExecutor;
pub use dml_cost::DmlOptimizer;
pub use domain_ddl::DomainExecutor;
pub use drop_table::DropTableExecutor;
pub use errors::ExecutorError;
pub use evaluator::clear_in_subquery_cache;
pub use evaluator::ExpressionEvaluator;
pub use explain::{ExplainExecutor, ExplainResult, PlanNode};
pub use grant::GrantExecutor;
pub use index_ddl::{
    AnalyzeExecutor, CreateIndexExecutor, DropIndexExecutor, IndexExecutor, ReindexExecutor,
};
pub use insert::InsertExecutor;
pub use introspection::IntrospectionExecutor;
pub use memory::QueryArena;
pub use persistence::load_sql_dump;
pub use pipeline::{
    ColumnarPipeline, ExecutionContext, ExecutionPipeline, NativeColumnarPipeline, PipelineInput,
    PipelineOutput, RowOrientedPipeline,
};
pub use privilege_checker::PrivilegeChecker;
pub use revoke::RevokeExecutor;
pub use role_ddl::RoleExecutor;
pub use schema_ddl::SchemaExecutor;
pub use select::{SelectExecutor, SelectResult};
pub use select_into::SelectIntoExecutor;
pub use session::{PreparedExecutionResult, Session, SessionError, SessionMut};
pub use timeout::TimeoutContext;
pub use transaction::{
    BeginTransactionExecutor, CommitExecutor, ReleaseSavepointExecutor, RollbackExecutor,
    RollbackToSavepointExecutor, SavepointExecutor,
};
pub use trigger_ddl::TriggerExecutor;
pub use trigger_execution::TriggerFirer;
pub use truncate_table::TruncateTableExecutor;
pub use type_ddl::TypeExecutor;
pub use update::UpdateExecutor;
pub use view_ddl::ViewExecutor;

#[cfg(test)]
mod tests;
