//! Query plan caching module
//!
//! Provides query plan caching infrastructure to optimize repeated query patterns.
//! Queries with identical structure (different literals) reuse cached plans.

pub mod integration;
pub mod parameterized;
pub mod prepared_statement;
mod query_plan_cache;
mod query_result_cache;
mod query_signature;
pub mod table_extractor;

pub use integration::{CacheManager, CachedQueryContext};
pub use parameterized::{LiteralExtractor, LiteralValue, ParameterPosition, ParameterizedPlan};
pub use prepared_statement::{
    arena_prepared::{ArenaBindError, ArenaParseError, ArenaPreparedStatement},
    CachedPlan, ColumnProjection, PkDeletePlan, PkPointLookupPlan, PreparedStatement, PreparedStatementCache,
    PreparedStatementCacheStats, PreparedStatementError, ProjectionPlan, ResolvedProjection,
    SimpleFastPathPlan,
};
pub use query_plan_cache::{CacheStats, QueryPlanCache};
pub use query_result_cache::QueryResultCache;
pub use query_signature::QuerySignature;
pub use table_extractor::{extract_tables_from_select, extract_tables_from_statement};
