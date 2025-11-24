//! Database adapter for SQLLogicTest runner.
//!
//! This module has been split into focused submodules for better organization:
//! - `pool`: Thread-local database pooling
//! - `cache`: Query result caching and invalidation
//! - `timing`: Statement timing and performance instrumentation
//! - `batching`: Automatic INSERT batching for test data loading
//! - `executor`: Core SQL execution and statement dispatching
//! - `adapter`: Main VibeSqlDB implementation tying everything together

mod pool;
mod cache;
mod timing;
mod batching;
mod executor;
mod adapter;

// Re-export the main adapter type
pub use adapter::VibeSqlDB;
