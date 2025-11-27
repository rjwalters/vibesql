//! Core execution logic for sqllogictest runner.

mod core;
mod parallel;
mod record_processor;
mod validator;

// Re-export public types and traits
pub use core::{
    AsyncDB, DB, DialectStats, Partitioner, Runner, RunnerLocals, SqlDialect, default_partitioner,
};
