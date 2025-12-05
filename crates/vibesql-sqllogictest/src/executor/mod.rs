//! Core execution logic for sqllogictest runner.

mod core;
mod parallel;
mod record_processor;
mod validator;

// Re-export public types and traits
pub use core::{
    default_partitioner, AsyncDB, DialectStats, Partitioner, Runner, RunnerLocals, SqlDialect, DB,
};
