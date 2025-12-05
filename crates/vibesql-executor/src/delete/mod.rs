//! DELETE statement execution
//!
//! This module provides functionality for executing DELETE statements in SQL,
//! including WHERE clause evaluation, referential integrity checks, and
//! comprehensive test coverage.

pub mod executor;
pub mod integrity;

// Re-export the public API
pub use executor::execute_delete_with_trigger_context;
pub use executor::DeleteExecutor;

#[cfg(test)]
mod tests;
