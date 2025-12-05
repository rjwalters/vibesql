//! Non-aggregation execution module
//!
//! This module implements execution strategies for non-aggregated SELECT queries.
//! It provides different execution paths optimized for different query patterns:
//!
//! - **Iterator-based execution**: For simple queries without ORDER BY, DISTINCT, or window functions.
//!   Uses lazy evaluation to minimize memory usage and enable early termination with LIMIT.
//!
//! - **Materialized execution**: For complex queries requiring ORDER BY, DISTINCT, or window functions.
//!   Includes optimizations like SIMD filtering and spatial indexes.
//!
//! - **SELECT without FROM**: Special case for evaluating constant expressions.
//!
//! ## Module Structure
//!
//! - `validation`: Validates WHERE clause subqueries before execution
//! - `simd`: SIMD filtering optimization using Apache Arrow
//! - `iterator`: Iterator-based execution for simple queries
//! - `materialized`: Materialized execution for complex queries
//! - `without_from`: SELECT without FROM clause execution

// Re-export the builder module so implementations can use SelectExecutor
use super::builder;

// Internal modules
mod iterator;
mod materialized;
mod simd;
mod validation;
mod without_from;

// Re-export the public interface
// The execute_without_aggregation and execute_select_without_from methods
// are available on SelectExecutor through the impl blocks in the sub-modules
