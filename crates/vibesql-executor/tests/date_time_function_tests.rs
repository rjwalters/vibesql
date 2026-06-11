//! Tests for SQL CORE Phase 3A date/time functions
//!
//! This test module contains comprehensive tests for date/time functions.
//! Tests are organized into focused sub-modules for better maintainability.
//!
//! ## Module Organization
//!
//! - **fixtures**: Common test helpers and datetime value fixtures
//! - **current_datetime**: CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP and aliases
//! - **precision**: Precision arguments for fractional seconds
//! - **extraction**: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND extraction
//! - **nested_operations**: Nested and combined datetime operations
//! - **strftime**: SQLite-compatible STRFTIME formatting

mod common;

// Include the sub-modules from the date_time_function_tests directory
#[path = "date_time_function_tests/fixtures.rs"]
mod fixtures;

#[path = "date_time_function_tests/current_datetime.rs"]
mod current_datetime;

#[path = "date_time_function_tests/precision.rs"]
mod precision;

#[path = "date_time_function_tests/extraction.rs"]
mod extraction;

#[path = "date_time_function_tests/nested_operations.rs"]
mod nested_operations;

#[path = "date_time_function_tests/strftime.rs"]
mod strftime;
