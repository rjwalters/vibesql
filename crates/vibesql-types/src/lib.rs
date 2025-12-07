//! SQL:1999 Type System
//!
//! This crate provides the type system for SQL:1999, including:
//! - Data type definitions (INTEGER, VARCHAR, BOOLEAN, etc.)
//! - SQL values representation
//! - Type compatibility and coercion rules
//! - Type checking utilities

mod data_type;
mod sql_mode;
mod sql_value;
mod temporal;

// Re-export all public types to maintain the same public API
pub use data_type::DataType;
pub use sql_mode::types::{TypeBehavior, ValueType};
pub use sql_mode::{ConcatOperator, DivisionBehavior, OperatorBehavior};
pub use sql_mode::{MySqlModeFlags, SqlMode};
pub use sql_value::{SqlValue, StringValue};
pub use temporal::{Date, Interval, IntervalField, Time, Timestamp};
