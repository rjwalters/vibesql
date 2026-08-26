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
pub use data_type::{DataType, TypeAffinity};
pub use sql_mode::{
    types::{TypeBehavior, ValueType},
    ConcatOperator, DivisionBehavior, MySqlModeFlags, OperatorBehavior, SqlMode,
};
pub use sql_value::{
    exact_mixed_numeric_cmp, exceeds_f64_exact_integer_range, total_order_cmp, SqlValue,
    StringValue,
};
pub use temporal::{Date, Interval, IntervalField, Time, Timestamp};
