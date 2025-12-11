//! Date and time function implementations for SQL scalar functions
//!
//! This module contains all date/time manipulation and extraction functions including:
//! - Current date/time functions (CURRENT_DATE/CURDATE, CURRENT_TIME/CURTIME,
//!   CURRENT_TIMESTAMP/NOW, DATETIME)
//! - Date/time extraction (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, EXTRACT)
//! - Date arithmetic (DATE_ADD/ADDDATE, DATE_SUB/SUBDATE, DATEDIFF)
//! - Age calculation (AGE)
//!
//! These functions implement SQL:1999 standard date/time operations.
//!
//! # Module Organization
//!
//! The datetime functions are organized into logical modules:
//! - `current` - Current date/time functions (CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP,
//!   DATETIME)
//! - `extract` - Field extraction (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
//! - `arithmetic` - Date/time arithmetic (DATEDIFF, DATE_ADD, DATE_SUB, AGE, EXTRACT)

mod arithmetic;
mod current;
mod extract;

// Re-export all public functions
// Re-export helper for use by arithmetic operators
pub(crate) use arithmetic::date_add_subtract;
pub(super) use arithmetic::{age, date_add, date_sub, datediff, extract};
pub(super) use current::{current_date, current_time, current_timestamp, datetime};
pub(super) use extract::{day, hour, minute, month, second, year};
