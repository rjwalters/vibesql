//! String function implementations for SQL scalar functions
//!
//! This module contains all string manipulation functions, organized by category:
//! - Case conversion (UPPER, LOWER) - see `case.rs`
//! - Trimming operations (TRIM, LTRIM, RTRIM) - see `trim.rs`
//! - Length measurements (LENGTH, CHAR_LENGTH, OCTET_LENGTH) - see `length.rs`
//! - Substring operations (SUBSTRING, LEFT, RIGHT) - see `substring.rs`
//! - String transformations (REPLACE, REVERSE) - see `transform.rs`
//! - String concatenation (CONCAT) - see `concat.rs`
//! - String search (POSITION, INSTR, LOCATE) - see `search.rs`
//!
//! All functions follow SQL:1999 standard where applicable.

// Submodules
pub(super) mod case;
pub(super) mod concat;
pub(super) mod length;
pub(super) mod search;
pub(super) mod substring;
pub(super) mod transform;
pub(super) mod trim;

// Re-export functions for use by parent module
pub(super) use case::{lower, upper};
pub(super) use concat::concat;
pub(super) use length::{char_length, length, octet_length};
pub(super) use search::{instr, locate, position};
pub(super) use substring::{left, right, substring};
pub(super) use transform::{replace, reverse};
pub(super) use trim::{ltrim, rtrim, trim};

// Note: trim_advanced is exported directly from the trim module with pub(crate) visibility
