//! Arena-allocated AST types for improved parsing performance.
//!
//! This module provides arena-based versions of AST types that use bump allocation
//! instead of individual heap allocations. This can significantly improve parsing
//! performance for complex queries by:
//!
//! - Reducing allocation overhead (O(1) bump allocation vs heap allocation)
//! - Improving cache locality (contiguous memory layout)
//! - Enabling batch deallocation (single `drop(arena)` frees everything)
//!
//! # Usage
//!
//! ```text
//! use bumpalo::Bump;
//! use vibesql_ast::arena::Expression;
//!
//! let arena = Bump::new();
//! // Parser allocates from arena
//! let expr = arena.alloc(Expression::Literal(SqlValue::Integer(42)));
//! // All allocations freed when arena is dropped
//! ```

mod convert;
mod ddl;
mod dml;
mod expression;
mod interner;
mod select;

pub use convert::Converter;
pub use ddl::*;
pub use dml::*;
pub use expression::*;
pub use interner::{ArenaInterner, Symbol};
pub use select::*;

// Re-export Bump for convenience
pub use bumpalo::Bump;
