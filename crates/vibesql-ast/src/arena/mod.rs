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
//! ```ignore
//! use bumpalo::Bump;
//! use vibesql_ast::arena::Expression;
//!
//! let arena = Bump::new();
//! // Parser allocates from arena
//! let expr = arena.alloc(Expression::Literal(SqlValue::Integer(42)));
//! // All allocations freed when arena is dropped
//! ```
//!
//! # Conversion to Standard Types
//!
//! Arena types can be converted to standard heap-allocated types using `From` traits:
//!
//! ```ignore
//! use bumpalo::Bump;
//! use vibesql_ast::{SelectStmt, arena};
//!
//! let arena = Bump::new();
//! let arena_stmt: &arena::SelectStmt = /* ... */;
//! let std_stmt: SelectStmt = SelectStmt::from(arena_stmt);
//! ```

mod convert;
mod expression;
mod select;

pub use expression::*;
pub use select::*;

// Re-export Bump for convenience
pub use bumpalo::Bump;
