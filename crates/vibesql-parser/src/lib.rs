//! SQL:1999 Parser crate.
//!
//! Provides tokenization and parsing of SQL statements into the shared AST.
//!
//! # Arena-allocated Parser
//!
//! For performance-critical code paths, the [`arena_parser`] module provides
//! an arena-based parser that allocates AST nodes from a bump allocator.

pub mod arena_parser;

mod keywords;
mod lexer;
mod parser;
#[cfg(test)]
mod tests;
mod token;

pub use keywords::Keyword;
pub use lexer::{Lexer, LexerError};
pub use parser::{ParseError, Parser};
pub use token::Token;
