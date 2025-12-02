//! SQL:1999 Parser crate.
//!
//! Provides tokenization and parsing of SQL statements into the shared AST.

mod interner;
mod keywords;
mod lexer;
mod parser;
#[cfg(test)]
mod tests;
mod token;

pub use interner::{IdentifierInterner, StringSymbol};
pub use keywords::Keyword;
pub use lexer::{Lexer, LexerError, TokenStream};
pub use parser::{ParseError, Parser};
pub use token::Token;
