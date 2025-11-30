use std::fmt;

use crate::keywords::Keyword;

/// SQL Token produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// SQL keyword (SELECT, FROM, etc.)
    Keyword(Keyword),
    /// Identifier (table name, column name, etc.)
    Identifier(String),
    /// Delimited identifier ("columnName" - case-sensitive, can use reserved words)
    DelimitedIdentifier(String),
    /// Numeric literal (42, 3.14, etc.)
    Number(String),
    /// String literal ('hello')
    String(String),
    /// Single character symbols (+, -, *, /, =, <, >, etc.)
    Symbol(char),
    /// Multi-character operators (<=, >=, !=, <>, ||)
    Operator(String),
    /// Session variable (@@variable, @@session.variable, @@global.variable)
    SessionVariable(String),
    /// User variable (@variable)
    UserVariable(String),
    /// Parameter placeholder (?) for prepared statements
    /// The index is assigned during parsing (0-indexed, in order of appearance)
    Placeholder,
    /// Semicolon (statement terminator)
    Semicolon,
    /// Comma (separator)
    Comma,
    /// Left parenthesis
    LParen,
    /// Right parenthesis
    RParen,
    /// End of input
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Keyword(kw) => write!(f, "Keyword({})", kw),
            Token::Identifier(id) => write!(f, "Identifier({})", id),
            Token::DelimitedIdentifier(id) => write!(f, "DelimitedIdentifier(\"{}\")", id),
            Token::Number(n) => write!(f, "Number({})", n),
            Token::String(s) => write!(f, "String('{}')", s),
            Token::Symbol(c) => write!(f, "Symbol({})", c),
            Token::Operator(op) => write!(f, "Operator({})", op),
            Token::SessionVariable(v) => write!(f, "SessionVariable({})", v),
            Token::UserVariable(v) => write!(f, "UserVariable({})", v),
            Token::Placeholder => write!(f, "Placeholder"),
            Token::Semicolon => write!(f, "Semicolon"),
            Token::Comma => write!(f, "Comma"),
            Token::LParen => write!(f, "LParen"),
            Token::RParen => write!(f, "RParen"),
            Token::Eof => write!(f, "Eof"),
        }
    }
}
