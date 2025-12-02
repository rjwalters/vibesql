use std::fmt;

use crate::interner::StringSymbol;
use crate::keywords::Keyword;

/// SQL Token produced by the lexer.
///
/// Identifier-based tokens (Identifier, DelimitedIdentifier, NamedPlaceholder,
/// SessionVariable, UserVariable) use interned StringSymbol for:
/// - Memory efficiency: identical identifiers share storage
/// - Fast equality: O(1) symbol comparison instead of O(n) string comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Token {
    /// SQL keyword (SELECT, FROM, etc.)
    Keyword(Keyword),
    /// Identifier (table name, column name, etc.) - interned for deduplication
    Identifier(StringSymbol),
    /// Delimited identifier ("columnName" - case-sensitive, can use reserved words) - interned
    DelimitedIdentifier(StringSymbol),
    /// Numeric literal (42, 3.14, etc.) - stored as symbol for consistency
    Number(StringSymbol),
    /// String literal ('hello') - stored as symbol for consistency
    String(StringSymbol),
    /// Single character symbols (+, -, *, /, =, <, >, etc.)
    Symbol(char),
    /// Multi-character operators (<=, >=, !=, <>, ||) - interned
    Operator(StringSymbol),
    /// Session variable (@@variable, @@session.variable, @@global.variable) - interned
    SessionVariable(StringSymbol),
    /// User variable (@variable) - interned
    UserVariable(StringSymbol),
    /// Parameter placeholder (?) for prepared statements
    /// The index is assigned during parsing (0-indexed, in order of appearance)
    Placeholder,
    /// Numbered parameter placeholder ($1, $2, etc.) for prepared statements
    /// PostgreSQL-style: 1-indexed as written in SQL ($1 = first parameter)
    NumberedPlaceholder(usize),
    /// Named parameter placeholder (:name) for prepared statements - interned
    /// Used by many ORMs and applications for readability
    NamedPlaceholder(StringSymbol),
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
        // Note: Display shows token type and symbol index (not resolved string)
        // For full string display, use Token::display_with_interner()
        match self {
            Token::Keyword(kw) => write!(f, "Keyword({})", kw),
            Token::Identifier(sym) => write!(f, "Identifier({:?})", sym),
            Token::DelimitedIdentifier(sym) => write!(f, "DelimitedIdentifier({:?})", sym),
            Token::Number(sym) => write!(f, "Number({:?})", sym),
            Token::String(sym) => write!(f, "String({:?})", sym),
            Token::Symbol(c) => write!(f, "Symbol({})", c),
            Token::Operator(sym) => write!(f, "Operator({:?})", sym),
            Token::SessionVariable(sym) => write!(f, "SessionVariable({:?})", sym),
            Token::UserVariable(sym) => write!(f, "UserVariable({:?})", sym),
            Token::Placeholder => write!(f, "Placeholder"),
            Token::NumberedPlaceholder(n) => write!(f, "NumberedPlaceholder(${})", n),
            Token::NamedPlaceholder(sym) => write!(f, "NamedPlaceholder({:?})", sym),
            Token::Semicolon => write!(f, "Semicolon"),
            Token::Comma => write!(f, "Comma"),
            Token::LParen => write!(f, "LParen"),
            Token::RParen => write!(f, "RParen"),
            Token::Eof => write!(f, "Eof"),
        }
    }
}

impl Token {
    /// Display the token with resolved strings from the interner.
    pub fn display_with_interner<'a>(
        &'a self,
        interner: &'a crate::interner::IdentifierInterner,
    ) -> TokenDisplay<'a> {
        TokenDisplay { token: self, interner }
    }
}

/// Helper for displaying tokens with resolved strings.
pub struct TokenDisplay<'a> {
    token: &'a Token,
    interner: &'a crate::interner::IdentifierInterner,
}

impl<'a> fmt::Display for TokenDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.token {
            Token::Keyword(kw) => write!(f, "Keyword({})", kw),
            Token::Identifier(sym) => {
                write!(f, "Identifier({})", self.interner.resolve_unchecked(*sym))
            }
            Token::DelimitedIdentifier(sym) => {
                write!(f, "DelimitedIdentifier(\"{}\")", self.interner.resolve_unchecked(*sym))
            }
            Token::Number(sym) => {
                write!(f, "Number({})", self.interner.resolve_unchecked(*sym))
            }
            Token::String(sym) => {
                write!(f, "String('{}')", self.interner.resolve_unchecked(*sym))
            }
            Token::Symbol(c) => write!(f, "Symbol({})", c),
            Token::Operator(sym) => {
                write!(f, "Operator({})", self.interner.resolve_unchecked(*sym))
            }
            Token::SessionVariable(sym) => {
                write!(f, "SessionVariable({})", self.interner.resolve_unchecked(*sym))
            }
            Token::UserVariable(sym) => {
                write!(f, "UserVariable({})", self.interner.resolve_unchecked(*sym))
            }
            Token::Placeholder => write!(f, "Placeholder"),
            Token::NumberedPlaceholder(n) => write!(f, "NumberedPlaceholder(${})", n),
            Token::NamedPlaceholder(sym) => {
                write!(f, "NamedPlaceholder(:{})", self.interner.resolve_unchecked(*sym))
            }
            Token::Semicolon => write!(f, "Semicolon"),
            Token::Comma => write!(f, "Comma"),
            Token::LParen => write!(f, "LParen"),
            Token::RParen => write!(f, "RParen"),
            Token::Eof => write!(f, "Eof"),
        }
    }
}
