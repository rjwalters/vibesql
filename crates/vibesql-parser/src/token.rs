use std::fmt;

use crate::keywords::Keyword;

/// Multi-character operators that require heap allocation if stored as String.
/// Using an enum eliminates allocation and enables fast matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiCharOperator {
    /// <= (less than or equal)
    LessEqual,
    /// >= (greater than or equal)
    GreaterEqual,
    /// != (not equal)
    NotEqual,
    /// <> (not equal, SQL standard)
    NotEqualAlt,
    /// == (equal, SQLite compatibility - synonym for =)
    DoubleEqual,
    /// || (string concatenation)
    Concat,
    /// <-> (cosine distance - pgvector compatible)
    CosineDistance,
    /// <#> (negative inner product - pgvector compatible)
    NegativeInnerProduct,
    /// <=> (L2/Euclidean distance - pgvector compatible)
    L2Distance,
}

impl fmt::Display for MultiCharOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MultiCharOperator::LessEqual => write!(f, "<="),
            MultiCharOperator::GreaterEqual => write!(f, ">="),
            MultiCharOperator::NotEqual => write!(f, "!="),
            MultiCharOperator::NotEqualAlt => write!(f, "<>"),
            MultiCharOperator::DoubleEqual => write!(f, "=="),
            MultiCharOperator::Concat => write!(f, "||"),
            MultiCharOperator::CosineDistance => write!(f, "<->"),
            MultiCharOperator::NegativeInnerProduct => write!(f, "<#>"),
            MultiCharOperator::L2Distance => write!(f, "<=>"),
        }
    }
}

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
    Operator(MultiCharOperator),
    /// Session variable (@@variable, @@session.variable, @@global.variable)
    SessionVariable(String),
    /// User variable (@variable)
    UserVariable(String),
    /// Parameter placeholder (?) for prepared statements
    /// The index is assigned during parsing (0-indexed, in order of appearance)
    Placeholder,
    /// Numbered parameter placeholder ($1, $2, etc.) for prepared statements
    /// PostgreSQL-style: 1-indexed as written in SQL ($1 = first parameter)
    NumberedPlaceholder(usize),
    /// Named parameter placeholder (:name) for prepared statements
    /// Used by many ORMs and applications for readability
    NamedPlaceholder(String),
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

impl Token {
    /// Convert token back to valid SQL string.
    /// This is the inverse of lexing - it produces SQL that can be re-parsed.
    pub fn to_sql(&self) -> String {
        match self {
            Token::Keyword(kw) => kw.to_string(),
            Token::Identifier(id) => id.clone(),
            Token::DelimitedIdentifier(id) => format!("\"{}\"", id),
            Token::Number(n) => n.clone(),
            Token::String(s) => format!("'{}'", s.replace('\'', "''")),
            Token::Symbol(c) => c.to_string(),
            Token::Operator(op) => op.to_string(),
            Token::SessionVariable(v) => format!("@@{}", v),
            Token::UserVariable(v) => format!("@{}", v),
            Token::Placeholder => "?".to_string(),
            Token::NumberedPlaceholder(n) => format!("${}", n),
            Token::NamedPlaceholder(name) => format!(":{}", name),
            Token::Semicolon => ";".to_string(),
            Token::Comma => ",".to_string(),
            Token::LParen => "(".to_string(),
            Token::RParen => ")".to_string(),
            Token::Eof => String::new(),
        }
    }
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
            Token::NumberedPlaceholder(n) => write!(f, "NumberedPlaceholder(${})", n),
            Token::NamedPlaceholder(name) => write!(f, "NamedPlaceholder(:{})", name),
            Token::Semicolon => write!(f, "Semicolon"),
            Token::Comma => write!(f, "Comma"),
            Token::LParen => write!(f, "LParen"),
            Token::RParen => write!(f, "RParen"),
            Token::Eof => write!(f, "Eof"),
        }
    }
}
