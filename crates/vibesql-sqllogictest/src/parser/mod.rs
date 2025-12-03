//! Sqllogictest parser.

// Submodules
pub mod location;
pub mod error_parser;
pub mod retry_parser;
pub mod directive_parser;
pub mod record_parser;
pub mod records;
pub mod parser_core;

// Re-exports from submodules
pub use self::location::Location;
pub use self::error_parser::ExpectedError;
pub use self::retry_parser::RetryConfig;
pub use self::directive_parser::{Control, Condition, Connection, SortMode, ResultMode, ControlItem};
pub use self::record_parser::{StatementExpect, QueryExpect};
pub use self::records::{Record, Injected};
pub use self::parser_core::{parse, parse_with_name, parse_file};

/// The error type for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
#[error("parse error at {loc}: {kind}")]
pub struct ParseError {
    kind: ParseErrorKind,
    loc: Location,
}

impl ParseError {
    /// Returns the corresponding [`ParseErrorKind`] for this error.
    pub fn kind(&self) -> ParseErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> Location {
        self.loc.clone()
    }
}

/// The error kind for parsing sqllogictest.
#[derive(thiserror::Error, Debug, Eq, PartialEq, Clone)]
#[non_exhaustive]
pub enum ParseErrorKind {
    #[error("unexpected token: {0:?}")]
    UnexpectedToken(String),
    #[error("unexpected EOF")]
    UnexpectedEOF,
    #[error("invalid sort mode: {0:?}")]
    InvalidSortMode(String),
    #[error("invalid line: {0:?}")]
    InvalidLine(String),
    #[error("invalid type character: {0:?} in type string")]
    InvalidType(char),
    #[error("invalid number: {0:?}")]
    InvalidNumber(String),
    #[error("invalid error message: {0:?}")]
    InvalidErrorMessage(String),
    #[error("duplicated error messages after error` and under `----`")]
    DuplicatedErrorMessage,
    #[error("invalid retry config: {0:?}")]
    InvalidRetryConfig(String),
    #[error("statement should have no result, use `query` instead")]
    StatementHasResults,
    #[error("invalid duration: {0:?}")]
    InvalidDuration(String),
    #[error("invalid control: {0:?}")]
    InvalidControl(String),
    #[error("invalid include file pattern: {0}")]
    InvalidIncludeFile(String),
    #[error("no files found for include file pattern: {0:?}")]
    EmptyIncludeFile(String),
    #[error("no such file")]
    FileNotFound,
}

impl ParseErrorKind {
    pub(crate) fn at(self, loc: Location) -> ParseError {
        ParseError { kind: self, loc }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnType, DefaultColumnType};

    #[test]
    fn test_trailing_comment() {
        let script = "\
# comment 1
#  comment 2
";
        let records = parse::<DefaultColumnType>(script).unwrap();
        assert_eq!(
            records,
            vec![Record::Comment(vec![
                " comment 1".to_string(),
                "  comment 2".to_string(),
            ]),]
        );
    }

    // NOTE: Upstream tests that require external test files from sqllogictest-rs
    // repository (../tests/slt/*.slt, etc.) have been removed as they don't exist
    // in this repository and would fail with --include-ignored.

    #[test]
    fn test_fail_unknown_type() {
        let script = "\
query IA
select * from unknown_type
----
";

        let error_kind = parse::<CustomColumnType>(script).unwrap_err().kind;

        assert_eq!(error_kind, ParseErrorKind::InvalidType('A'));
    }

    #[test]
    fn test_parse_no_types() {
        let script = "\
query
select * from foo;
----
";
        let records = parse::<DefaultColumnType>(script).unwrap();

        assert_eq!(
            records,
            vec![Record::Query {
                loc: Location::new("<unknown>", 1),
                conditions: vec![],
                connection: Connection::Default,
                sql: "select * from foo;".to_string(),
                expected: QueryExpect::empty_results(),
                retry: None,
            }]
        );
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    pub enum CustomColumnType {
        Integer,
        Boolean,
    }

    impl ColumnType for CustomColumnType {
        fn from_char(value: char) -> Option<Self> {
            match value {
                'I' => Some(Self::Integer),
                'B' => Some(Self::Boolean),
                _ => None,
            }
        }

        fn to_char(&self) -> char {
            match self {
                Self::Integer => 'I',
                Self::Boolean => 'B',
            }
        }
    }
}
