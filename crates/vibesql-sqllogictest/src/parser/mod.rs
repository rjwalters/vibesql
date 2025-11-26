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
    use std::io::Write;
    use std::path::Path;

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

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_include_glob() {
        let records =
            parse_file::<DefaultColumnType>("../tests/slt/include/include_1.slt").unwrap();
        assert_eq!(15, records.len());
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_basic() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/basic.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_condition() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/condition.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_file_level_sort_mode() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/file_level_sort_mode.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_rowsort() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/rowsort.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_valuesort() {
        parse_roundtrip::<DefaultColumnType>("../tests/slt/valuesort.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_substitution() {
        parse_roundtrip::<DefaultColumnType>("../tests/substitution/basic.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_test_dir_escape() {
        parse_roundtrip::<DefaultColumnType>("../tests/test_dir_escape/test_dir_escape.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_validator() {
        parse_roundtrip::<DefaultColumnType>("../tests/validator/validator.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_custom_type() {
        parse_roundtrip::<CustomColumnType>("../tests/custom_type/custom_type.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_system_command() {
        parse_roundtrip::<DefaultColumnType>("../tests/system_command/system_command.slt")
    }

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

    /// Verifies Display impl is consistent with parsing by ensuring
    /// roundtrip parse(unparse(parse())) is consistent
    #[track_caller]
    fn parse_roundtrip<T: ColumnType>(filename: impl AsRef<Path>) {
        let filename = filename.as_ref();
        let records = parse_file::<T>(filename).expect("parsing to complete");

        let unparsed = records
            .iter()
            .map(|record| record.to_string())
            .collect::<Vec<_>>();

        let output_contents = unparsed.join("\n");

        // The original and parsed records should be logically equivalent
        let mut output_file = tempfile::NamedTempFile::new().expect("Error creating tempfile");
        output_file
            .write_all(output_contents.as_bytes())
            .expect("Unable to write file");
        output_file.flush().unwrap();

        let output_path = output_file.into_temp_path();
        let reparsed_records =
            parse_file(&output_path).expect("reparsing to complete successfully");

        let records = normalize_filename(records);
        let reparsed_records = normalize_filename(reparsed_records);

        pretty_assertions::assert_eq!(records, reparsed_records, "Mismatch in reparsed records");
    }

    /// Replaces the actual filename in all Records with
    /// "__FILENAME__" so different files with the same contents can
    /// compare equal
    fn normalize_filename<T: ColumnType>(records: Vec<Record<T>>) -> Vec<Record<T>> {
        records
            .into_iter()
            .map(|mut record| {
                match &mut record {
                    Record::Include { loc, .. } => normalize_loc(loc),
                    Record::Statement { loc, .. } => normalize_loc(loc),
                    Record::System { loc, .. } => normalize_loc(loc),
                    Record::Query { loc, .. } => normalize_loc(loc),
                    Record::Sleep { loc, .. } => normalize_loc(loc),
                    Record::Subtest { loc, .. } => normalize_loc(loc),
                    Record::Halt { loc, .. } => normalize_loc(loc),
                    Record::HashThreshold { loc, .. } => normalize_loc(loc),
                    Record::Dialect { loc, .. } => normalize_loc(loc),
                    // even though these variants don't include a
                    // location include them in this match statement
                    // so if new variants are added, this match
                    // statement must be too.
                    Record::Condition(_)
                    | Record::Connection(_)
                    | Record::Comment(_)
                    | Record::Control(_)
                    | Record::Newline
                    | Record::Injected(_) => {}
                };
                record
            })
            .collect()
    }

    // Normalize a location
    fn normalize_loc(loc: &mut Location) {
        loc.file = std::sync::Arc::from("__FILENAME__");
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

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_statement_retry() {
        parse_roundtrip::<DefaultColumnType>("../tests/no_run/statement_retry.slt")
    }

    #[test]
    #[ignore] // Requires external test files from upstream repository
    fn test_query_retry() {
        parse_roundtrip::<DefaultColumnType>("../tests/no_run/query_retry.slt")
    }
}
