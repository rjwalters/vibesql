//! Compatibility layer re-exporting the refactored runner components.
//!
//! This module maintains backward compatibility by re-exporting the main [`Runner`] struct
//! and types that were previously defined here, which have been refactored into separate,
//! focused modules:
//!
//! - [`error_handling`] - Error types and formatting
//! - [`output`] - Record output handling and validation
//! - [`executor`] - Core execution logic and database traits
//! - [`result_updater`] - Query result updating

// Re-export the main Runner struct
pub use crate::executor::{default_partitioner, AsyncDB, Partitioner, Runner, DB};

// Re-export error types
pub use crate::error_handling::{
    format_column_diff, format_diff, AnyError, ParallelTestError, ParallelTestErrorDisplay,
    RecordKind, TestError, TestErrorDisplay, TestErrorKind, TestErrorKindDisplay,
};

// Re-export output types
pub use crate::output::{
    default_column_validator, default_normalizer, default_validator, strict_column_validator,
    ColumnTypeValidator, DBOutput, Normalizer, RecordOutput, Validator,
};

// Re-export result updater
pub use crate::result_updater::update_record_with_output;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultColumnType;
    use std::sync::Arc;

    #[test]
    fn test_query_replacement_no_changes() {
        let record = "query   I?\n\
                    select * from foo;\n\
                    ----\n\
                    3\n\
                    4";
        TestCase {
            // keep the input values
            input: record,

            // Model a run that produced a 3,4 as output (one row with two columns)
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(record),
        }
        .run()
    }

    #[test]
    fn test_query_replacement() {
        TestCase {
            // input should be ignored
            input: "query III\n\
                    select * from foo;\n\
                    ----\n\
                    1 2",

            // Model a run that produced a 3,4 as output (one row with two columns)
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "query I?\n\
                 select * from foo;\n\
                 ----\n\
                 3\n\
                 4",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_replacement_no_input() {
        TestCase {
            // input has no query results
            input: "query\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a 3,4 as output (one row with two columns)
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "query I?\n\
                 select * from foo;\n\
                 ----\n\
                 3\n\
                 4",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_replacement_no_output() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model nothing was output
            record_output: RecordOutput::Nothing,

            // No update
            expected: None,
        }
        .run()
    }

    #[test]
    fn test_query_replacement_error() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a "MyAwesomeDB Error"
            record_output: query_output_error("MyAwesomeDB Error"),

            expected: Some(
                "query error TestError: MyAwesomeDB Error\n\
                 select * from foo;\n",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_replacement_error_multiline() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a "MyAwesomeDB Error"
            record_output: query_output_error("MyAwesomeDB Error\n\nCaused by:\n  Inner Error"),

            expected: Some(
                "query error
select * from foo;
----
TestError: MyAwesomeDB Error

Caused by:
  Inner Error",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_query_output() {
        TestCase {
            // input has no query results
            input: "statement ok\n\
                    create table foo;",

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "statement ok\n\
                 create table foo;",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_statement_output() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            expected: Some(
                "statement count 3\n\
                 select * from foo;",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_output() {
        TestCase {
            // statement that has no output
            input: "statement ok\n\
                    insert into foo values(2);",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            // Note the the output does not include 3 (statement
            // count) Rationale is if the record is statement count
            // <n>, n will be updated to real count. If the record is
            // statement ok (which means we don't care the number of
            // affected rows), it won't be updated.
            expected: Some(
                "statement ok\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_to_ok() {
        TestCase {
            // statement expected error
            input: "statement error\n\
                    insert into foo values(2);",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            expected: Some(
                "statement ok\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_no_error() {
        TestCase {
            // statement expected error
            input: "statement error\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // Input didn't have an expected error, so output is not to expect the message, then no
            // update
            expected: None,
        }
        .run()
    }

    #[test]
    fn test_statement_error_new_error() {
        TestCase {
            // statement expected error
            input: "statement error bar\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: foo\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_new_error_multiline() {
        TestCase {
            // statement expected error
            input: "statement error bar\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo\n\nCaused by:\n  Inner Error"),

            // expect the output includes foo
            expected: Some(
                "statement error
insert into foo values(2);
----
TestError: foo

Caused by:
  Inner Error",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_ok_to_error() {
        TestCase {
            // statement was ok
            input: "statement ok\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: foo\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_ok_to_error_multiline() {
        TestCase {
            // statement was ok
            input: "statement ok\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo\n\nCaused by:\n  Inner Error"),

            // expect the output includes foo
            expected: Some(
                "statement error
insert into foo values(2);
----
TestError: foo

Caused by:
  Inner Error",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_special_chars() {
        TestCase {
            // statement expected error
            input: "statement error tbd\n\
                    inser into foo values(2);",

            // Model a run that produced an error message that contains regex special characters
            record_output: statement_output_error("The operation (inser) is not supported. Did you mean [insert]?"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: The operation \\(inser\\) is not supported\\. Did you mean \\[insert\\]\\?\n\
                 inser into foo values(2);",
            ),
        }
            .run()
    }

    #[test]
    fn test_statement_keep_error_regex_when_matches() {
        TestCase {
            // statement expected error
            input: "statement error TestError: The operation \\([a-z]+\\) is not supported.*\n\
                    inser into foo values(2);",

            // Model a run that produced an error message that contains regex special characters
            record_output: statement_output_error(
                "The operation (inser) is not supported. Did you mean [insert]?",
            ),

            // no update expected
            expected: None,
        }
        .run()
    }

    #[test]
    fn test_query_error_special_chars() {
        TestCase {
            // statement expected error
            input: "query error tbd\n\
                    selec *;",

            // Model a run that produced an error message that contains regex special characters
            record_output: query_output_error("The operation (selec) is not supported. Did you mean [select]?"),

            // expect the output includes foo
            expected: Some(
                "query error TestError: The operation \\(selec\\) is not supported\\. Did you mean \\[select\\]\\?\n\
                 selec *;",
            ),
        }
            .run()
    }

    #[test]
    fn test_query_error_special_chars_when_matches() {
        TestCase {
            // statement expected error
            input: "query error TestError: The operation \\([a-z]+\\) is not supported.*\n\
                    selec *;",

            // Model a run that produced an error message that contains regex special characters
            record_output: query_output_error(
                "The operation (selec) is not supported. Did you mean [select]?",
            ),

            // no update expected
            expected: None,
        }
        .run()
    }

    #[derive(Debug)]
    struct TestCase<'a> {
        input: &'a str,
        record_output: RecordOutput<DefaultColumnType>,
        expected: Option<&'a str>,
    }

    impl TestCase<'_> {
        #[track_caller]
        fn run(self) {
            let Self { input, record_output, expected } = self;
            println!("TestCase");
            println!("**input:\n{input}\n");
            println!("**record_output:\n{record_output:#?}\n");
            println!("**expected:\n{}\n", expected.unwrap_or(""));
            let input = parse_to_record(input);
            let expected = expected.map(parse_to_record);
            let output = update_record_with_output(
                &input,
                &record_output,
                " ",
                default_validator,
                default_normalizer,
                strict_column_validator,
            );

            assert_eq!(
                &output,
                &expected,
                "\n\noutput:\n\n{}\n\nexpected:\n\n{}",
                output.as_ref().map(|r| r.to_string()).unwrap_or_else(|| "None".into()),
                expected.as_ref().map(|r| r.to_string()).unwrap_or_else(|| "None".into()),
            );
        }
    }

    fn parse_to_record(s: &str) -> crate::parser::Record<DefaultColumnType> {
        let mut records = crate::parser::parse(s).unwrap();
        assert_eq!(records.len(), 1);
        records.pop().unwrap()
    }

    /// Returns a RecordOutput that models the successful execution of a query
    fn query_output(
        rows: &[&[&str]],
        types: Vec<DefaultColumnType>,
    ) -> RecordOutput<DefaultColumnType> {
        let rows = rows
            .iter()
            .map(|cols| cols.iter().map(|c| c.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        RecordOutput::Query { types, rows, error: None }
    }

    /// Returns a RecordOutput that models the error of a query
    fn query_output_error(error_message: &str) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Query {
            types: vec![],
            rows: vec![],
            error: Some(Arc::new(TestError(error_message.to_string()))),
        }
    }

    fn statement_output(count: u64) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Statement { count, error: None }
    }

    /// RecordOutput that models a statement with error
    fn statement_output_error(error_message: &str) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Statement {
            count: 0,
            error: Some(Arc::new(TestError(error_message.to_string()))),
        }
    }

    #[derive(Debug)]
    struct TestError(String);
    impl std::error::Error for TestError {}
    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError: {}", self.0)
        }
    }

    #[test]
    fn test_default_validator_ignore_simple() {
        let normalizer = default_normalizer;
        let actual = vec![vec!["foo".to_string(), "bar".to_string()]];
        let expected = vec!["foo<slt:ignore>bar".to_string()];
        assert!(default_validator(normalizer, &actual, &expected));
    }

    #[test]
    fn test_default_validator_ignore_multiple_fragments() {
        let normalizer = default_normalizer;
        let actual = vec![vec!["one".to_string(), "two".to_string(), "three".to_string()]];
        let expected = vec!["one<slt:ignore>three".to_string()];
        assert!(default_validator(normalizer, &actual, &expected));
    }

    #[test]
    fn test_default_validator_ignore_fail() {
        let normalizer = default_normalizer;
        let actual = vec![vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]];
        let expected = vec!["alpha<slt:ignore>delta".to_string()];
        assert!(!default_validator(normalizer, &actual, &expected));
    }
}
