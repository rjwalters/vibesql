//! Error types and formatting for sqllogictest execution.

use std::{fmt::Display, sync::Arc};

use itertools::Itertools;
use owo_colors::OwoColorize;
use similar::{Change, ChangeTag, TextDiff};

use crate::parser::ParseErrorKind;

/// Type-erased error type.
pub type AnyError = Arc<dyn std::error::Error + Send + Sync>;

/// The error type for running sqllogictest.
///
/// For colored error message, use `self.display()`.
#[derive(thiserror::Error, Clone)]
#[error("{kind}\nat {loc}\n")]
pub struct TestError {
    kind: TestErrorKind,
    loc: crate::parser::Location,
}

impl TestError {
    pub fn display(&self, colorize: bool) -> TestErrorDisplay<'_> {
        TestErrorDisplay { err: self, colorize }
    }
}

/// Overrides the `Display` implementation of [`TestError`] to support controlling colorization.
pub struct TestErrorDisplay<'a> {
    err: &'a TestError,
    colorize: bool,
}

impl Display for TestErrorDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}\nat {}\n", self.err.kind.display(self.colorize), self.err.loc)
    }
}

/// For colored error message, use `self.display()`.
#[derive(Clone, Debug, thiserror::Error)]
pub struct ParallelTestError {
    pub errors: Vec<TestError>,
}

impl Display for ParallelTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "parallel test failed")?;
        write!(f, "Caused by:")?;
        for i in &self.errors {
            writeln!(f, "{i}")?;
        }
        Ok(())
    }
}

/// Overrides the `Display` implementation of [`ParallelTestError`] to support controlling
/// colorization.
pub struct ParallelTestErrorDisplay<'a> {
    err: &'a ParallelTestError,
    colorize: bool,
}

impl Display for ParallelTestErrorDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "parallel test failed")?;
        write!(f, "Caused by:")?;
        for i in &self.err.errors {
            writeln!(f, "{}", i.display(self.colorize))?;
        }
        Ok(())
    }
}

impl ParallelTestError {
    pub fn display(&self, colorize: bool) -> ParallelTestErrorDisplay<'_> {
        ParallelTestErrorDisplay { err: self, colorize }
    }
}

impl std::fmt::Debug for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl TestError {
    /// Returns the corresponding [`TestErrorKind`] for this error.
    pub fn kind(&self) -> TestErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> crate::parser::Location {
        self.loc.clone()
    }
}

#[derive(Debug, Clone)]
pub enum RecordKind {
    Statement,
    Query,
}

impl std::fmt::Display for RecordKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordKind::Statement => write!(f, "statement"),
            RecordKind::Query => write!(f, "query"),
        }
    }
}

/// The error kind for running sqllogictest.
///
/// For colored error message, use `self.display()`.
#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum TestErrorKind {
    #[error("parse error: {0}")]
    ParseError(ParseErrorKind),
    #[error("{kind} is expected to fail, but actually succeed:\n[SQL] {sql}")]
    Ok { sql: String, kind: RecordKind },
    #[error("{kind} failed: {err}\n[SQL] {sql}")]
    Fail { sql: String, err: AnyError, kind: RecordKind },
    #[error("system command failed: {err}\n[CMD] {command}")]
    SystemFail { command: String, err: AnyError },
    #[error(
        "system command stdout mismatch:\n[command] {command}\n[Diff] (-expected|+actual)\n{}",
        TextDiff::from_lines(.expected_stdout, .actual_stdout).iter_all_changes().format_with("\n", |diff, f| format_diff(&diff, f, false))
    )]
    SystemStdoutMismatch { command: String, expected_stdout: String, actual_stdout: String },
    // Remember to also update [`TestErrorKindDisplay`] if this message is changed.
    #[error("{kind} is expected to fail with error:\n\t{expected_err}\nbut got error:\n\t{err}\n[SQL] {sql}")]
    ErrorMismatch { sql: String, err: AnyError, expected_err: String, kind: RecordKind },
    #[error("statement is expected to affect {expected} rows, but actually {actual}\n[SQL] {sql}")]
    StatementResultMismatch { sql: String, expected: u64, actual: String },
    // Remember to also update [`TestErrorKindDisplay`] if this message is changed.
    #[error(
        "query result mismatch:\n[SQL] {sql}\n[Diff] (-expected|+actual)\n{}",
        TextDiff::from_lines(.expected, .actual).iter_all_changes().format_with("\n", |diff, f| format_diff(&diff, f, false))
    )]
    QueryResultMismatch { sql: String, expected: String, actual: String },
    #[error(
        "query columns mismatch:\n[SQL] {sql}\n{}",
        format_column_diff(expected, actual, false)
    )]
    QueryResultColumnsMismatch { sql: String, expected: String, actual: String },
}

impl From<crate::parser::ParseError> for TestError {
    fn from(e: crate::parser::ParseError) -> Self {
        TestError { kind: TestErrorKind::ParseError(e.kind()), loc: e.location() }
    }
}

impl TestErrorKind {
    pub fn at(self, loc: crate::parser::Location) -> TestError {
        TestError { kind: self, loc }
    }

    pub fn display(&self, colorize: bool) -> TestErrorKindDisplay<'_> {
        TestErrorKindDisplay { error: self, colorize }
    }
}

/// Overrides the `Display` implementation of [`TestErrorKind`] to support controlling colorization.
pub struct TestErrorKindDisplay<'a> {
    error: &'a TestErrorKind,
    colorize: bool,
}

impl Display for TestErrorKindDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.colorize {
            return write!(f, "{}", self.error);
        }
        match self.error {
            TestErrorKind::ErrorMismatch { sql, err, expected_err, kind } => write!(
                f,
                "{kind} is expected to fail with error:\n\t{}\nbut got error:\n\t{}\n[SQL] {sql}",
                expected_err.bright_green(),
                err.bright_red(),
            ),
            TestErrorKind::QueryResultMismatch { sql, expected, actual } => write!(
                f,
                "query result mismatch:\n[SQL] {sql}\n[Diff] ({}|{})\n{}",
                "-expected".bright_red(),
                "+actual".bright_green(),
                TextDiff::from_lines(expected, actual)
                    .iter_all_changes()
                    .format_with("\n", |diff, f| format_diff(&diff, f, true))
            ),
            TestErrorKind::QueryResultColumnsMismatch { sql, expected, actual } => {
                write!(
                    f,
                    "query columns mismatch:\n[SQL] {sql}\n{}",
                    format_column_diff(expected, actual, true)
                )
            }
            TestErrorKind::SystemStdoutMismatch { command, expected_stdout, actual_stdout } => {
                write!(
                    f,
                    "system command stdout mismatch:\n[command] {command}\n[Diff] (-expected|+actual)\n{}",
                    TextDiff::from_lines(expected_stdout, actual_stdout)
                        .iter_all_changes()
                        .format_with("\n", |diff, f|{ format_diff(&diff, f, true)})
                )
            }
            _ => write!(f, "{}", self.error),
        }
    }
}

pub fn format_diff(
    diff: &Change<&str>,
    f: &mut dyn FnMut(&dyn Display) -> std::fmt::Result,
    colorize: bool,
) -> std::fmt::Result {
    match diff.tag() {
        ChangeTag::Equal => {
            f(&diff.value().lines().format_with("\n", |line, f| f(&format_args!("    {line}"))))
        }
        ChangeTag::Insert => f(&diff.value().lines().format_with("\n", |line, f| {
            if colorize {
                f(&format_args!("+   {line}").bright_green())
            } else {
                f(&format_args!("+   {line}"))
            }
        })),
        ChangeTag::Delete => f(&diff.value().lines().format_with("\n", |line, f| {
            if colorize {
                f(&format_args!("-   {line}").bright_red())
            } else {
                f(&format_args!("-   {line}"))
            }
        })),
    }
}

pub fn format_column_diff(expected: &str, actual: &str, colorize: bool) -> String {
    let (expected, actual) = TextDiff::from_chars(expected, actual).iter_all_changes().fold(
        ("".to_string(), "".to_string()),
        |(expected, actual), change| match change.tag() {
            ChangeTag::Equal => {
                (format!("{}{}", expected, change.value()), format!("{}{}", actual, change.value()))
            }
            ChangeTag::Delete => (
                if colorize {
                    format!("{}[{}]", expected, change.value().bright_red())
                } else {
                    format!("{}[{}]", expected, change.value())
                },
                actual,
            ),
            ChangeTag::Insert => (
                expected,
                if colorize {
                    format!("{}[{}]", actual, change.value().bright_green())
                } else {
                    format!("{}[{}]", actual, change.value())
                },
            ),
        },
    );
    format!("[Expected] {expected}\n[Actual  ] {actual}")
}
