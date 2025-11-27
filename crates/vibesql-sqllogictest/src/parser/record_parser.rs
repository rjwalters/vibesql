//! Parsing of individual SQL record types (statement, query, system).

#![allow(clippy::while_let_loop)]

use std::fmt;
use std::iter::Peekable;

use itertools::Itertools;

use crate::{ColumnType, Location, ParseError, ParseErrorKind};
use crate::error_parser::ExpectedError;
use crate::retry_parser::RetryConfig;
use crate::directive_parser::{SortMode, ResultMode, ControlItem};

const RESULTS_DELIMITER: &str = "----";

/// Check if a line is likely a directive (not SQL content).
/// Directives start with known keywords like "statement", "query", "include", etc.
fn is_directive_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("statement ")
        || trimmed.starts_with("query ")
        || trimmed.starts_with("system ")
        || trimmed.starts_with("include ")
        || trimmed.starts_with("halt")
        || trimmed.starts_with("subtest ")
        || trimmed.starts_with("sleep ")
        || trimmed.starts_with("skipif ")
        || trimmed.starts_with("onlyif ")
        || trimmed.starts_with("connection ")
        || trimmed.starts_with("hash-threshold ")
        || trimmed.starts_with("control ")
        || trimmed.starts_with("#")
}

/// Expectation for a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementExpect {
    /// Statement should succeed.
    Ok,
    /// Statement should succeed and affect the given number of rows.
    Count(u64),
    /// Statement should fail with the given error message.
    Error(ExpectedError),
}

/// Expectation for a query.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpect<T: ColumnType> {
    /// Query should succeed and return the given results.
    Results {
        types: Vec<T>,
        sort_mode: Option<SortMode>,
        result_mode: Option<ResultMode>,
        label: Option<String>,
        results: Vec<String>,
    },
    /// Query should fail with the given error message.
    Error(ExpectedError),
}

impl<T: ColumnType> QueryExpect<T> {
    /// Creates a new [`QueryExpect`] with empty results.
    pub(crate) fn empty_results() -> Self {
        Self::Results {
            types: Vec::new(),
            sort_mode: None,
            result_mode: None,
            label: None,
            results: Vec::new(),
        }
    }
}

/// Parse one or more lines until empty line or a delimiter.
pub(crate) fn parse_lines<'a>(
    lines: &mut Peekable<impl Iterator<Item = (usize, &'a str)>>,
    loc: &Location,
    delimiter: Option<&str>,
) -> Result<(String, bool), ParseError> {
    let mut found_delimiter = false;
    let mut out = match lines.next() {
        Some((_, line)) => Ok(line.into()),
        None => Err(ParseErrorKind::UnexpectedEOF.at(loc.clone().next_line())),
    }?;

    loop {
        // Peek at the next line without consuming it
        let next_line = match lines.peek() {
            Some((_, line)) => *line,
            None => break, // End of file
        };

        if next_line.is_empty() {
            break;
        }
        if let Some(delimiter) = delimiter {
            if next_line == delimiter {
                found_delimiter = true;
                lines.next(); // Consume the delimiter
                break;
            }
        }
        // Stop if we encounter a directive line (e.g., "statement ok" without blank line separator)
        // This prevents consuming the next directive as part of the current SQL statement
        if is_directive_line(next_line) {
            break; // Don't consume the directive line - leave it for the main parser
        }

        // Now consume the line since we've decided to include it
        let (_, line) = lines.next().unwrap();
        out += "\n";
        out += line;
    }

    Ok((out, found_delimiter))
}

/// Parse multiline output under `----`.
pub(crate) fn parse_multiple_result<'a>(
    lines: &mut Peekable<impl Iterator<Item = (usize, &'a str)>>,
) -> String {
    let mut results = String::new();

    while let Some((_, line)) = lines.next() {
        // 2 consecutive empty lines
        if line.is_empty() && lines.peek().map(|(_, l)| l.is_empty()).unwrap_or(true) {
            lines.next();
            break;
        }
        results += line;
        results.push('\n');
    }

    results.trim().to_string()
}

/// Parse multiline error message under `----`.
pub(crate) fn parse_multiline_error<'a>(
    lines: &mut Peekable<impl Iterator<Item = (usize, &'a str)>>,
) -> ExpectedError {
    ExpectedError::Multiline(parse_multiple_result(lines))
}

// Format functions for unparsing records

/// Unparse a statement record.
pub(crate) fn fmt_statement(
    f: &mut fmt::Formatter<'_>,
    sql: &str,
    expected: &StatementExpect,
    retry: &Option<RetryConfig>,
) -> fmt::Result {
    write!(f, "statement ")?;
    match expected {
        StatementExpect::Ok => write!(f, "ok")?,
        StatementExpect::Count(cnt) => write!(f, "count {cnt}")?,
        StatementExpect::Error(err) => err.fmt_inline(f)?,
    }
    if let Some(retry) = retry {
        write!(
            f,
            " retry {} backoff {}",
            retry.attempts,
            humantime::format_duration(retry.backoff)
        )?;
    }
    writeln!(f)?;
    // statement always end with a blank line
    writeln!(f, "{sql}")?;

    if let StatementExpect::Error(err) = expected {
        err.fmt_multiline(f, RESULTS_DELIMITER)?;
    }
    Ok(())
}

/// Unparse a query record.
pub(crate) fn fmt_query<T: ColumnType>(
    f: &mut fmt::Formatter<'_>,
    sql: &str,
    expected: &QueryExpect<T>,
    retry: &Option<RetryConfig>,
) -> fmt::Result {
    write!(f, "query ")?;
    match expected {
        QueryExpect::Results {
            types,
            sort_mode,
            label,
            ..
        } => {
            write!(f, "{}", types.iter().map(|c| c.to_char()).join(""))?;
            if let Some(sort_mode) = sort_mode {
                write!(f, " {}", sort_mode.as_str())?;
            }
            if let Some(label) = label {
                write!(f, " {label}")?;
            }
        }
        QueryExpect::Error(err) => err.fmt_inline(f)?,
    }
    if let Some(retry) = retry {
        write!(
            f,
            " retry {} backoff {}",
            retry.attempts,
            humantime::format_duration(retry.backoff)
        )?;
    }
    writeln!(f)?;
    writeln!(f, "{sql}")?;

    match expected {
        QueryExpect::Results { results, .. } => {
            write!(f, "{}", RESULTS_DELIMITER)?;
            for result in results {
                write!(f, "\n{result}")?;
            }

            // query always ends with a blank line
            writeln!(f)?
        }
        QueryExpect::Error(err) => err.fmt_multiline(f, RESULTS_DELIMITER)?,
    }
    Ok(())
}

/// Unparse a system record.
pub(crate) fn fmt_system(
    f: &mut fmt::Formatter<'_>,
    command: &str,
    stdout: &Option<String>,
    retry: &Option<RetryConfig>,
) -> fmt::Result {
    writeln!(f, "system ok\n{command}")?;
    if let Some(retry) = retry {
        write!(
            f,
            " retry {} backoff {}",
            retry.attempts,
            humantime::format_duration(retry.backoff)
        )?;
    }
    if let Some(stdout) = stdout {
        writeln!(f, "----\n{}\n", stdout.trim())?;
    }
    Ok(())
}
