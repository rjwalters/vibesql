//! Core parsing functions for sqllogictest.

use std::path::Path;
use itertools::Itertools;

use crate::ColumnType;
use super::location::Location;
use super::error_parser::ExpectedError;
use super::retry_parser::parse_retry_config;
use super::directive_parser::{Control, Condition, Connection, SortMode, ResultMode, ControlItem};
use super::record_parser::{StatementExpect, QueryExpect, parse_lines, parse_multiple_result, parse_multiline_error};
use super::records::{Record, Injected};
use super::{ParseError, ParseErrorKind};

const RESULTS_DELIMITER: &str = "----";

/// Parse a sqllogictest script into a list of records.
pub fn parse<T: ColumnType>(script: &str) -> Result<Vec<Record<T>>, ParseError> {
    parse_inner(&Location::new("<unknown>", 0), script)
}

/// Parse a sqllogictest script into a list of records with a given script name.
pub fn parse_with_name<T: ColumnType>(
    script: &str,
    name: impl Into<std::sync::Arc<str>>,
) -> Result<Vec<Record<T>>, ParseError> {
    parse_inner(&Location::new(name, 0), script)
}

#[allow(clippy::collapsible_match)]
fn parse_inner<T: ColumnType>(loc: &Location, script: &str) -> Result<Vec<Record<T>>, ParseError> {
    let mut lines = script.lines().enumerate().peekable();
    let mut records = vec![];
    let mut conditions = vec![];
    let mut connection = Connection::Default;
    let mut comments = vec![];

    while let Some((num, line)) = lines.next() {
        if let Some(text) = line.strip_prefix('#') {
            comments.push(text.to_string());
            if lines.peek().is_none() {
                // Special handling for the case where the last line is a comment.
                records.push(Record::Comment(comments));
                break;
            }
            continue;
        }
        if !comments.is_empty() {
            records.push(Record::Comment(comments));
            comments = vec![];
        }

        if line.is_empty() {
            records.push(Record::Newline);
            continue;
        }

        let mut loc = loc.clone();
        loc.line = num as u32 + 1;

        // Strip inline comments (lines starting with # are already handled above)
        let line_without_comment = if let Some(comment_pos) = line.find('#') {
            // Only treat # as a comment if it's preceded by whitespace
            // This prevents treating # inside other contexts as a comment marker
            if line[..comment_pos].ends_with(|c: char| c.is_whitespace()) {
                &line[..comment_pos]
            } else {
                line
            }
        } else {
            line
        };

        let tokens: Vec<&str> = line_without_comment.split_whitespace().collect();
        match tokens.as_slice() {
            [] => continue,
            ["include", included] => records.push(Record::Include {
                loc,
                filename: included.to_string(),
            }),
            ["halt"] => {
                records.push(Record::Halt { loc });
            }
            ["subtest", name] => {
                records.push(Record::Subtest {
                    loc,
                    name: name.to_string(),
                });
            }
            ["sleep", dur] => {
                records.push(Record::Sleep {
                    duration: humantime::parse_duration(dur).map_err(|_| {
                        ParseErrorKind::InvalidDuration(dur.to_string()).at(loc.clone())
                    })?,
                    loc,
                });
            }
            ["dialect", mode] => {
                records.push(Record::Dialect {
                    loc,
                    mode: mode.to_string(),
                });
            }
            ["skipif", label] => {
                let cond = Condition::SkipIf {
                    label: label.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["onlyif", label] => {
                let cond = Condition::OnlyIf {
                    label: label.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["connection", name] => {
                let conn = Connection::new(name);
                connection = conn.clone();
                records.push(Record::Connection(conn));
            }
            ["statement", res @ ..] => {
                let (mut expected, res) = match res {
                    ["ok", retry @ ..] => (StatementExpect::Ok, retry),
                    ["error", res @ ..] => {
                        if res.len() == 4 && res[0] == "retry" && res[2] == "backoff" {
                            // `statement error retry <num> backoff <duration>`
                            // To keep syntax simple, let's assume the error message must be multiline.
                            (StatementExpect::Error(ExpectedError::Empty), res)
                        } else {
                            let error = ExpectedError::parse_inline_tokens(res)
                                .map_err(|e| e.at(loc.clone()))?;
                            (StatementExpect::Error(error), &[][..])
                        }
                    }
                    ["count", count_str, retry @ ..] => {
                        let count = count_str.parse::<u64>().map_err(|_| {
                            ParseErrorKind::InvalidNumber((*count_str).into()).at(loc.clone())
                        })?;
                        (StatementExpect::Count(count), retry)
                    }
                    _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
                };

                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                let (sql, has_results) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;

                if has_results {
                    if let StatementExpect::Error(e) = &mut expected {
                        // If no inline error message is specified, it might be a multiline error.
                        if e.is_empty() {
                            *e = parse_multiline_error(&mut lines);
                        } else {
                            return Err(ParseErrorKind::DuplicatedErrorMessage.at(loc.clone()));
                        }
                    } else {
                        return Err(ParseErrorKind::StatementHasResults.at(loc.clone()));
                    }
                }

                records.push(Record::Statement {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    sql,
                    expected,
                    retry,
                });
            }
            ["query", res @ ..] => {
                let (mut expected, res) = match res {
                    ["error", res @ ..] => {
                        if res.len() == 4 && res[0] == "retry" && res[2] == "backoff" {
                            // `query error retry <num> backoff <duration>`
                            // To keep syntax simple, let's assume the error message must be multiline.
                            (QueryExpect::Error(ExpectedError::Empty), res)
                        } else {
                            let error = ExpectedError::parse_inline_tokens(res)
                                .map_err(|e| e.at(loc.clone()))?;
                            (QueryExpect::Error(error), &[][..])
                        }
                    }
                    [type_str, res @ ..] => {
                        // query <type-string> [<sort-mode>] [<label>] [retry <attempts> backoff <backoff>]
                        let types = type_str
                            .chars()
                            .map(|ch| {
                                T::from_char(ch)
                                    .ok_or_else(|| ParseErrorKind::InvalidType(ch).at(loc.clone()))
                            })
                            .try_collect()?;
                        let sort_mode = res.first().and_then(|&s| SortMode::try_from_str(s).ok()); // Could be `retry` or label

                        // To support `retry`, we assume the label must *not* be "retry"
                        let label_start = if sort_mode.is_some() { 1 } else { 0 };
                        let res = &res[label_start..];
                        let label = res.first().and_then(|&s| {
                            if s != "retry" {
                                Some(s.to_owned())
                            } else {
                                None // `retry` is not a valid label
                            }
                        });

                        let retry_start = if label.is_some() { 1 } else { 0 };
                        let res = &res[retry_start..];
                        (
                            QueryExpect::Results {
                                types,
                                sort_mode,
                                result_mode: None,
                                label,
                                results: Vec::new(),
                            },
                            res,
                        )
                    }
                    [] => (QueryExpect::empty_results(), &[][..]),
                };

                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                // The SQL for the query is found on second and subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let (sql, has_result) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                if has_result {
                    match &mut expected {
                        // Lines following the "----" are expected results of the query, one value
                        // per line.
                        QueryExpect::Results { results, .. } => {
                            for (_, line) in &mut lines {
                                if line.is_empty() {
                                    break;
                                }
                                results.push(line.to_string());
                            }
                        }
                        // If no inline error message is specified, it might be a multiline error.
                        QueryExpect::Error(e) => {
                            if e.is_empty() {
                                *e = parse_multiline_error(&mut lines);
                            } else {
                                return Err(ParseErrorKind::DuplicatedErrorMessage.at(loc.clone()));
                            }
                        }
                    }
                }
                records.push(Record::Query {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    connection: std::mem::take(&mut connection),
                    sql,
                    expected,
                    retry,
                });
            }
            ["system", "ok", res @ ..] => {
                let retry = parse_retry_config(res).map_err(|e| e.at(loc.clone()))?;

                // TODO: we don't support asserting error message for system command
                // The command is found on second and subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let (command, has_result) = parse_lines(&mut lines, &loc, Some(RESULTS_DELIMITER))?;
                let stdout = if has_result {
                    Some(parse_multiple_result(&mut lines))
                } else {
                    None
                };
                records.push(Record::System {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    command,
                    stdout,
                    retry,
                });
            }
            ["control", res @ ..] => match res {
                ["resultmode", result_mode] => match ResultMode::try_from_str(result_mode) {
                    Ok(result_mode) => {
                        records.push(Record::Control(Control::ResultMode(result_mode)))
                    }
                    Err(k) => return Err(k.at(loc)),
                },
                ["sortmode", sort_mode] => match SortMode::try_from_str(sort_mode) {
                    Ok(sort_mode) => records.push(Record::Control(Control::SortMode(sort_mode))),
                    Err(k) => return Err(k.at(loc)),
                },
                ["substitution", on_off] => match bool::try_from_str(on_off) {
                    Ok(on_off) => records.push(Record::Control(Control::Substitution(on_off))),
                    Err(k) => return Err(k.at(loc)),
                },
                _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
            },
            ["hash-threshold", threshold] => {
                records.push(Record::HashThreshold {
                    loc: loc.clone(),
                    threshold: threshold.parse::<u64>().map_err(|_| {
                        ParseErrorKind::InvalidNumber((*threshold).into()).at(loc.clone())
                    })?,
                });
            }
            _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
        }
    }
    Ok(records)
}

/// Parse a sqllogictest file. The included scripts are inserted after the `include` record.
pub fn parse_file<T: ColumnType>(filename: impl AsRef<Path>) -> Result<Vec<Record<T>>, ParseError> {
    let filename = filename.as_ref().to_str().unwrap();
    parse_file_inner(Location::new(filename, 0))
}

fn parse_file_inner<T: ColumnType>(loc: Location) -> Result<Vec<Record<T>>, ParseError> {
    let path = Path::new(loc.file());
    if !path.exists() {
        return Err(ParseErrorKind::FileNotFound.at(loc.clone()));
    }
    let script = std::fs::read_to_string(path).unwrap();
    let mut records = vec![];
    for rec in parse_inner(&loc, &script)? {
        records.push(rec.clone());

        if let Record::Include { filename, loc } = rec {
            let complete_filename = {
                let mut path_buf = path.to_path_buf();
                path_buf.pop();
                path_buf.push(filename.clone());
                path_buf.as_os_str().to_string_lossy().to_string()
            };

            let mut iter = glob::glob(&complete_filename)
                .map_err(|e| ParseErrorKind::InvalidIncludeFile(e.to_string()).at(loc.clone()))?
                .peekable();
            if iter.peek().is_none() {
                return Err(ParseErrorKind::EmptyIncludeFile(filename).at(loc.clone()));
            }
            for included_file in iter {
                let included_file = included_file.map_err(|e| {
                    ParseErrorKind::InvalidIncludeFile(e.to_string()).at(loc.clone())
                })?;
                let included_file = included_file.as_os_str().to_string_lossy().to_string();

                records.push(Record::Injected(Injected::BeginInclude(
                    included_file.clone(),
                )));
                records.extend(parse_file_inner(loc.include(&included_file))?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultColumnType;
    use crate::directive_parser::Condition;

    #[test]
    fn test_skipif_with_inline_comment() {
        // Test that skipif mysql # comment is parsed correctly
        let script = r#"skipif mysql # not compatible
query I
SELECT 1
----
1
"#;
        let records: Vec<Record<DefaultColumnType>> = parse(script).unwrap();

        // Should have: Condition(SkipIf{mysql}), Query{...}
        let mut found_skipif = false;
        for record in &records {
            if let Record::Condition(Condition::SkipIf { label }) = record {
                assert_eq!(label, "mysql");
                found_skipif = true;
            }
        }
        assert!(found_skipif, "Should have parsed skipif mysql correctly");
    }

    #[test]
    fn test_onlyif_with_inline_comment() {
        // Test that onlyif mysql # comment is parsed correctly
        let script = r#"onlyif mysql # MySQL-specific syntax
statement ok
SELECT 1
"#;
        let records: Vec<Record<DefaultColumnType>> = parse(script).unwrap();

        let mut found_onlyif = false;
        for record in &records {
            if let Record::Condition(Condition::OnlyIf { label }) = record {
                assert_eq!(label, "mysql");
                found_onlyif = true;
            }
        }
        assert!(found_onlyif, "Should have parsed onlyif mysql correctly");
    }

    #[test]
    fn test_multiple_skipif_with_comments() {
        // Test multiple skipif directives as found in real test files
        let script = r#"skipif mysql # not compatible
skipif postgresql # PostgreSQL requires AS
query I
SELECT 1
----
1
"#;
        let records: Vec<Record<DefaultColumnType>> = parse(script).unwrap();

        let mut mysql_skipif = false;
        let mut postgresql_skipif = false;
        for record in &records {
            if let Record::Condition(Condition::SkipIf { label }) = record {
                match label.as_str() {
                    "mysql" => mysql_skipif = true,
                    "postgresql" => postgresql_skipif = true,
                    _ => {}
                }
            }
        }
        assert!(mysql_skipif, "Should have parsed skipif mysql");
        assert!(postgresql_skipif, "Should have parsed skipif postgresql");
    }

    #[test]
    fn test_dialect_mysql() {
        let script = r#"dialect mysql

query I
SELECT 5 DIV 2
----
2
"#;
        let records: Vec<Record<DefaultColumnType>> = parse(script).unwrap();

        let mut found_dialect = false;
        for record in &records {
            if let Record::Dialect { mode, .. } = record {
                assert_eq!(mode, "mysql");
                found_dialect = true;
            }
        }
        assert!(found_dialect, "Should have parsed dialect mysql");
    }

    #[test]
    fn test_dialect_sqlite() {
        let script = r#"dialect sqlite

query I
SELECT 5 / 2
----
2
"#;
        let records: Vec<Record<DefaultColumnType>> = parse(script).unwrap();

        let mut found_dialect = false;
        for record in &records {
            if let Record::Dialect { mode, .. } = record {
                assert_eq!(mode, "sqlite");
                found_dialect = true;
            }
        }
        assert!(found_dialect, "Should have parsed dialect sqlite");
    }

    #[test]
    fn test_multiple_dialect_switches() {
        let script = r#"dialect mysql

query I
SELECT 1
----
1

dialect sqlite

query I
SELECT 2
----
2

dialect mysql

query I
SELECT 3
----
3
"#;
        let records: Vec<Record<DefaultColumnType>> = parse(script).unwrap();

        let dialects: Vec<&str> = records
            .iter()
            .filter_map(|r| {
                if let Record::Dialect { mode, .. } = r {
                    Some(mode.as_str())
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(dialects, vec!["mysql", "sqlite", "mysql"]);
    }

    #[test]
    fn test_dialect_display() {
        let script = "dialect mysql";
        let records: Vec<Record<DefaultColumnType>> = parse(script).unwrap();

        let dialect_record = records.iter().find(|r| matches!(r, Record::Dialect { .. }));
        assert!(dialect_record.is_some());
        assert_eq!(format!("{}", dialect_record.unwrap()), "dialect mysql");
    }
}
