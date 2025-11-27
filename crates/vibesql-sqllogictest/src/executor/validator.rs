//! Result validation and comparison logic.

use std::sync::Arc;

use itertools::Itertools;

use crate::{MakeConnection, ColumnType};
use crate::error_handling::{TestError, TestErrorKind, RecordKind};
use crate::output::RecordOutput;
use crate::parser::*;
use super::core::{AsyncDB, Runner};

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Run a single record without retry.
    pub(super) async fn run_async_no_retry(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        let result = self.apply_record(record.clone()).await;

        // Helper to check if this record type should be tracked for dialect stats
        let is_testable_record = matches!(
            &record,
            Record::Statement { .. } | Record::Query { .. }
        );

        // Track dialect statistics for Statement and Query records
        // We capture the current dialect before validation since it may have been switched
        let current_dialect = self.current_dialect.clone();

        match (record, &result) {
            (_, RecordOutput::Nothing) => {
                // Skipped or control records - don't track
            }
            // Tolerate the mismatched return type...
            (
                Record::Statement {
                    sql, expected, loc, ..
                },
                RecordOutput::Query {
                    error: None, rows, ..
                },
            ) => {
                if let StatementExpect::Error(_) = expected {
                    self.dialect_stats.record(&current_dialect, false);
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Query,
                    }
                    .at(loc));
                }
                if let StatementExpect::Count(expected_count) = expected {
                    if expected_count != rows.len() as u64 {
                        self.dialect_stats.record(&current_dialect, false);
                        return Err(TestErrorKind::StatementResultMismatch {
                            sql,
                            expected: expected_count,
                            actual: format!("returned {} rows", rows.len()),
                        }
                        .at(loc));
                    }
                }
            }
            (
                Record::Query {
                    loc, sql, expected, ..
                },
                RecordOutput::Statement { error: None, .. },
            ) => match expected {
                QueryExpect::Error(_) => {
                    self.dialect_stats.record(&current_dialect, false);
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Query,
                    }
                    .at(loc))
                }
                QueryExpect::Results { results, .. } if !results.is_empty() => {
                    self.dialect_stats.record(&current_dialect, false);
                    return Err(TestErrorKind::QueryResultMismatch {
                        sql,
                        expected: results.join("\n"),
                        actual: "".to_string(),
                    }
                    .at(loc))
                }
                QueryExpect::Results { .. } => {}
            },
            (
                Record::Statement {
                    loc,
                    connection: _,
                    conditions: _,
                    sql,
                    expected,
                    retry: _,
                },
                RecordOutput::Statement { count, error },
            ) => match (error, expected) {
                (None, StatementExpect::Error(_)) => {
                    self.dialect_stats.record(&current_dialect, false);
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Statement,
                    }
                    .at(loc))
                }
                (None, StatementExpect::Count(expected_count)) => {
                    if expected_count != *count {
                        self.dialect_stats.record(&current_dialect, false);
                        return Err(TestErrorKind::StatementResultMismatch {
                            sql,
                            expected: expected_count,
                            actual: format!("affected {count} rows"),
                        }
                        .at(loc));
                    }
                }
                (None, StatementExpect::Ok) => {}
                (Some(e), StatementExpect::Error(expected_error)) => {
                    if !expected_error.is_match(&e.to_string()) {
                        self.dialect_stats.record(&current_dialect, false);
                        return Err(TestErrorKind::ErrorMismatch {
                            sql,
                            err: Arc::clone(e),
                            expected_err: expected_error.to_string(),
                            kind: RecordKind::Statement,
                        }
                        .at(loc));
                    }
                }
                (Some(e), StatementExpect::Count(_) | StatementExpect::Ok) => {
                    self.dialect_stats.record(&current_dialect, false);
                    return Err(TestErrorKind::Fail {
                        sql,
                        err: Arc::clone(e),
                        kind: RecordKind::Statement,
                    }
                    .at(loc));
                }
            },
            (
                Record::Query {
                    loc,
                    conditions: _,
                    connection: _,
                    sql,
                    expected,
                    retry: _,
                },
                RecordOutput::Query { types, rows, error },
            ) => {
                match (error, expected) {
                    (None, QueryExpect::Error(_)) => {
                        self.dialect_stats.record(&current_dialect, false);
                        return Err(TestErrorKind::Ok {
                            sql,
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                    (Some(e), QueryExpect::Error(expected_error)) => {
                        if !expected_error.is_match(&e.to_string()) {
                            self.dialect_stats.record(&current_dialect, false);
                            return Err(TestErrorKind::ErrorMismatch {
                                sql,
                                err: Arc::clone(e),
                                expected_err: expected_error.to_string(),
                                kind: RecordKind::Query,
                            }
                            .at(loc));
                        }
                    }
                    (Some(e), QueryExpect::Results { .. }) => {
                        self.dialect_stats.record(&current_dialect, false);
                        return Err(TestErrorKind::Fail {
                            sql,
                            err: Arc::clone(e),
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                    (
                        None,
                        QueryExpect::Results {
                            types: expected_types,
                            results: expected_results,
                            ..
                        },
                    ) => {
                        if !(self.column_type_validator)(types, &expected_types) {
                            self.dialect_stats.record(&current_dialect, false);
                            return Err(TestErrorKind::QueryResultColumnsMismatch {
                                sql,
                                expected: expected_types.iter().map(|c| c.to_char()).join(""),
                                actual: types.iter().map(|c| c.to_char()).join(""),
                            }
                            .at(loc));
                        }

                        let actual_results = match self.result_mode {
                            Some(ResultMode::ValueWise) => rows
                                .iter()
                                .flat_map(|strs| strs.iter())
                                .map(|str| vec![str.to_string()])
                                .collect_vec(),
                            // default to rowwise
                            _ => rows.clone(),
                        };

                        if !(self.validator)(self.normalizer, &actual_results, &expected_results) {
                            // Flatten the rows so each column value is on its own line for error reporting
                            let output_rows: Vec<String> =
                                rows.iter().flat_map(|strs| strs.iter().cloned()).collect_vec();
                            self.dialect_stats.record(&current_dialect, false);
                            return Err(TestErrorKind::QueryResultMismatch {
                                sql,
                                expected: expected_results.join("\n"),
                                actual: output_rows.join("\n"),
                            }
                            .at(loc));
                        }
                    }
                };
            }
            (
                Record::System {
                    loc,
                    conditions: _,
                    command,
                    stdout: expected_stdout,
                    retry: _,
                },
                RecordOutput::System {
                    error,
                    stdout: actual_stdout,
                },
            ) => {
                if let Some(err) = error {
                    return Err(TestErrorKind::SystemFail {
                        command,
                        err: Arc::clone(err),
                    }
                    .at(loc));
                }
                match (expected_stdout, actual_stdout) {
                    (None, _) => {}
                    (Some(expected_stdout), actual_stdout) => {
                        let actual_stdout = actual_stdout.clone().unwrap_or_default();
                        // TODO: support newlines contained in expected_stdout
                        if expected_stdout != actual_stdout.trim() {
                            return Err(TestErrorKind::SystemStdoutMismatch {
                                command,
                                expected_stdout,
                                actual_stdout,
                            }
                            .at(loc));
                        }
                    }
                }
            }
            _ => unreachable!(),
        }

        // Record successful test execution in dialect stats
        // Only track Statement and Query records (is_testable_record is true for those)
        // RecordOutput::Nothing means the record was skipped, so we don't track it
        if is_testable_record && !matches!(result, RecordOutput::Nothing) {
            self.dialect_stats.record(&current_dialect, true);
        }

        Ok(result)
    }
}

// Public function called from core.rs
pub(super) async fn run_async_no_retry<D: AsyncDB, M: MakeConnection<Conn = D>>(
    runner: &mut Runner<D, M>,
    record: Record<D::ColumnType>,
) -> Result<RecordOutput<D::ColumnType>, TestError> {
    runner.run_async_no_retry(record).await
}
