//! Query result updating and record output processing.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use rand::Rng;

use crate::output::{RecordOutput, ColumnTypeValidator, Normalizer, Validator};
use crate::parser::*;
use crate::executor::Runner;
use crate::{ColumnType, AsyncDB, MakeConnection};

/// Updates the specified [`Record`] with the [`QueryOutput`] produced
/// by a Database, returning `Some(new_record)`.
///
/// If an update is not supported or not necessary, returns `None`
pub fn update_record_with_output<T: ColumnType>(
    record: &Record<T>,
    record_output: &RecordOutput<T>,
    _col_separator: &str,
    validator: Validator,
    normalizer: Normalizer,
    column_type_validator: ColumnTypeValidator<T>,
) -> Option<Record<T>> {
    match (record.clone(), record_output) {
        (_, RecordOutput::Nothing) => None,
        // statement, query
        (
            Record::Statement {
                sql,
                loc,
                conditions,
                connection,
                expected: mut expected @ (StatementExpect::Ok | StatementExpect::Count(_)),
                retry,
            },
            RecordOutput::Query {
                error: None, rows, ..
            },
        ) => {
            // statement ok
            // SELECT ...
            //
            // This case can be used when we want to only ensure the query succeeds,
            // but don't care about the output.
            // DuckDB has a few of these.

            if let StatementExpect::Count(expected_count) = &mut expected {
                *expected_count = rows.len() as u64;
            }

            Some(Record::Statement {
                sql,
                loc,
                conditions,
                connection,
                expected,
                retry,
            })
        }
        // query, statement
        (
            Record::Query {
                sql,
                loc,
                conditions,
                connection,
                expected: _,
                retry,
            },
            RecordOutput::Statement { error: None, count },
        ) => Some(Record::Statement {
            sql,
            loc,
            conditions,
            connection,
            expected: StatementExpect::Count(*count),
            retry,
        }),
        // statement, statement
        (
            Record::Statement {
                loc,
                conditions,
                connection,
                sql,
                expected,
                retry,
            },
            RecordOutput::Statement { count, error },
        ) => match (error, expected) {
            // Ok
            (None, expected) => Some(Record::Statement {
                sql,
                loc,
                conditions,
                connection,
                expected: match expected {
                    StatementExpect::Count(_) => StatementExpect::Count(*count),
                    StatementExpect::Error(_) | StatementExpect::Ok => StatementExpect::Ok,
                },
                retry,
            }),
            // Error match
            (Some(e), StatementExpect::Error(expected_error))
                if expected_error.is_match(&e.to_string()) =>
            {
                None
            }
            // Error mismatch, update expected error
            (Some(e), r) => {
                let reference = match &r {
                    StatementExpect::Error(e) => Some(e),
                    StatementExpect::Count(_) | StatementExpect::Ok => None,
                };
                Some(Record::Statement {
                    sql,
                    expected: StatementExpect::Error(ExpectedError::from_actual_error(
                        reference,
                        &e.to_string(),
                    )),
                    loc,
                    conditions,
                    connection,
                    retry,
                })
            }
        },
        // query, query
        (
            Record::Query {
                loc,
                conditions,
                connection,
                sql,
                expected,
                retry,
            },
            RecordOutput::Query { types, rows, error },
        ) => match (error, expected) {
            // Error match
            (Some(e), QueryExpect::Error(expected_error))
                if expected_error.is_match(&e.to_string()) =>
            {
                None
            }
            // Error mismatch
            (Some(e), r) => {
                let reference = match &r {
                    QueryExpect::Error(e) => Some(e),
                    QueryExpect::Results { .. } => None,
                };
                Some(Record::Query {
                    sql,
                    expected: QueryExpect::Error(ExpectedError::from_actual_error(
                        reference,
                        &e.to_string(),
                    )),
                    loc,
                    conditions,
                    connection,
                    retry,
                })
            }
            (None, expected) => {
                let results = match &expected {
                    // If validation is successful, we respect the original file's expected results.
                    QueryExpect::Results {
                        results: expected_results,
                        ..
                    } if validator(normalizer, rows, expected_results) => expected_results.clone(),
                    // Flatten the rows so each column value becomes its own line (SQLLogicTest format)
                    _ => rows.iter().flat_map(|cols| cols.iter().cloned()).collect(),
                };
                let types = match &expected {
                    // If validation is successful, we respect the original file's expected types.
                    QueryExpect::Results {
                        types: expected_types,
                        ..
                    } if column_type_validator(types, expected_types) => expected_types.clone(),
                    _ => types.clone(),
                };
                Some(Record::Query {
                    sql,
                    loc,
                    conditions,
                    connection,
                    expected: match expected {
                        QueryExpect::Results {
                            sort_mode,
                            label,
                            result_mode,
                            ..
                        } => QueryExpect::Results {
                            results,
                            types,
                            sort_mode,
                            result_mode,
                            label,
                        },
                        QueryExpect::Error(_) => QueryExpect::Results {
                            results,
                            types,
                            sort_mode: None,
                            result_mode: None,
                            label: None,
                        },
                    },
                    retry,
                })
            }
        },
        (
            Record::System {
                loc,
                conditions,
                command,
                stdout: _,
                retry,
            },
            RecordOutput::System {
                stdout: actual_stdout,
                error,
            },
        ) => {
            if let Some(error) = error {
                tracing::error!(
                    ?error,
                    command,
                    "system command failed while updating the record. It will be unchanged."
                );
            }
            Some(Record::System {
                loc,
                conditions,
                command,
                stdout: actual_stdout.clone(),
                retry,
            })
        }

        // No update possible, return the original record
        _ => None,
    }
}

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Updates a test file with the output produced by a Database. It is an utility function
    /// wrapping [`update_test_file_with_runner`].
    ///
    /// Specifically, it will create `"{filename}.temp"` to buffer the updated records and then
    /// override the original file with it.
    ///
    /// Some other notes:
    /// - empty lines at the end of the file are cleaned.
    /// - `halt` and `include` are correctly handled.
    pub async fn update_test_file(
        &mut self,
        filename: impl AsRef<Path>,
        col_separator: &str,
        validator: Validator,
        normalizer: Normalizer,
        column_type_validator: ColumnTypeValidator<D::ColumnType>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use fs_err::{File, OpenOptions};

        fn create_outfile(filename: impl AsRef<Path>) -> std::io::Result<(PathBuf, File)> {
            let filename = filename.as_ref();
            let outfilename = format!(
                "{}{:010}{}",
                filename.file_name().unwrap().to_str().unwrap().to_owned(),
                rand::rng().random_range(0..10_000_000),
                ".temp"
            );
            let outfilename = filename.parent().unwrap().join(outfilename);
            // create a temp file in read-write mode
            let outfile = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .read(true)
                .open(&outfilename)?;
            Ok((outfilename, outfile))
        }

        fn override_with_outfile(
            filename: &String,
            outfilename: &PathBuf,
            outfile: &mut File,
        ) -> std::io::Result<()> {
            // check whether outfile ends with multiple newlines, which happens if
            // - the last record is statement/query
            // - the original file ends with multiple newlines

            const N: usize = 8;
            let mut buf = [0u8; N];
            loop {
                outfile.seek(SeekFrom::End(-(N as i64))).unwrap();
                outfile.read_exact(&mut buf).unwrap();
                let num_newlines = buf.iter().rev().take_while(|&&b| b == b'\n').count();
                assert!(num_newlines > 0);

                if num_newlines > 1 {
                    // if so, remove the last ones
                    outfile
                        .set_len(outfile.metadata().unwrap().len() - num_newlines as u64 + 1)
                        .unwrap();
                }

                if num_newlines == 1 || num_newlines < N {
                    break;
                }
            }

            outfile.flush()?;
            fs_err::rename(outfilename, filename)?;

            Ok(())
        }

        struct Item {
            filename: String,
            outfilename: PathBuf,
            outfile: File,
            halt: bool,
        }

        let filename = filename.as_ref();
        let records = parse_file(filename)?;

        let (outfilename, outfile) = create_outfile(filename)?;
        let mut stack = vec![Item {
            filename: filename.to_string_lossy().to_string(),
            outfilename,
            outfile,
            halt: false,
        }];

        for record in records {
            let Item {
                filename,
                outfilename,
                outfile,
                halt,
            } = stack.last_mut().unwrap();

            match &record {
                Record::Injected(Injected::BeginInclude(filename)) => {
                    let (outfilename, outfile) = create_outfile(filename)?;
                    stack.push(Item {
                        filename: filename.clone(),
                        outfilename,
                        outfile,
                        halt: false,
                    });
                }
                Record::Injected(Injected::EndInclude(_)) => {
                    override_with_outfile(filename, outfilename, outfile)?;
                    stack.pop();
                }
                _ => {
                    if *halt {
                        writeln!(outfile, "{record}")?;
                        continue;
                    }
                    if matches!(record, Record::Halt { .. }) {
                        *halt = true;
                        writeln!(outfile, "{record}")?;
                        tracing::info!(
                            "halt record found, all following records will be written AS IS"
                        );
                        continue;
                    }
                    let record_output = self.apply_record(record.clone()).await;
                    let record = update_record_with_output(
                        &record,
                        &record_output,
                        col_separator,
                        validator,
                        normalizer,
                        column_type_validator,
                    )
                    .unwrap_or(record);
                    writeln!(outfile, "{record}")?;
                }
            }
        }

        let Item {
            filename,
            outfilename,
            outfile,
            halt: _,
        } = stack.last_mut().unwrap();
        override_with_outfile(filename, outfilename, outfile)?;

        Ok(())
    }
}
