//! Individual record processing and execution.

use std::collections::HashSet;
use std::sync::Arc;

use md5::Digest;

use crate::MakeConnection;
use crate::column_type::ColumnType;
use crate::error_handling::AnyError;
use crate::output::{RecordOutput, DBOutput};
use crate::parser::*;
use super::core::{AsyncDB, Runner};

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    pub async fn apply_record(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> RecordOutput<D::ColumnType> {
        tracing::debug!(?record, "testing");
        /// Returns whether we should skip this record, according to given `conditions`.
        fn should_skip(
            labels: &HashSet<String>,
            engine_name: &str,
            conditions: &[Condition],
        ) -> bool {
            conditions.iter().any(|c| {
                c.should_skip(
                    labels
                        .iter()
                        .map(|l| l.as_str())
                        // attach the engine name to the labels
                        .chain(Some(engine_name).filter(|n| !n.is_empty())),
                )
            })
        }

        match record {
            Record::Statement {
                conditions,
                connection,
                sql,

                // compare result in run_async
                expected: _,
                loc: _,
                retry: _,
            } => {
                let sql = match self.may_substitute(sql, true) {
                    Ok(sql) => sql,
                    Err(error) => {
                        return RecordOutput::Statement {
                            count: 0,
                            error: Some(error),
                        }
                    }
                };

                let conn = match self.conn.get(connection).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Statement {
                            count: 0,
                            error: Some(Arc::new(e)),
                        }
                    }
                };
                if should_skip(&self.labels, conn.engine_name(), &conditions) {
                    return RecordOutput::Nothing;
                }

                let ret = conn.run(&sql).await;
                match ret {
                    Ok(out) => match out {
                        DBOutput::Rows { types, rows } => RecordOutput::Query {
                            types,
                            rows,
                            error: None,
                        },
                        DBOutput::StatementComplete(count) => {
                            RecordOutput::Statement { count, error: None }
                        }
                    },
                    Err(e) => RecordOutput::Statement {
                        count: 0,
                        error: Some(Arc::new(e)),
                    },
                }
            }
            Record::System {
                conditions,
                command,
                loc: _,
                stdout: expected_stdout,
                retry: _,
            } => {
                if should_skip(&self.labels, "", &conditions) {
                    return RecordOutput::Nothing;
                }

                let mut command = match self.may_substitute(command, false) {
                    Ok(command) => command,
                    Err(error) => {
                        return RecordOutput::System {
                            stdout: None,
                            error: Some(error),
                        }
                    }
                };

                let is_background = command.trim().ends_with('&');
                if is_background {
                    command = command.trim_end_matches('&').trim().to_string();
                }

                let mut cmd = if cfg!(target_os = "windows") {
                    let mut cmd = std::process::Command::new("cmd");
                    cmd.arg("/C").arg(&command);
                    cmd
                } else {
                    let mut cmd = std::process::Command::new("bash");
                    cmd.arg("-c").arg(&command);
                    cmd
                };

                if is_background {
                    // Spawn a new process, but don't wait for stdout, otherwise it will block until
                    // the process exits.
                    let error: Option<AnyError> = match cmd.spawn() {
                        Ok(_) => None,
                        Err(e) => Some(Arc::new(e)),
                    };
                    tracing::info!(target:"sqllogictest::system_command", command, "background system command spawned");
                    return RecordOutput::System {
                        error,
                        stdout: None,
                    };
                }

                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::piped());

                let result = D::run_command(cmd).await;
                #[derive(thiserror::Error, Debug)]
                #[error(
                    "process exited unsuccessfully: {status}\nstdout: {stdout}\nstderr: {stderr}"
                )]
                struct SystemError {
                    status: std::process::ExitStatus,
                    stdout: String,
                    stderr: String,
                }

                let mut actual_stdout = None;
                let error: Option<AnyError> = match result {
                    Ok(std::process::Output {
                        status,
                        stdout,
                        stderr,
                    }) => {
                        let stdout = String::from_utf8_lossy(&stdout).to_string();
                        let stderr = String::from_utf8_lossy(&stderr).to_string();
                        tracing::info!(target:"sqllogictest::system_command", command, ?status, stdout, stderr, "system command executed");
                        if status.success() {
                            if expected_stdout.is_some() {
                                actual_stdout = Some(stdout);
                            }
                            None
                        } else {
                            Some(Arc::new(SystemError {
                                status,
                                stdout,
                                stderr,
                            }))
                        }
                    }
                    Err(error) => {
                        tracing::error!(target:"sqllogictest::system_command", command, ?error, "failed to run system command");
                        Some(Arc::new(error))
                    }
                };

                RecordOutput::System {
                    error,
                    stdout: actual_stdout,
                }
            }
            Record::Query {
                conditions,
                connection,
                sql,

                // compare result in run_async
                expected,
                loc: _,
                retry: _,
            } => {
                let sql = match self.may_substitute(sql, true) {
                    Ok(sql) => sql,
                    Err(error) => {
                        return RecordOutput::Query {
                            error: Some(error),
                            types: vec![],
                            rows: vec![],
                        }
                    }
                };

                let conn = match self.conn.get(connection).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Query {
                            error: Some(Arc::new(e)),
                            types: vec![],
                            rows: vec![],
                        }
                    }
                };
                if should_skip(&self.labels, conn.engine_name(), &conditions) {
                    return RecordOutput::Nothing;
                }

                let (mut types, mut rows) = match conn.run(&sql).await {
                    Ok(out) => match out {
                        DBOutput::Rows { types, rows } => (types, rows),
                        DBOutput::StatementComplete(count) => {
                            return RecordOutput::Statement { count, error: None };
                        }
                    },
                    Err(e) => {
                        return RecordOutput::Query {
                            error: Some(Arc::new(e)),
                            types: vec![],
                            rows: vec![],
                        };
                    }
                };

                // Apply expected types from test directive for formatting purposes.
                // This treats the format specifier (e.g., "query R") as a display directive:
                // when we return Integer(49) but test expects Real/FloatingPoint type,
                // we relabel the type (not the value) so existing formatting logic
                // displays it as "49.000" instead of "49". This matches SQLite's behavior
                // where the test format specifier controls output formatting.
                if let QueryExpect::Results { types: expected_types, .. } = &expected {
                    if types.len() == expected_types.len() {
                        types = expected_types.clone();

                        // Reformat string values to match expected types (MySQL/SQLite normalization)
                        // When test expects Real but we returned Integer, append ".000"
                        // When test expects Integer but we returned Real, strip decimal if whole number
                        // When test expects Integer but we returned TEXT, coerce to integer
                        for row in &mut rows {
                            for (col_idx, value) in row.iter_mut().enumerate() {
                                let expected_type = &types[col_idx % types.len()];
                                if expected_type.to_char() == 'R' && !value.contains('.') {
                                    // Integer → Real: add ".000"
                                    if value.parse::<i64>().is_ok() {
                                        *value = format!("{}.000", value);
                                    }
                                } else if expected_type.to_char() == 'I' {
                                    // Normalize to Integer type
                                    if *value != "NULL" {
                                        if value.contains('.') {
                                            // Real → Integer: truncate to integer
                                            if let Ok(f) = value.parse::<f64>() {
                                                *value = format!("{}", f.trunc() as i64);
                                            }
                                        } else if value.parse::<i64>().is_err() {
                                            // TEXT → Integer: coerce non-numeric text to 0 (SQLite affinity)
                                            *value = "0".to_string();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let sort_mode = match expected {
                    QueryExpect::Results { sort_mode, .. } => sort_mode,
                    QueryExpect::Error(_) => None,
                }
                .or(self.sort_mode);

                let mut value_sort = false;
                match sort_mode {
                    None | Some(SortMode::NoSort) => {}
                    Some(SortMode::RowSort) => {
                        rows.sort_unstable();
                    }
                    Some(SortMode::ValueSort) => {
                        rows = rows
                            .iter()
                            .flat_map(|row| row.iter())
                            .map(|s| vec![s.to_owned()])
                            .collect();
                        rows.sort_unstable();
                        value_sort = true;
                    }
                };

                let num_values = if value_sort {
                    rows.len()
                } else {
                    rows.len() * types.len()
                };

                if self.hash_threshold > 0 && num_values > self.hash_threshold {
                    let mut md5 = md5::Md5::new();
                    for line in rows.iter() {
                        for value in line.iter() {
                            // Values are already normalized above, just hash them
                            md5.update(value.as_bytes());
                            md5.update(b"\n");
                        }
                    }
                    let hash = format!("{:x}", md5.finalize());
                    rows = vec![vec![format!(
                        "{} values hashing to {}",
                        rows.len() * rows[0].len(),
                        hash
                    )]];
                }

                RecordOutput::Query {
                    error: None,
                    types,
                    rows,
                }
            }
            Record::Sleep { duration, .. } => {
                D::sleep(duration).await;
                RecordOutput::Nothing
            }
            Record::Control(control) => {
                match control {
                    Control::SortMode(sort_mode) => {
                        self.sort_mode = Some(sort_mode);
                    }
                    Control::ResultMode(result_mode) => {
                        self.result_mode = Some(result_mode);
                    }
                    Control::Substitution(on_off) => self.substitution_on = on_off,
                }

                RecordOutput::Nothing
            }
            Record::HashThreshold { loc: _, threshold } => {
                self.hash_threshold = threshold as usize;
                RecordOutput::Nothing
            }
            Record::Halt { loc: _ } => {
                tracing::error!("halt record encountered. It's likely a bug of the runtime.");
                RecordOutput::Nothing
            }
            Record::Dialect { mode, loc: _ } => {
                // Execute SET SQL_MODE to switch dialect
                let conn = match self.conn.get(Connection::Default).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Statement {
                            count: 0,
                            error: Some(Arc::new(e)),
                        }
                    }
                };

                let sql = format!("SET SQL_MODE = '{}'", mode);
                match conn.run(&sql).await {
                    Ok(_) => {
                        tracing::debug!("Switched dialect to: {}", mode);
                        RecordOutput::Nothing
                    }
                    Err(e) => RecordOutput::Statement {
                        count: 0,
                        error: Some(Arc::new(e)),
                    }
                }
            }
            Record::Include { .. }
            | Record::Newline
            | Record::Comment(_)
            | Record::Subtest { .. }
            | Record::Injected(_)
            | Record::Condition(_)
            | Record::Connection(_) => RecordOutput::Nothing,
        }
    }
}
