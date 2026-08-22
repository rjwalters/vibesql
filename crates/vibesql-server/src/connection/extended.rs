//! Extended query protocol handling
//!
//! This module implements PostgreSQL's extended query protocol, which provides
//! prepared statement and portal support for improved performance and security.
//!
//! ## Protocol Flow
//!
//! A typical extended query sequence:
//! 1. Parse - Create prepared statement from SQL text
//! 2. Bind - Bind parameters to prepared statement, creating a portal
//! 3. Describe (optional) - Get metadata about statement or portal
//! 4. Execute - Execute the portal and return results
//! 5. Sync - End the extended query sequence
//!
//! ## Unnamed vs Named
//!
//! Both prepared statements and portals can be unnamed (empty string name).
//! Unnamed objects are automatically replaced when new ones are created.
//! Named objects persist until explicitly closed.

use std::collections::HashMap;

use bytes::BytesMut;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};
use tracing::{debug, warn};
use vibesql_ast::{SelectItem, Statement};
use vibesql_parser::Parser;

use crate::{
    protocol::{BackendMessage, FieldDescription, TransactionStatus},
    session::{ExecutionResult, Session},
};

/// PostgreSQL OID for TEXT type (used as default for unspecified parameter types)
const TEXT_OID: i32 = 25;

/// Resolve a query's output column names and send the appropriate Describe
/// reply: `RowDescription` for a SELECT (column names resolved against the
/// schema, so `SELECT *` / `table.*` report their real columns — #5429), or
/// `NoData` for a non-SELECT / unresolvable query.
///
/// Resolution goes through [`Session::describe_columns`], which uses the same
/// `derive_column_names` logic as the executed simple-query path
/// (`execute_with_columns`), so the metadata clients fetch via Describe matches
/// the rows they later receive. If no session is available, or schema-aware
/// resolution fails, it falls back to the static-AST extractor
/// ([`extract_select_columns`]) which handles explicit/aliased columns.
async fn send_row_description(
    session: &Option<Session>,
    query: &str,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
) -> anyhow::Result<()> {
    // Prefer schema-aware resolution (expands `*` / `table.*`).
    if let Some(s) = session.as_ref() {
        match s.describe_columns(query).await {
            Ok(Some(columns)) => {
                send_fields(write_half, write_buf, columns).await?;
                return Ok(());
            }
            // Not a SELECT (or resolved to no columns) - NoData.
            Ok(None) => {
                send_message(write_half, write_buf, &BackendMessage::NoData).await?;
                return Ok(());
            }
            // Schema-aware resolution failed (e.g. table not yet created at
            // Describe time): fall through to the static-AST extractor, which
            // still covers explicit/aliased columns. Errors surface at Execute.
            Err(e) => {
                debug!("describe_columns failed, falling back to static AST: {}", e);
            }
        }
    }

    // Fallback: static AST extraction (no schema, so `*` yields NoData).
    match Parser::parse_sql(query) {
        Ok(parsed) => match extract_select_columns(&parsed) {
            Some(columns) => send_fields(write_half, write_buf, columns).await?,
            None => send_message(write_half, write_buf, &BackendMessage::NoData).await?,
        },
        // Parse failed - NoData (error will occur at execute time).
        Err(_) => send_message(write_half, write_buf, &BackendMessage::NoData).await?,
    }

    Ok(())
}

/// Send a `RowDescription` for the given resolved column names.
async fn send_fields(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    columns: Vec<String>,
) -> anyhow::Result<()> {
    let fields: Vec<FieldDescription> = columns
        .into_iter()
        .map(|name| FieldDescription {
            name,
            table_oid: 0,
            column_attr_number: 0,
            data_type_oid: TEXT_OID,
            data_type_size: -1,
            type_modifier: -1,
            format_code: 0,
        })
        .collect();
    send_message(write_half, write_buf, &BackendMessage::RowDescription { fields }).await
}

/// Extract column names from a SELECT statement without executing it
fn extract_select_columns(stmt: &Statement) -> Option<Vec<String>> {
    match stmt {
        Statement::Select(select_stmt) => {
            let mut columns = Vec::new();
            for (idx, item) in select_stmt.select_list.iter().enumerate() {
                match item {
                    SelectItem::Wildcard { .. } => {
                        // Can't determine column names for * without schema info
                        return None;
                    }
                    SelectItem::QualifiedWildcard { .. } => {
                        // Can't determine column names for table.* without schema info
                        return None;
                    }
                    SelectItem::Expression { alias, source_text, expr } => {
                        if let Some(alias) = alias {
                            columns.push(alias.clone());
                        } else if let Some(text) = source_text {
                            columns.push(text.clone());
                        } else {
                            // Extract name from expression
                            columns.push(expr_to_column_name(expr, idx));
                        }
                    }
                }
            }
            Some(columns)
        }
        _ => None,
    }
}

/// Convert an expression to a column name
fn expr_to_column_name(expr: &vibesql_ast::Expression, idx: usize) -> String {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) => col_id.column_canonical().to_string(),
        vibesql_ast::Expression::Literal(lit) => lit.to_string(),
        _ => format!("column{}", idx + 1),
    }
}

/// A prepared statement in the extended query protocol
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Original SQL query with $1, $2, ... placeholders
    pub query: String,
    /// Parameter type OIDs (0 means unspecified)
    pub param_types: Vec<i32>,
}

/// A portal (bound prepared statement ready for execution)
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields may be used for future enhancements
pub struct Portal {
    /// Name of the source prepared statement
    pub statement_name: String,
    /// Bound parameter values
    pub param_values: Vec<Option<Vec<u8>>>,
    /// Parameter format codes
    pub param_formats: Vec<i16>,
    /// Result column format codes (0=text, 1=binary)
    pub result_formats: Vec<i16>,
    /// The prepared statement's query with parameters substituted
    pub bound_query: String,
}

/// Storage for prepared statements and portals within a connection
#[derive(Debug, Default)]
pub struct ExtendedQueryState {
    /// Named prepared statements (empty string key = unnamed statement)
    pub statements: HashMap<String, PreparedStatement>,
    /// Named portals (empty string key = unnamed portal)
    pub portals: HashMap<String, Portal>,
}

impl ExtendedQueryState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a prepared statement (replaces existing with same name)
    pub fn store_statement(&mut self, name: String, stmt: PreparedStatement) {
        self.statements.insert(name, stmt);
    }

    /// Get a prepared statement by name
    pub fn get_statement(&self, name: &str) -> Option<&PreparedStatement> {
        self.statements.get(name)
    }

    /// Store a portal (replaces existing with same name)
    pub fn store_portal(&mut self, name: String, portal: Portal) {
        self.portals.insert(name, portal);
    }

    /// Get a portal by name
    pub fn get_portal(&self, name: &str) -> Option<&Portal> {
        self.portals.get(name)
    }

    /// Close a prepared statement and any portals that depend on it
    pub fn close_statement(&mut self, name: &str) -> bool {
        // Remove portals that reference this statement
        self.portals.retain(|_, p| p.statement_name != name);
        self.statements.remove(name).is_some()
    }

    /// Close a portal
    pub fn close_portal(&mut self, name: &str) -> bool {
        self.portals.remove(name).is_some()
    }
}

/// Handle Parse message - create a prepared statement
pub async fn handle_parse(
    _session: &mut Option<Session>,
    extended_state: &mut ExtendedQueryState,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    name: String,
    query: String,
    param_types: Vec<i32>,
) -> anyhow::Result<bool> {
    debug!("Parse: name='{}', query='{}', params={:?}", name, query, param_types);

    // Validate the query syntax (optional - could defer to execute time)
    if let Err(e) = Parser::parse_sql(&query) {
        // Send error response
        send_error(write_half, write_buf, "42601", &format!("syntax error: {}", e)).await?;
        return Ok(false);
    }

    // Store the prepared statement
    let stmt = PreparedStatement { query, param_types };
    extended_state.store_statement(name, stmt);

    // Send ParseComplete
    send_message(write_half, write_buf, &BackendMessage::ParseComplete).await?;
    Ok(true)
}

/// Handle Bind message - bind parameters to a prepared statement
#[allow(clippy::too_many_arguments)]
pub async fn handle_bind(
    _session: &mut Option<Session>,
    extended_state: &mut ExtendedQueryState,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    portal_name: String,
    statement_name: String,
    param_formats: Vec<i16>,
    param_values: Vec<Option<Vec<u8>>>,
    result_formats: Vec<i16>,
) -> anyhow::Result<bool> {
    debug!(
        "Bind: portal='{}', statement='{}', params={}",
        portal_name,
        statement_name,
        param_values.len()
    );

    // Get the prepared statement
    let stmt = match extended_state.get_statement(&statement_name) {
        Some(s) => s.clone(),
        None => {
            send_error(
                write_half,
                write_buf,
                "26000",
                &format!("prepared statement \"{}\" does not exist", statement_name),
            )
            .await?;
            return Ok(false);
        }
    };

    // Substitute parameters into the query
    let bound_query = substitute_parameters(&stmt.query, &param_values, &param_formats);

    // Create the portal
    let portal =
        Portal { statement_name, param_values, param_formats, result_formats, bound_query };
    extended_state.store_portal(portal_name, portal);

    // Send BindComplete
    send_message(write_half, write_buf, &BackendMessage::BindComplete).await?;
    Ok(true)
}

/// Handle Describe message - get information about a statement or portal
pub async fn handle_describe(
    session: &Option<Session>,
    extended_state: &ExtendedQueryState,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    target_type: u8,
    name: String,
) -> anyhow::Result<bool> {
    debug!("Describe: type='{}', name='{}'", target_type as char, name);

    match target_type {
        b'S' => {
            // Describe prepared statement
            let stmt = match extended_state.get_statement(&name) {
                Some(s) => s.clone(),
                None => {
                    send_error(
                        write_half,
                        write_buf,
                        "26000",
                        &format!("prepared statement \"{}\" does not exist", name),
                    )
                    .await?;
                    return Ok(false);
                }
            };

            // Send ParameterDescription
            let param_types: Vec<i32> =
                stmt.param_types.iter().map(|&t| if t == 0 { TEXT_OID } else { t }).collect();
            send_message(
                write_half,
                write_buf,
                &BackendMessage::ParameterDescription { param_types },
            )
            .await?;

            // Resolve the SELECT's output column names against the schema so
            // that `SELECT *` / `table.*` report their real columns in the
            // RowDescription (#5429), matching the simple-query path.
            send_row_description(session, &stmt.query, write_half, write_buf).await?;
        }
        b'P' => {
            // Describe portal
            let portal = match extended_state.get_portal(&name) {
                Some(p) => p.clone(),
                None => {
                    send_error(
                        write_half,
                        write_buf,
                        "34000",
                        &format!("portal \"{}\" does not exist", name),
                    )
                    .await?;
                    return Ok(false);
                }
            };

            // Resolve the bound query's output column names (with `*` expanded
            // against the schema) for the RowDescription (#5429).
            send_row_description(session, &portal.bound_query, write_half, write_buf).await?;
        }
        _ => {
            send_error(
                write_half,
                write_buf,
                "08P01",
                &format!("invalid Describe target type: {}", target_type),
            )
            .await?;
            return Ok(false);
        }
    }

    Ok(true)
}

/// Handle Execute message - execute a portal
pub async fn handle_execute(
    session: &mut Option<Session>,
    extended_state: &ExtendedQueryState,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    portal_name: String,
    max_rows: i32,
) -> anyhow::Result<bool> {
    debug!("Execute: portal='{}', max_rows={}", portal_name, max_rows);

    let portal = match extended_state.get_portal(&portal_name) {
        Some(p) => p.clone(),
        None => {
            send_error(
                write_half,
                write_buf,
                "34000",
                &format!("portal \"{}\" does not exist", portal_name),
            )
            .await?;
            return Ok(false);
        }
    };

    // Execute the bound query
    let session = match session.as_mut() {
        Some(s) => s,
        None => {
            send_error(write_half, write_buf, "08003", "no session available").await?;
            return Ok(false);
        }
    };

    // Execute the bound query string
    match session.execute(&portal.bound_query).await {
        Ok(result) => {
            // Handle the result based on type
            match result {
                ExecutionResult::Select { rows, columns: _ } => {
                    // Note: In extended query protocol, RowDescription is sent by Describe,
                    // not by Execute. Execute only sends DataRow messages and CommandComplete.

                    // Send data rows
                    for (row_count, row) in rows.iter().enumerate() {
                        if max_rows > 0 && row_count >= max_rows as usize {
                            // More rows exist but we've hit the limit
                            send_message(write_half, write_buf, &BackendMessage::PortalSuspended)
                                .await?;
                            return Ok(true);
                        }

                        let values: Vec<Option<Vec<u8>>> =
                            row.values.iter().map(|v| Some(v.to_string().into_bytes())).collect();
                        send_message(write_half, write_buf, &BackendMessage::DataRow { values })
                            .await?;
                    }

                    // Send command complete
                    let tag = format!("SELECT {}", rows.len());
                    send_message(write_half, write_buf, &BackendMessage::CommandComplete { tag })
                        .await?;
                }
                ExecutionResult::Insert { rows_affected } => {
                    let tag = format!("INSERT 0 {}", rows_affected);
                    send_message(write_half, write_buf, &BackendMessage::CommandComplete { tag })
                        .await?;
                }
                ExecutionResult::Update { rows_affected } => {
                    let tag = format!("UPDATE {}", rows_affected);
                    send_message(write_half, write_buf, &BackendMessage::CommandComplete { tag })
                        .await?;
                }
                ExecutionResult::Delete { rows_affected } => {
                    let tag = format!("DELETE {}", rows_affected);
                    send_message(write_half, write_buf, &BackendMessage::CommandComplete { tag })
                        .await?;
                }
                ExecutionResult::CreateTable => {
                    send_message(
                        write_half,
                        write_buf,
                        &BackendMessage::CommandComplete { tag: "CREATE TABLE".to_string() },
                    )
                    .await?;
                }
                ExecutionResult::CreateIndex => {
                    send_message(
                        write_half,
                        write_buf,
                        &BackendMessage::CommandComplete { tag: "CREATE INDEX".to_string() },
                    )
                    .await?;
                }
                ExecutionResult::DropTable => {
                    send_message(
                        write_half,
                        write_buf,
                        &BackendMessage::CommandComplete { tag: "DROP TABLE".to_string() },
                    )
                    .await?;
                }
                ExecutionResult::DropIndex => {
                    send_message(
                        write_half,
                        write_buf,
                        &BackendMessage::CommandComplete { tag: "DROP INDEX".to_string() },
                    )
                    .await?;
                }
                ExecutionResult::Begin => {
                    send_message(
                        write_half,
                        write_buf,
                        &BackendMessage::CommandComplete { tag: "BEGIN".to_string() },
                    )
                    .await?;
                }
                ExecutionResult::Commit => {
                    send_message(
                        write_half,
                        write_buf,
                        &BackendMessage::CommandComplete { tag: "COMMIT".to_string() },
                    )
                    .await?;
                }
                ExecutionResult::Rollback => {
                    send_message(
                        write_half,
                        write_buf,
                        &BackendMessage::CommandComplete { tag: "ROLLBACK".to_string() },
                    )
                    .await?;
                }
                _ => {
                    send_message(
                        write_half,
                        write_buf,
                        &BackendMessage::CommandComplete { tag: "OK".to_string() },
                    )
                    .await?;
                }
            }
        }
        Err(e) => {
            // Preserve a structured SqlError's SQLSTATE (#5383: NotLeader
            // redirects, staleness refusals); other errors keep the
            // protocol-level syntax-error code this path always used.
            match e.downcast_ref::<crate::replication::SqlError>() {
                Some(sql_error) => {
                    send_sql_error(write_half, write_buf, sql_error).await?;
                }
                None => {
                    send_error(write_half, write_buf, "42000", &e.to_string()).await?;
                }
            }
            return Ok(false);
        }
    }

    Ok(true)
}

/// Handle Close message - close a statement or portal
pub async fn handle_close(
    extended_state: &mut ExtendedQueryState,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    target_type: u8,
    name: String,
) -> anyhow::Result<bool> {
    debug!("Close: type='{}', name='{}'", target_type as char, name);

    match target_type {
        b'S' => {
            extended_state.close_statement(&name);
        }
        b'P' => {
            extended_state.close_portal(&name);
        }
        _ => {
            warn!("Invalid Close target type: {}", target_type);
        }
    }

    send_message(write_half, write_buf, &BackendMessage::CloseComplete).await?;
    Ok(true)
}

/// Handle Sync message - end of extended query sequence
pub async fn handle_sync(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
) -> anyhow::Result<()> {
    debug!("Sync");
    send_message(
        write_half,
        write_buf,
        &BackendMessage::ReadyForQuery { status: TransactionStatus::Idle },
    )
    .await
}

/// Handle Flush message - flush output buffer
pub async fn handle_flush(write_half: &mut OwnedWriteHalf) -> anyhow::Result<()> {
    debug!("Flush");
    write_half.flush().await?;
    Ok(())
}

/// Substitute $1, $2, ... parameters with their values
fn substitute_parameters(
    query: &str,
    param_values: &[Option<Vec<u8>>],
    param_formats: &[i16],
) -> String {
    let mut result = query.to_string();

    // Replace parameters in reverse order to avoid offset issues
    for (i, value) in param_values.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match value {
            Some(bytes) => {
                // Check format: 0 = text, 1 = binary
                let format = if param_formats.is_empty() {
                    0
                } else if param_formats.len() == 1 {
                    param_formats[0]
                } else {
                    param_formats.get(i).copied().unwrap_or(0)
                };

                if format == 0 {
                    // Text format - convert to string and quote
                    let text = String::from_utf8_lossy(bytes);
                    format!("'{}'", text.replace('\'', "''"))
                } else {
                    // Binary format - encode as hex literal
                    let hex_str: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
                    format!("X'{}'", hex_str)
                }
            }
            None => "NULL".to_string(),
        };
        result = result.replace(&placeholder, &replacement);
    }

    result
}

/// Send a backend message
async fn send_message(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    msg: &BackendMessage,
) -> anyhow::Result<()> {
    write_buf.clear();
    msg.encode(write_buf);
    write_half.write_all(write_buf).await?;
    Ok(())
}

/// Send an error response
async fn send_error(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    code: &str,
    message: &str,
) -> anyhow::Result<()> {
    let mut fields = HashMap::new();
    fields.insert(b'S', "ERROR".to_string());
    fields.insert(b'V', "ERROR".to_string());
    fields.insert(b'C', code.to_string());
    fields.insert(b'M', message.to_string());

    send_message(write_half, write_buf, &BackendMessage::ErrorResponse { fields }).await
}

/// Send a structured [`SqlError`](crate::replication::SqlError) with its
/// SQLSTATE, detail, and hint fields (#5383).
async fn send_sql_error(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    error: &crate::replication::SqlError,
) -> anyhow::Result<()> {
    let mut fields = HashMap::new();
    fields.insert(b'S', "ERROR".to_string());
    fields.insert(b'V', "ERROR".to_string());
    fields.insert(b'C', error.code.to_string());
    fields.insert(b'M', error.message.clone());
    if let Some(detail) = &error.detail {
        fields.insert(b'D', detail.clone());
    }
    if let Some(hint) = &error.hint {
        fields.insert(b'H', hint.clone());
    }

    send_message(write_half, write_buf, &BackendMessage::ErrorResponse { fields }).await
}
