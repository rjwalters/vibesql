use std::time::Instant;

use vibesql_parser::parse_with_arena_fallback;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

// Submodules
mod copy_handler;
pub mod display;
pub mod validation;

#[cfg(test)]
mod tests;

pub struct SqlExecutor {
    db: Database,
    timing_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Cell values: None represents SQL NULL, Some(s) represents actual data.
    /// This distinction is important for output formatting - NULL values should
    /// be displayed differently than the literal string "NULL".
    pub rows: Vec<Vec<Option<String>>>,
    pub columns: Vec<String>,
    pub row_count: usize,
    pub execution_time_ms: Option<f64>,
    /// Optional informational message from DDL operations (e.g., "Index created successfully").
    /// This message should be displayed in interactive formats but suppressed in raw format.
    pub message: Option<String>,
}

use crate::util::is_memory_database;

/// Format SqlValue for output in SQLite-compatible format
/// - Booleans are displayed as 0/1 instead of FALSE/TRUE
/// - Other values use their standard Display format
fn format_sql_value(v: &SqlValue) -> String {
    match v {
        SqlValue::Boolean(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        _ => format!("{}", v),
    }
}

impl SqlExecutor {
    pub fn new(database: Option<String>) -> anyhow::Result<Self> {
        // Treat :memory: as an in-memory database (no file path)
        let database = database.filter(|p| !is_memory_database(p));

        // Load database from file if provided, otherwise create new in-memory database
        let db = if let Some(db_path) = database {
            // Check if file exists
            if std::path::Path::new(&db_path).exists() {
                // Try auto-detecting format first (handles binary, compressed, JSON)
                // Fall back to SQL dump if that fails
                match Database::load(&db_path) {
                    Ok(db) => db,
                    Err(ref e) if e.to_string().contains("SQLite database detected") => {
                        // Auto-import SQLite database
                        let result = crate::sqlite_io::import_sqlite(&db_path).map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to read binary SQLite database at {}: {}. \
                                 If this file is a VibeSQL SQL dump, rename it with a .sql extension \
                                 to load it in SQL dump format.",
                                db_path,
                                e
                            )
                        })?;
                        for warning in &result.warnings {
                            eprintln!("{}", warning);
                        }
                        eprintln!(
                            "Imported SQLite database: {} tables, {} rows",
                            result.tables_imported, result.rows_imported
                        );
                        result.database
                    }
                    Err(_) => {
                        // Fall back to SQL dump loading (requires executor for parsing)
                        vibesql_executor::load_sql_dump(&db_path)
                            .map_err(|e| anyhow::anyhow!("Failed to load database: {}", e))?
                    }
                }
            } else {
                // File doesn't exist, create new database
                // (Will be saved when user uses \save or when modifications occur)
                Database::new()
            }
        } else {
            // No database file specified, use in-memory database
            Database::new()
        };

        Ok(SqlExecutor { db, timing_enabled: false })
    }

    pub fn execute(&mut self, sql: &str) -> anyhow::Result<QueryResult> {
        let start = Instant::now();

        // Parse SQL using arena fallback for SELECT statements (preserves original case in source_text)
        let statement = parse_with_arena_fallback(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Execute statement through appropriate executor
        let mut result = QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
            row_count: 0,
            execution_time_ms: None,
            message: None,
        };

        match statement {
            vibesql_ast::Statement::Select(select_stmt) => {
                // Execute SELECT and format results with column names
                let executor = vibesql_executor::SelectExecutor::new(&self.db);
                match executor.execute_with_columns(&select_stmt) {
                    Ok(select_result) => {
                        result.row_count = select_result.rows.len();
                        // Use column names from the executor result
                        result.columns = select_result.columns;
                        // Convert rows to string representation using SQLite-compatible format
                        // NULL values are represented as None to distinguish from the literal string "NULL"
                        for row in select_result.rows {
                            let row_strs: Vec<Option<String>> = row
                                .values
                                .iter()
                                .map(|v| if v.is_null() { None } else { Some(format_sql_value(v)) })
                                .collect();
                            result.rows.push(row_strs);
                        }
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                match vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                match vibesql_executor::InsertExecutor::execute(&mut self.db, &insert_stmt) {
                    Ok(affected_rows) => {
                        // Track changes count for changes() and total_changes() functions
                        self.db.set_last_changes_count(affected_rows);
                        self.db.increment_total_changes_count(affected_rows);
                        result.row_count = affected_rows;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                match vibesql_executor::UpdateExecutor::execute(&update_stmt, &mut self.db) {
                    Ok(affected_rows) => {
                        // Track changes count for changes() and total_changes() functions
                        self.db.set_last_changes_count(affected_rows);
                        self.db.increment_total_changes_count(affected_rows);
                        result.row_count = affected_rows;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                match vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut self.db) {
                    Ok(affected_rows) => {
                        // Track changes count for changes() and total_changes() functions
                        self.db.set_last_changes_count(affected_rows);
                        self.db.increment_total_changes_count(affected_rows);
                        result.row_count = affected_rows;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateView(mut view_stmt) => {
                // Store original SQL for sqlite_master compatibility
                view_stmt.sql_definition = Some(sql.to_string());
                match vibesql_executor::advanced_objects::execute_create_view(
                    &view_stmt,
                    &mut self.db,
                ) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropView(drop_stmt) => {
                match vibesql_executor::advanced_objects::execute_drop_view(
                    &drop_stmt,
                    &mut self.db,
                ) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                match vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::TruncateTable(truncate_stmt) => {
                match vibesql_executor::TruncateTableExecutor::execute(&truncate_stmt, &mut self.db)
                {
                    Ok(rows_deleted) => {
                        result.row_count = rows_deleted;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateTrigger(trigger_stmt) => {
                match vibesql_executor::TriggerExecutor::create_trigger(&mut self.db, &trigger_stmt)
                {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::AlterTrigger(alter_stmt) => {
                match vibesql_executor::TriggerExecutor::alter_trigger(&mut self.db, &alter_stmt) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropTrigger(drop_stmt) => {
                match vibesql_executor::TriggerExecutor::drop_trigger(&mut self.db, &drop_stmt) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::SetVariable(set_var_stmt) => {
                match vibesql_executor::SchemaExecutor::execute_set_variable(
                    &set_var_stmt,
                    &mut self.db,
                ) {
                    Ok(_) => {
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Reindex(reindex_stmt) => {
                match vibesql_executor::ReindexExecutor::execute(&reindex_stmt, &self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Analyze(analyze_stmt) => {
                match vibesql_executor::AnalyzeExecutor::execute(&analyze_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Explain(explain_stmt) => {
                match vibesql_executor::ExplainExecutor::execute(&explain_stmt, &self.db) {
                    Ok(explain_result) => {
                        if explain_stmt.query_plan {
                            // SQLite-compatible EXPLAIN QUERY PLAN format
                            let output = explain_result.to_sqlite_eqp();
                            // Use "detail" as column name (matches SQLite's actual column)
                            // The "QUERY PLAN" header is now included in the data for TCL test compatibility
                            result.columns = vec!["detail".to_string()];
                            // Split output into rows for better display
                            for line in output.lines() {
                                result.rows.push(vec![Some(line.to_string())]);
                            }
                        } else {
                            // SQLite-compatible EXPLAIN format (VM bytecode style)
                            let vm_output = explain_result.to_sqlite_vm();
                            result.columns = vibesql_executor::SqliteVmOutput::column_names()
                                .iter()
                                .map(|s| s.to_string())
                                .collect();
                            for row in vm_output.to_rows() {
                                result.rows.push(row.into_iter().map(Some).collect());
                            }
                        }
                        result.row_count = result.rows.len();
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateIndex(index_stmt) => {
                match vibesql_executor::CreateIndexExecutor::execute(&index_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropIndex(drop_stmt) => {
                match vibesql_executor::DropIndexExecutor::execute(&drop_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::AlterTable(alter_stmt) => {
                match vibesql_executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::BeginTransaction(begin_stmt) => {
                match vibesql_executor::BeginTransactionExecutor::execute(&begin_stmt, &mut self.db)
                {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Commit(commit_stmt) => {
                match vibesql_executor::CommitExecutor::execute(&commit_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Rollback(rollback_stmt) => {
                match vibesql_executor::RollbackExecutor::execute(&rollback_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Savepoint(savepoint_stmt) => {
                match vibesql_executor::SavepointExecutor::execute(&savepoint_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::RollbackToSavepoint(rollback_stmt) => {
                match vibesql_executor::RollbackToSavepointExecutor::execute(
                    &rollback_stmt,
                    &mut self.db,
                ) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::ReleaseSavepoint(release_stmt) => {
                match vibesql_executor::ReleaseSavepointExecutor::execute(
                    &release_stmt,
                    &mut self.db,
                ) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::ShowTables(show_stmt) => {
                result = self.execute_show_tables(&show_stmt)?;
            }
            vibesql_ast::Statement::ShowDatabases(show_stmt) => {
                result = self.execute_show_databases(&show_stmt)?;
            }
            vibesql_ast::Statement::ShowColumns(show_stmt) => {
                result = self.execute_show_columns(&show_stmt)?;
            }
            vibesql_ast::Statement::ShowIndex(show_stmt) => {
                result = self.execute_show_index(&show_stmt)?;
            }
            vibesql_ast::Statement::ShowCreateTable(show_stmt) => {
                result = self.execute_show_create_table(&show_stmt)?;
            }
            vibesql_ast::Statement::Describe(desc_stmt) => {
                result = self.execute_describe(&desc_stmt)?;
            }
            vibesql_ast::Statement::CreateAssertion(create_stmt) => {
                match vibesql_executor::advanced_objects::execute_create_assertion(
                    &create_stmt,
                    &mut self.db,
                ) {
                    Ok(()) => {
                        result.message =
                            Some(format!("Assertion '{}' created", create_stmt.assertion_name));
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropAssertion(drop_stmt) => {
                match vibesql_executor::advanced_objects::execute_drop_assertion(
                    &drop_stmt,
                    &mut self.db,
                ) {
                    Ok(()) => {
                        result.message =
                            Some(format!("Assertion '{}' dropped", drop_stmt.assertion_name));
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Pragma(pragma_stmt) => {
                result = self.execute_pragma(&pragma_stmt)?;
            }
            _ => {
                return Err(anyhow::anyhow!("Statement type not yet supported in CLI"));
            }
        }

        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if self.timing_enabled {
            result.execution_time_ms = Some(elapsed);
        }

        Ok(result)
    }

    pub fn toggle_timing(&mut self) {
        self.timing_enabled = !self.timing_enabled;
        let state = if self.timing_enabled { "on" } else { "off" };
        println!("Timing is {}", state);
    }

    /// Save database to SQL dump file
    pub fn save_database(&self, path: &str) -> anyhow::Result<()> {
        self.db
            .save_sql_dump(path)
            .map_err(|e| anyhow::anyhow!("Failed to save database to {}: {}", path, e))
    }

    /// Execute SHOW TABLES statement
    fn execute_show_tables(
        &self,
        stmt: &vibesql_ast::ShowTablesStmt,
    ) -> anyhow::Result<QueryResult> {
        let tables = self.db.list_tables();

        // Apply LIKE filter if specified
        let filtered_tables: Vec<String> = if let Some(pattern) = &stmt.like_pattern {
            let regex_pattern = like_to_regex(pattern);
            let re = regex::Regex::new(&regex_pattern)
                .map_err(|e| anyhow::anyhow!("Invalid LIKE pattern: {}", e))?;
            tables.into_iter().filter(|t| re.is_match(t)).collect()
        } else {
            tables
        };

        // Note: WHERE clause filtering would require expression evaluation
        // For now, we support LIKE pattern only

        let rows: Vec<Vec<Option<String>>> =
            filtered_tables.iter().map(|t| vec![Some(t.clone())]).collect();
        let row_count = rows.len();

        Ok(QueryResult {
            columns: vec!["Tables_in_database".to_string()],
            rows,
            row_count,
            execution_time_ms: None,
            message: None,
        })
    }

    /// Execute SHOW DATABASES statement
    fn execute_show_databases(
        &self,
        stmt: &vibesql_ast::ShowDatabasesStmt,
    ) -> anyhow::Result<QueryResult> {
        let schemas = self.db.catalog.list_schemas();

        // Apply LIKE filter if specified
        let filtered_schemas: Vec<String> = if let Some(pattern) = &stmt.like_pattern {
            let regex_pattern = like_to_regex(pattern);
            let re = regex::Regex::new(&regex_pattern)
                .map_err(|e| anyhow::anyhow!("Invalid LIKE pattern: {}", e))?;
            schemas.into_iter().filter(|s| re.is_match(s)).collect()
        } else {
            schemas
        };

        let rows: Vec<Vec<Option<String>>> =
            filtered_schemas.iter().map(|s| vec![Some(s.clone())]).collect();
        let row_count = rows.len();

        Ok(QueryResult {
            columns: vec!["Database".to_string()],
            rows,
            row_count,
            execution_time_ms: None,
            message: None,
        })
    }

    /// Execute SHOW COLUMNS statement
    fn execute_show_columns(
        &self,
        stmt: &vibesql_ast::ShowColumnsStmt,
    ) -> anyhow::Result<QueryResult> {
        let normalized_name = stmt.table_name.to_uppercase();
        let table = self
            .db
            .get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", stmt.table_name))?;

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();

        for column in &table.schema.columns {
            // Check LIKE pattern if specified
            if let Some(pattern) = &stmt.like_pattern {
                let regex_pattern = like_to_regex(pattern);
                let re = regex::Regex::new(&regex_pattern)
                    .map_err(|e| anyhow::anyhow!("Invalid LIKE pattern: {}", e))?;
                if !re.is_match(&column.name) {
                    continue;
                }
            }

            let nullable = if column.nullable { "YES" } else { "NO" };
            let default_val =
                column.default_value.as_ref().map(|v| format!("{:?}", v)).unwrap_or_default();

            // Check if column is part of primary key
            let key = if table
                .schema
                .primary_key
                .as_ref()
                .map(|pk| pk.contains(&column.name))
                .unwrap_or(false)
            {
                "PRI"
            } else {
                ""
            };

            let row = if stmt.full {
                // SHOW FULL COLUMNS returns additional fields
                vec![
                    Some(column.name.clone()),
                    Some(display::format_data_type(&column.data_type)),
                    Some(String::new()), // Collation - not yet supported
                    Some(nullable.to_string()),
                    Some(key.to_string()),
                    Some(default_val),
                    Some(String::new()), // Extra
                    Some(String::new()), // Privileges
                    Some(String::new()), // Comment
                ]
            } else {
                vec![
                    Some(column.name.clone()),
                    Some(display::format_data_type(&column.data_type)),
                    Some(nullable.to_string()),
                    Some(key.to_string()),
                    Some(default_val),
                    Some(String::new()), // Extra
                ]
            };

            rows.push(row);
        }

        let row_count = rows.len();

        let columns = if stmt.full {
            vec![
                "Field".to_string(),
                "Type".to_string(),
                "Collation".to_string(),
                "Null".to_string(),
                "Key".to_string(),
                "Default".to_string(),
                "Extra".to_string(),
                "Privileges".to_string(),
                "Comment".to_string(),
            ]
        } else {
            vec![
                "Field".to_string(),
                "Type".to_string(),
                "Null".to_string(),
                "Key".to_string(),
                "Default".to_string(),
                "Extra".to_string(),
            ]
        };

        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None, message: None })
    }

    /// Execute SHOW INDEX statement
    fn execute_show_index(&self, stmt: &vibesql_ast::ShowIndexStmt) -> anyhow::Result<QueryResult> {
        let normalized_name = stmt.table_name.to_lowercase();

        // Verify table exists
        let _ = self
            .db
            .get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", stmt.table_name))?;

        let index_names = self.db.list_indexes();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();

        for index_name in index_names {
            if let Some(index_meta) = self.db.get_index(&index_name) {
                if index_meta.table_name == normalized_name {
                    // Add one row per column in the index
                    for (seq, col) in index_meta.columns.iter().enumerate() {
                        rows.push(vec![
                            Some(normalized_name.clone()),                               // Table
                            Some(if index_meta.unique { "0" } else { "1" }.to_string()), // Non_unique
                            Some(index_meta.index_name.clone()),                         // Key_name
                            Some((seq + 1).to_string()), // Seq_in_index
                            Some(col.expect_column_name().to_string()), // Column_name
                            Some("A".to_string()),       // Collation (always Ascending for now)
                            Some(String::new()),         // Cardinality
                            Some(String::new()),         // Sub_part
                            Some(String::new()),         // Packed
                            Some(String::new()),         // Null
                            Some("BTREE".to_string()),   // Index_type
                            Some(String::new()),         // Comment
                        ]);
                    }
                }
            }
        }

        let row_count = rows.len();

        Ok(QueryResult {
            columns: vec![
                "Table".to_string(),
                "Non_unique".to_string(),
                "Key_name".to_string(),
                "Seq_in_index".to_string(),
                "Column_name".to_string(),
                "Collation".to_string(),
                "Cardinality".to_string(),
                "Sub_part".to_string(),
                "Packed".to_string(),
                "Null".to_string(),
                "Index_type".to_string(),
                "Comment".to_string(),
            ],
            rows,
            row_count,
            execution_time_ms: None,
            message: None,
        })
    }

    /// Execute SHOW CREATE TABLE statement
    fn execute_show_create_table(
        &self,
        stmt: &vibesql_ast::ShowCreateTableStmt,
    ) -> anyhow::Result<QueryResult> {
        let normalized_name = stmt.table_name.to_lowercase();
        let table = self
            .db
            .get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", stmt.table_name))?;

        // Build CREATE TABLE statement
        let mut create_sql = format!("CREATE TABLE {} (\n", normalized_name);

        // Add columns
        let mut column_defs: Vec<String> = Vec::new();
        for column in &table.schema.columns {
            let mut def =
                format!("  {} {}", column.name, display::format_data_type(&column.data_type));
            if !column.nullable {
                def.push_str(" NOT NULL");
            }
            if let Some(default) = &column.default_value {
                def.push_str(&format!(" DEFAULT {:?}", default));
            }
            column_defs.push(def);
        }

        // Add primary key constraint
        if let Some(pk_cols) = &table.schema.primary_key {
            column_defs.push(format!("  PRIMARY KEY ({})", pk_cols.join(", ")));
        }

        // Add unique constraints
        for unique_cols in &table.schema.unique_constraints {
            column_defs.push(format!("  UNIQUE ({})", unique_cols.join(", ")));
        }

        // Add foreign key constraints
        for fk in &table.schema.foreign_keys {
            column_defs.push(format!(
                "  FOREIGN KEY ({}) REFERENCES {}({})",
                fk.column_names.join(", "),
                fk.parent_table,
                fk.parent_column_names.join(", ")
            ));
        }

        create_sql.push_str(&column_defs.join(",\n"));
        create_sql.push_str("\n)");

        Ok(QueryResult {
            columns: vec!["Table".to_string(), "Create Table".to_string()],
            rows: vec![vec![Some(normalized_name), Some(create_sql)]],
            row_count: 1,
            execution_time_ms: None,
            message: None,
        })
    }

    /// Execute DESCRIBE statement
    fn execute_describe(&self, stmt: &vibesql_ast::DescribeStmt) -> anyhow::Result<QueryResult> {
        // DESCRIBE is equivalent to SHOW COLUMNS FROM
        let show_stmt = vibesql_ast::ShowColumnsStmt {
            table_name: stmt.table_name.clone(),
            database: None,
            full: false,
            like_pattern: stmt.column_pattern.clone(),
            where_clause: None,
        };
        self.execute_show_columns(&show_stmt)
    }

    /// Execute PRAGMA statement
    ///
    /// Implements SQLite-compatible PRAGMA statements for session configuration.
    /// Supports:
    /// - PRAGMA full_column_names (get/set)
    /// - PRAGMA short_column_names (get/set)
    fn execute_pragma(&mut self, stmt: &vibesql_ast::PragmaStmt) -> anyhow::Result<QueryResult> {
        let pragma_name = stmt.name.to_uppercase();

        // Handle PRAGMAs that take table name arguments (not boolean set/query)
        // These use function-style syntax: PRAGMA name(table_name)
        match pragma_name.as_str() {
            "FOREIGN_KEY_LIST" => {
                return self.execute_pragma_foreign_key_list(stmt);
            }
            "FOREIGN_KEY_CHECK" => {
                return self.execute_pragma_foreign_key_check(stmt);
            }
            _ => {}
        }

        // Handle setting vs querying
        if let Some(value) = &stmt.value {
            // SET operation
            let bool_value = pragma_value_to_bool(value);

            match pragma_name.as_str() {
                "FULL_COLUMN_NAMES" => {
                    self.db.set_full_column_names(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "SHORT_COLUMN_NAMES" => {
                    self.db.set_short_column_names(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "CASE_SENSITIVE_LIKE" => {
                    self.db.set_case_sensitive_like(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "REVERSE_UNORDERED_SELECTS" => {
                    self.db.set_reverse_unordered_selects(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "FOREIGN_KEYS" => {
                    self.db.set_foreign_keys_enabled(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                _ => {
                    // Unknown pragma - silently ignore for SQLite compatibility
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
            }
        } else {
            // QUERY operation - return current value
            match pragma_name.as_str() {
                "FULL_COLUMN_NAMES" => {
                    let value = if self.db.full_column_names() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["full_column_names".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "SHORT_COLUMN_NAMES" => {
                    let value = if self.db.short_column_names() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["short_column_names".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "CASE_SENSITIVE_LIKE" => {
                    let value = if self.db.case_sensitive_like() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["case_sensitive_like".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "REVERSE_UNORDERED_SELECTS" => {
                    let value = if self.db.reverse_unordered_selects() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["reverse_unordered_selects".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "INTEGRITY_CHECK" => {
                    // SQLite compatibility: Return "ok" if no corruption detected
                    // Since we're in-memory and don't have B-tree corruption scenarios,
                    // we always return "ok"
                    Ok(QueryResult {
                        columns: vec!["integrity_check".to_string()],
                        rows: vec![vec![Some("ok".to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "FOREIGN_KEYS" => {
                    let value = if self.db.foreign_keys_enabled() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["foreign_keys".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                _ => {
                    // Unknown pragma - return empty result for compatibility
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
            }
        }
    }

    /// PRAGMA foreign_key_list(table_name)
    /// Returns FK metadata: id, seq, table, from, to, on_update, on_delete, match
    fn execute_pragma_foreign_key_list(
        &self,
        stmt: &vibesql_ast::PragmaStmt,
    ) -> anyhow::Result<QueryResult> {
        let table_name = match &stmt.value {
            Some(vibesql_ast::PragmaValue::Identifier(name)) => name.clone(),
            Some(vibesql_ast::PragmaValue::String(name)) => name.clone(),
            _ => {
                return Ok(QueryResult {
                    columns: vec![
                        "id".to_string(),
                        "seq".to_string(),
                        "table".to_string(),
                        "from".to_string(),
                        "to".to_string(),
                        "on_update".to_string(),
                        "on_delete".to_string(),
                        "match".to_string(),
                    ],
                    rows: Vec::new(),
                    row_count: 0,
                    execution_time_ms: None,
                    message: None,
                });
            }
        };

        let columns = vec![
            "id".to_string(),
            "seq".to_string(),
            "table".to_string(),
            "from".to_string(),
            "to".to_string(),
            "on_update".to_string(),
            "on_delete".to_string(),
            "match".to_string(),
        ];

        let mut rows = Vec::new();
        if let Some(schema) = self.db.catalog.get_table(&table_name) {
            for (fk_id, fk) in schema.foreign_keys.iter().enumerate() {
                for (seq, (col_name, parent_col_name)) in fk
                    .column_names
                    .iter()
                    .zip(fk.parent_column_names.iter())
                    .enumerate()
                {
                    let on_update = match &fk.on_update {
                        vibesql_catalog::ReferentialAction::NoAction => "NO ACTION",
                        vibesql_catalog::ReferentialAction::Restrict => "RESTRICT",
                        vibesql_catalog::ReferentialAction::Cascade => "CASCADE",
                        vibesql_catalog::ReferentialAction::SetNull => "SET NULL",
                        vibesql_catalog::ReferentialAction::SetDefault => "SET DEFAULT",
                    };
                    let on_delete = match &fk.on_delete {
                        vibesql_catalog::ReferentialAction::NoAction => "NO ACTION",
                        vibesql_catalog::ReferentialAction::Restrict => "RESTRICT",
                        vibesql_catalog::ReferentialAction::Cascade => "CASCADE",
                        vibesql_catalog::ReferentialAction::SetNull => "SET NULL",
                        vibesql_catalog::ReferentialAction::SetDefault => "SET DEFAULT",
                    };
                    rows.push(vec![
                        Some(fk_id.to_string()),
                        Some(seq.to_string()),
                        Some(fk.parent_table.clone()),
                        Some(col_name.clone()),
                        Some(parent_col_name.clone()),
                        Some(on_update.to_string()),
                        Some(on_delete.to_string()),
                        Some("NONE".to_string()),
                    ]);
                }
            }
        }

        let row_count = rows.len();
        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None, message: None })
    }

    /// PRAGMA foreign_key_check or PRAGMA foreign_key_check(table_name)
    /// Returns rows for any FK violations: table, rowid, parent, fkid
    fn execute_pragma_foreign_key_check(
        &self,
        stmt: &vibesql_ast::PragmaStmt,
    ) -> anyhow::Result<QueryResult> {
        let columns = vec![
            "table".to_string(),
            "rowid".to_string(),
            "parent".to_string(),
            "fkid".to_string(),
        ];

        // Schema-qualified pragma handling. VibeSQL only carries a single schema today,
        // so:
        //   PRAGMA <unknown>.foreign_key_check;            -> return empty (no tables in that schema)
        //   PRAGMA <unknown>.foreign_key_check(table);     -> error "no such table: <schema>.<table>"
        // "main" and the current schema both refer to the only available schema.
        let current_schema = self.db.catalog.get_current_schema().to_string();
        if let Some(ref schema) = stmt.database {
            let is_current = schema.eq_ignore_ascii_case(&current_schema)
                || schema.eq_ignore_ascii_case("main");
            if !is_current {
                let table_part = match &stmt.value {
                    Some(vibesql_ast::PragmaValue::Identifier(name)) => Some(name.clone()),
                    Some(vibesql_ast::PragmaValue::String(name)) => Some(name.clone()),
                    _ => None,
                };
                if let Some(t) = table_part {
                    anyhow::bail!("no such table: {}.{}", schema, t);
                }
                return Ok(QueryResult {
                    columns,
                    rows: Vec::new(),
                    row_count: 0,
                    execution_time_ms: None,
                    message: None,
                });
            }
        }

        // Tuple is (table, rowid_or_null, parent, fk_id). None rowid means WITHOUT ROWID,
        // which SQLite reports as NULL.
        let mut rows: Vec<(String, Option<i64>, String, usize)> = Vec::new();
        let table_name = match &stmt.value {
            Some(vibesql_ast::PragmaValue::Identifier(name)) => Some(name.clone()),
            Some(vibesql_ast::PragmaValue::String(name)) => Some(name.clone()),
            _ => None,
        };

        // Collect tables to check
        let tables_to_check: Vec<String> = if let Some(ref name) = table_name {
            vec![name.clone()]
        } else {
            self.db.catalog.list_tables()
        };

        for tbl_name in &tables_to_check {
            let (fk_constraints, rowid_alias_idx, without_rowid) =
                if let Some(schema) = self.db.catalog.get_table(tbl_name) {
                    (
                        schema.foreign_keys.clone(),
                        schema.rowid_alias_column,
                        schema.without_rowid,
                    )
                } else {
                    continue;
                };

            if fk_constraints.is_empty() {
                continue;
            }

            // Get all rows from the child table
            // Note: tables are stored with qualified names (schema.table)
            let qualified_name = format!(
                "{}.{}",
                self.db.catalog.get_current_schema(),
                tbl_name
            );
            let child_rows: Vec<_> = if let Some(table) = self.db.tables.get(&qualified_name) {
                table.scan_live().map(|(id, row)| (id, row.clone())).collect()
            } else if let Some(table) = self.db.tables.get(tbl_name.as_str()) {
                table.scan_live().map(|(id, row)| (id, row.clone())).collect()
            } else {
                continue;
            };

            // Compute SQLite-compatible rowid for each child row.
            // - WITHOUT ROWID tables: report NULL rowid
            // - INTEGER PRIMARY KEY tables: rowid is the IPK column value
            // - Other tables: rowid is the 1-based physical index (storage starts at 0)
            let row_with_rowid: Vec<(Option<i64>, &vibesql_storage::Row)> = child_rows
                .iter()
                .map(|(phys_idx, row)| {
                    if without_rowid {
                        return (None, row);
                    }
                    let rowid = match rowid_alias_idx
                        .and_then(|idx| row.values.get(idx))
                    {
                        Some(vibesql_types::SqlValue::Integer(v)) => *v,
                        _ => (*phys_idx as i64) + 1,
                    };
                    (Some(rowid), row)
                })
                .collect();

            for (fk_id, fk) in fk_constraints.iter().enumerate() {
                // Get parent column collations so we can match SQLite's FK comparison rules
                // (numeric coercion + parent-column collation, e.g. NOCASE).
                let parent_column_collations: Vec<Option<String>> = if let Some(parent_schema) =
                    self.db.catalog.get_table(&fk.parent_table)
                {
                    fk.parent_column_indices
                        .iter()
                        .map(|&idx| {
                            parent_schema
                                .columns
                                .get(idx)
                                .and_then(|c| c.collation.clone())
                        })
                        .collect()
                } else {
                    vec![None; fk.parent_column_indices.len()]
                };

                // Get parent table data
                let parent_qualified = format!(
                    "{}.{}",
                    self.db.catalog.get_current_schema(),
                    &fk.parent_table
                );
                let parent_rows: Vec<_> =
                    if let Some(parent_table) = self.db.tables.get(&parent_qualified) {
                        parent_table
                            .scan_live()
                            .map(|(_, row)| row.clone())
                            .collect()
                    } else if let Some(parent_table) =
                        self.db.tables.get(&fk.parent_table)
                    {
                        parent_table
                            .scan_live()
                            .map(|(_, row)| row.clone())
                            .collect()
                    } else {
                        // Parent table doesn't exist - every row whose FK columns are all
                        // non-NULL is a violation. NULL FK values never violate (matches SQLite).
                        for (rowid, child_row) in &row_with_rowid {
                            let any_null = fk.column_indices.iter().any(|&idx| {
                                matches!(
                                    child_row.values.get(idx),
                                    Some(vibesql_types::SqlValue::Null) | None
                                )
                            });
                            if any_null {
                                continue;
                            }
                            rows.push((
                                tbl_name.clone(),
                                *rowid,
                                fk.parent_table.clone(),
                                fk_id,
                            ));
                        }
                        continue;
                    };

                // Check each child row against parent rows
                for (rowid, child_row) in &row_with_rowid {
                    let child_values: Vec<_> = fk
                        .column_indices
                        .iter()
                        .map(|&idx| {
                            if idx < child_row.values.len() {
                                &child_row.values[idx]
                            } else {
                                &vibesql_types::SqlValue::Null
                            }
                        })
                        .collect();

                    // Skip if any FK value is NULL (NULL doesn't violate FK)
                    if child_values
                        .iter()
                        .any(|v| matches!(v, vibesql_types::SqlValue::Null))
                    {
                        continue;
                    }

                    // Check if matching parent row exists
                    let found = parent_rows.iter().any(|parent_row| {
                        fk.parent_column_indices
                            .iter()
                            .zip(child_values.iter())
                            .enumerate()
                            .all(|(i, (&parent_idx, child_val))| {
                                if parent_idx < parent_row.values.len() {
                                    fk_values_equal(
                                        child_val,
                                        &parent_row.values[parent_idx],
                                        parent_column_collations
                                            .get(i)
                                            .and_then(|c| c.as_deref()),
                                    )
                                } else {
                                    false
                                }
                            })
                    });

                    if !found {
                        rows.push((
                            tbl_name.clone(),
                            *rowid,
                            fk.parent_table.clone(),
                            fk_id,
                        ));
                    }
                }
            }
        }

        // Sort violations by (table, rowid, fk_id) so output matches SQLite's btree order.
        rows.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(a.3.cmp(&b.3))
        });

        let final_rows: Vec<Vec<Option<String>>> = rows
            .into_iter()
            .map(|(t, rid, p, fk)| {
                vec![
                    Some(t),
                    rid.map(|v| v.to_string()),
                    Some(p),
                    Some(fk.to_string()),
                ]
            })
            .collect();

        let row_count = final_rows.len();
        Ok(QueryResult {
            columns,
            rows: final_rows,
            row_count,
            execution_time_ms: None,
            message: None,
        })
    }
}

/// SQLite-style equality for FOREIGN KEY comparisons.
///
/// SQLite considers a child value to match a parent value when:
/// - they are equal under VibeSQL's strict typed equality, OR
/// - both can be coerced to the same numeric value (e.g. INTEGER 88 == TEXT "88"), OR
/// - both are textual and equal under the parent column's collation (e.g. NOCASE).
fn fk_values_equal(
    child: &vibesql_types::SqlValue,
    parent: &vibesql_types::SqlValue,
    parent_collation: Option<&str>,
) -> bool {
    if child == parent {
        return true;
    }
    if let (Some(c), Some(p)) = (sql_value_as_f64(child), sql_value_as_f64(parent)) {
        if c == p {
            return true;
        }
    }
    if let (Some(c), Some(p)) = (sql_value_as_text(child), sql_value_as_text(parent)) {
        match parent_collation.map(|s| s.to_ascii_lowercase()) {
            Some(ref name) if name == "nocase" => return c.eq_ignore_ascii_case(p),
            Some(ref name) if name == "rtrim" => {
                return c.trim_end_matches(' ') == p.trim_end_matches(' ');
            }
            _ => {}
        }
    }
    false
}

fn sql_value_as_f64(v: &vibesql_types::SqlValue) -> Option<f64> {
    use vibesql_types::SqlValue::*;
    match v {
        Integer(i) => Some(*i as f64),
        Smallint(i) => Some(*i as f64),
        Bigint(i) => Some(*i as f64),
        Unsigned(i) => Some(*i as f64),
        Float(f) => Some(*f as f64),
        Real(r) => Some(*r as f64),
        Double(d) | Numeric(d) => Some(*d),
        Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        Character(s) | Varchar(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn sql_value_as_text(v: &vibesql_types::SqlValue) -> Option<&str> {
    use vibesql_types::SqlValue::*;
    match v {
        Character(s) | Varchar(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Convert PRAGMA value to boolean
/// ON/1/TRUE -> true, OFF/0/FALSE -> false
fn pragma_value_to_bool(value: &vibesql_ast::PragmaValue) -> bool {
    match value {
        vibesql_ast::PragmaValue::Identifier(ident) => {
            let upper = ident.to_uppercase();
            matches!(upper.as_str(), "ON" | "TRUE" | "YES")
        }
        vibesql_ast::PragmaValue::Number(num) => num != "0",
        vibesql_ast::PragmaValue::SignedNumber(num) => num != "0" && num != "-0",
        vibesql_ast::PragmaValue::String(s) => {
            let upper = s.to_uppercase();
            matches!(upper.as_str(), "ON" | "TRUE" | "YES" | "1")
        }
    }
}

/// Convert SQL LIKE pattern to regex pattern
fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    regex
}
