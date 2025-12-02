use std::time::Instant;

use vibesql_parser::Parser;
use vibesql_storage::Database;

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
    pub rows: Vec<Vec<String>>,
    pub columns: Vec<String>,
    pub row_count: usize,
    pub execution_time_ms: Option<f64>,
}

impl SqlExecutor {
    pub fn new(database: Option<String>) -> anyhow::Result<Self> {
        // Load database from file if provided, otherwise create new in-memory database
        let db = if let Some(db_path) = database {
            // Check if file exists
            if std::path::Path::new(&db_path).exists() {
                // Load existing database from SQL dump using shared executor function
                vibesql_executor::load_sql_dump(&db_path)
                    .map_err(|e| anyhow::anyhow!("Failed to load database: {}", e))?
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

        // Parse SQL
        let statement = Parser::parse_sql(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Execute statement through appropriate executor
        let mut result = QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
            row_count: 0,
            execution_time_ms: None,
        };

        match statement {
            vibesql_ast::Statement::Select(select_stmt) => {
                // Execute SELECT and format results
                let executor = vibesql_executor::SelectExecutor::new(&self.db);
                match executor.execute(&select_stmt) {
                    Ok(rows) => {
                        result.row_count = rows.len();
                        // Convert rows to string representation
                        if !rows.is_empty() {
                            // Get column names from the select statement
                            result.columns = vec!["Column".to_string(); rows[0].values.len()];
                            for row in rows {
                                let row_strs: Vec<String> =
                                    row.values.iter().map(|v| format!("{:?}", v)).collect();
                                result.rows.push(row_strs);
                            }
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
                        result.row_count = affected_rows;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                match vibesql_executor::UpdateExecutor::execute(&update_stmt, &mut self.db) {
                    Ok(affected_rows) => {
                        result.row_count = affected_rows;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                match vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut self.db) {
                    Ok(affected_rows) => {
                        result.row_count = affected_rows;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateView(view_stmt) => {
                match vibesql_executor::advanced_objects::execute_create_view(&view_stmt, &mut self.db) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropView(drop_stmt) => {
                match vibesql_executor::advanced_objects::execute_drop_view(&drop_stmt, &mut self.db) {
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
                match vibesql_executor::SchemaExecutor::execute_set_variable(&set_var_stmt, &mut self.db) {
                    Ok(_) => {
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Reindex(reindex_stmt) => {
                match vibesql_executor::ReindexExecutor::execute(&reindex_stmt, &self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Analyze(analyze_stmt) => {
                match vibesql_executor::AnalyzeExecutor::execute(&analyze_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateIndex(index_stmt) => {
                match vibesql_executor::CreateIndexExecutor::execute(&index_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropIndex(drop_stmt) => {
                match vibesql_executor::DropIndexExecutor::execute(&drop_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::AlterTable(alter_stmt) => {
                match vibesql_executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::BeginTransaction(begin_stmt) => {
                match vibesql_executor::BeginTransactionExecutor::execute(&begin_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Commit(commit_stmt) => {
                match vibesql_executor::CommitExecutor::execute(&commit_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Rollback(rollback_stmt) => {
                match vibesql_executor::RollbackExecutor::execute(&rollback_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Savepoint(savepoint_stmt) => {
                match vibesql_executor::SavepointExecutor::execute(&savepoint_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::RollbackToSavepoint(rollback_stmt) => {
                match vibesql_executor::RollbackToSavepointExecutor::execute(&rollback_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::ReleaseSavepoint(release_stmt) => {
                match vibesql_executor::ReleaseSavepointExecutor::execute(&release_stmt, &mut self.db) {
                    Ok(msg) => {
                        println!("{}", msg);
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

        let rows: Vec<Vec<String>> = filtered_tables.iter().map(|t| vec![t.clone()]).collect();
        let row_count = rows.len();

        Ok(QueryResult {
            columns: vec!["Tables_in_database".to_string()],
            rows,
            row_count,
            execution_time_ms: None,
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

        let rows: Vec<Vec<String>> = filtered_schemas.iter().map(|s| vec![s.clone()]).collect();
        let row_count = rows.len();

        Ok(QueryResult {
            columns: vec!["Database".to_string()],
            rows,
            row_count,
            execution_time_ms: None,
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

        let mut rows: Vec<Vec<String>> = Vec::new();

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
            let default_val = column
                .default_value
                .as_ref()
                .map(|v| format!("{:?}", v))
                .unwrap_or_default();

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
                    column.name.clone(),
                    display::format_data_type(&column.data_type),
                    String::new(), // Collation - not yet supported
                    nullable.to_string(),
                    key.to_string(),
                    default_val,
                    String::new(), // Extra
                    String::new(), // Privileges
                    String::new(), // Comment
                ]
            } else {
                vec![
                    column.name.clone(),
                    display::format_data_type(&column.data_type),
                    nullable.to_string(),
                    key.to_string(),
                    default_val,
                    String::new(), // Extra
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

        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None })
    }

    /// Execute SHOW INDEX statement
    fn execute_show_index(
        &self,
        stmt: &vibesql_ast::ShowIndexStmt,
    ) -> anyhow::Result<QueryResult> {
        let normalized_name = stmt.table_name.to_uppercase();

        // Verify table exists
        let _ = self
            .db
            .get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", stmt.table_name))?;

        let index_names = self.db.list_indexes();
        let mut rows: Vec<Vec<String>> = Vec::new();

        for index_name in index_names {
            if let Some(index_meta) = self.db.get_index(&index_name) {
                if index_meta.table_name == normalized_name {
                    // Add one row per column in the index
                    for (seq, col) in index_meta.columns.iter().enumerate() {
                        rows.push(vec![
                            normalized_name.clone(),           // Table
                            if index_meta.unique { "0" } else { "1" }.to_string(), // Non_unique
                            index_meta.index_name.clone(),     // Key_name
                            (seq + 1).to_string(),             // Seq_in_index
                            col.column_name.clone(),           // Column_name
                            "A".to_string(),                   // Collation (always Ascending for now)
                            String::new(),                     // Cardinality
                            String::new(),                     // Sub_part
                            String::new(),                     // Packed
                            String::new(),                     // Null
                            "BTREE".to_string(),               // Index_type
                            String::new(),                     // Comment
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
        })
    }

    /// Execute SHOW CREATE TABLE statement
    fn execute_show_create_table(
        &self,
        stmt: &vibesql_ast::ShowCreateTableStmt,
    ) -> anyhow::Result<QueryResult> {
        let normalized_name = stmt.table_name.to_uppercase();
        let table = self
            .db
            .get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", stmt.table_name))?;

        // Build CREATE TABLE statement
        let mut create_sql = format!("CREATE TABLE {} (\n", normalized_name);

        // Add columns
        let mut column_defs: Vec<String> = Vec::new();
        for column in &table.schema.columns {
            let mut def = format!(
                "  {} {}",
                column.name,
                display::format_data_type(&column.data_type)
            );
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
            rows: vec![vec![normalized_name, create_sql]],
            row_count: 1,
            execution_time_ms: None,
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
