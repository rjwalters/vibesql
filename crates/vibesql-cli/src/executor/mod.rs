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
            vibesql_ast::Statement::Explain(explain_stmt) => {
                match vibesql_executor::ExplainExecutor::execute(&explain_stmt, &self.db) {
                    Ok(explain_result) => {
                        // Format output based on requested format
                        let output = match explain_stmt.format {
                            vibesql_ast::ExplainFormat::Text => explain_result.to_text(),
                            vibesql_ast::ExplainFormat::Json => explain_result.to_json(),
                        };
                        // Return as a single row with the plan output
                        result.columns = vec!["QUERY PLAN".to_string()];
                        // Split output into rows for better display
                        for line in output.lines() {
                            result.rows.push(vec![line.to_string()]);
                        }
                        result.row_count = result.rows.len();
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
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
}
