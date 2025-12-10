//! DDL/DML execution methods

use super::database::Database;
use super::types::ExecuteResult;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
impl Database {
    /// Executes a DDL or DML statement (CREATE TABLE, INSERT, UPDATE, DELETE)
    /// Returns a JSON string with the result
    pub fn execute(&mut self, sql: &str) -> Result<JsValue, JsValue> {
        // Parse the SQL
        let stmt = vibesql_parser::Parser::parse_sql(sql)
            .map_err(|e| JsValue::from_str(&format!("Parse error: {:?}", e)))?;

        // Execute based on statement type
        match stmt {
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Table '{}' created successfully", create_stmt.table_name),
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                let message = vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                vibesql_executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let row_count = insert_stmt.values.len();
                let result = ExecuteResult {
                    rows_affected: row_count,
                    message: format!(
                        "{} row{} inserted into '{}'",
                        row_count,
                        if row_count == 1 { "" } else { "s" },
                        insert_stmt.table_name
                    ),
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::Select(_) => {
                Err(JsValue::from_str("Use query() method for SELECT statements"))
            }
            vibesql_ast::Statement::BeginTransaction(begin_stmt) => {
                let message = vibesql_executor::BeginTransactionExecutor::execute(&begin_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::Commit(commit_stmt) => {
                let message = vibesql_executor::CommitExecutor::execute(&commit_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::Rollback(rollback_stmt) => {
                let message = vibesql_executor::RollbackExecutor::execute(&rollback_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::AlterTable(alter_stmt) => {
                let message = vibesql_executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateSchema(create_stmt) => {
                let message = vibesql_executor::SchemaExecutor::execute_create_schema(&create_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropSchema(drop_stmt) => {
                let message = vibesql_executor::SchemaExecutor::execute_drop_schema(&drop_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::SetSchema(set_stmt) => {
                let message = vibesql_executor::SchemaExecutor::execute_set_schema(&set_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::SetCatalog(set_stmt) => {
                let message = vibesql_executor::SchemaExecutor::execute_set_catalog(&set_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::SetNames(set_stmt) => {
                let message = vibesql_executor::SchemaExecutor::execute_set_names(&set_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::SetTimeZone(set_stmt) => {
                let message = vibesql_executor::SchemaExecutor::execute_set_time_zone(&set_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::Grant(grant_stmt) => {
                let message = vibesql_executor::GrantExecutor::execute_grant(&grant_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::Revoke(revoke_stmt) => {
                let message = vibesql_executor::RevokeExecutor::execute_revoke(&revoke_stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

                let result = ExecuteResult {
                    rows_affected: 0,
                    message,
                };

                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            // Advanced SQL:1999 objects
            vibesql_ast::Statement::CreateDomain(stmt) => {
                vibesql_executor::advanced_objects::execute_create_domain(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Domain '{}' created successfully", stmt.domain_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropDomain(stmt) => {
                vibesql_executor::advanced_objects::execute_drop_domain(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Domain '{}' dropped successfully", stmt.domain_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateSequence(stmt) => {
                vibesql_executor::advanced_objects::execute_create_sequence(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Sequence '{}' created successfully", stmt.sequence_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropSequence(stmt) => {
                vibesql_executor::advanced_objects::execute_drop_sequence(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Sequence '{}' dropped successfully", stmt.sequence_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateType(stmt) => {
                vibesql_executor::advanced_objects::execute_create_type(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Type '{}' created successfully", stmt.type_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropType(stmt) => {
                vibesql_executor::advanced_objects::execute_drop_type(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Type '{}' dropped successfully", stmt.type_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateCollation(stmt) => {
                vibesql_executor::advanced_objects::execute_create_collation(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Collation '{}' created successfully", stmt.collation_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropCollation(stmt) => {
                vibesql_executor::advanced_objects::execute_drop_collation(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Collation '{}' dropped successfully", stmt.collation_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateCharacterSet(stmt) => {
                vibesql_executor::advanced_objects::execute_create_character_set(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Character set '{}' created successfully", stmt.charset_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropCharacterSet(stmt) => {
                vibesql_executor::advanced_objects::execute_drop_character_set(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Character set '{}' dropped successfully", stmt.charset_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateTranslation(stmt) => {
                vibesql_executor::advanced_objects::execute_create_translation(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Translation '{}' created successfully", stmt.translation_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropTranslation(stmt) => {
                vibesql_executor::advanced_objects::execute_drop_translation(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Translation '{}' dropped successfully", stmt.translation_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateView(mut stmt) => {
                // Store original SQL for sqlite_master compatibility
                stmt.sql_definition = Some(sql.to_string());
                vibesql_executor::advanced_objects::execute_create_view(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("View '{}' created successfully", stmt.view_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropView(stmt) => {
                vibesql_executor::advanced_objects::execute_drop_view(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("View '{}' dropped successfully", stmt.view_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateTrigger(stmt) => {
                vibesql_executor::advanced_objects::execute_create_trigger(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Trigger '{}' created successfully", stmt.trigger_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            vibesql_ast::Statement::DropTrigger(stmt) => {
                vibesql_executor::advanced_objects::execute_drop_trigger(&stmt, &mut self.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;
                let result = ExecuteResult {
                    rows_affected: 0,
                    message: format!("Trigger '{}' dropped successfully", stmt.trigger_name),
                };
                serde_wasm_bindgen::to_value(&result)
                    .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
            }
            _ => Err(JsValue::from_str(&format!(
                "Statement type not yet supported in WASM: {:?}",
                stmt
            ))),
        }
    }
}
