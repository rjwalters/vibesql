//! DML and transaction execution for WASM bindings
//!
//! This module handles data manipulation (INSERT, UPDATE, DELETE) and
//! transaction control (BEGIN, COMMIT, ROLLBACK, SAVEPOINT) operations.

use wasm_bindgen::prelude::*;

use crate::{Database, ExecuteResult};

/// Executes a DDL or DML statement (CREATE TABLE, INSERT, UPDATE, DELETE)
/// Returns a JSON string with the result
pub fn execute_statement(db: &mut Database, sql: &str) -> Result<JsValue, JsValue> {
    // Parse the SQL
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .map_err(|e| JsValue::from_str(&format!("Parse error: {:?}", e)))?;

    // Execute based on statement type
    match stmt {
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db.db)
                .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult {
                rows_affected: 0,
                message: format!("Table '{}' created successfully", create_stmt.table_name),
            };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::DropTable(drop_stmt) => {
            let message = vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut db.db)
                .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            let row_count = vibesql_executor::InsertExecutor::execute(&mut db.db, &insert_stmt)
                .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

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
            let message =
                vibesql_executor::BeginTransactionExecutor::execute(&begin_stmt, &mut db.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::Commit(commit_stmt) => {
            let message = vibesql_executor::CommitExecutor::execute(&commit_stmt, &mut db.db)
                .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::Rollback(rollback_stmt) => {
            let message = vibesql_executor::RollbackExecutor::execute(&rollback_stmt, &mut db.db)
                .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::Savepoint(savepoint_stmt) => {
            let message = vibesql_executor::SavepointExecutor::execute(&savepoint_stmt, &mut db.db)
                .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::RollbackToSavepoint(rollback_to_stmt) => {
            let message = vibesql_executor::RollbackToSavepointExecutor::execute(
                &rollback_to_stmt,
                &mut db.db,
            )
            .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::ReleaseSavepoint(release_stmt) => {
            let message =
                vibesql_executor::ReleaseSavepointExecutor::execute(&release_stmt, &mut db.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::CreateRole(create_role_stmt) => {
            let message =
                vibesql_executor::RoleExecutor::execute_create_role(&create_role_stmt, &mut db.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::DropRole(drop_role_stmt) => {
            let message =
                vibesql_executor::RoleExecutor::execute_drop_role(&drop_role_stmt, &mut db.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::CreateDomain(create_domain_stmt) => {
            let message = vibesql_executor::DomainExecutor::execute_create_domain(
                &create_domain_stmt,
                &mut db.db,
            )
            .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::DropDomain(drop_domain_stmt) => {
            let message = vibesql_executor::DomainExecutor::execute_drop_domain(
                &drop_domain_stmt,
                &mut db.db,
            )
            .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::CreateSequence(create_seq_stmt) => {
            vibesql_executor::advanced_objects::execute_create_sequence(
                &create_seq_stmt,
                &mut db.db,
            )
            .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult {
                rows_affected: 0,
                message: format!(
                    "Sequence '{}' created successfully",
                    create_seq_stmt.sequence_name
                ),
            };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::DropSequence(drop_seq_stmt) => {
            vibesql_executor::advanced_objects::execute_drop_sequence(&drop_seq_stmt, &mut db.db)
                .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult {
                rows_affected: 0,
                message: format!("Sequence '{}' dropped successfully", drop_seq_stmt.sequence_name),
            };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::AlterSequence(alter_seq_stmt) => {
            vibesql_executor::advanced_objects::execute_alter_sequence(&alter_seq_stmt, &mut db.db)
                .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult {
                rows_affected: 0,
                message: format!(
                    "Sequence '{}' altered successfully",
                    alter_seq_stmt.sequence_name
                ),
            };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::CreateType(create_type_stmt) => {
            let message =
                vibesql_executor::TypeExecutor::execute_create_type(&create_type_stmt, &mut db.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::DropType(drop_type_stmt) => {
            let message =
                vibesql_executor::TypeExecutor::execute_drop_type(&drop_type_stmt, &mut db.db)
                    .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult { rows_affected: 0, message };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::CreateTrigger(create_trigger_stmt) => {
            vibesql_executor::advanced_objects::execute_create_trigger(
                &create_trigger_stmt,
                &mut db.db,
            )
            .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult {
                rows_affected: 0,
                message: format!(
                    "Trigger '{}' created successfully",
                    create_trigger_stmt.trigger_name
                ),
            };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        vibesql_ast::Statement::DropTrigger(drop_trigger_stmt) => {
            vibesql_executor::advanced_objects::execute_drop_trigger(
                &drop_trigger_stmt,
                &mut db.db,
            )
            .map_err(|e| JsValue::from_str(&format!("Execution error: {:?}", e)))?;

            let result = ExecuteResult {
                rows_affected: 0,
                message: format!(
                    "Trigger '{}' dropped successfully",
                    drop_trigger_stmt.trigger_name
                ),
            };

            serde_wasm_bindgen::to_value(&result)
                .map_err(|e| JsValue::from_str(&format!("Serialization error: {:?}", e)))
        }
        _ => {
            Err(JsValue::from_str(&format!("Statement type not yet supported in WASM: {:?}", stmt)))
        }
    }
}
