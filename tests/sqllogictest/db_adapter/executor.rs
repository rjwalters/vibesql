//! Core SQL execution and statement dispatching.
//!
//! This module handles parsing SQL statements and dispatching them to the appropriate
//! executors. It coordinates with caching, batching, and timing modules to optimize
//! and instrument query execution.

use std::{collections::HashSet, env};
use sqllogictest::{DBOutput, DefaultColumnType};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use super::{batching::BatchingManager, cache::CacheManager, timing::truncate_sql};
use super::super::execution::TestError;

/// Execute a SQL statement with caching, batching, and timing support.
pub fn execute_sql(
    sql: &str,
    db: &mut Database,
    cache_manager: &mut CacheManager,
    batching_manager: &mut BatchingManager,
) -> Result<DBOutput<DefaultColumnType>, TestError> {
    use std::time::Instant;

    let profile_queries = env::var("SQLLOGICTEST_PROFILE").is_ok();
    let total_start = if profile_queries { Some(Instant::now()) } else { None };

    let parse_start = if profile_queries { Some(Instant::now()) } else { None };
    let stmt = Parser::parse_sql(sql)
        .map_err(|e| TestError::Execution(format!("Parse error: {:?}", e)))?;
    let parse_time = parse_start.map(|s| s.elapsed());

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            // Commit any pending implicit transaction before running query
            batching_manager.commit_if_needed(db)?;

            // Try cache first if enabled
            // Include SqlMode in signature to prevent cross-dialect cache hits
            if cache_manager.enabled {
                let signature = cache_manager.get_signature_with_mode(sql, &db.sql_mode());
                if let Some((cached_rows, _schema)) = cache_manager.cache.get(&signature) {
                    cache_manager.record_hit();
                    return format_query_result(cached_rows);
                }
                cache_manager.record_miss();
            }

            // Cache miss or cache disabled - execute query
            let exec_start = if profile_queries { Some(Instant::now()) } else { None };
            let executor = SelectExecutor::new(db);
            let rows = executor
                .execute(&select_stmt)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            let exec_time = exec_start.map(|s| s.elapsed());

            // Log profiling info if enabled
            if let (Some(parse_elapsed), Some(exec_elapsed), Some(total_elapsed)) =
               (parse_time, exec_time, total_start.map(|s| s.elapsed())) {
                if exec_elapsed.as_millis() > 10 {  // Only log queries >10ms
                    let sql_preview = truncate_sql(sql, 80);
                    let query_count = 0; // Query count would need to be passed in
                    eprintln!("🔍 Query #{}: parse={:.2}ms, exec={:.2}ms, total={:.2}ms | {}",
                        query_count,
                        parse_elapsed.as_secs_f64() * 1000.0,
                        exec_elapsed.as_secs_f64() * 1000.0,
                        total_elapsed.as_secs_f64() * 1000.0,
                        sql_preview);
                }
            }

            // Cache the result if cache is enabled
            // Include SqlMode in signature to prevent cross-dialect cache hits
            if cache_manager.enabled {
                use vibesql_catalog::{ColumnSchema, TableSchema};
                use vibesql_executor::schema::CombinedSchema;

                let signature = cache_manager.get_signature_with_mode(sql, &db.sql_mode());

                // Create a simple schema from the result rows
                let schema = if let Some(first_row) = rows.first() {
                    let columns: Vec<ColumnSchema> = first_row.values.iter().enumerate().map(|(i, val)| {
                        ColumnSchema {
                            name: format!("col{}", i),
                            data_type: val.get_type(),
                            nullable: val.is_null(),
                            default_value: None,
                        }
                    }).collect();
                    let table_schema = TableSchema::new("result".to_string(), columns);
                    CombinedSchema::from_table("result".to_string(), table_schema)
                } else {
                    // Empty result - create empty schema
                    let table_schema = TableSchema::new("result".to_string(), vec![]);
                    CombinedSchema::from_table("result".to_string(), table_schema)
                };

                // Extract table names from SELECT statement for cache invalidation
                let tables = extract_table_names(&select_stmt);

                cache_manager.cache.insert(signature, rows.clone(), schema, tables);
            }

            format_query_result(rows)
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::CreateTableExecutor::execute(&create_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            // Automatic INSERT batching for test data loading optimization
            batching_manager.begin_if_needed(db)?;
            batching_manager.record_insert();

            // Invalidate cache for this table
            if cache_manager.enabled {
                cache_manager.invalidate_table(&insert_stmt.table_name);
            }

            let rows_affected =
                vibesql_executor::InsertExecutor::execute(db, &insert_stmt)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(rows_affected as u64))
        }
        vibesql_ast::Statement::Update(update_stmt) => {
            // Commit any pending implicit transaction before UPDATE
            batching_manager.commit_if_needed(db)?;

            // Invalidate cache for this table
            if cache_manager.enabled {
                cache_manager.invalidate_table(&update_stmt.table_name);
            }

            let rows_affected =
                vibesql_executor::UpdateExecutor::execute(&update_stmt, db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(rows_affected as u64))
        }
        vibesql_ast::Statement::Delete(delete_stmt) => {
            // Commit any pending implicit transaction before DELETE
            batching_manager.commit_if_needed(db)?;

            // Invalidate cache for this table
            if cache_manager.enabled {
                cache_manager.invalidate_table(&delete_stmt.table_name);
            }

            let rows_affected =
                vibesql_executor::DeleteExecutor::execute(&delete_stmt, db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(rows_affected as u64))
        }
        vibesql_ast::Statement::DropTable(drop_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            // Invalidate cache for this table
            if cache_manager.enabled {
                cache_manager.invalidate_table(&drop_stmt.table_name);
            }

            vibesql_executor::DropTableExecutor::execute(&drop_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::AlterTable(alter_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::AlterTableExecutor::execute(&alter_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::CreateSchema(create_schema_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::SchemaExecutor::execute_create_schema(&create_schema_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::DropSchema(drop_schema_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::SchemaExecutor::execute_drop_schema(&drop_schema_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::SetSchema(set_schema_stmt) => {
            vibesql_executor::SchemaExecutor::execute_set_schema(&set_schema_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::SetCatalog(set_stmt) => {
            vibesql_executor::SchemaExecutor::execute_set_catalog(&set_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::SetNames(set_stmt) => {
            vibesql_executor::SchemaExecutor::execute_set_names(&set_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::SetVariable(set_var_stmt) => {
            vibesql_executor::SchemaExecutor::execute_set_variable(&set_var_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::SetTimeZone(set_stmt) => {
            vibesql_executor::SchemaExecutor::execute_set_time_zone(&set_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::Grant(grant_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::GrantExecutor::execute_grant(&grant_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::Revoke(revoke_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::RevokeExecutor::execute_revoke(&revoke_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::CreateRole(create_role_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::RoleExecutor::execute_create_role(&create_role_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::DropRole(drop_role_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::RoleExecutor::execute_drop_role(&drop_role_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::CreateDomain(create_domain_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::DomainExecutor::execute_create_domain(&create_domain_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::DropDomain(drop_domain_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::DomainExecutor::execute_drop_domain(&drop_domain_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::CreateType(create_type_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::TypeExecutor::execute_create_type(&create_type_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::DropType(drop_type_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::TypeExecutor::execute_drop_type(&drop_type_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::CreateAssertion(create_assertion_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::advanced_objects::execute_create_assertion(
                &create_assertion_stmt,
                db,
            )
            .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::DropAssertion(drop_assertion_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::advanced_objects::execute_drop_assertion(
                &drop_assertion_stmt,
                db,
            )
            .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::CreateView(create_view_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::advanced_objects::execute_create_view(&create_view_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::DropView(drop_view_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::advanced_objects::execute_drop_view(&drop_view_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::CreateIndex(create_index_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::IndexExecutor::execute(&create_index_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::DropIndex(drop_index_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::IndexExecutor::execute_drop(&drop_index_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::Analyze(analyze_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::AnalyzeExecutor::execute(&analyze_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::Reindex(reindex_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::IndexExecutor::execute_reindex(&reindex_stmt, db)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::CreateTrigger(create_trigger_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::TriggerExecutor::create_trigger(db, &create_trigger_stmt)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::DropTrigger(drop_trigger_stmt) => {
            // Commit any pending implicit transaction before DDL
            batching_manager.commit_if_needed(db)?;

            vibesql_executor::TriggerExecutor::drop_trigger(db, &drop_trigger_stmt)
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::BeginTransaction(_) => {
            // Commit any pending implicit transaction before explicit transaction
            batching_manager.commit_if_needed(db)?;

            db.begin_transaction()
                .map_err(|e| TestError::Execution(format!("Transaction error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::Commit(_) => {
            // This should not have an implicit transaction, but check anyway
            batching_manager.commit_if_needed(db)?;

            db.commit_transaction()
                .map_err(|e| TestError::Execution(format!("Transaction error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::Rollback(_) => {
            // Rollback any implicit transaction if exists
            batching_manager.rollback_if_needed();

            db.rollback_transaction()
                .map_err(|e| TestError::Execution(format!("Transaction error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::Savepoint(savepoint_stmt) => {
            // Commit any pending implicit transaction before savepoint
            batching_manager.commit_if_needed(db)?;

            db.create_savepoint(savepoint_stmt.name.clone())
                .map_err(|e| TestError::Execution(format!("Transaction error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::RollbackToSavepoint(rollback_stmt) => {
            // Commit any pending implicit transaction before rollback to savepoint
            batching_manager.commit_if_needed(db)?;

            db.rollback_to_savepoint(rollback_stmt.name.clone())
                .map_err(|e| TestError::Execution(format!("Transaction error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        vibesql_ast::Statement::ReleaseSavepoint(release_stmt) => {
            // Commit any pending implicit transaction before release savepoint
            batching_manager.commit_if_needed(db)?;

            db.release_savepoint(release_stmt.name.clone())
                .map_err(|e| TestError::Execution(format!("Transaction error: {:?}", e)))?;
            Ok(DBOutput::StatementComplete(0))
        }
        // Unimplemented statements return success for now
        vibesql_ast::Statement::SetTransaction(_)
        | vibesql_ast::Statement::CreateSequence(_)
        | vibesql_ast::Statement::DropSequence(_)
        | vibesql_ast::Statement::AlterSequence(_)
        | vibesql_ast::Statement::CreateCollation(_)
        | vibesql_ast::Statement::DropCollation(_)
        | vibesql_ast::Statement::CreateCharacterSet(_)
        | vibesql_ast::Statement::DropCharacterSet(_)
        | vibesql_ast::Statement::CreateTranslation(_)
        | vibesql_ast::Statement::DropTranslation(_)
        | vibesql_ast::Statement::DeclareCursor(_)
        | vibesql_ast::Statement::OpenCursor(_)
        | vibesql_ast::Statement::Fetch(_)
        | vibesql_ast::Statement::CloseCursor(_)
        | vibesql_ast::Statement::CreateProcedure(_)
        | vibesql_ast::Statement::DropProcedure(_)
        | vibesql_ast::Statement::CreateFunction(_)
        | vibesql_ast::Statement::DropFunction(_)
        | vibesql_ast::Statement::Call(_)
        | vibesql_ast::Statement::TruncateTable(_)
        | vibesql_ast::Statement::ShowTables(_)
        | vibesql_ast::Statement::ShowDatabases(_)
        | vibesql_ast::Statement::ShowColumns(_)
        | vibesql_ast::Statement::ShowIndex(_)
        | vibesql_ast::Statement::ShowCreateTable(_)
        | vibesql_ast::Statement::Describe(_)
        | vibesql_ast::Statement::AlterTrigger(_) => Ok(DBOutput::StatementComplete(0)),
    }
}

/// Format query result rows for SQLLogicTest.
fn format_query_result(
    rows: Vec<vibesql_storage::Row>,
) -> Result<DBOutput<DefaultColumnType>, TestError> {
    if rows.is_empty() {
        return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
    }

    let types: Vec<DefaultColumnType> = rows[0]
        .values
        .iter()
        .map(|val| match val {
            SqlValue::Integer(_)
            | SqlValue::Smallint(_)
            | SqlValue::Bigint(_)
            | SqlValue::Unsigned(_) => DefaultColumnType::Integer,
            SqlValue::Float(_)
            | SqlValue::Real(_)
            | SqlValue::Double(_)
            | SqlValue::Numeric(_) => DefaultColumnType::FloatingPoint,
            SqlValue::Varchar(_)
            | SqlValue::Character(_)
            | SqlValue::Date(_)
            | SqlValue::Time(_)
            | SqlValue::Timestamp(_)
            | SqlValue::Interval(_) => DefaultColumnType::Text,
            SqlValue::Boolean(_) => DefaultColumnType::Integer,
            SqlValue::Null => DefaultColumnType::Any,
        })
        .collect();

    use super::super::formatting::format_sql_value;
    let formatted_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .enumerate()
                .map(|(col_idx, val)| format_sql_value(val, types.get(col_idx)))
                .collect()
        })
        .collect();

    Ok(DBOutput::Rows { types, rows: formatted_rows })
}

/// Extract table names from a SELECT statement for cache invalidation.
fn extract_table_names(select: &vibesql_ast::SelectStmt) -> HashSet<String> {
    let mut tables = HashSet::new();

    // Extract from FROM clause
    if let Some(ref from) = select.from {
        extract_table_names_from_from(from, &mut tables);
    }

    tables
}

fn extract_table_names_from_from(
    from: &vibesql_ast::FromClause,
    tables: &mut HashSet<String>,
) {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            // Handle schema.table format
            let table_name = if let Some(pos) = name.rfind('.') {
                &name[pos + 1..]
            } else {
                name
            };
            tables.insert(table_name.to_string());
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            extract_table_names_from_from(left, tables);
            extract_table_names_from_from(right, tables);
        }
        vibesql_ast::FromClause::Subquery { query, .. } => {
            // Recursively extract from subquery
            let subquery_tables = extract_table_names(query);
            tables.extend(subquery_tables);
        }
    }
}
