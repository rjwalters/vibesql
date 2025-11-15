//! SQLLogicTest integration for comprehensive SQL correctness testing.

use async_trait::async_trait;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

#[derive(Debug)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

struct NistMemSqlDB {
    db: Database,
    cache: std::sync::Arc<vibesql_executor::QueryPlanCache>,
}

impl NistMemSqlDB {
    fn new() -> Self {
        Self {
            db: Database::new(),
            cache: std::sync::Arc::new(vibesql_executor::QueryPlanCache::new(1000)),
        }
    }

    /// Format result rows for SQLLogicTest
    /// Returns multi-column rows as Vec<Vec<String>> where each inner Vec represents one row.
    /// The sqllogictest library will handle sorting and formatting based on the sort mode.
    fn format_result_rows(
        &self,
        rows: &[vibesql_storage::Row],
        types: Vec<DefaultColumnType>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let mut formatted_rows: Vec<Vec<String>> = Vec::new();

        // Build formatted output: each row contains all column values
        for row in rows {
            let mut formatted_row = Vec::new();
            for (col_idx, val) in row.values.iter().enumerate() {
                let formatted_val = self.format_sql_value(val, types.get(col_idx));
                formatted_row.push(formatted_val);
            }
            formatted_rows.push(formatted_row);
        }

        Ok(DBOutput::Rows { types, rows: formatted_rows })
    }

    fn execute_sql(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let stmt =
            Parser::parse_sql(sql).map_err(|e| TestError(format!("Parse error: {:?}", e)))?;

        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&self.db);
                let rows = executor
                    .execute(&select_stmt)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                self.format_query_result(rows)
            }
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                // Extract table name for cache invalidation
                let table_name = if let Some(pos) = create_stmt.table_name.rfind('.') {
                    &create_stmt.table_name[pos + 1..]
                } else {
                    &create_stmt.table_name
                };

                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;

                // Invalidate cache for this table
                self.cache.invalidate_table(table_name);

                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                let rows_affected = vibesql_executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                let rows_affected = vibesql_executor::UpdateExecutor::execute(&update_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                let rows_affected = vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                // Extract table name for cache invalidation
                let table_name = if let Some(pos) = drop_stmt.table_name.rfind('.') {
                    &drop_stmt.table_name[pos + 1..]
                } else {
                    &drop_stmt.table_name
                };

                vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;

                // Invalidate cache for this table
                self.cache.invalidate_table(table_name);

                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::AlterTable(alter_stmt) => {
                vibesql_executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateSchema(create_schema_stmt) => {
                vibesql_executor::SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropSchema(drop_schema_stmt) => {
                vibesql_executor::SchemaExecutor::execute_drop_schema(&drop_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetSchema(set_schema_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_schema(&set_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetCatalog(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_catalog(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateIndex(create_index_stmt) => {
                vibesql_executor::IndexExecutor::execute(&create_index_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropIndex(drop_index_stmt) => {
                vibesql_executor::IndexExecutor::execute_drop(&drop_index_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Reindex(reindex_stmt) => {
                vibesql_executor::IndexExecutor::execute_reindex(&reindex_stmt, &self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetNames(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_names(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetTimeZone(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_time_zone(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Grant(grant_stmt) => {
                vibesql_executor::GrantExecutor::execute_grant(&grant_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Revoke(revoke_stmt) => {
                vibesql_executor::RevokeExecutor::execute_revoke(&revoke_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateRole(create_role_stmt) => {
                vibesql_executor::RoleExecutor::execute_create_role(&create_role_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropRole(drop_role_stmt) => {
                vibesql_executor::RoleExecutor::execute_drop_role(&drop_role_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateDomain(create_domain_stmt) => {
                vibesql_executor::DomainExecutor::execute_create_domain(&create_domain_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropDomain(drop_domain_stmt) => {
                vibesql_executor::DomainExecutor::execute_drop_domain(&drop_domain_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateType(create_type_stmt) => {
                vibesql_executor::TypeExecutor::execute_create_type(&create_type_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropType(drop_type_stmt) => {
                vibesql_executor::TypeExecutor::execute_drop_type(&drop_type_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateAssertion(create_assertion_stmt) => {
                vibesql_executor::advanced_objects::execute_create_assertion(
                    &create_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropAssertion(drop_assertion_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_assertion(
                    &drop_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateProcedure(create_proc_stmt) => {
                vibesql_executor::advanced_objects::execute_create_procedure(
                    &create_proc_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropProcedure(drop_proc_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_procedure(
                    &drop_proc_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateFunction(create_func_stmt) => {
                vibesql_executor::advanced_objects::execute_create_function(
                    &create_func_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropFunction(drop_func_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_function(
                    &drop_func_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Call(call_stmt) => {
                vibesql_executor::advanced_objects::execute_call(&call_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateTrigger(stmt) => {
                vibesql_executor::TriggerExecutor::create_trigger(&mut self.db, &stmt)
                    .map(|_msg| DBOutput::StatementComplete(0))
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))
            }
            vibesql_ast::Statement::DropTrigger(stmt) => {
                vibesql_executor::TriggerExecutor::drop_trigger(&mut self.db, &stmt)
                    .map(|_msg| DBOutput::StatementComplete(0))
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))
            }
            vibesql_ast::Statement::CreateView(create_view_stmt) => {
                vibesql_executor::ViewExecutor::execute_create_view(&create_view_stmt, &mut self.db)
                    .map(|_msg| DBOutput::StatementComplete(0))
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))
            }
            vibesql_ast::Statement::DropView(drop_view_stmt) => {
                vibesql_executor::ViewExecutor::execute_drop_view(&drop_view_stmt, &mut self.db)
                    .map(|_msg| DBOutput::StatementComplete(0))
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))
            }
            vibesql_ast::Statement::TruncateTable(_)
            | vibesql_ast::Statement::ShowTables(_)
            | vibesql_ast::Statement::ShowDatabases(_)
            | vibesql_ast::Statement::ShowColumns(_)
            | vibesql_ast::Statement::ShowIndex(_)
            | vibesql_ast::Statement::ShowCreateTable(_)
            | vibesql_ast::Statement::Describe(_)
            | vibesql_ast::Statement::BeginTransaction(_)
            | vibesql_ast::Statement::Commit(_)
            | vibesql_ast::Statement::Rollback(_)
            | vibesql_ast::Statement::Savepoint(_)
            | vibesql_ast::Statement::RollbackToSavepoint(_)
            | vibesql_ast::Statement::ReleaseSavepoint(_)
            | vibesql_ast::Statement::SetTransaction(_)
            | vibesql_ast::Statement::SetVariable(_)
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
            | vibesql_ast::Statement::CloseCursor(_) => Ok(DBOutput::StatementComplete(0)),
        }
    }

    fn format_query_result(
        &self,
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

        self.format_result_rows(&rows, types)
    }

    fn format_sql_value(
        &self,
        value: &SqlValue,
        expected_type: Option<&DefaultColumnType>,
    ) -> String {
        match value {
            // Integer types - return plain strings, let sqllogictest record_processor
            // add ".000" when test expects Real type
            SqlValue::Integer(i) => i.to_string(),
            SqlValue::Smallint(i) => i.to_string(),
            SqlValue::Bigint(i) => i.to_string(),
            SqlValue::Unsigned(i) => i.to_string(),
            SqlValue::Numeric(_) => value.to_string(), /* Use Display trait for consistent */
            // formatting
            SqlValue::Float(f) | SqlValue::Real(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Double(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Date(d) => d.to_string(),
            SqlValue::Time(d) => d.to_string(),
            SqlValue::Timestamp(d) => d.to_string(),
            SqlValue::Interval(d) => d.to_string(),
        }
    }
}

#[async_trait]
impl AsyncDB for NistMemSqlDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_sql(sql)
    }

    async fn shutdown(&mut self) {
        // Log cache statistics on shutdown
        let stats = self.cache.stats();
        eprintln!("Cache statistics:");
        eprintln!("  Hits: {}", stats.hits);
        eprintln!("  Misses: {}", stats.misses);
        eprintln!("  Hit rate: {:.2}%", stats.hit_rate * 100.0);
        eprintln!("  Evictions: {}", stats.evictions);
        eprintln!("  Final size: {}", stats.size);
    }
}

#[tokio::test]
async fn test_basic_select() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    let script = r#"
statement ok
CREATE TABLE test (x INTEGER, y INTEGER)

statement ok
INSERT INTO test VALUES (1, 2)

statement ok
INSERT INTO test VALUES (3, 4)

query II rowsort
SELECT * FROM test
----
1
2
3
4

query I
SELECT x FROM test WHERE y = 4
----
3
"#;

    tester.run_script(script).expect("Basic SELECT test should pass");
}

#[tokio::test]
async fn test_arithmetic() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    let script = r#"
query I
SELECT 1 + 1
----
2

query I
SELECT 10 - 3
----
7

query I
SELECT 4 * 5
----
20
"#;

    tester.run_script(script).expect("Arithmetic test should pass");
}

// Issue #919: Reproduction test for infinite loop in IN subquery evaluation
// This test contains the exact query from slt_good_32.test line 780 that causes a hang
#[tokio::test]
async fn test_issue_919_in_subquery_hang() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    let script = r#"
hash-threshold 8

statement ok
CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)

statement ok
INSERT INTO tab0 VALUES(0,1058,996.42,'dpqjl',1029,993.33,'ixhua')

statement ok
INSERT INTO tab0 VALUES(1,1060,1001.62,'xlshf',1030,998.70,'raykp')

statement ok
INSERT INTO tab0 VALUES(2,1061,1002.38,'cffkv',1031,1000.64,'oiimp')

statement ok
INSERT INTO tab0 VALUES(3,1062,1003.98,'rqvjo',1033,1001.43,'ymmtc')

statement ok
INSERT INTO tab0 VALUES(4,1063,1004.20,'bapcy',1034,1002.90,'cizha')

statement ok
INSERT INTO tab0 VALUES(5,1064,1005.45,'vlixf',1035,1003.57,'gxput')

statement ok
INSERT INTO tab0 VALUES(6,1065,1008.91,'evlsa',1036,1004.80,'ctyjb')

statement ok
INSERT INTO tab0 VALUES(7,1066,1009.64,'uaiby',1037,1005.89,'nwyak')

statement ok
INSERT INTO tab0 VALUES(8,1067,1010.72,'dyeih',1038,1006.22,'fwrms')

statement ok
INSERT INTO tab0 VALUES(9,1068,1011.34,'luoso',1039,1007.34,'fgwoy')

query I rowsort
SELECT pk FROM tab0 WHERE col3 >= 94 OR (col1 IN (63.39,21.7,52.63,42.27,35.11,72.69)) OR col3 > 30 AND col0 IN (SELECT col3 FROM tab0 WHERE col1 < 71.54) OR (col3 > 35)
----
10 values hashing to e20b902b49a98b1a05ed62804c757f94
"#;

    tester.run_script(script).expect("IN subquery test should pass");
}

// Issue #1170: Reproduction test for multi-column SELECT column ordering
#[tokio::test]
async fn test_issue_1170_multi_column_select_order() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    // Test with the exact syntax from the issue
    // Multi-column results should display each value on a separate line
    let script = r#"
query II
SELECT + + 74 AS col0, 50 col1
----
74
50
"#;

    tester.run_script(script).expect("Multi-column SELECT order test should pass");
}

// Issue #1190: Reproduction test for 3-value queries returning hashes
#[tokio::test]
async fn test_issue_1190_three_values_no_hash() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    let script = r#"
statement ok
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)

statement ok
INSERT INTO tab0 VALUES (35, 97, 1)

statement ok
INSERT INTO tab0 VALUES (36, 98, 2)

statement ok
INSERT INTO tab0 VALUES (37, 99, 3)

query I
SELECT - 57 col2 FROM tab0
----
-57
-57
-57
"#;

    tester.run_script(script).expect("3-value query should not be hashed");
}

// Issue #1479: Test that database state persists across statements (trigger duplicate detection)
#[tokio::test]
async fn test_issue_1479_trigger_duplicate_detection() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    // Test 1: Basic trigger duplicate detection
    let script = r#"
statement ok
CREATE TABLE t1(x INTEGER, y VARCHAR(8))

statement ok
CREATE TRIGGER t1r1 AFTER UPDATE ON t1 FOR EACH ROW BEGIN END

statement error
CREATE TRIGGER t1r1 AFTER UPDATE ON t1 FOR EACH ROW BEGIN END

statement ok
DROP TRIGGER t1r1

statement error
DROP TRIGGER t1r1
"#;

    tester.run_script(script).expect("Trigger duplicate detection test should pass");
}

// Issue #1479: Extended test for state persistence across multiple DDL operations
#[tokio::test]
async fn test_issue_1479_state_persistence() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    let script = r#"
statement ok
CREATE TABLE test_table(id INTEGER, name VARCHAR(50))

statement ok
INSERT INTO test_table VALUES (1, 'Alice')

statement ok
INSERT INTO test_table VALUES (2, 'Bob')

query I
SELECT COUNT(*) FROM test_table
----
2

statement ok
CREATE TRIGGER test_trigger AFTER UPDATE ON test_table FOR EACH ROW BEGIN END

statement error
CREATE TRIGGER test_trigger AFTER INSERT ON test_table FOR EACH ROW BEGIN END

query I
SELECT COUNT(*) FROM test_table
----
2

statement ok
DROP TRIGGER test_trigger

statement error
DROP TRIGGER test_trigger

query I
SELECT COUNT(*) FROM test_table
----
2
"#;

    tester.run_script(script).expect("State persistence test should pass");
}

// Test for benchmarking: Run a test file from environment variable
// Usage: SQLLOGICTEST_FILE=path/to/file.test cargo test -p vibesql --test sqllogictest_runner run_single_test_file
#[tokio::test]
async fn run_single_test_file() {
    use std::path::Path;

    // Skip test if SQLLOGICTEST_FILE environment variable is not set
    let test_file = match std::env::var("SQLLOGICTEST_FILE") {
        Ok(file) => file,
        Err(_) => {
            eprintln!("Skipping run_single_test_file: SQLLOGICTEST_FILE environment variable not set");
            eprintln!("Usage: SQLLOGICTEST_FILE=path/to/file.test cargo test -p vibesql --test sqllogictest_runner run_single_test_file");
            return;
        }
    };

    let full_path = if Path::new(&test_file).is_absolute() {
        test_file.clone()
    } else {
        format!("third_party/sqllogictest/test/{}", test_file)
    };

    let contents = std::fs::read_to_string(&full_path)
        .unwrap_or_else(|e| panic!("Failed to read test file {}: {}", full_path, e));

    let mut tester = sqllogictest::Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    // Set hash threshold to 8 (SQLLogicTest default) - results with more than 8 values will be hashed
    tester.with_hash_threshold(8);

    tester.run_script(&contents)
        .unwrap_or_else(|e| panic!("Test failed for {}: {}", test_file, e));
}
#[tokio::test]
async fn test_div_integer_formatting() {
    use vibesql_executor::SelectExecutor;
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    let db = Database::new();
    let stmt = Parser::parse_sql("SELECT 47 DIV -94").unwrap();
    
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values.len(), 1);
            
            // Should return Integer(0), not Numeric or Float
            match &rows[0].values[0] {
                vibesql_types::SqlValue::Integer(val) => {
                    assert_eq!(*val, 0);
                },
                other => panic!("Expected Integer, got {:?}", other),
            }
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[tokio::test]
async fn test_div_more_cases() {
    use vibesql_executor::SelectExecutor;
    use vibesql_parser::Parser;
    use vibesql_storage::Database;
    use vibesql_types::SqlValue;

    let db = Database::new();
    
    let cases = vec![
        ("SELECT 10 DIV 3", 3i64),
        ("SELECT -10 DIV 3", -3i64),
        ("SELECT 100 DIV 10", 10i64),
        ("SELECT 47 DIV -94", 0i64),
    ];
    
    for (sql, expected) in cases {
        let stmt = Parser::parse_sql(sql).unwrap();
        match stmt {
            vibesql_ast::Statement::Select(select) => {
                let executor = SelectExecutor::new(&db);
                let rows = executor.execute(&select).unwrap();
                match &rows[0].values[0] {
                    SqlValue::Integer(val) => {
                        assert_eq!(*val, expected, "Failed for SQL: {}", sql);
                    },
                    other => panic!("Expected Integer for '{}', got {:?}", sql, other),
                }
            },
            _ => panic!("Expected SELECT statement"),
        }
    }
}
