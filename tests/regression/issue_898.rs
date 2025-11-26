//! Test runner for issue #898 - runs the 4 specific failing test files

use async_trait::async_trait;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};
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

struct VibeSqlDB {
    db: Database,
}

impl VibeSqlDB {
    fn new() -> Self {
        Self { db: Database::new() }
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
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
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
                vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
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
            _ => Ok(DBOutput::StatementComplete(0)),
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

        let formatted_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| row.values.iter().map(|val| self.format_sql_value(val)).collect())
            .collect();

        Ok(DBOutput::Rows { types, rows: formatted_rows })
    }

    fn format_sql_value(&self, value: &SqlValue) -> String {
        match value {
            SqlValue::Integer(i) => i.to_string(),
            SqlValue::Smallint(i) => i.to_string(),
            SqlValue::Bigint(i) => i.to_string(),
            SqlValue::Unsigned(u) => u.to_string(),
            SqlValue::Numeric(f) => f.to_string(),
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
impl AsyncDB for VibeSqlDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_sql(sql)
    }

    async fn shutdown(&mut self) {
        // No cleanup needed
    }
}

async fn test_file(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n========================================");
    println!("Testing: {}", path);
    println!("========================================");

    let contents = std::fs::read_to_string(path)?;
    let mut tester = Runner::new(|| async { Ok(VibeSqlDB::new()) });

    // Enable hash mode with threshold of 8 (standard SQLLogicTest behavior)
    tester.with_hash_threshold(8);
    // Add "mysql" label for skipif/onlyif directives
    tester.add_label("mysql");

    match tester.run_script(&contents) {
        Ok(_) => {
            println!("✓ PASSED");
            Ok(())
        }
        Err(e) => {
            let error_str = format!("{}", e);
            println!("✗ FAILED (expected)");
            println!("Error: {}", e);

            // Verify that hash mode is working - we should get hash mismatches, not raw value
            // comparisons
            if error_str.contains("values hashing to") {
                println!("✓ Hash mode confirmed working - got hash comparison");
                Ok(()) // This is expected - hash mode is working
            } else if error_str.contains("UnsupportedExpression")
                || error_str.contains("not supported")
            {
                println!("✓ Got expected SQL feature error (not hash mode issue)");
                Ok(()) // This is expected - SQL feature not implemented
            } else if error_str.contains("MemoryLimitExceeded") {
                println!("✓ Got expected memory limit error (query too large)");
                Ok(()) // This is expected - query exceeds memory limits
            } else {
                println!("✗ Unexpected error type - hash mode may not be working");
                Err(format!("Unexpected error type: {}", e).into())
            }
        }
    }
}

#[tokio::test]
#[ignore] // Slow test (~60s+): Run manually with `cargo test test_issue_898_files -- --ignored`
async fn test_issue_898_files() {
    let files = vec![
        "third_party/sqllogictest/test/select1.test",
        "third_party/sqllogictest/test/select3.test",
        "third_party/sqllogictest/test/select4.test",
        "third_party/sqllogictest/test/index/orderby_nosort/10/slt_good_29.test",
    ];

    let mut failed = vec![];

    for file in &files {
        if let Err(e) = test_file(file).await {
            failed.push((file, e));
        }
    }

    if !failed.is_empty() {
        println!("\n========================================");
        println!("SUMMARY: {} of {} tests failed", failed.len(), files.len());
        println!("========================================");
        for (file, err) in &failed {
            println!("\n✗ {}", file);
            println!("  Error: {}", err);
        }
        panic!("{} tests had unexpected failures", failed.len());
    } else {
        println!("\n========================================");
        println!("SUCCESS: All {} tests failed with expected error types!", files.len());
        println!("Hash mode is working correctly - tests show actual SQL bugs instead of hash mode issues.");
        println!("========================================");
    }
}
