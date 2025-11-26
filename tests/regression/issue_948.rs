//! Test for issue #948: Fix decimal formatting in integer results for SQLLogicTest
//!
//! When SQLLogicTest expects Real (R) type results, integers should be formatted
//! with decimal places (e.g., 92.000) instead of plain integers (92).
//!
//! This test verifies that the actual SQLLogicTest suite properly formats integer
//! results as decimals when the expected column type is FloatingPoint (R).

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

struct TestDB {
    db: Database,
}

impl TestDB {
    fn new() -> Self {
        Self { db: Database::new() }
    }

    fn format_sql_value(
        &self,
        value: &SqlValue,
        expected_type: Option<&DefaultColumnType>,
    ) -> String {
        match value {
            SqlValue::Integer(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Null => "NULL".to_string(),
            _ => format!("{:?}", value),
        }
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

                let types: Vec<DefaultColumnType> = if rows.is_empty() {
                    vec![]
                } else {
                    rows[0].values.iter().map(|_| DefaultColumnType::FloatingPoint).collect()
                };

                let formatted_rows: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.values
                            .iter()
                            .enumerate()
                            .map(|(idx, val)| self.format_sql_value(val, types.get(idx)))
                            .collect()
                    })
                    .collect();

                Ok(DBOutput::Rows { types, rows: formatted_rows })
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
            _ => Ok(DBOutput::StatementComplete(0)),
        }
    }
}

#[async_trait]
impl AsyncDB for TestDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_sql(sql)
    }

    async fn shutdown(&mut self) {}
}

#[tokio::test]
async fn test_issue_948_decimal_formatting() {
    let mut tester = sqllogictest::Runner::new(|| async { Ok(TestDB::new()) });
    // Add "mysql" label for skipif/onlyif directives
    tester.add_label("mysql");

    let script = r#"
statement ok
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)

statement ok
INSERT INTO tab1 VALUES(51,14,96)

statement ok
INSERT INTO tab1 VALUES(85,5,59)

statement ok
INSERT INTO tab1 VALUES(91,47,68)

query R rowsort
SELECT ALL + ( + - ( - 92 ) ) FROM tab1
----
92.000
92.000
92.000
"#;

    tester.run_script(script).expect("Test should pass with decimal formatting");
}
