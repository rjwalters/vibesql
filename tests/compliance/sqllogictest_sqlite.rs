//! SQLite wrapper for sqllogictest framework comparison testing.

use async_trait::async_trait;
use md5::{Digest, Md5};
use rusqlite::{Connection, types::ValueRef};
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};

#[derive(Debug)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

struct SqliteDB {
    conn: Connection,
}

impl SqliteDB {
    fn new() -> Result<Self, TestError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| TestError(format!("Failed to create connection: {}", e)))?;
        Ok(Self { conn })
    }

    /// Format and optionally hash result rows (matching vibesql's logic)
    fn format_result_rows(
        &self,
        rows: &[Vec<String>],
        types: Vec<DefaultColumnType>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let total_values: usize = rows.iter().map(|r| r.len()).sum();

        if total_values > 8 {
            // Hash the results for large result sets
            let mut hasher = Md5::new();
            let mut sort_keys: Vec<_> = rows.iter().map(|row| row.join(" ")).collect();
            sort_keys.sort();
            for key in &sort_keys {
                hasher.update(key);
                hasher.update("\n");
            }
            let hash = format!("{:x}", hasher.finalize());
            let hash_string = format!("{} values hashing to {}", total_values, hash);
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![hash_string]],
            })
        } else {
            // Return rows as-is with one type per column
            // The sqllogictest framework handles flattening for comparison
            Ok(DBOutput::Rows { types, rows: rows.to_vec() })
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let trimmed = sql.trim();

        // Check if this is a query (SELECT) or a statement (INSERT, UPDATE, etc.)
        let is_query = trimmed.to_uppercase().starts_with("SELECT");

        if is_query {
            self.execute_query(sql)
        } else {
            self.execute_statement(sql)
        }
    }

    fn execute_query(&self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let mut stmt = self.conn.prepare(sql)
            .map_err(|e| TestError(format!("Failed to prepare query: {}", e)))?;

        let column_count = stmt.column_count();
        let mut rows = Vec::new();
        let mut types = Vec::new();

        let mut db_rows = stmt.query([])
            .map_err(|e| TestError(format!("Failed to execute query: {}", e)))?;

        // Determine column types from first row
        let mut first_row = true;
        while let Some(row) = db_rows.next()
            .map_err(|e| TestError(format!("Failed to fetch row: {}", e)))? {

            let mut row_values = Vec::new();
            for i in 0..column_count {
                let value = row.get_ref(i)
                    .map_err(|e| TestError(format!("Failed to get column {}: {}", i, e)))?;

                // Determine type from first row
                if first_row {
                    types.push(match value {
                        ValueRef::Integer(_) => DefaultColumnType::Integer,
                        ValueRef::Real(_) => DefaultColumnType::FloatingPoint,
                        ValueRef::Text(_) => DefaultColumnType::Text,
                        ValueRef::Blob(_) => DefaultColumnType::Text,
                        ValueRef::Null => DefaultColumnType::Any,
                    });
                }

                row_values.push(Self::format_value(value, types.get(i)));
            }

            rows.push(row_values);
            first_row = false;
        }

        if rows.is_empty() {
            return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
        }

        self.format_result_rows(&rows, types)
    }

    fn execute_statement(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let rows_affected = self.conn.execute(sql, [])
            .map_err(|e| TestError(format!("Failed to execute statement: {}", e)))?;
        Ok(DBOutput::StatementComplete(rows_affected as u64))
    }

    fn format_value(value: ValueRef, expected_type: Option<&DefaultColumnType>) -> String {
        match value {
            ValueRef::Integer(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", i as f64)
                } else {
                    i.to_string()
                }
            }
            ValueRef::Real(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
            ValueRef::Blob(b) => format!("{:?}", b),
            ValueRef::Null => "NULL".to_string(),
        }
    }
}

#[async_trait]
impl AsyncDB for SqliteDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_sql(sql)
    }

    async fn shutdown(&mut self) {
        // SQLite connection will be closed automatically on drop
    }
}

#[tokio::test]
async fn test_basic_select() {
    let mut tester = sqllogictest::Runner::new(|| async { SqliteDB::new() });

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
    let mut tester = sqllogictest::Runner::new(|| async { SqliteDB::new() });

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

#[tokio::test]
async fn test_data_types() {
    let mut tester = sqllogictest::Runner::new(|| async { SqliteDB::new() });

    let script = r#"
statement ok
CREATE TABLE types_test (id INTEGER, name TEXT, value REAL)

statement ok
INSERT INTO types_test VALUES (1, 'hello', 3.14)

query ITR
SELECT * FROM types_test
----
1
hello
3.14
"#;

    tester.run_script(script).expect("Data types test should pass");
}
