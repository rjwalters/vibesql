//! Regression tests for issue #5712 (distinct2-5020): a volatile (non-deterministic)
//! projected expression referenced by ORDER BY must be evaluated exactly once.
//!
//! `SELECT DISTINCT abs(random())%5 AS r FROM cnt ORDER BY r` must produce a
//! result where the ORDER BY sorts on the *projected* value of `r`, not on a
//! freshly re-evaluated `abs(random())%5`. If the expression were re-evaluated
//! during sort-key computation, `random()` would be called a second time and the
//! sort key would no longer match the output value — yielding unsorted output
//! and/or values outside the projected DISTINCT set.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn setup_db() -> Database {
    let mut db = Database::new();

    let create = Parser::parse_sql("CREATE TABLE cnt(a)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // 50 rows, mirroring the distinct2-5020 fixture (WITH RECURSIVE 1..50).
    for i in 1..=50 {
        let sql = format!("INSERT INTO cnt VALUES ({i})");
        let stmt = Parser::parse_sql(&sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
    }

    db
}

fn execute_query(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

fn int_values(rows: &[vibesql_storage::Row]) -> Vec<i64> {
    rows.iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn distinct_volatile_alias_order_by_is_evaluated_once() {
    let db = setup_db();

    // Run several times — random() makes this probabilistic, and a regression
    // (re-evaluation) would almost certainly surface across repeated runs.
    for _ in 0..20 {
        let rows = execute_query(&db, "SELECT DISTINCT abs(random())%5 AS r FROM cnt ORDER BY r");
        let vals = int_values(&rows);

        // With 50 rows and 5 buckets, all of 0..=4 are present with overwhelming
        // probability; assert the exact SQLite expectation.
        assert_eq!(vals, vec![0, 1, 2, 3, 4], "DISTINCT volatile ORDER BY mismatch");

        // Output must be sorted (the sort key must equal the projected value).
        let mut sorted = vals.clone();
        sorted.sort();
        assert_eq!(vals, sorted, "result not sorted by projected output value");
    }
}

#[test]
fn volatile_alias_order_by_positional_is_evaluated_once() {
    let db = setup_db();

    for _ in 0..20 {
        let rows = execute_query(&db, "SELECT DISTINCT abs(random())%5 AS r FROM cnt ORDER BY 1");
        let vals = int_values(&rows);
        let mut sorted = vals.clone();
        sorted.sort();
        assert_eq!(vals, sorted, "ORDER BY 1 on volatile alias must sort projected output");
        // Every value must be a valid bucket (no leaked re-evaluated key).
        assert!(vals.iter().all(|v| (0..=4).contains(v)));
    }
}
