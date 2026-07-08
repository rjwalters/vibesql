//! Tests for the view-expansion reference guard (#5394, SQLite ticket
//! `[d58ccbb3f1b]`, exercised by view3.test).
//!
//! VibeSQL materializes views lazily by executing their queries, so a doubling
//! view nest (`v2=v1∪v1`, `v4=v2∪v2`, …) expands exponentially and would hang.
//! The guard detects, statically and cheaply, when any view would be referenced
//! more than 65535 times after full expansion and raises SQLite's error instead.

use vibesql_ast::Statement;
use vibesql_executor::{ExecutorError, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn run_ddl(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    match stmt {
        Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateView(s) => {
            vibesql_executor::advanced_objects::execute_create_view(&s, db).unwrap();
        }
        Statement::Insert(s) => {
            vibesql_executor::InsertExecutor::execute(db, &s).unwrap();
        }
        other => panic!("unsupported DDL in test: {:?}", other),
    }
}

fn run_select(db: &Database, sql: &str) -> Result<(), ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let executor = SelectExecutor::new(db);
    executor.execute_with_columns(&select).map(|_| ())
}

/// Build the canonical view3.test doubling nest up to `v{2^levels}`.
fn build_doubling_nest(db: &mut Database, levels: u32) {
    run_ddl(db, "CREATE TABLE t1(x)");
    run_ddl(db, "INSERT INTO t1 VALUES(5)");
    run_ddl(db, "CREATE VIEW v1 AS SELECT x*2 FROM t1");
    let mut prev = 1u64;
    for _ in 0..levels {
        let next = prev * 2;
        run_ddl(
            db,
            &format!("CREATE VIEW v{next} AS SELECT * FROM v{prev} UNION SELECT * FROM v{prev}"),
        );
        prev = next;
    }
}

#[test]
fn view3_doubling_nest_errors_instead_of_hanging() {
    // 15 levels => v32768; `SELECT * FROM v32768 UNION SELECT * FROM v32768`
    // expands to 2^16 = 65536 references to v1, which exceeds the 65535 cap.
    let mut db = Database::new();
    build_doubling_nest(&mut db, 15);

    let err = run_select(&db, "SELECT * FROM v32768 UNION SELECT * FROM v32768")
        .expect_err("expected reference-limit error, not a hang");
    let msg = err.to_string();
    assert!(
        msg.contains("too many references") && msg.contains("65535"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn shallow_doubling_nest_executes_normally() {
    // 6 levels => v64; well under the limit, must still execute and return the
    // single distinct value (10) without error.
    let mut db = Database::new();
    build_doubling_nest(&mut db, 6);

    run_select(&db, "SELECT * FROM v64 UNION SELECT * FROM v64")
        .expect("shallow nest should execute fine");
}

#[test]
fn ordinary_view_query_is_unaffected() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t(a, b)");
    run_ddl(&mut db, "INSERT INTO t VALUES(1, 2)");
    run_ddl(&mut db, "CREATE VIEW v AS SELECT a FROM t");
    run_select(&db, "SELECT * FROM v").expect("plain view query should work");
}

#[test]
fn cte_named_like_a_view_is_not_counted() {
    // A CTE shadowing a view name must not be charged against the view's
    // reference budget. Build a heavy view `h`, then a query whose top level
    // uses a CTE named `h` (shadowing the view) — it must NOT trip the guard.
    let mut db = Database::new();
    build_doubling_nest(&mut db, 15); // creates v1..v32768; rename target below
    run_ddl(&mut db, "CREATE TABLE base(x)");
    run_ddl(&mut db, "INSERT INTO base VALUES(7)");
    run_ddl(&mut db, "CREATE VIEW v32768x AS SELECT * FROM v32768");

    // CTE `v32768x` shadows the heavy view; the query only touches the CTE.
    run_select(&db, "WITH v32768x AS (SELECT 1 AS x) SELECT * FROM v32768x")
        .expect("CTE shadowing a heavy view must not trip the guard");
}
