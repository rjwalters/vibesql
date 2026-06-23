//! Regression tests for outer-correlated aggregates inside a bare scalar
//! subquery in a HAVING clause (in-23.0, sqlite3 forum/forumpost/dc16ec63d3).
//!
//! A HAVING clause only exists on an aggregating outer query, so a bare
//! (FROM-less) scalar subquery whose aggregate references an outer column is NOT
//! a "misuse of aggregate": it borrows the outer aggregation context and
//! collapses over the current group, exactly like the SELECT-list case (#5104).
//! Previously VibeSQL validated HAVING with `SubqueryContext::WhereOrEqual` and
//! wrongly rejected:
//!
//! ```sql
//! SELECT a0.a, group_concat(a1.a) AS b
//!   FROM t4 AS a0 JOIN t4 AS a1
//!  GROUP BY a0.a
//! HAVING (SELECT sum( (a1.a == +a0.a COLLATE NOCASE) IN (SELECT b FROM t4)));
//! ```
//!
//! with `misuse of aggregate: sum()`. Expected output verified against
//! sqlite3 3.51.0.

use vibesql_ast::Statement;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn run(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateIndex(s) => {
            vibesql_executor::CreateIndexExecutor::execute(&s, db).unwrap();
        }
        Statement::Insert(i) => {
            vibesql_executor::InsertExecutor::execute(db, &i).unwrap();
        }
        other => panic!("unsupported setup statement: {other:?}"),
    }
}

fn rows_as_strings(db: &Database, sql: &str) -> Vec<Vec<String>> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
    let Statement::Select(select) = stmt else { panic!("expected SELECT, got {stmt:?}") };
    let rows = SelectExecutor::new(db).execute(&select).expect("SELECT failed");
    rows.iter().map(|r| r.values.iter().map(|v| v.to_string()).collect()).collect()
}

fn setup(db: &mut Database) {
    run(db, "CREATE TABLE t4(a TEXT, b INT);");
    run(db, "INSERT INTO t4(a,b) VALUES('abc',0),('ABC',1),('def',2);");
    run(db, "CREATE INDEX t4x ON t4(a, +a COLLATE NOCASE);");
}

/// in-23.0: `sum( <outer-correlated bool> IN (SELECT b FROM t4) )` inside a bare
/// scalar subquery in HAVING must be accepted, not rejected as misuse.
#[test]
fn having_bare_subquery_aggregate_in_subquery_is_not_misuse() {
    let mut db = Database::new();
    setup(&mut db);

    let rows = rows_as_strings(
        &db,
        "SELECT a0.a, group_concat(a1.a) AS b \
           FROM t4 AS a0 JOIN t4 AS a1 \
          GROUP BY a0.a \
         HAVING (SELECT sum( (a1.a == +a0.a COLLATE NOCASE) IN (SELECT b FROM t4)));",
    );

    // sqlite3 3.51.0: every group survives the HAVING (the sum is non-zero/true).
    assert_eq!(
        rows,
        vec![
            vec!["ABC".to_string(), "abc,ABC,def".to_string()],
            vec!["abc".to_string(), "abc,ABC,def".to_string()],
            vec!["def".to_string(), "abc,ABC,def".to_string()],
        ],
    );
}

/// in-23.0-b: same shape with GLOB instead of `==` — also accepted.
#[test]
fn having_bare_subquery_aggregate_in_subquery_glob_is_not_misuse() {
    let mut db = Database::new();
    setup(&mut db);

    let rows = rows_as_strings(
        &db,
        "SELECT a0.a, group_concat(a1.a) AS b \
           FROM t4 AS a0 JOIN t4 AS a1 \
          GROUP BY a0.a \
         HAVING (SELECT sum( (a1.a GLOB +a0.a COLLATE NOCASE) IN (SELECT b FROM t4)));",
    );

    assert_eq!(
        rows,
        vec![
            vec!["ABC".to_string(), "abc,ABC,def".to_string()],
            vec!["abc".to_string(), "abc,ABC,def".to_string()],
            vec!["def".to_string(), "abc,ABC,def".to_string()],
        ],
    );
}
