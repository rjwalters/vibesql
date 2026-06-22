//! Pseudo-variable (`NEW.col` / `OLD.col`) error-message compatibility tests
//! (issue #5714, SQLite `selectC-2.1`).
//!
//! When a trigger body references a pseudo-variable column that does not exist
//! on the triggering table (e.g. `new.x` where the NEW row has no column `x`),
//! SQLite reports a plain column-resolution error: `no such column: new.x`.
//!
//! VibeSQL previously surfaced `Unsupported expression: Pseudo-variable NEW.x
//! is only valid within trigger bodies` from the evaluator's no-trigger-context
//! arm, which both used the wrong wording and the wrong casing. This test pins
//! the SQLite-compatible message.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, TriggerExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec_ok(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

/// selectC-2.1: a trigger body referencing an unknown pseudo-variable column
/// (`new.x`) must fail with SQLite's `no such column: new.x` when the trigger
/// fires, not with an "unsupported expression" message.
#[test]
fn new_pseudo_var_unknown_column_reports_no_such_column() {
    let mut db = Database::new();
    exec_ok(&mut db, "CREATE TABLE t21a(a,b);");
    exec_ok(&mut db, "INSERT INTO t21a VALUES(1,2);");
    exec_ok(&mut db, "CREATE TABLE t21b(n);");
    exec_ok(
        &mut db,
        "CREATE TRIGGER r21 AFTER INSERT ON t21b BEGIN \
           SELECT a FROM t21a WHERE a>new.x UNION ALL \
           SELECT b FROM t21a WHERE b>new.x ORDER BY 1 LIMIT 2; \
         END;",
    );

    // Firing the trigger evaluates `new.x` against the NEW row of t21b (which
    // only has column `n`), so the column reference is unresolved.
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql("INSERT INTO t21b VALUES(6);").expect("parse failed");
    let Statement::Insert(insert) = stmt else { panic!("expected INSERT") };
    let err = InsertExecutor::execute(&mut db, &insert)
        .expect_err("trigger body referencing new.x should error");

    let msg = err.to_string();
    assert_eq!(
        msg, "no such column: new.x",
        "expected SQLite-compatible column-resolution error, got: {msg}"
    );
}
