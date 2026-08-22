//! Execution tests for `CASE ... END` expressions inside trigger bodies (#5439).
//!
//! Regression for the trigger-body statement splitter / token collector: the
//! `END` of a `CASE ... END` expression inside a body statement was treated as
//! the trigger-body terminator, truncating the body so that statements after a
//! CASE-bearing statement were silently dropped (and create-time parsing failed
//! with `incomplete input`).
//!
//! Behavior verified against sqlite3 3.51.0:
//! - `BEGIN UPDATE ... CASE ... END; INSERT INTO log ...; END` fires BOTH statements (the
//!   CASE...END does not truncate the body).
//! - Multiple CASE expressions across statements, nested CASE, and CASE in a WHERE clause all leave
//!   the trailing statements intact.
//! - `INSTEAD OF INSERT` with `SELECT CASE WHEN ... THEN raise(IGNORE) END` followed by an `INSERT
//!   INTO base` skips the matching row but still runs the base write for non-matching rows.

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use super::super::*;

/// Execute setup SQL that is expected to succeed.
fn exec_ok(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            crate::advanced_objects::execute_create_trigger(&s, db).expect("CREATE TRIGGER failed");
        }
        Statement::CreateView(s) => {
            crate::advanced_objects::execute_create_view(&s, db).expect("CREATE VIEW failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).expect("UPDATE failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).expect("DELETE failed");
        }
        other => panic!("Unsupported setup statement: {:?}", other),
    }
}

/// Read column `col` of every row in `table`, ordered by physical position.
fn column_values(db: &vibesql_storage::Database, table: &str, col: &str) -> Vec<SqlValue> {
    let schema = db.catalog.get_table(table).expect("table exists");
    let idx = schema.columns.iter().position(|c| c.name == col).expect("column exists");
    db.get_table(table)
        .expect("table exists")
        .scan()
        .iter()
        .map(|row| row.values[idx].clone())
        .collect()
}

fn strings(db: &vibesql_storage::Database, table: &str, col: &str) -> Vec<String> {
    column_values(db, table, col)
        .into_iter()
        .map(|v| match v {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
            other => panic!("expected text, got {:?}", other),
        })
        .collect()
}

fn ints(db: &vibesql_storage::Database, table: &str, col: &str) -> Vec<i64> {
    column_values(db, table, col)
        .into_iter()
        .map(|v| match v {
            SqlValue::Integer(i) => i,
            SqlValue::Bigint(i) => i,
            other => panic!("expected integer, got {:?}", other),
        })
        .collect()
}

#[test]
fn case_end_then_insert_fires_both_statements() {
    // sqlite3 3.51.0: both the UPDATE (with CASE...END) and the trailing INSERT
    // fire — the CASE's END does not terminate the body.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (msg VARCHAR(16))");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
         UPDATE t SET v = CASE WHEN NEW.v > 0 THEN 1 ELSE 0 END; \
         INSERT INTO log VALUES ('x'); \
         END",
    );

    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 5)");

    // Trailing INSERT ran: log has one row.
    assert_eq!(strings(&db, "log", "msg"), vec!["x".to_string()]);
    // The CASE-driven UPDATE ran: v became 1.
    assert_eq!(ints(&db, "t", "v"), vec![1]);
}

#[test]
fn multiple_case_expressions_across_statements() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (msg VARCHAR(16))");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
         UPDATE t SET v = CASE WHEN NEW.v > 0 THEN 1 ELSE 0 END; \
         INSERT INTO log VALUES (CASE WHEN NEW.v > 0 THEN 'pos' ELSE 'neg' END); \
         END",
    );

    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 5)");

    assert_eq!(ints(&db, "t", "v"), vec![1]);
    assert_eq!(strings(&db, "log", "msg"), vec!["pos".to_string()]);
}

#[test]
fn nested_case_end_does_not_truncate_body() {
    // sqlite3 3.51.0: a nested CASE...END (inner CASE in the THEN of the outer)
    // closes both blocks before the body terminator; trailing INSERT fires.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (msg VARCHAR(16))");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
         UPDATE t SET v = CASE WHEN NEW.v > 0 \
           THEN CASE WHEN NEW.v > 10 THEN 99 ELSE 5 END \
           ELSE 0 END; \
         INSERT INTO log VALUES ('done'); \
         END",
    );

    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 20)");

    assert_eq!(ints(&db, "t", "v"), vec![99]);
    assert_eq!(strings(&db, "log", "msg"), vec!["done".to_string()]);
}

#[test]
fn case_in_where_clause_does_not_truncate_body() {
    let mut db = vibesql_storage::Database::new();
    // This test's subject is CASE-body parsing, not recursion. The trigger body
    // performs an `UPDATE t` while firing on `t`, which — now that nested UPDATEs
    // correctly fire the target's UPDATE triggers (#5535) — would recurse under
    // the default `recursive_triggers = on` (sqlite3 3.51.0 errors here with
    // "too many levels of trigger recursion"). Set the pragma OFF to assert the
    // single-fire shape this test cares about (matching sqlite3's CLI default and
    // this test's original intent), keeping the recursion suppressed.
    db.set_recursive_triggers(false);
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (msg VARCHAR(16))");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 0)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN \
         UPDATE t SET v = 7 WHERE id = CASE WHEN NEW.id > 0 THEN NEW.id ELSE -1 END; \
         INSERT INTO log VALUES ('w'); \
         END",
    );

    // Fire the AFTER UPDATE trigger. With recursive_triggers off the inner
    // UPDATE does not re-fire `tr`, so the body runs exactly once.
    exec_ok(&mut db, "UPDATE t SET v = 1 WHERE id = 1");

    assert_eq!(strings(&db, "log", "msg"), vec!["w".to_string()]);
}

#[test]
fn instead_of_insert_case_raise_ignore_body_fires_and_resolves_new() {
    // #5439 (parse) + #5445 (fire): the direct repro, now asserted end-to-end.
    //
    // Parse side (#5439/#5444): before the fix the CREATE TRIGGER failed with
    // `incomplete input` because the CASE's `END` was treated as the body
    // terminator, dropping the trailing base INSERT. The body must retain BOTH
    // statements.
    //
    // Fire side (#5445): firing additionally requires resolving `NEW.id` inside
    // a from-less `SELECT CASE WHEN NEW.id = 1 THEN raise(IGNORE) END`. Before
    // #5445 this failed at fire time with "Column reference requires FROM clause"
    // (the from-less SELECT path did not receive the trigger's NEW/OLD context).
    //
    // sqlite3 3.51.0: inserting (1, 100) is skipped by raise(IGNORE); inserting
    // (2, 200) reaches the base table.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");

    let sql = "CREATE TRIGGER trg INSTEAD OF INSERT ON vw BEGIN \
               SELECT CASE WHEN NEW.id = 1 THEN raise(IGNORE) END; \
               INSERT INTO base (id, v) VALUES (NEW.id, NEW.v); \
               END";
    let stmt = Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("CREATE TRIGGER with CASE...END must parse (#5439): {:?}", e));
    match stmt {
        Statement::CreateTrigger(s) => {
            // Body retains BOTH the CASE-bearing SELECT and the trailing INSERT.
            let vibesql_ast::TriggerAction::RawSql(body) = &s.triggered_action;
            let up = body.to_uppercase();
            assert!(up.contains("CASE"), "body lost the CASE expression: {}", body);
            assert!(
                up.contains("INSERT INTO BASE"),
                "body truncated before the trailing INSERT (the #5439 bug): {}",
                body
            );
            crate::advanced_objects::execute_create_trigger(&s, &mut db)
                .expect("CREATE TRIGGER should succeed");
        }
        other => panic!("expected CreateTrigger, got {:?}", other),
    }

    // Fire the INSTEAD OF INSERT trigger for both rows.
    exec_ok(&mut db, "INSERT INTO vw (id, v) VALUES (1, 100)");
    exec_ok(&mut db, "INSERT INTO vw (id, v) VALUES (2, 200)");

    // raise(IGNORE) skipped the NEW.id = 1 row; only the NEW.id = 2 row reached base.
    assert_eq!(ints(&db, "base", "id"), vec![2]);
    assert_eq!(ints(&db, "base", "v"), vec![200]);
}

#[test]
fn from_less_select_new_resolves_in_trigger_body() {
    // #5445: a standalone from-less `SELECT NEW.col` body statement (no CASE)
    // resolves the firing row's NEW context and runs without error. sqlite3
    // 3.51.0 accepts this; the trailing INSERT logs NEW.v + 1.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (val INTEGER)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
         SELECT NEW.id; \
         INSERT INTO log VALUES (NEW.v + 1); \
         END",
    );

    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (5, 99)");

    assert_eq!(ints(&db, "log", "val"), vec![100]);
}

#[test]
fn from_less_select_old_raise_ignore_in_delete_trigger() {
    // #5445: OLD pseudo-variable in a from-less SELECT CASE inside an AFTER
    // DELETE trigger. sqlite3 3.51.0: raise(IGNORE) abandons the trigger program
    // for the OLD.id = 1 row (so it is not logged) but does not undo the delete;
    // the OLD.id = 2 row is logged.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE d (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE dlog (val INTEGER)");
    exec_ok(&mut db, "INSERT INTO d (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO d (id, v) VALUES (2, 20)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trd AFTER DELETE ON d BEGIN \
         SELECT CASE WHEN OLD.id = 1 THEN raise(IGNORE) END; \
         INSERT INTO dlog VALUES (OLD.v); \
         END",
    );

    exec_ok(&mut db, "DELETE FROM d");

    // Only the OLD.id = 2 row was logged; both rows were still deleted.
    assert_eq!(ints(&db, "dlog", "val"), vec![20]);
    assert!(column_values(&db, "d", "id").is_empty(), "all rows should be deleted");
}

#[test]
fn from_less_select_expression_over_new_and_old() {
    // #5445: an expression combining NEW and OLD in a from-less SELECT CASE
    // inside an AFTER UPDATE trigger. sqlite3 3.51.0: when NEW.v - OLD.v > 5 the
    // row is ignored (not logged); otherwise NEW.v is logged.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (val INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN \
         SELECT CASE WHEN NEW.v - OLD.v > 5 THEN raise(IGNORE) END; \
         INSERT INTO log VALUES (NEW.v); \
         END",
    );

    exec_ok(&mut db, "UPDATE t SET v = 12 WHERE id = 1"); // delta 2, not ignored
    exec_ok(&mut db, "UPDATE t SET v = 100 WHERE id = 1"); // delta 88, ignored

    assert_eq!(ints(&db, "log", "val"), vec![12]);
}

#[test]
fn insert_select_from_less_resolves_old_in_trigger_body() {
    // #5470 (trigger5.test 1.1): a trigger body of the form
    //   `INSERT INTO log SELECT <expr referencing OLD>;`
    // (an INSERT whose source is a *from-less* SELECT) must resolve the firing
    // row's OLD context. Before the fix the INSERT...SELECT source executor was
    // built with `SelectExecutor::new` (no trigger context), so the OLD column
    // references failed at fire time with "Column reference requires FROM
    // clause" and the AFTER DELETE trigger never logged anything.
    //
    // sqlite3 3.51.0: deleting the single row logs the rendered string built
    // from OLD.a / OLD.b / OLD.c.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE item (a INTEGER, b INTEGER, c INTEGER)");
    exec_ok(&mut db, "CREATE TABLE undo (msg VARCHAR(64))");
    exec_ok(&mut db, "INSERT INTO item (a, b, c) VALUES (1, 2, 3)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER DELETE ON item FOR EACH ROW BEGIN \
         INSERT INTO undo SELECT 'a=' || OLD.a || ',b=' || OLD.b || ',c=' || OLD.c; \
         END",
    );

    exec_ok(&mut db, "DELETE FROM item WHERE a = 1");

    assert_eq!(strings(&db, "undo", "msg"), vec!["a=1,b=2,c=3".to_string()]);
    assert!(column_values(&db, "item", "a").is_empty(), "row should be deleted");
}

#[test]
fn insert_select_from_less_resolves_new_in_trigger_body() {
    // #5470 (triggerG.test 100 class): an AFTER INSERT trigger whose body uses
    //   `INSERT INTO log SELECT <expr referencing NEW>;`
    // (from-less SELECT) must resolve the firing row's NEW context in the
    // INSERT...SELECT source. Same root cause as the OLD case above.
    //
    // sqlite3 3.51.0: inserting c = 2 then c = 7 logs NEW.c * 100 = 200, 700.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (c INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (val INTEGER)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW BEGIN \
         INSERT INTO log SELECT NEW.c * 100; \
         END",
    );

    exec_ok(&mut db, "INSERT INTO t (c) VALUES (2)");
    exec_ok(&mut db, "INSERT INTO t (c) VALUES (7)");

    let mut logged = ints(&db, "log", "val");
    logged.sort_unstable();
    assert_eq!(logged, vec![200, 700]);
}

#[test]
fn instead_of_insert_case_then_base_write_fires_both() {
    // End-to-end INSTEAD OF variant that avoids the from-less `NEW` resolution
    // limitation: the CASE...END lives in the INSERT's VALUES (which carries the
    // NEW row context), so both the conditional column and the base write run.
    // sqlite3 3.51.0: base receives one row with v = 'pos' (NEW.v = 10 > 0).
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, label VARCHAR(8))");
    exec_ok(&mut db, "CREATE TABLE log (msg VARCHAR(8))");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, label FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF INSERT ON vw BEGIN \
         INSERT INTO base (id, label) VALUES (NEW.id, CASE WHEN NEW.id > 0 THEN 'pos' ELSE 'neg' END); \
         INSERT INTO log VALUES ('fired'); \
         END",
    );

    exec_ok(&mut db, "INSERT INTO vw (id, label) VALUES (10, 'ignored')");

    assert_eq!(ints(&db, "base", "id"), vec![10]);
    assert_eq!(strings(&db, "base", "label"), vec!["pos".to_string()]);
    // The statement after the CASE-bearing INSERT also fired.
    assert_eq!(strings(&db, "log", "msg"), vec!["fired".to_string()]);
}
