//! Trigger/DML semantics from the #5840 batch, verified against sqlite3 3.51.0:
//!
//! * Item 2 — REPLACE conflict-resolution DELETE triggers fire *iff* `recursive_triggers` is ON
//!   (SQLite lang_conflict.html). With the default (OFF) the implicit conflict-delete is silent;
//!   the row is still removed.
//! * Item 5a — `changes()` immediately after an INSERT/UPDATE/DELETE on a view is always 0 (SQLite
//!   R-09813-48563), because no physical table rows were modified by the statement itself.
//! * Item 5b — inside a trigger body each INSERT/UPDATE/DELETE sets `changes()` to the rows *it*
//!   modified, and the value is saved before the body runs and restored afterward, so a
//!   sub-trigger's nested DML never leaks into the caller's `changes()` (SQLite R-32918-61474 /
//!   R-17146-37073).

use vibesql_executor::{
    CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor, TriggerExecutor,
    UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a single statement, preserving the raw SQL for triggers.
fn exec(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateView(s) => {
            vibesql_executor::advanced_objects::execute_create_view(&s, db)
                .expect("CREATE VIEW failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        // Mirror what the session layer does after a top-level DML statement:
        // publish the executor's returned row count as changes(). For a view
        // DML the executor returns 0 (Item 5a), so changes() lands at 0.
        Statement::Insert(s) => {
            let n = InsertExecutor::execute(db, &s).expect("INSERT failed");
            db.set_last_changes_count(n);
        }
        Statement::Update(s) => {
            let n = UpdateExecutor::execute(&s, db).expect("UPDATE failed");
            db.set_last_changes_count(n);
        }
        Statement::Delete(s) => {
            let n = DeleteExecutor::execute(&s, db).expect("DELETE failed");
            db.set_last_changes_count(n);
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

/// Run a SELECT and return rows as `Vec<Vec<SqlValue>>` (live rows only).
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    use vibesql_ast::Statement;
    let Statement::Select(select) = Parser::parse_sql(sql).expect("parse failed") else {
        panic!("expected SELECT");
    };
    SelectExecutor::new(db)
        .execute(&select)
        .expect("SELECT failed")
        .into_iter()
        .map(|row| row.values.to_vec())
        .collect()
}

fn texts(db: &Database, sql: &str) -> Vec<String> {
    query(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect()
}

/// Extract the first column of every row as an i64.
fn ints(db: &Database, sql: &str) -> Vec<i64> {
    query(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            SqlValue::Bigint(n) => *n,
            SqlValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Item 2: REPLACE conflict-delete triggers gated on recursive_triggers.
// ---------------------------------------------------------------------------

/// With the default `recursive_triggers = OFF`, an INSERT OR REPLACE that clears
/// a conflicting row does NOT fire that row's DELETE trigger, but the row is
/// still replaced (triggerC-5.3 shape).
#[test]
fn replace_conflict_delete_trigger_suppressed_when_recursion_off() {
    let mut db = Database::new();
    assert!(!db.recursive_triggers(), "default is OFF");
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b)");
    exec(&mut db, "CREATE TABLE log(x)");
    exec(&mut db, "INSERT INTO t VALUES(1, 'a')");
    exec(
        &mut db,
        "CREATE TRIGGER t_del AFTER DELETE ON t BEGIN INSERT INTO log VALUES(old.b); END",
    );

    exec(&mut db, "INSERT OR REPLACE INTO t VALUES(1, 'b')");

    assert!(query(&db, "SELECT x FROM log").is_empty(), "conflict-delete trigger must not fire");
    assert_eq!(texts(&db, "SELECT b FROM t"), vec!["b".to_string()], "row was still replaced");
}

/// With `recursive_triggers = ON`, the same REPLACE fires the conflict row's
/// DELETE trigger (triggerC-5.2 shape).
#[test]
fn replace_conflict_delete_trigger_fires_when_recursion_on() {
    let mut db = Database::new();
    db.set_recursive_triggers(true);
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b)");
    exec(&mut db, "CREATE TABLE log(x)");
    exec(&mut db, "INSERT INTO t VALUES(1, 'a')");
    exec(
        &mut db,
        "CREATE TRIGGER t_del AFTER DELETE ON t BEGIN INSERT INTO log VALUES(old.b); END",
    );

    exec(&mut db, "INSERT OR REPLACE INTO t VALUES(1, 'b')");

    assert_eq!(texts(&db, "SELECT x FROM log"), vec!["a".to_string()], "trigger fires for old row");
}

// ---------------------------------------------------------------------------
// Item 5a: changes() is 0 after DML on a view.
// ---------------------------------------------------------------------------

#[test]
fn changes_is_zero_after_instead_of_view_dml() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE base(x, y)");
    exec(&mut db, "CREATE TABLE log(x)");
    exec(&mut db, "INSERT INTO base VALUES(1, 2), (3, 4)");
    exec(&mut db, "CREATE VIEW v AS SELECT * FROM base");
    exec(
        &mut db,
        "CREATE TRIGGER v_i INSTEAD OF INSERT ON v BEGIN INSERT INTO log VALUES('i'); END",
    );

    // A real-table insert sets changes() to its row count...
    exec(&mut db, "INSERT INTO base VALUES(5, 6)");
    assert_eq!(db.last_changes_count(), 1);

    // ...but a view insert leaves changes() at 0 even though the INSTEAD OF
    // trigger fired (verified via the log table).
    exec(&mut db, "INSERT INTO v VALUES(7, 8)");
    assert_eq!(db.last_changes_count(), 0, "changes() is 0 for a view INSERT");
    assert_eq!(texts(&db, "SELECT x FROM log"), vec!["i".to_string()], "trigger still fired");
}

// ---------------------------------------------------------------------------
// Item 5b: changes() inside a trigger body reflects the nested DML and is
// saved/restored around the trigger program.
// ---------------------------------------------------------------------------

/// e_changes-6.1 shape: nested INSERTs inside trigger bodies each update
/// changes(); after a sub-trigger runs the value is restored so the enclosing
/// statement observes only its own row count.
#[test]
fn changes_inside_trigger_body_reflects_nested_dml_and_restores() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b)");
    exec(&mut db, "CREATE TABLE t2(a, b)");
    exec(&mut db, "CREATE TABLE t3(a, b)");
    exec(&mut db, "CREATE TABLE log(x)");
    exec(
        &mut db,
        "CREATE TRIGGER t1_i BEFORE INSERT ON t1 BEGIN \
         INSERT INTO t2 VALUES(new.a, new.b), (new.a, new.b); \
         INSERT INTO log VALUES('t2->' || changes()); END",
    );
    exec(
        &mut db,
        "CREATE TRIGGER t2_i AFTER INSERT ON t2 BEGIN \
         INSERT INTO t3 VALUES(new.a, new.b), (new.a, new.b), (new.a, new.b); \
         INSERT INTO log VALUES('t3->' || changes()); END",
    );

    exec(&mut db, "INSERT INTO t1 VALUES('+', 'o')");

    // Each t2 insert (2 of them) fires t2_i, which inserts 3 t3 rows and logs
    // 't3->3'; the enclosing t1_i then logs 't2->2' (its own 2-row t2 insert,
    // not the sub-trigger's t3 rows).
    assert_eq!(
        texts(&db, "SELECT x FROM log"),
        vec!["t3->3".to_string(), "t3->3".to_string(), "t2->2".to_string()],
    );

    // The top-level statement's changes() is its own direct row count (1),
    // unaffected by the nested trigger DML.
    assert_eq!(db.last_changes_count(), 1, "outer INSERT changes() restored to 1");
}

// ---------------------------------------------------------------------------
// Item 2b: a REPLACE whose single new row conflicts with MULTIPLE existing
// rows deletes them one at a time. With recursive_triggers ON, each conflict
// row's DELETE triggers fire interleaved (BEFORE R -> remove R -> AFTER R)
// before the next conflict row, so a trigger body that reads the table sees
// the row count DECREMENT between conflict deletions — it must not observe a
// stale, frozen pre-deletion count. Verified against sqlite3 3.51.0.
// ---------------------------------------------------------------------------

#[test]
fn replace_multi_conflict_delete_triggers_see_decrementing_count() {
    let mut db = Database::new();
    db.set_recursive_triggers(true);
    // A single inserted row (id=3) conflicts with id=1 on `a` AND id=2 on `b`,
    // forcing two conflict deletions within one REPLACE.
    exec(&mut db, "CREATE TABLE t(id INTEGER PRIMARY KEY, a UNIQUE, b UNIQUE)");
    exec(&mut db, "CREATE TABLE log(cnt)");
    exec(&mut db, "INSERT INTO t VALUES(1, 'a1', 'b1')");
    exec(&mut db, "INSERT INTO t VALUES(2, 'a2', 'b2')");
    exec(
        &mut db,
        "CREATE TRIGGER bd BEFORE DELETE ON t BEGIN \
         INSERT INTO log VALUES((SELECT count(*) FROM t)); END",
    );
    exec(
        &mut db,
        "CREATE TRIGGER ad AFTER DELETE ON t BEGIN \
         INSERT INTO log VALUES(-(SELECT count(*) FROM t)); END",
    );

    exec(&mut db, "INSERT OR REPLACE INTO t VALUES(3, 'a1', 'b2')");

    // Interleaved: BEFORE(first)=2, AFTER(first)=1, BEFORE(second)=1,
    // AFTER(second)=0. AFTER counts are stored negated to distinguish them.
    // Stale-state behavior would instead log 2, -2, 2, -2 (all BEFOREs see the
    // pre-deletion count and all AFTERs see the post-batch count).
    assert_eq!(
        ints(&db, "SELECT cnt FROM log"),
        vec![2, -1, 1, 0],
        "conflict-delete trigger bodies must see the table shrink between deletions"
    );

    // Exactly the new row survives.
    assert_eq!(ints(&db, "SELECT id FROM t"), vec![3]);
    assert_eq!(texts(&db, "SELECT a FROM t"), vec!["a1".to_string()]);
    assert_eq!(texts(&db, "SELECT b FROM t"), vec!["b2".to_string()]);
}

// ---------------------------------------------------------------------------
// Item 3: a BEFORE UPDATE trigger that writes to the SAME row the parent
// statement is updating must not have its write clobbered. SQLite applies the
// trigger's write, then overlays only the parent's SET columns — so a
// trigger's write to a column the parent does NOT set survives, and IS visible
// to AFTER triggers, RETURNING, and index maintenance. Verified against
// sqlite3 3.51.0.
// ---------------------------------------------------------------------------

#[test]
fn before_update_trigger_same_row_write_survives_for_unset_column() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b, c)");
    exec(&mut db, "INSERT INTO t VALUES(1, 'a0', 'b0', 'c0')");
    // BEFORE trigger writes column c; the parent statement only sets column a.
    exec(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t BEGIN \
         UPDATE t SET c = 'trig_c' WHERE id = 1; END",
    );

    exec(&mut db, "UPDATE t SET a = 'a_parent' WHERE id = 1");

    // Parent wins on the column it set (a); trigger's write to c survives; b is
    // untouched by either.
    assert_eq!(texts(&db, "SELECT a FROM t"), vec!["a_parent".to_string()]);
    assert_eq!(texts(&db, "SELECT b FROM t"), vec!["b0".to_string()]);
    assert_eq!(texts(&db, "SELECT c FROM t"), vec!["trig_c".to_string()]);
}

#[test]
fn before_update_trigger_write_to_parent_column_is_overwritten() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b)");
    exec(&mut db, "INSERT INTO t VALUES(1, 'a0', 'b0')");
    // Both the trigger and the parent write column a; the parent's value wins.
    exec(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t BEGIN \
         UPDATE t SET a = 'trig_a' WHERE id = 1; END",
    );

    exec(&mut db, "UPDATE t SET a = 'a_parent' WHERE id = 1");

    assert_eq!(texts(&db, "SELECT a FROM t"), vec!["a_parent".to_string()]);
    assert_eq!(texts(&db, "SELECT b FROM t"), vec!["b0".to_string()]);
}

#[test]
fn before_update_trigger_same_row_write_visible_to_after_trigger() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(id INTEGER PRIMARY KEY, a, c)");
    exec(&mut db, "CREATE TABLE log(x)");
    exec(&mut db, "INSERT INTO t VALUES(1, 'a0', 'c0')");
    exec(
        &mut db,
        "CREATE TRIGGER b4 BEFORE UPDATE ON t BEGIN \
         UPDATE t SET c = 'trig_c' WHERE id = 1; END",
    );
    // The AFTER trigger's NEW.c must reflect the BEFORE trigger's write.
    exec(&mut db, "CREATE TRIGGER af AFTER UPDATE ON t BEGIN INSERT INTO log VALUES(new.c); END");

    exec(&mut db, "UPDATE t SET a = 'a_parent' WHERE id = 1");

    assert_eq!(texts(&db, "SELECT c FROM t"), vec!["trig_c".to_string()]);
    // AFTER fires twice (verified against sqlite3 3.51.0): once for the BEFORE
    // trigger's own nested `UPDATE t SET c` and once for the parent UPDATE. In
    // both firings NEW.c must reflect the trigger's same-row write ('trig_c') —
    // never the pre-trigger value 'c0'.
    assert_eq!(
        texts(&db, "SELECT x FROM log"),
        vec!["trig_c".to_string(), "trig_c".to_string()],
        "AFTER trigger NEW.* reflects the BEFORE trigger's same-row write"
    );
}
