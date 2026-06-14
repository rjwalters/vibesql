//! Subquery-in-WHEN trigger firing tests (#5581).
//!
//! SQLite allows a trigger's WHEN clause to contain a subquery, e.g.
//! `WHEN (SELECT count(*) FROM tbl) = 0`. The trigger fires only when the
//! subquery condition holds, evaluated against the table state as-of trigger
//! firing. This reproduces SQLite's `trigger2-3.2` scenario.
//!
//! Expectations verified against sqlite3 3.51.0:
//! ```text
//! CREATE TABLE tbl (a, b, c, d);
//! CREATE TABLE log (a);
//! INSERT INTO log VALUES (0);
//! CREATE TRIGGER t1 BEFORE INSERT ON tbl WHEN new.a > 20 ...;
//! CREATE TRIGGER t2 BEFORE INSERT ON tbl WHEN (SELECT count(*) FROM tbl) = 0 ...;
//! -- inserting (0,..) into an empty tbl  -> log = 1  (t2 fires)
//! -- inserting (0,..) into a non-empty tbl -> log = 0 (neither fires)
//! -- inserting (200,..)                   -> log = 1  (t1 fires)
//! ```

use vibesql_executor::{
    CreateTableExecutor, InsertExecutor, SelectExecutor, TriggerExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) {
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
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).expect("UPDATE failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

fn query_scalar_i64(db: &Database, sql: &str) -> i64 {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let Statement::Select(select) = stmt else { panic!("expected SELECT") };
    let rows = SelectExecutor::new(db).execute(&select).expect("SELECT failed");
    match &rows[0].values[0] {
        SqlValue::Integer(n) => *n,
        other => panic!("expected integer, got {other:?}"),
    }
}

/// trigger2-3.2: a WHEN subquery (`(SELECT count(*) FROM tbl) = 0`) is parsed,
/// evaluated at fire time against current table state, and the trigger fires
/// exactly when the subquery condition holds. Matches sqlite3 3.51.0 `{1 0 1}`.
#[test]
fn when_subquery_fires_per_subquery_result_trigger2_3_2() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE tbl (a, b, c, d);");
    exec(&mut db, "CREATE TABLE log (a);");
    exec(&mut db, "INSERT INTO log VALUES (0);");
    exec(
        &mut db,
        "CREATE TRIGGER t1 BEFORE INSERT ON tbl WHEN new.a > 20 \
         BEGIN UPDATE log SET a = a + 1; END;",
    );
    exec(
        &mut db,
        "CREATE TRIGGER t2 BEFORE INSERT ON tbl WHEN (SELECT count(*) FROM tbl) = 0 \
         BEGIN UPDATE log SET a = a + 1; END;",
    );

    // tbl is empty -> the subquery WHEN of t2 is true -> log becomes 1.
    exec(&mut db, "INSERT INTO tbl VALUES(0, 0, 0, 0);");
    assert_eq!(query_scalar_i64(&db, "SELECT a FROM log;"), 1, "empty-table insert: t2 should fire");
    exec(&mut db, "UPDATE log SET a = 0;");

    // tbl now has a row -> subquery WHEN of t2 is false, a=0 so t1 false -> log stays 0.
    exec(&mut db, "INSERT INTO tbl VALUES(0, 0, 0, 0);");
    assert_eq!(query_scalar_i64(&db, "SELECT a FROM log;"), 0, "non-empty insert: neither fires");
    exec(&mut db, "UPDATE log SET a = 0;");

    // a=200 -> t1 (new.a > 20) fires; t2's subquery WHEN is false -> log becomes 1.
    exec(&mut db, "INSERT INTO tbl VALUES(200, 0, 0, 0);");
    assert_eq!(query_scalar_i64(&db, "SELECT a FROM log;"), 1, "a>20 insert: t1 should fire");
}

/// #5585: a *correlated* WHEN subquery — one whose inner SELECT references the
/// firing row's NEW pseudo-column — evaluates against current DB state and the
/// trigger fires per the subquery result. Before the fix, the inner subquery's
/// SelectExecutor carried no trigger context, so `NEW.a` failed at fire time
/// with "Pseudo-variable NEW.a is only valid within trigger bodies".
///
/// Verified against sqlite3 3.51.0:
/// ```text
/// CREATE TABLE t(a);
/// CREATE TABLE seen(a);
/// CREATE TABLE log(n); INSERT INTO log VALUES(0);
/// CREATE TRIGGER tr AFTER INSERT ON t
///   WHEN (SELECT count(*) FROM seen WHERE a = NEW.a) = 0
///   BEGIN UPDATE log SET n = n + 1; INSERT INTO seen VALUES(NEW.a); END;
/// INSERT INTO t VALUES(5);  -- fires: seen has no a=5 -> log=1, seen={5}
/// INSERT INTO t VALUES(5);  -- does not fire: seen already has a=5
/// INSERT INTO t VALUES(7);  -- fires: log=2, seen={5,7}
/// -- final: log.n = 2, seen = 5,7
/// ```
#[test]
fn when_correlated_subquery_referencing_new_fires_per_condition_5585() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t (a);");
    exec(&mut db, "CREATE TABLE seen (a);");
    exec(&mut db, "CREATE TABLE log (n);");
    exec(&mut db, "INSERT INTO log VALUES (0);");
    exec(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t \
         WHEN (SELECT count(*) FROM seen WHERE a = NEW.a) = 0 \
         BEGIN UPDATE log SET n = n + 1; INSERT INTO seen VALUES(NEW.a); END;",
    );

    // First insert of 5: seen has no a=5 -> WHEN is true -> fires.
    exec(&mut db, "INSERT INTO t VALUES(5);");
    assert_eq!(query_scalar_i64(&db, "SELECT n FROM log;"), 1, "first 5: trigger should fire");

    // Second insert of 5: seen already has a=5 -> WHEN is false -> does not fire.
    exec(&mut db, "INSERT INTO t VALUES(5);");
    assert_eq!(query_scalar_i64(&db, "SELECT n FROM log;"), 1, "second 5: trigger should not fire");

    // Insert of 7: seen has no a=7 -> WHEN is true -> fires.
    exec(&mut db, "INSERT INTO t VALUES(7);");
    assert_eq!(query_scalar_i64(&db, "SELECT n FROM log;"), 2, "7: trigger should fire");

    // seen = {5, 7} (matches sqlite3 3.51.0).
    assert_eq!(query_scalar_i64(&db, "SELECT count(*) FROM seen;"), 2, "seen should hold {{5,7}}");
    assert_eq!(
        query_scalar_i64(&db, "SELECT count(*) FROM seen WHERE a = 5;"),
        1,
        "seen should contain exactly one a=5"
    );
    assert_eq!(
        query_scalar_i64(&db, "SELECT count(*) FROM seen WHERE a = 7;"),
        1,
        "seen should contain exactly one a=7"
    );
}

/// #5585: a correlated WHEN subquery returning a scalar value (not just a
/// count) referencing NEW. Mirrors the issue's `(SELECT val FROM other WHERE
/// other.id = NEW.id) > 0` shape. Verified against sqlite3 3.51.0: with
/// other = {(1,10),(2,0),(3,5)}, inserting ids 1,2,3 fires only for ids whose
/// matching `val > 0`, i.e. hit = {1,3}.
#[test]
fn when_correlated_scalar_subquery_referencing_new_5585() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t (id, x);");
    exec(&mut db, "CREATE TABLE other (id, val);");
    exec(&mut db, "CREATE TABLE hit (id);");
    exec(&mut db, "INSERT INTO other VALUES(1, 10);");
    exec(&mut db, "INSERT INTO other VALUES(2, 0);");
    exec(&mut db, "INSERT INTO other VALUES(3, 5);");
    exec(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t \
         WHEN (SELECT val FROM other WHERE other.id = NEW.id) > 0 \
         BEGIN INSERT INTO hit VALUES(NEW.id); END;",
    );

    exec(&mut db, "INSERT INTO t VALUES(1, 'a');");
    exec(&mut db, "INSERT INTO t VALUES(2, 'b');");
    exec(&mut db, "INSERT INTO t VALUES(3, 'c');");

    assert_eq!(query_scalar_i64(&db, "SELECT count(*) FROM hit;"), 2, "hit should be {{1,3}}");
    assert_eq!(
        query_scalar_i64(&db, "SELECT count(*) FROM hit WHERE id = 1;"),
        1,
        "id 1 (val 10 > 0) should fire"
    );
    assert_eq!(
        query_scalar_i64(&db, "SELECT count(*) FROM hit WHERE id = 2;"),
        0,
        "id 2 (val 0, not > 0) should not fire"
    );
    assert_eq!(
        query_scalar_i64(&db, "SELECT count(*) FROM hit WHERE id = 3;"),
        1,
        "id 3 (val 5 > 0) should fire"
    );
}

/// #5585: a correlated EXISTS subquery in a trigger WHEN clause referencing NEW.
/// Verified against sqlite3 3.51.0: with other = {2,4}, inserting ids 1,2,4
/// fires only when NEW.id EXISTS in other, i.e. hit = {2,4}.
#[test]
fn when_correlated_exists_subquery_referencing_new_5585() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t (id);");
    exec(&mut db, "CREATE TABLE other (id);");
    exec(&mut db, "CREATE TABLE hit (id);");
    exec(&mut db, "INSERT INTO other VALUES(2);");
    exec(&mut db, "INSERT INTO other VALUES(4);");
    exec(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t \
         WHEN EXISTS (SELECT 1 FROM other WHERE other.id = NEW.id) \
         BEGIN INSERT INTO hit VALUES(NEW.id); END;",
    );

    exec(&mut db, "INSERT INTO t VALUES(1);");
    exec(&mut db, "INSERT INTO t VALUES(2);");
    exec(&mut db, "INSERT INTO t VALUES(4);");

    assert_eq!(query_scalar_i64(&db, "SELECT count(*) FROM hit;"), 2, "hit should be {{2,4}}");
    assert_eq!(query_scalar_i64(&db, "SELECT count(*) FROM hit WHERE id = 1;"), 0, "id 1 not in other");
    assert_eq!(query_scalar_i64(&db, "SELECT count(*) FROM hit WHERE id = 2;"), 1, "id 2 in other");
    assert_eq!(query_scalar_i64(&db, "SELECT count(*) FROM hit WHERE id = 4;"), 1, "id 4 in other");
}

/// #5585 regression guard: a *non-trigger* correlated scalar subquery (ordinary
/// query, no trigger context) is unaffected by the pseudo-variable substitution
/// path. The subquery here correlates to the outer table's column, not NEW/OLD.
#[test]
fn non_trigger_correlated_scalar_subquery_unaffected_5585() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE outer_t (id, label);");
    exec(&mut db, "CREATE TABLE inner_t (id, val);");
    exec(&mut db, "INSERT INTO outer_t VALUES(1, 'a');");
    exec(&mut db, "INSERT INTO outer_t VALUES(2, 'b');");
    exec(&mut db, "INSERT INTO inner_t VALUES(1, 100);");
    exec(&mut db, "INSERT INTO inner_t VALUES(2, 200);");

    // Correlated scalar subquery against the outer row's column; no trigger.
    assert_eq!(
        query_scalar_i64(
            &db,
            "SELECT (SELECT val FROM inner_t WHERE inner_t.id = outer_t.id) \
             FROM outer_t WHERE id = 2;",
        ),
        200,
        "ordinary correlated scalar subquery must resolve to inner_t.val for id=2",
    );
}
