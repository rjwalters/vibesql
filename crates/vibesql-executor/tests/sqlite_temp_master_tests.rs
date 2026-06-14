//! Integration tests for schema-aware catalog indexes and `sqlite_temp_master`.
//!
//! Issue #5513: A temp-table index lives in the session temp schema. It must be
//! listed in `sqlite_temp_master` (not `sqlite_master`), dropped with its temp
//! table, and able to coexist with a same-named main-schema index. This matches
//! SQLite 3.51.0:
//!
//! ```sql
//! CREATE TEMP TABLE t(a);
//! CREATE INDEX i ON t(a);
//! SELECT name,type FROM sqlite_temp_master;  -- lists t and i
//! SELECT name FROM sqlite_master;            -- lists neither
//! ```

use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DropIndexExecutor, DropTableExecutor, SelectExecutor,
    TriggerExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

fn exec_create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE")
        }
        other => panic!("expected CREATE TABLE, got {other:?}"),
    };
}

fn exec_create_index(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE INDEX");
    match stmt {
        vibesql_ast::Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db).expect("CREATE INDEX")
        }
        other => panic!("expected CREATE INDEX, got {other:?}"),
    };
}

fn exec_drop_index(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse DROP INDEX");
    match stmt {
        vibesql_ast::Statement::DropIndex(s) => {
            DropIndexExecutor::execute(&s, db).expect("DROP INDEX")
        }
        other => panic!("expected DROP INDEX, got {other:?}"),
    };
}

/// Execute an INSERT statement.
fn exec_insert(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse INSERT");
    match stmt {
        vibesql_ast::Statement::Insert(s) => {
            vibesql_executor::InsertExecutor::execute(db, &s).expect("INSERT");
        }
        other => panic!("expected INSERT, got {other:?}"),
    }
}

fn exec_drop_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse DROP TABLE");
    match stmt {
        vibesql_ast::Statement::DropTable(s) => {
            DropTableExecutor::execute(&s, db).expect("DROP TABLE")
        }
        other => panic!("expected DROP TABLE, got {other:?}"),
    };
}

fn exec_create_trigger(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TRIGGER");
    match stmt {
        vibesql_ast::Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger(db, &s).expect("CREATE TRIGGER");
        }
        other => panic!("expected CREATE TRIGGER, got {other:?}"),
    };
}

fn exec_create_view(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE VIEW");
    match stmt {
        vibesql_ast::Statement::CreateView(s) => {
            ViewExecutor::execute_create_view(&s, db).expect("CREATE VIEW");
        }
        other => panic!("expected CREATE VIEW, got {other:?}"),
    };
}

/// Run a SELECT and return whether it errored (used to assert "view errors on
/// use" parity after its underlying table is dropped).
fn select_is_err(db: &Database, sql: &str) -> bool {
    let stmt = match Parser::parse_sql(sql) {
        Ok(s) => s,
        Err(_) => return true,
    };
    match stmt {
        vibesql_ast::Statement::Select(s) => {
            SelectExecutor::new(db).execute_with_columns(&s).is_err()
        }
        other => panic!("expected SELECT, got {other:?}"),
    }
}

fn select(db: &Database, sql: &str) -> (Vec<String>, Vec<Row>) {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    match stmt {
        vibesql_ast::Statement::Select(s) => {
            let executor = SelectExecutor::new(db);
            let r = executor.execute_with_columns(&s).expect("SELECT");
            (r.columns, r.rows)
        }
        other => panic!("expected SELECT, got {other:?}"),
    }
}

/// Pull the `name` (column 0) string values out of a result set.
fn names(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected text name, got {other:?}"),
        })
        .collect()
}

/// sqlite3 parity: a temp table and its index appear in `sqlite_temp_master`.
#[test]
fn temp_table_and_index_listed_in_temp_master() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TEMP TABLE t (a)");
    exec_create_index(&mut db, "CREATE INDEX i ON t(a)");

    let (_, rows) = select(&db, "SELECT name FROM sqlite_temp_master ORDER BY name");
    let listed = names(&rows);
    assert!(listed.contains(&"t".to_string()), "temp table t should be in sqlite_temp_master: {listed:?}");
    assert!(listed.contains(&"i".to_string()), "temp index i should be in sqlite_temp_master: {listed:?}");
}

/// sqlite3 parity: temp objects are absent from `sqlite_master`.
#[test]
fn temp_index_absent_from_sqlite_master() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TEMP TABLE t (a)");
    exec_create_index(&mut db, "CREATE INDEX i ON t(a)");

    let (_, rows) = select(&db, "SELECT name FROM sqlite_master");
    let listed = names(&rows);
    assert!(!listed.contains(&"t".to_string()), "temp table must NOT be in sqlite_master: {listed:?}");
    assert!(!listed.contains(&"i".to_string()), "temp index must NOT be in sqlite_master: {listed:?}");
}

/// A main-table index stays in `sqlite_master` and is absent from temp_master.
#[test]
fn main_index_in_master_not_temp_master() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a)");
    exec_create_index(&mut db, "CREATE INDEX mi ON base(a)");

    let (_, master) = select(&db, "SELECT name FROM sqlite_master");
    assert!(names(&master).contains(&"mi".to_string()), "main index should be in sqlite_master");

    let (_, temp) = select(&db, "SELECT name FROM sqlite_temp_master");
    assert!(!names(&temp).contains(&"mi".to_string()), "main index must NOT be in sqlite_temp_master");
}

/// Count the rows of a single-column COUNT(*) result.
fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let (_, rows) = select(db, sql);
    assert_eq!(rows.len(), 1, "expected one result row for {sql}");
    match &rows[0].values[0] {
        SqlValue::Integer(n) => *n,
        SqlValue::Bigint(n) => *n,
        other => panic!("expected integer scalar, got {other:?}"),
    }
}

/// #5540 (was #5513's documented SQL-level limitation): a temp-schema index and
/// a main-schema index can share a bare name at the SQL level, because the
/// storage index manager is now schema-aware. Both must coexist, both must be
/// usable, and an unqualified DROP INDEX must resolve temp-shadows-main —
/// matching sqlite3 3.51.0:
///
/// ```sql
/// CREATE TABLE main_t(a); CREATE TEMP TABLE temp_t(a);
/// CREATE INDEX ix ON main_t(a);  -- main.ix
/// CREATE INDEX ix ON temp_t(a);  -- temp.ix  (succeeds in sqlite3)
/// ```
#[test]
fn sql_level_same_name_index_across_schemas_coexist() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE main_t (a)");
    exec_create_table(&mut db, "CREATE TEMP TABLE temp_t (a)");

    // Distinct data so a live SELECT through each index is observable.
    exec_insert(&mut db, "INSERT INTO main_t VALUES (1)");
    exec_insert(&mut db, "INSERT INTO main_t VALUES (2)");
    exec_insert(&mut db, "INSERT INTO temp.temp_t VALUES (10)");

    // main.ix and temp.ix share the bare name `ix` — both CREATE INDEX succeed.
    exec_create_index(&mut db, "CREATE INDEX ix ON main_t(a)");
    exec_create_index(&mut db, "CREATE INDEX ix ON temp_t(a)");

    // Both indexes are registered in their respective schemas' master tables.
    let (_, master) = select(&db, "SELECT name FROM sqlite_master WHERE type='index'");
    assert!(names(&master).contains(&"ix".to_string()), "main.ix must be in sqlite_master");
    let (_, temp_master) =
        select(&db, "SELECT name FROM sqlite_temp_master WHERE type='index'");
    assert!(
        names(&temp_master).contains(&"ix".to_string()),
        "temp.ix must be in sqlite_temp_master"
    );

    // Both indexes are usable: a live SELECT through each returns the right rows.
    assert_eq!(scalar_i64(&db, "SELECT count(*) FROM main_t WHERE a = 1"), 1);
    assert_eq!(scalar_i64(&db, "SELECT count(*) FROM main_t WHERE a = 2"), 1);
    assert_eq!(scalar_i64(&db, "SELECT count(*) FROM temp.temp_t WHERE a = 10"), 1);
    // The temp index does not leak the main table's values, and vice versa.
    assert_eq!(scalar_i64(&db, "SELECT count(*) FROM temp.temp_t WHERE a = 1"), 0);
    assert_eq!(scalar_i64(&db, "SELECT count(*) FROM main_t WHERE a = 10"), 0);

    // Unqualified DROP INDEX resolves temp-shadows-main: it drops temp.ix and
    // leaves main.ix intact (matching sqlite3's name resolution).
    exec_drop_index(&mut db, "DROP INDEX ix");

    let (_, temp_after) =
        select(&db, "SELECT name FROM sqlite_temp_master WHERE type='index'");
    assert!(
        !names(&temp_after).contains(&"ix".to_string()),
        "unqualified DROP INDEX must remove the temp index first"
    );
    let (_, master_after) = select(&db, "SELECT name FROM sqlite_master WHERE type='index'");
    assert!(
        names(&master_after).contains(&"ix".to_string()),
        "main.ix must survive an unqualified DROP INDEX that resolved to temp.ix"
    );

    // The surviving main index still works.
    assert_eq!(scalar_i64(&db, "SELECT count(*) FROM main_t WHERE a = 2"), 1);

    // A second unqualified DROP INDEX now removes main.ix.
    exec_drop_index(&mut db, "DROP INDEX ix");
    let (_, master_final) = select(&db, "SELECT name FROM sqlite_master WHERE type='index'");
    assert!(
        !names(&master_final).contains(&"ix".to_string()),
        "the second DROP INDEX removes the remaining main index"
    );
}

/// A temp index is dropped together with its temp table, and dropping the temp
/// table does not disturb a main index on another table.
#[test]
fn temp_index_dropped_with_temp_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t (a)");
    exec_create_index(&mut db, "CREATE INDEX mi ON base(a)"); // main index
    exec_create_index(&mut db, "CREATE INDEX ti ON t(a)"); // temp index

    // Before: temp_master lists the temp index; master lists the main index.
    let (_, before) = select(&db, "SELECT name FROM sqlite_temp_master WHERE type='index'");
    assert!(names(&before).contains(&"ti".to_string()));

    // Drop the temp table. Use the schema-qualified name because an unqualified
    // DROP TABLE does not yet resolve into the temp schema (separate pre-existing
    // limitation, unrelated to #5513's index work).
    exec_drop_table(&mut db, "DROP TABLE temp.t");

    // After: temp index gone from temp_master.
    let (_, after_temp) = select(&db, "SELECT name FROM sqlite_temp_master WHERE type='index'");
    assert!(
        !names(&after_temp).contains(&"ti".to_string()),
        "temp index should be dropped with the temp table"
    );

    // Main index on the other table survives.
    let (_, after_main) = select(&db, "SELECT name FROM sqlite_master WHERE type='index'");
    assert!(
        names(&after_main).contains(&"mi".to_string()),
        "main index must survive dropping the temp table"
    );
}

/// Issue #5583 — temp lifecycle: a temp trigger defined ON a temp table is
/// dropped together with that table, while a temp VIEW referencing the table is
/// NOT auto-dropped (it remains in sqlite_temp_master and errors on use).
///
/// Verified against sqlite3 3.51.0:
///
/// ```text
/// CREATE TEMP TABLE t(a);
/// CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END;
/// CREATE TEMP VIEW v AS SELECT * FROM t;
/// DROP TABLE t;                  -- tr gone, v remains
/// SELECT type,name FROM sqlite_temp_master;  -- only: view v
/// SELECT * FROM v;               -- Error: no such table: t
/// ```
#[test]
fn temp_trigger_dropped_with_temp_table_view_remains() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TEMP TABLE t (a)");
    exec_create_trigger(&mut db, "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END");
    exec_create_view(&mut db, "CREATE TEMP VIEW v AS SELECT * FROM t");

    // Before the drop: both the trigger and the view are surfaced by temp_master.
    let (_, before) = select(&db, "SELECT name FROM sqlite_temp_master WHERE type='trigger'");
    assert!(names(&before).contains(&"tr".to_string()), "temp trigger should be listed before drop");
    let (_, before_v) = select(&db, "SELECT name FROM sqlite_temp_master WHERE type='view'");
    assert!(names(&before_v).contains(&"v".to_string()), "temp view should be listed before drop");

    // Drop the temp table (schema-qualified: unqualified DROP TABLE not resolving
    // into temp is a separate pre-existing limitation, same as the index test).
    exec_drop_table(&mut db, "DROP TABLE temp.t");

    // The temp trigger ON the table is gone (matches sqlite3).
    let (_, after_tr) = select(&db, "SELECT name FROM sqlite_temp_master WHERE type='trigger'");
    assert!(
        !names(&after_tr).contains(&"tr".to_string()),
        "temp trigger ON the dropped table must be cascade-dropped"
    );

    // The view referencing the table is NOT auto-dropped (matches sqlite3).
    let (_, after_v) = select(&db, "SELECT name FROM sqlite_temp_master WHERE type='view'");
    assert!(
        names(&after_v).contains(&"v".to_string()),
        "a view merely referencing the dropped table must NOT be auto-dropped"
    );

    // ...but the surviving view errors on use, since its base table is gone.
    assert!(
        select_is_err(&db, "SELECT * FROM v"),
        "the surviving view must error on use after its base table is dropped"
    );
}

/// Issue #5583 — main-schema parity: `DROP TABLE` drops a trigger defined ON the
/// table, but NOT a trigger defined ON a *different* table even if it references
/// the dropped one in its body.
///
/// Verified against sqlite3 3.51.0:
///
/// ```text
/// CREATE TABLE t(a);  CREATE TABLE other(b);
/// CREATE TRIGGER tr  AFTER INSERT ON t     BEGIN SELECT 1; END;
/// CREATE TRIGGER tr2 AFTER INSERT ON other BEGIN INSERT INTO t VALUES(1); END;
/// DROP TABLE t;       -- tr gone, tr2 remains (it is ON other)
/// ```
#[test]
fn main_drop_table_drops_only_triggers_on_that_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t (a)");
    exec_create_table(&mut db, "CREATE TABLE other (b)");
    exec_create_trigger(&mut db, "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END");
    exec_create_trigger(
        &mut db,
        "CREATE TRIGGER tr2 AFTER INSERT ON other BEGIN INSERT INTO t VALUES (1); END",
    );

    exec_drop_table(&mut db, "DROP TABLE t");

    let (_, after) = select(&db, "SELECT name FROM sqlite_master WHERE type='trigger'");
    let listed = names(&after);
    assert!(
        !listed.contains(&"tr".to_string()),
        "trigger ON the dropped table must be removed: {listed:?}"
    );
    assert!(
        listed.contains(&"tr2".to_string()),
        "trigger ON a different table (even if it references the dropped one) must survive: {listed:?}"
    );
}

/// Issue #5583 — connection-close / schema isolation: each connection owns its
/// own `Catalog` (per `Catalog::new()` session), so temp triggers and views are
/// connection-local by construction and a fresh connection sees none of the
/// previous connection's temp objects. (Connection close is structural: the
/// `Catalog` — including the flat view/trigger maps and the session temp schema —
/// is dropped wholesale.)
#[test]
fn temp_trigger_and_view_are_connection_local() {
    // Connection A creates temp objects.
    let mut db_a = Database::new();
    exec_create_table(&mut db_a, "CREATE TEMP TABLE t (a)");
    exec_create_trigger(&mut db_a, "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END");
    exec_create_view(&mut db_a, "CREATE TEMP VIEW v AS SELECT * FROM t");

    let (_, a_objs) = select(&db_a, "SELECT name FROM sqlite_temp_master");
    let a_listed = names(&a_objs);
    assert!(a_listed.contains(&"tr".to_string()));
    assert!(a_listed.contains(&"v".to_string()));

    // A separate connection B sees none of A's temp objects.
    let db_b = Database::new();
    let (_, b_objs) = select(&db_b, "SELECT name FROM sqlite_temp_master");
    let b_listed = names(&b_objs);
    assert!(
        !b_listed.contains(&"tr".to_string()),
        "connection B must not see connection A's temp trigger: {b_listed:?}"
    );
    assert!(
        !b_listed.contains(&"v".to_string()),
        "connection B must not see connection A's temp view: {b_listed:?}"
    );

    // Closing connection A (drop) releases its temp objects with the Catalog.
    drop(db_a);
}
