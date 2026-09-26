//! Regression test for issue #6737: `sqlite_schema`'s trigger listing order
//! must stay in creation order after a `RENAME TABLE` restores 2+ triggers on
//! the same table, regardless of `Catalog::iter_triggers()`'s `HashMap`
//! iteration order (which reseeds per process and is therefore stable within
//! a single test run but not across process launches).
//!
//! `execute_rename_table` (`crates/vibesql-executor/src/alter/table_options.rs`)
//! snapshots every trigger targeting the renamed table, drops the table, then
//! re-inserts the snapshotted triggers via `create_trigger` — which
//! unconditionally assigns each restored trigger a *fresh* `creation_seq`.
//! Before the fix, the snapshot was a bare `Vec<TriggerDefinition>` collected
//! from `iter_triggers()`'s `HashMap` order, so when 2+ triggers were
//! restored together their *relative* `creation_seq` (and therefore their
//! `sqlite_schema` listing order) depended on that per-process HashMap
//! iteration order instead of their real original creation order.
//!
//! This reproduces the exact repro from #6737: two triggers (`Trigger1`,
//! `tr2`) end up targeting the same table across two separate
//! `ALTER TABLE ... RENAME TO` operations, so the second rename's restore
//! loop snapshots and re-inserts both of them together.

use vibesql_ast::Statement;
use vibesql_executor::{
    AlterTableExecutor, CreateTableExecutor, DeleteExecutor, SelectExecutor, TriggerExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a single SQL statement, preserving verbatim source text where the
/// executor supports it (needed for `sqlite_schema.sql` to round-trip the
/// original CREATE TRIGGER text).
fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute_with_source(&s, db, Some(sql))
                .expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        Statement::AlterTable(s) => {
            AlterTableExecutor::execute_with_source(&s, db, Some(sql)).expect("ALTER TABLE failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).expect("DELETE failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

/// Return the `name` column for every row of `SELECT name FROM sqlite_schema
/// WHERE type='trigger'`, in result order (i.e. `sqlite_schema`'s default
/// creation-order listing, since the query has no ORDER BY).
fn trigger_names_in_schema_order(db: &Database) -> Vec<String> {
    let stmt = Parser::parse_sql("SELECT name FROM sqlite_schema WHERE type='trigger'")
        .expect("parse SELECT");
    let Statement::Select(select) = stmt else { panic!("expected SELECT") };
    let rows = SelectExecutor::new(db).execute(&select).expect("SELECT failed");
    rows.into_iter()
        .map(|row| match &row.values[0] {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
            other => panic!("expected text name value, got {other:?}"),
        })
        .collect()
}

/// altertab3-29.7: after two `ALTER TABLE ... RENAME TO` operations restore
/// `Trigger1` and `tr2` together on the second rename, `sqlite_schema` must
/// list them in creation order (`Trigger1` before `tr2`) every time — never
/// flipped by `HashMap` iteration order.
#[test]
fn triggers_restored_together_on_rename_keep_creation_order() {
    let mut db = Database::new();

    exec(&mut db, "CREATE TABLE t1(x, y)");
    exec(
        &mut db,
        "CREATE TRIGGER Trigger1 DELETE ON t1 BEGIN SELECT t1.*, t1.x FROM t1 ORDER BY t1.x; END",
    );
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN x TO z");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t2");
    exec(
        &mut db,
        "CREATE TRIGGER tr2 AFTER DELETE ON t2 BEGIN SELECT z, y FROM (SELECT t2.* FROM t2); END",
    );
    exec(&mut db, "DELETE FROM t2");
    // Both Trigger1 and tr2 now target t2 and get snapshotted + restored
    // together by this rename's cascade-drop/recreate.
    exec(&mut db, "ALTER TABLE t2 RENAME TO t3");

    // Catalog-level invariant, independent of any particular HashMap seed:
    // Trigger1's restored creation_seq must still sort before tr2's.
    let trigger1_seq = db
        .catalog
        .creation_seq("main", "Trigger1")
        .expect("Trigger1 must have a recorded creation_seq after restore");
    let tr2_seq = db
        .catalog
        .creation_seq("main", "tr2")
        .expect("tr2 must have a recorded creation_seq after restore");
    assert!(
        trigger1_seq < tr2_seq,
        "Trigger1 (seq={trigger1_seq}) must sort before tr2 (seq={tr2_seq}) — a rename that \
         restores 2+ triggers together must preserve their original relative creation order \
         (#6737), not assign fresh ordinals in HashMap iteration order"
    );

    // End-to-end: sqlite_schema's default (no ORDER BY) listing must reflect
    // that same creation order.
    let names = trigger_names_in_schema_order(&db);
    assert_eq!(
        names,
        vec!["Trigger1", "tr2"],
        "sqlite_schema must list triggers restored together by RENAME TABLE in their \
         original creation order (#6737)"
    );
}
