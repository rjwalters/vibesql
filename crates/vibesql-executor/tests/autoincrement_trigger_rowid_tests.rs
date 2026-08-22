//! Regression tests: an auto-assigned INTEGER PRIMARY KEY must be allocated
//! AFTER any BEFORE INSERT trigger has run (issue #6173, autoinc-3928).
//!
//! SQLite does not allocate the real rowid for a NULL/omitted INTEGER PRIMARY
//! KEY until the row is physically written — which happens *after* the BEFORE
//! INSERT trigger fires. A BEFORE INSERT trigger body may itself insert rows
//! into the same table (directly, or via recursive triggers), advancing the
//! table's max rowid and, for AUTOINCREMENT, the `sqlite_sequence` high-water
//! mark. If the outer row's rowid were fixed *before* the trigger ran it would
//! collide with a rowid the trigger just consumed, producing duplicate rowids.
//!
//! Every expectation below was verified against sqlite3 3.51.0 (autoinc.test
//! section 3928, run under the default `recursive_triggers = off`, which bounds
//! the mutually-recursive BEFORE/AFTER cascade to 13 rows).

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor, TriggerExecutor};
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
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    SelectExecutor::new(db)
        .execute(&select)
        .expect("SELECT failed")
        .into_iter()
        .map(|row| row.values.to_vec())
        .collect()
}

fn ints(rows: &[Vec<SqlValue>], col: usize) -> Vec<i64> {
    rows.iter()
        .map(|r| match &r[col] {
            SqlValue::Integer(v) => *v,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

/// A BEFORE INSERT trigger that inserts into the same AUTOINCREMENT table must
/// not cause the outer row to reuse a rowid the trigger already consumed.
/// Every rowid in the table must be distinct.
#[test]
fn before_insert_trigger_into_same_autoincrement_table_yields_unique_rowids() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b)");
    // recursive_triggers defaults to off (matching sqlite3); set explicitly.
    db.set_recursive_triggers(false);
    exec(
        &mut db,
        "CREATE TRIGGER t_before BEFORE INSERT ON t BEGIN \
         INSERT INTO t(b) VALUES('before'); END",
    );

    exec(&mut db, "INSERT INTO t(b) VALUES('outer')");

    let rows = query(&db, "SELECT a, b FROM t ORDER BY a");
    let rowids = ints(&rows, 0);
    let mut sorted = rowids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        rowids.len(),
        "AUTOINCREMENT rowids must be unique even when a BEFORE INSERT trigger \
         inserts into the same table; got {rowids:?}"
    );

    // The sqlite_sequence high-water mark must equal the true maximum rowid the
    // table ever held (not the stale pre-trigger value).
    let seq = query(&db, "SELECT seq FROM sqlite_sequence WHERE name='t'");
    let max_rowid = *rowids.iter().max().unwrap();
    assert_eq!(ints(&seq, 0), vec![max_rowid], "sqlite_sequence.seq must track the true max rowid");
}

/// The exact autoinc-3928.1/.2 shape: mutually-recursive BEFORE and AFTER
/// INSERT triggers on an AUTOINCREMENT table. sqlite3 3.51.0 produces 13 rows
/// with rowids 1..13 and `sqlite_sequence.seq = 13`.
#[test]
fn autoinc_3928_before_and_after_triggers_produce_contiguous_unique_rowids() {
    let mut db = Database::new();
    db.set_recursive_triggers(false);
    exec(&mut db, "CREATE TABLE t3928(a INTEGER PRIMARY KEY AUTOINCREMENT, b)");
    exec(
        &mut db,
        "CREATE TRIGGER t3928r1 BEFORE INSERT ON t3928 BEGIN \
         INSERT INTO t3928(b) VALUES('before1'); \
         INSERT INTO t3928(b) VALUES('before2'); END",
    );
    exec(
        &mut db,
        "CREATE TRIGGER t3928r2 AFTER INSERT ON t3928 BEGIN \
         INSERT INTO t3928(b) VALUES('after1'); \
         INSERT INTO t3928(b) VALUES('after2'); END",
    );

    exec(&mut db, "INSERT INTO t3928(b) VALUES('test')");

    let rows = query(&db, "SELECT a FROM t3928 ORDER BY a");
    let rowids = ints(&rows, 0);
    assert_eq!(
        rowids,
        (1..=13).collect::<Vec<_>>(),
        "recursive BEFORE/AFTER trigger inserts into an AUTOINCREMENT table must \
         produce contiguous, unique rowids 1..13 (autoinc-3928.1)"
    );

    let seq = query(&db, "SELECT seq FROM sqlite_sequence WHERE name='t3928'");
    assert_eq!(
        ints(&seq, 0),
        vec![13],
        "sqlite_sequence.seq must be 13 after the recursive trigger cascade \
         (autoinc-3928.2)"
    );
}
