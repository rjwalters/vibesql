//! Regression tests for #5543: BEFORE/AFTER UPDATE triggers must observe the
//! *live* column values / aggregates of the table at each phase of a per-row
//! UPDATE — the UPDATE analogue of the #5523/#5542 DELETE `count(*)` fix.
//!
//! SQLite fires UPDATE row triggers interleaved per row (#5486): for each
//! affected row R it runs BEFORE(R), applies R's change, then runs AFTER(R)
//! before moving to R+1. A trigger body that reads the table mid-statement —
//! `(SELECT sum(col) FROM t)`, `(SELECT max(col) FROM t)`, or a columnar-served
//! value read — must therefore see exactly the updates applied so far.
//!
//! Before this fix `update_row_selective` invalidated only the *table-level*
//! columnar cache; the *database-level* columnar LRU (`Database::get_columnar`),
//! which serves the columnar aggregate path for row-oriented tables, was
//! invalidated only *after* the whole UPDATE loop. The AFTER trigger (and the
//! next row's BEFORE trigger) then observed pre-update column values. All
//! expected sequences below were verified live against sqlite3 3.51.0.

use vibesql_ast::Statement;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger(db, &s).unwrap();
        }
        Statement::Insert(s) => {
            crate::InsertExecutor::execute(db, &s).unwrap();
        }
        Statement::Update(s) => {
            crate::UpdateExecutor::execute(&s, db).unwrap();
        }
        Statement::Delete(s) => {
            crate::delete::DeleteExecutor::execute(&s, db).unwrap();
        }
        other => panic!("unsupported statement: {other:?}"),
    }
}

/// Read the `t` column of the `log` table, in insertion (rowid) order, as a
/// `Vec<String>`. Uses a LIVE SELECT through the executor so the assertion
/// exercises the same read path the triggers observe.
fn log_values(db: &Database) -> Vec<String> {
    let stmt = vibesql_parser::Parser::parse_sql("SELECT t FROM log").expect("parse select");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("expected SELECT, got {other:?}"),
    };
    let result =
        crate::SelectExecutor::new(db).execute_with_columns(&select).expect("run select");
    result
        .rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// `t1(a PK, b)` plus a `log` table seeded with three rows.
fn setup_table(db: &mut Database) {
    exec(db, "CREATE TABLE t1(a INT PRIMARY KEY, b INT)");
    exec(db, "CREATE TABLE log(t)");
    exec(db, "INSERT INTO t1 VALUES(1,10),(2,20),(3,30)");
}

/// AFTER UPDATE trigger logging the updated column plus running aggregates over
/// the (now-mutated) table. Single-row UPDATE.
///
/// sqlite3 3.51.0: `1:100:sum=150:max=100:cnt=3` — the AFTER trigger sees row 1
/// already changed (b=100), so sum=100+20+30=150 and max=100.
#[test]
fn after_update_trigger_reads_applied_value_single_row() {
    let mut db = Database::new();
    setup_table(&mut db);
    exec(
        &mut db,
        "CREATE TRIGGER tr_after AFTER UPDATE ON t1 BEGIN \
         INSERT INTO log VALUES(old.a || ':' || new.b \
         || ':sum=' || (SELECT sum(b) FROM t1) \
         || ':max=' || (SELECT max(b) FROM t1) \
         || ':cnt=' || (SELECT count(*) FROM t1)); END",
    );

    exec(&mut db, "UPDATE t1 SET b=100 WHERE a=1");

    assert_eq!(
        log_values(&db),
        vec!["1:100:sum=150:max=100:cnt=3".to_string()],
        "AFTER UPDATE must see row 1's applied value (b=100)"
    );
}

/// AFTER UPDATE trigger over a multi-row UPDATE: each AFTER(R) must reflect the
/// running aggregates as rows are applied one-by-one.
///
/// sqlite3 3.51.0 (`UPDATE t1 SET b=b+1`, PK order):
///   1:11:sum=63:max=31:cnt=3   (after row1: 11+20+30=61? no — see below)
/// Verified live: rows become b=11,21,31. The aggregates use the *running*
/// applied state per AFTER(R):
///   AFTER(1): b=11,20,30 -> sum=61, max=30
///   AFTER(2): b=11,21,30 -> sum=62, max=30
///   AFTER(3): b=11,21,31 -> sum=63, max=31
#[test]
fn after_update_trigger_reads_applied_value_multi_row() {
    let mut db = Database::new();
    setup_table(&mut db);
    exec(
        &mut db,
        "CREATE TRIGGER tr_after AFTER UPDATE ON t1 BEGIN \
         INSERT INTO log VALUES(old.a || ':' || new.b \
         || ':sum=' || (SELECT sum(b) FROM t1) \
         || ':max=' || (SELECT max(b) FROM t1)); END",
    );

    exec(&mut db, "UPDATE t1 SET b=b+1");

    assert_eq!(
        log_values(&db),
        vec![
            "1:11:sum=61:max=30".to_string(),
            "2:21:sum=62:max=30".to_string(),
            "3:31:sum=63:max=31".to_string(),
        ],
        "each AFTER(R) must observe the running applied aggregates"
    );
}

/// BEFORE UPDATE trigger over a multi-row UPDATE: BEFORE(R) runs before R is
/// applied but must see rows 1..R-1 already updated.
///
/// sqlite3 3.51.0 (`UPDATE t1 SET b=b+100`, PK order):
///   BEFORE(1): no rows applied -> b=10,20,30 -> sum=60, max=30
///   BEFORE(2): row1 applied   -> b=110,20,30 -> sum=160, max=110
///   BEFORE(3): rows1,2 applied -> b=110,120,30 -> sum=260, max=120
#[test]
fn before_update_trigger_sees_prior_rows_applied_multi_row() {
    let mut db = Database::new();
    setup_table(&mut db);
    exec(
        &mut db,
        "CREATE TRIGGER tr_before BEFORE UPDATE ON t1 BEGIN \
         INSERT INTO log VALUES('B:' || old.a \
         || ':sum=' || (SELECT sum(b) FROM t1) \
         || ':max=' || (SELECT max(b) FROM t1)); END",
    );

    exec(&mut db, "UPDATE t1 SET b=b+100");

    assert_eq!(
        log_values(&db),
        vec![
            "B:1:sum=60:max=30".to_string(),
            "B:2:sum=160:max=110".to_string(),
            "B:3:sum=260:max=120".to_string(),
        ],
        "BEFORE(R) must observe rows 1..R-1 already applied"
    );
}

/// Combined BEFORE + AFTER UPDATE triggers over a multi-row UPDATE: the
/// interleaving must be BEFORE(1) AFTER(1) BEFORE(2) AFTER(2) BEFORE(3)
/// AFTER(3), with each phase seeing the running applied state.
///
/// sqlite3 3.51.0 (`UPDATE t1 SET b=b+100`, PK order):
///   B:1:sum=60   (none applied)
///   A:1:sum=160  (row1 applied)
///   B:2:sum=160  (row1 applied)
///   A:2:sum=260  (rows1,2 applied)
///   B:3:sum=260  (rows1,2 applied)
///   A:3:sum=360  (all applied)
#[test]
fn combined_before_after_update_trigger_interleaved_sum() {
    let mut db = Database::new();
    setup_table(&mut db);
    exec(
        &mut db,
        "CREATE TRIGGER tr_before BEFORE UPDATE ON t1 BEGIN \
         INSERT INTO log VALUES('B:' || old.a || ':sum=' || (SELECT sum(b) FROM t1)); END",
    );
    exec(
        &mut db,
        "CREATE TRIGGER tr_after AFTER UPDATE ON t1 BEGIN \
         INSERT INTO log VALUES('A:' || old.a || ':sum=' || (SELECT sum(b) FROM t1)); END",
    );

    exec(&mut db, "UPDATE t1 SET b=b+100");

    assert_eq!(
        log_values(&db),
        vec![
            "B:1:sum=60".to_string(),
            "A:1:sum=160".to_string(),
            "B:2:sum=160".to_string(),
            "A:2:sum=260".to_string(),
            "B:3:sum=260".to_string(),
            "A:3:sum=360".to_string(),
        ],
        "interleaved BEFORE/AFTER must each observe the running applied sum"
    );
}

/// No-trigger control: a plain UPDATE (the fast bulk `else` branch that the
/// per-row columnar invalidation deliberately does NOT touch) must still apply
/// every row, and a subsequent LIVE aggregate read must reflect all updates.
/// This guards the perf-sensitive no-trigger path.
#[test]
fn no_trigger_update_applies_all_rows() {
    let mut db = Database::new();
    setup_table(&mut db);

    exec(&mut db, "UPDATE t1 SET b=b+100");

    let stmt = vibesql_parser::Parser::parse_sql("SELECT sum(b), max(b), count(*) FROM t1")
        .expect("parse");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("expected SELECT, got {other:?}"),
    };
    let result =
        crate::SelectExecutor::new(&db).execute_with_columns(&select).expect("run select");
    // sum=110+120+130=360, max=130, count=3
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], SqlValue::Integer(360));
    assert_eq!(result.rows[0].values[1], SqlValue::Integer(130));
    assert_eq!(result.rows[0].values[2], SqlValue::Integer(3));
}
