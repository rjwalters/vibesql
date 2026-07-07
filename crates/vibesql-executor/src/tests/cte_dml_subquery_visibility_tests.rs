//! Tests for CTE visibility inside DML sub-contexts (Gap 1 of issue #5941).
//!
//! A `WITH` clause attached to an UPDATE/INSERT/DELETE statement must be
//! visible to scalar subqueries evaluated while executing that DML — including
//! correlated subqueries in `UPDATE ... SET` expressions and scalar subqueries
//! inside a trigger body's `INSERT ... WITH ... SELECT`.
//!
//! Previously the correlated-subquery path and the trigger early-return path in
//! `ExpressionEvaluator::execute_scalar_subquery_rows` built their inner
//! `SelectExecutor` without threading the enclosing `cte_context`, so the CTE
//! table was invisible ("no such table") or silently resolved to a same-named
//! catalog table. These tests reproduce with1.test 4.3 and 27.1.

use vibesql_ast::Statement;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a single DDL/DML statement.
fn exec(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger(db, &s).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
            .map_err(|e| e.to_string()),
        Statement::Delete(s) => crate::delete::DeleteExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) deleted", count))
            .map_err(|e| e.to_string()),
        Statement::Update(s) => crate::update::UpdateExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) updated", count))
            .map_err(|e| e.to_string()),
        other => Err(format!("Unsupported statement type: {:?}", other)),
    }
}

/// Run a SELECT and return every row's values as `Vec<Vec<SqlValue>>`.
fn query_rows(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse select");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("expected SELECT, got {:?}", other),
    };
    let result = crate::SelectExecutor::new(db).execute(&select).expect("run select");
    result.iter().map(|r| r.values.to_vec()).collect()
}

fn int(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

fn text(s: &str) -> SqlValue {
    SqlValue::Varchar(arcstr::ArcStr::from(s))
}

/// with1.test 4.3 — a WITH clause on an UPDATE must be visible to a correlated
/// scalar subquery in the SET expression.
///
/// `WITH uset(a,b) AS (...) UPDATE t1 SET x = COALESCE((SELECT b FROM uset WHERE a=x), x)`
/// The subquery is correlated to the UPDATE target column `x` AND references the
/// CTE `uset`. Before the fix the correlated path dropped `cte_context`, so
/// `uset` failed to resolve. Expected result: {1 3 8 9}.
#[test]
fn cte_visible_to_correlated_subquery_in_update_set() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(x)").unwrap();
    // Mirror with1.test state after tests 4.1/4.2: rows 1,3,2,4.
    exec(&mut db, "INSERT INTO t1 VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO t1 VALUES(3)").unwrap();
    exec(&mut db, "INSERT INTO t1 VALUES(2)").unwrap();
    exec(&mut db, "INSERT INTO t1 VALUES(4)").unwrap();

    exec(
        &mut db,
        "WITH uset(a, b) AS ( SELECT 2, 8 UNION ALL SELECT 4, 9 ) \
         UPDATE t1 SET x = COALESCE( (SELECT b FROM uset WHERE a=x), x )",
    )
    .expect("UPDATE with CTE-referencing correlated subquery should succeed");

    let rows = query_rows(&db, "SELECT x FROM t1");
    let got: Vec<SqlValue> = rows.into_iter().map(|mut r| r.remove(0)).collect();
    // 1 unchanged, 3 unchanged, 2->8, 4->9
    assert_eq!(got, vec![int(1), int(3), int(8), int(9)]);
}

/// A non-correlated scalar subquery referencing the UPDATE's WITH clause must
/// also resolve (guards the else-branch of the correlated path).
#[test]
fn cte_visible_to_noncorrelated_subquery_in_update_set() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t2(x)").unwrap();
    exec(&mut db, "INSERT INTO t2 VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO t2 VALUES(2)").unwrap();

    exec(
        &mut db,
        "WITH c(v) AS ( SELECT 100 ) \
         UPDATE t2 SET x = x + (SELECT v FROM c)",
    )
    .expect("UPDATE with CTE-referencing non-correlated subquery should succeed");

    let rows = query_rows(&db, "SELECT x FROM t2 ORDER BY x");
    let got: Vec<SqlValue> = rows.into_iter().map(|mut r| r.remove(0)).collect();
    assert_eq!(got, vec![int(101), int(102)]);
}

/// with1.test 27.1 — inside a trigger body, an `INSERT ... WITH map(k,v) AS (...)
/// SELECT ..., (SELECT v FROM map WHERE k=new.k) ...` must resolve `map` to the
/// CTE, not to the real catalog table of the same name.
///
/// Before the fix the trigger early-return path in the scalar-subquery evaluator
/// dropped `cte_context`, so `(SELECT v FROM map ...)` resolved against the real
/// `map` table (values 'main1'/'main2') instead of the CTE ('cte1'/'cte2').
#[test]
fn cte_visible_to_scalar_subquery_in_trigger_body_insert() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(k)").unwrap();
    exec(&mut db, "CREATE TABLE log(k, cte_map, main_map)").unwrap();
    exec(&mut db, "CREATE TABLE map(k, v)").unwrap();
    exec(&mut db, "INSERT INTO map VALUES(1, 'main1'), (2, 'main2')").unwrap();

    exec(
        &mut db,
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN \
           INSERT INTO log \
             WITH map(k,v) AS (VALUES(1,'cte1'),(2,'cte2')) \
             SELECT \
               new.k, \
               (SELECT v FROM map WHERE k=new.k), \
               (SELECT v FROM main.map WHERE k=new.k); \
         END",
    )
    .expect("create trigger");

    exec(&mut db, "INSERT INTO t1 VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO t1 VALUES(2)").unwrap();

    let rows = query_rows(&db, "SELECT k, cte_map, main_map FROM log ORDER BY k");
    assert_eq!(
        rows,
        vec![vec![int(1), text("cte1"), text("main1")], vec![int(2), text("cte2"), text("main2")],]
    );
}
