//! Regression tests for issue #5353
//!
//! SQLite allows a `WITH` clause to precede a standalone `VALUES` statement
//! (a standalone VALUES is treated as a SELECT form), but VibeSQL rejected it
//! at parse time. Additionally, the standalone-VALUES execution path
//! (`execute_values()`) did not receive the enclosing statement's CTE context,
//! so subqueries inside VALUES rows could not reference CTE names — including
//! in compound positions like `SELECT 1 UNION VALUES((SELECT ... FROM cte))`.
//!
//! Repro from the issue:
//!
//! ```sql
//! WITH lll AS (SELECT 100 AS id) VALUES((SELECT max(id) FROM lll));
//! -- sqlite3 3.51.0: 100
//! -- VibeSQL used to fail: Parse error: Expected SELECT, INSERT, UPDATE, or
//! -- DELETE after WITH clause, found near "VALUES": syntax error
//! ```
//!
//! All expected values below were verified against sqlite3 3.51.0.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
        }
        vibesql_ast::Statement::CreateView(create_view) => {
            vibesql_executor::ViewExecutor::execute_create_view(&create_view, db).unwrap();
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

fn query(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e))
            .into_iter()
            .map(|row| row.values.to_vec())
            .collect()
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

/// Setup matching issue #5350: a view `lll` over `t1`, shadowed by a CTE.
fn setup_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, grp_id INTEGER)");
    run_stmt(&mut db, "INSERT INTO t1 VALUES (1,2),(2,3),(3,2)");
    run_stmt(
        &mut db,
        "CREATE VIEW lll AS \
         SELECT row_number() OVER (PARTITION BY grp_id) AS rn, grp_id, id FROM t1",
    );
    db
}

/// The exact repro from issue #5353 (sqlite3: 100).
#[test]
fn test_with_values_scalar_subquery() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
         VALUES((SELECT max(id) FROM lll WHERE grp_id = 2))",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(100)]]);
}

/// Minimal form (sqlite3: 1).
#[test]
fn test_with_values_minimal() {
    let db = vibesql_storage::Database::new();
    let rows = query(&db, "WITH c AS (SELECT 1) VALUES((SELECT * FROM c))");
    assert_eq!(rows, vec![vec![SqlValue::Integer(1)]]);
}

/// Multi-row VALUES with a CTE-referencing subquery (sqlite3: 1, 2, 5).
#[test]
fn test_with_values_multi_row() {
    let db = vibesql_storage::Database::new();
    let rows = query(&db, "WITH x AS (SELECT 5) VALUES(1),(2),((SELECT * FROM x))");
    assert_eq!(
        rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)], vec![SqlValue::Integer(5)]]
    );
}

/// Multiple rows each referencing the CTE (sqlite3: 10, 11).
#[test]
fn test_with_values_cte_referenced_in_each_row() {
    let db = vibesql_storage::Database::new();
    let rows =
        query(&db, "WITH c AS (SELECT 10 AS v) VALUES((SELECT v FROM c)),((SELECT v+1 FROM c))");
    assert_eq!(rows, vec![vec![SqlValue::Integer(10)], vec![SqlValue::Integer(11)]]);
}

/// Judge-confirmed gap from #5352: VALUES on the right side of a compound
/// must see the enclosing WITH (sqlite3: 1, 100).
#[test]
fn test_union_values_subquery_sees_outer_cte() {
    let db = setup_db();
    let mut rows = query(
        &db,
        "WITH lll AS (SELECT 100 AS id) SELECT 1 UNION VALUES((SELECT max(id) FROM lll))",
    );
    rows.sort();
    assert_eq!(rows, vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(100)]]);
}

/// VALUES on both sides of a compound under a WITH (sqlite3: 2, 9).
#[test]
fn test_with_values_union_values() {
    let db = vibesql_storage::Database::new();
    let mut rows = query(&db, "WITH c AS (SELECT 9) VALUES((SELECT * FROM c)) UNION VALUES(2)");
    rows.sort();
    assert_eq!(rows, vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(9)]]);
}

/// CTE names are ASCII case-insensitive (sqlite3: 1).
#[test]
fn test_with_values_case_insensitive_cte_reference() {
    let db = vibesql_storage::Database::new();
    let rows = query(&db, "WITH c AS (SELECT 1) VALUES((SELECT * FROM C))");
    assert_eq!(rows, vec![vec![SqlValue::Integer(1)]]);
}

/// The CTE shadows a same-named catalog view, like in #5350 (sqlite3: 100),
/// while a bare VALUES without the CTE still resolves the view (sqlite3: 3).
#[test]
fn test_with_values_cte_shadows_catalog_view() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
         VALUES((SELECT max(id) FROM lll WHERE grp_id = 2))",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(100)]]);

    // Control: no CTE, the subquery resolves to the catalog view.
    let rows = query(&db, "VALUES((SELECT max(id) FROM lll WHERE grp_id = 2))");
    assert_eq!(rows, vec![vec![SqlValue::Integer(3)]]);
}

/// Multiple chained CTEs before VALUES (sqlite3: 1).
#[test]
fn test_with_multiple_ctes_then_values() {
    let db = vibesql_storage::Database::new();
    let rows = query(&db, "WITH a AS (SELECT 1), b AS (SELECT * FROM a) VALUES((SELECT * FROM b))");
    assert_eq!(rows, vec![vec![SqlValue::Integer(1)]]);
}

/// WITH RECURSIVE before VALUES (sqlite3: 3).
#[test]
fn test_with_recursive_then_values() {
    let db = vibesql_storage::Database::new();
    let rows = query(
        &db,
        "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<3) \
         VALUES((SELECT max(x) FROM cnt))",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(3)]]);
}

/// EXISTS inside a VALUES row sees the CTE (sqlite3: 1).
#[test]
fn test_with_values_exists_subquery() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 100 AS id) \
         VALUES((SELECT EXISTS(SELECT 1 FROM lll WHERE id = 100)))",
    );
    // VibeSQL represents EXISTS results as Boolean (rendered as 1 by the CLI)
    assert_eq!(rows, vec![vec![SqlValue::Boolean(true)]]);
}

/// IN (SELECT ...) inside a VALUES row sees the CTE (sqlite3: 1).
#[test]
fn test_with_values_in_subquery() {
    let db = setup_db();
    let rows =
        query(&db, "WITH lll AS (SELECT 100 AS id) VALUES((SELECT 100 IN (SELECT id FROM lll)))");
    // VibeSQL represents IN results as Boolean (rendered as 1 by the CLI)
    assert_eq!(rows, vec![vec![SqlValue::Boolean(true)]]);
}

/// VALUES in FROM position with a CTE-referencing subquery (sqlite3: 11,
/// verified with `AS v`; the `v(x)` column alias is a VibeSQL extension).
#[test]
fn test_from_values_subquery_sees_cte() {
    let db = vibesql_storage::Database::new();
    let rows =
        query(&db, "WITH c AS (SELECT 11) SELECT * FROM (VALUES((SELECT * FROM c))) AS v(x)");
    assert_eq!(rows, vec![vec![SqlValue::Integer(11)]]);
}

// ---------------------------------------------------------------------------
// Regression tests for issue #6190 (values.test VALUES-clause semantics).
// Expected values verified against SQLite's values.test.
// ---------------------------------------------------------------------------

/// A `WITH v AS (VALUES ...)` CTE with no explicit column list auto-names its
/// columns `column1`, `column2`, ... (values.test 8.1.*). Previously the CTE
/// materialized zero named columns, so `column1` did not resolve.
#[test]
fn test_with_values_cte_auto_column_names() {
    let db = vibesql_storage::Database::new();
    let rows = query(&db, "WITH v AS (VALUES('a','b'),('c','d')) SELECT column1 FROM v");
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Varchar(arcstr::ArcStr::from("a"))],
            vec![SqlValue::Varchar(arcstr::ArcStr::from("c"))],
        ]
    );
}

/// `SELECT *` over such a CTE expands to the auto-named `column1`, `column2`.
#[test]
fn test_with_values_cte_star_expansion() {
    let db = vibesql_storage::Database::new();
    let rows = query(&db, "WITH v AS (VALUES('a','b'),('c','d')) SELECT * FROM v");
    assert_eq!(
        rows,
        vec![
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("a")),
                SqlValue::Varchar(arcstr::ArcStr::from("b"))
            ],
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("c")),
                SqlValue::Varchar(arcstr::ArcStr::from("d"))
            ],
        ]
    );
}

/// A VALUES clause as the left arm of a compound with a trailing `ORDER BY <n>`
/// must resolve the ordinal against the VALUES column count (values.test 9.1).
/// Previously the VALUES branch reported zero columns, so `ORDER BY 1` was
/// rejected as "1st ORDER BY term out of range".
#[test]
fn test_values_left_arm_compound_order_by() {
    let db = vibesql_storage::Database::new();
    let rows = query(&db, "VALUES(456),(123),(NULL) UNION ALL SELECT 122 ORDER BY 1");
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Null],
            vec![SqlValue::Integer(122)],
            vec![SqlValue::Integer(123)],
            vec![SqlValue::Integer(456)],
        ]
    );
}

/// `CREATE TABLE ... AS SELECT * FROM (VALUES ...)` names the derived columns
/// `column1`, `column2`, ... rather than erroring (values.test 17.1 / 17.2).
#[test]
fn test_ctas_from_values_star() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1 AS SELECT * FROM (VALUES(1,2),(3,4 IN (1,2,3)))");
    let rows = query(&db, "SELECT column1, column2 FROM t1");
    // `4 IN (1,2,3)` is a boolean predicate, stored internally as Boolean(false)
    // (rendered as 0 by the CLI, matching SQLite's values.test 17.2 output).
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(2)],
            vec![SqlValue::Integer(3), SqlValue::Boolean(false)],
        ]
    );
}
