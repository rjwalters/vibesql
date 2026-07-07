//! Regression tests for issue #5870
//!
//! When an unqualified column name in a join predicate exists in more than one
//! visible table, SQLite rejects the query with `ambiguous column name: <col>`.
//! VibeSQL used to silently resolve it to the leftmost table's column and return
//! results.
//!
//! Root cause: two equijoin fast paths resolved unqualified join-key columns with
//! `CombinedSchema::get_column_index(None, col)`, which falls back to leftmost-name
//! matching, bypassing the `is_column_ambiguous` check that lives on the full
//! expression-evaluator path:
//!   1. `select::join::join_analyzer::extract_column_index` (nested-loop + hash join)
//!   2. `columnar_execution::join_helpers::resolve_join_column_indices` (columnar path)
//!
//! Both are fixed to detect ambiguity for unqualified refs. USING/NATURAL join key
//! columns are exempt (issue #4517) and qualified refs are unaffected.
//!
//! These tests run through the default `SelectExecutor`, which exercises the
//! columnar path first (it falls back to the row-oriented path on the ambiguity
//! error), so they pin the fixed behavior regardless of which path is chosen.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{ExecutorError, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

fn run(db: &Database, sql: &str) -> Vec<Row> {
    let select = parse_select(sql);
    SelectExecutor::new(db).execute(&select).unwrap()
}

/// Execute expecting an `AmbiguousColumnName` error and assert the offending
/// column name matches. Also confirms the SQLite-compatible Display message.
fn assert_ambiguous(db: &Database, sql: &str, expected_col: &str) {
    let select = parse_select(sql);
    match SelectExecutor::new(db).execute(&select) {
        Err(ExecutorError::AmbiguousColumnName { column_name }) => {
            assert_eq!(
                column_name.to_lowercase(),
                expected_col.to_lowercase(),
                "wrong ambiguous column reported for: {sql}"
            );
            let msg = ExecutorError::AmbiguousColumnName { column_name }.to_string();
            assert_eq!(msg, format!("ambiguous column name: {expected_col}"));
        }
        Err(other) => panic!("expected AmbiguousColumnName for `{sql}`, got: {other:?}"),
        Ok(rows) => {
            panic!("expected AmbiguousColumnName for `{sql}`, got {} row(s) instead", rows.len())
        }
    }
}

/// Schema/data from the issue #5870 reproduction. Every table has an `id`
/// column, so unqualified `id` in a join predicate is ambiguous.
///
/// ```sql
/// CREATE TABLE a1(id INTEGER PRIMARY KEY, v);
/// CREATE TABLE b1(id INTEGER PRIMARY KEY, aid, v);
/// CREATE TABLE c1(id INTEGER PRIMARY KEY, bid, v);
/// INSERT INTO a1 VALUES(1,'a1'),(2,'a2'),(3,'a3');
/// INSERT INTO b1 VALUES(10,1,'b1'),(11,2,'b2');
/// INSERT INTO c1 VALUES(100,10,'c1');
/// ```
fn setup_db() -> Database {
    let mut db = Database::new();

    let a1 = TableSchema::with_primary_key(
        "A1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(a1).unwrap();

    let b1 = TableSchema::with_primary_key(
        "B1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("aid".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(b1).unwrap();

    let c1 = TableSchema::with_primary_key(
        "C1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("bid".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(c1).unwrap();

    for (id, v) in [(1, "a1"), (2, "a2"), (3, "a3")] {
        db.insert_row("A1", Row::new(vec![SqlValue::Integer(id), SqlValue::Varchar(v.into())]))
            .unwrap();
    }
    for (id, aid, v) in [(10, 1, "b1"), (11, 2, "b2")] {
        db.insert_row(
            "B1",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Integer(aid),
                SqlValue::Varchar(v.into()),
            ]),
        )
        .unwrap();
    }
    db.insert_row(
        "C1",
        Row::new(vec![
            SqlValue::Integer(100),
            SqlValue::Integer(10),
            SqlValue::Varchar("c1".into()),
        ]),
    )
    .unwrap();

    db
}

/// Primary reproducer: unqualified `id` in a 2-table ON clause is ambiguous.
/// (Was: silently resolved to a1.id and returned 2 rows.)
#[test]
fn test_ambiguous_unqualified_id_in_on_two_tables() {
    let db = setup_db();
    assert_ambiguous(&db, "SELECT a1.id FROM a1 JOIN b1 ON id=aid", "id");
}

/// Second reproducer: unqualified `id` in the second ON of a 3-table chain.
/// (Was: silently resolved to a1.id and returned 0 rows.)
#[test]
fn test_ambiguous_unqualified_id_in_on_three_tables() {
    let db = setup_db();
    assert_ambiguous(
        &db,
        "SELECT a1.id, b1.id, c1.id FROM a1 JOIN b1 ON b1.aid=a1.id JOIN c1 ON bid=id",
        "id",
    );
}

/// Predicate written in flipped order (`aid=id`) must be caught too.
#[test]
fn test_ambiguous_unqualified_id_in_on_flipped() {
    let db = setup_db();
    assert_ambiguous(&db, "SELECT a1.id FROM a1 JOIN b1 ON aid=id", "id");
}

/// Comma-join with the ambiguous equijoin key in the WHERE clause: the columnar
/// path extracts it as an equijoin key, hits the guard, and falls back to the
/// row path which raises the same error.
#[test]
fn test_ambiguous_unqualified_id_in_comma_join_where() {
    let db = setup_db();
    assert_ambiguous(&db, "SELECT a1.id FROM a1, b1 WHERE id=aid", "id");
}

/// Qualified refs in the ON clause are never ambiguous — must keep working.
#[test]
fn test_qualified_refs_in_on_not_ambiguous() {
    let db = setup_db();
    let rows = run(&db, "SELECT a1.id FROM a1 JOIN b1 ON b1.aid=a1.id ORDER BY 1");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
}

/// An unqualified column that exists in only ONE table stays unambiguous and
/// resolves correctly (`aid` lives only on b1).
#[test]
fn test_unqualified_single_table_column_not_ambiguous() {
    let db = setup_db();
    let rows = run(&db, "SELECT a1.id FROM a1 JOIN b1 ON aid=a1.id ORDER BY 1");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
}

/// Unambiguous 3-table chain (all predicates qualified/single-table) still works.
#[test]
fn test_unambiguous_three_table_chain() {
    let db = setup_db();
    let rows = run(
        &db,
        "SELECT a1.id, b1.id, c1.id FROM a1 JOIN b1 ON b1.aid=a1.id JOIN c1 ON c1.bid=b1.id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], SqlValue::Integer(10));
    assert_eq!(rows[0].values[2], SqlValue::Integer(100));
}

/// USING-join key columns are NOT ambiguous (issue #4517): the shared `x`
/// resolves to the coalesced join column, not an ambiguity error.
#[test]
fn test_using_join_key_not_ambiguous() {
    let mut db = Database::new();
    for t in ["A", "B"] {
        let schema = TableSchema::new(
            t.to_string(),
            vec![
                ColumnSchema::new("x".to_string(), DataType::Integer, true),
                ColumnSchema::new(
                    if t == "A" { "y" } else { "z" }.to_string(),
                    DataType::Integer,
                    true,
                ),
            ],
        );
        db.create_table(schema).unwrap();
    }
    db.insert_row("A", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    db.insert_row("B", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)])).unwrap();

    // Reference the USING key unqualified in the SELECT list: still not ambiguous.
    let rows = run(&db, "SELECT x FROM a JOIN b USING (x)");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));

    let rows = run(&db, "SELECT a.y FROM a JOIN b USING (x)");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(10));
}

/// NATURAL-join key columns are NOT ambiguous (issue #4517).
#[test]
fn test_natural_join_key_not_ambiguous() {
    let mut db = Database::new();
    for t in ["A", "B"] {
        let schema = TableSchema::new(
            t.to_string(),
            vec![
                ColumnSchema::new("x".to_string(), DataType::Integer, true),
                ColumnSchema::new(
                    if t == "A" { "y" } else { "z" }.to_string(),
                    DataType::Integer,
                    true,
                ),
            ],
        );
        db.create_table(schema).unwrap();
    }
    db.insert_row("A", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    db.insert_row("B", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)])).unwrap();

    let rows = run(&db, "SELECT a.y FROM a NATURAL JOIN b");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(10));
}

/// Self-join with aliases: an unqualified key present on both aliased instances
/// is ambiguous.
#[test]
fn test_self_join_unqualified_key_ambiguous() {
    let mut db = Database::new();
    let t = TableSchema::with_primary_key(
        "T".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("parent".to_string(), DataType::Integer, true),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(t).unwrap();
    db.insert_row(
        "T",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Varchar("root".into())]),
    )
    .unwrap();
    db.insert_row(
        "T",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Varchar("child".into()),
        ]),
    )
    .unwrap();

    assert_ambiguous(&db, "SELECT t1.name FROM t t1 JOIN t t2 ON id=parent", "id");

    // Qualified self-join key is fine.
    let rows = run(&db, "SELECT t1.name FROM t t1 JOIN t t2 ON t2.parent=t1.id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Varchar("root".into()));
}

/// The fix is not specific to rowid-alias `id` columns: a duplicate non-PK
/// column name used unqualified in an ON clause is ambiguous too.
#[test]
fn test_duplicate_non_pk_column_unqualified_ambiguous() {
    let mut db = Database::new();
    for (table, cols) in [("TA", vec!["w", "v"]), ("TB", vec!["w", "aw", "v"])] {
        let schema = TableSchema::new(
            table.to_string(),
            cols.iter()
                .map(|c| {
                    if *c == "v" {
                        ColumnSchema::new(
                            c.to_string(),
                            DataType::Varchar { max_length: None },
                            true,
                        )
                    } else {
                        ColumnSchema::new(c.to_string(), DataType::Integer, true)
                    }
                })
                .collect(),
        );
        db.create_table(schema).unwrap();
    }
    db.insert_row("TA", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("a".into())]))
        .unwrap();
    db.insert_row(
        "TB",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Varchar("b".into())]),
    )
    .unwrap();

    assert_ambiguous(&db, "SELECT ta.v FROM ta JOIN tb ON w=aw", "w");
}

// ---------------------------------------------------------------------------
// Issue #5926 regression: the ambiguity guard must respect the reference's
// ORIGINAL lexical scope.
//
// `SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders)` is
// UNAMBIGUOUS in SQLite: `orders` is only visible inside the subquery, so the
// outer `id` binds to `customers.id`. But the uncorrelated IN-subquery is
// planned as an internal SEMI join whose combined schema contains BOTH
// customers and orders (both have `id`). The #5870 guard used to fire on the
// outer `id` reference that got folded into the join predicate, wrongly raising
// "ambiguous column name: id". The fix qualifies the outer expression against
// its outer-only scope before flattening folds it into the predicate.
// ---------------------------------------------------------------------------

/// customers(id) and orders(id, customer_id, status) — both carry an `id`
/// column, so an unqualified `id` folded into a semi-join predicate spanning
/// both tables would trip the #5870 guard unless outer-scope resolution wins.
fn setup_semi_join_db() -> Database {
    let mut db = Database::new();

    let customers = TableSchema::with_primary_key(
        "customers".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        vec!["id".to_string()],
    );
    db.create_table(customers).unwrap();

    let orders = TableSchema::with_primary_key(
        "orders".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("customer_id".to_string(), DataType::Integer, true),
            ColumnSchema::new("status".to_string(), DataType::Integer, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(orders).unwrap();

    for id in [1, 2, 3] {
        db.insert_row("customers", Row::new(vec![SqlValue::Integer(id)])).unwrap();
    }
    // orders: customers 1 and 2 have orders; customer 3 has none. status=1 only on order 2.
    for (id, cust, status) in [(1, 1, 0), (2, 2, 1)] {
        db.insert_row(
            "orders",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Integer(cust),
                SqlValue::Integer(status),
            ]),
        )
        .unwrap();
    }

    db
}

fn first_col_ints(db: &Database, sql: &str) -> Vec<i64> {
    let mut out: Vec<i64> = run(db, sql)
        .iter()
        .map(|row| match &row.values[0] {
            SqlValue::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

/// Uncorrelated IN-subquery over a table that shares the `id` column name with
/// the outer table must NOT be treated as ambiguous — the outer `id` resolves to
/// `customers.id` in its own scope. SQLite returns {1, 2}.
#[test]
fn test_in_subquery_semi_join_outer_ref_not_ambiguous() {
    let db = setup_semi_join_db();
    assert_eq!(
        first_col_ints(
            &db,
            "SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders)"
        ),
        vec![1, 2],
        "outer `id` is unambiguous (orders only visible inside the subquery)"
    );

    // The implying filter narrows to customer 2; still unambiguous.
    assert_eq!(
        first_col_ints(
            &db,
            "SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE status = 1)"
        ),
        vec![2],
    );
}

/// NOT IN shares the same rewrite path (ANTI join). Customer 3 has no order, so
/// SQLite returns {3}. Must not raise an ambiguity error on the outer `id`.
#[test]
fn test_not_in_subquery_anti_join_outer_ref_not_ambiguous() {
    let db = setup_semi_join_db();
    assert_eq!(
        first_col_ints(
            &db,
            "SELECT id FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)"
        ),
        vec![3],
        "outer `id` is unambiguous; NOT IN yields the customer with no orders"
    );
}

/// The same shapes must hold on the row-oriented join path (columnar join
/// disabled). `VIBESQL_DISABLE_COLUMNAR_JOIN` is process-global; both paths
/// produce identical correct results, and the concurrent ambiguity tests raise
/// the same error on either path, so a transient flip cannot break them.
#[test]
fn test_in_and_not_in_subquery_outer_ref_row_path() {
    std::env::set_var("VIBESQL_DISABLE_COLUMNAR_JOIN", "1");
    let db = setup_semi_join_db();
    let in_ids = first_col_ints(
        &db,
        "SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders)",
    );
    let not_in_ids = first_col_ints(
        &db,
        "SELECT id FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)",
    );
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR_JOIN");

    assert_eq!(in_ids, vec![1, 2], "row path: IN-subquery outer `id` unambiguous");
    assert_eq!(not_in_ids, vec![3], "row path: NOT IN-subquery outer `id` unambiguous");
}

/// Guard against over-correction: genuine same-scope ambiguity in an explicit
/// two-table join must still error (the outer-scope exemption is specific to
/// subquery-flattened tables, not real joined tables).
#[test]
fn test_explicit_join_still_ambiguous_after_semi_join_fix() {
    let db = setup_semi_join_db();
    assert_ambiguous(
        &db,
        "SELECT customers.id FROM customers JOIN orders ON id = customer_id",
        "id",
    );
}

// ---------------------------------------------------------------------------
// Issue #5926 regression (second cycle): MULTI-TABLE outer FROM.
//
// The first fix only qualified the outer expression when the outer FROM was a
// single table. For a multi-table outer FROM the outer column stayed unqualified,
// so the subquery table pulled into the flattened SEMI/ANTI join still tripped the
// #5870 guard. SQLite scopes ambiguity to the OUTER FROM ONLY, never the subquery
// tables, so:
//   - a column unique among the outer tables (but shared with a subquery table)
//     must resolve and return rows;
//   - a column genuinely ambiguous AMONG the outer tables must still error.
//
// The fix resolves which outer table owns the unqualified column (via the catalog)
// and qualifies against exactly that table, regardless of outer table count.
// ---------------------------------------------------------------------------

/// Judge's reproducer: `a` exists on `t1` and `t3` only. In the outer FROM
/// `t1, t2` only `t1` has `a`, so it is UNAMBIGUOUS; `t3` lives inside the
/// subquery. sqlite3 3.51.0 returns `10,10,20,20`.
fn setup_multi_table_outer_db() -> Database {
    let mut db = Database::new();

    let t1 = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t1).unwrap();

    let t2 = TableSchema::new(
        "t2".to_string(),
        vec![
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
            ColumnSchema::new("w".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t2).unwrap();

    let t3 = TableSchema::new(
        "t3".to_string(),
        vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
    );
    db.create_table(t3).unwrap();

    for (a, v) in [(1, 10), (2, 20), (3, 30)] {
        db.insert_row("t1", Row::new(vec![SqlValue::Integer(a), SqlValue::Integer(v)])).unwrap();
    }
    for (b, w) in [(1, 100), (2, 200)] {
        db.insert_row("t2", Row::new(vec![SqlValue::Integer(b), SqlValue::Integer(w)])).unwrap();
    }
    for a in [1, 2] {
        db.insert_row("t3", Row::new(vec![SqlValue::Integer(a)])).unwrap();
    }

    db
}

/// Same as `setup_multi_table_outer_db` but `a` exists on BOTH `t1` and `t2`, so
/// `a` is GENUINELY ambiguous in the outer FROM `t1, t2` and must error.
fn setup_multi_table_outer_ambiguous_db() -> Database {
    let mut db = Database::new();

    let t1 = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t1).unwrap();

    let t2 = TableSchema::new(
        "t2".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("w".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t2).unwrap();

    let t3 = TableSchema::new(
        "t3".to_string(),
        vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
    );
    db.create_table(t3).unwrap();

    for (a, v) in [(1, 10), (2, 20), (3, 30)] {
        db.insert_row("t1", Row::new(vec![SqlValue::Integer(a), SqlValue::Integer(v)])).unwrap();
    }
    for (a, w) in [(1, 100), (2, 200)] {
        db.insert_row("t2", Row::new(vec![SqlValue::Integer(a), SqlValue::Integer(w)])).unwrap();
    }
    for a in [1, 2] {
        db.insert_row("t3", Row::new(vec![SqlValue::Integer(a)])).unwrap();
    }

    db
}

/// Multi-table outer FROM, column unique among the outer tables: must return
/// rows and match sqlite3 `{10,10,20,20}`, NOT raise an ambiguity error.
#[test]
fn test_multi_table_outer_in_subquery_unique_column_not_ambiguous() {
    let db = setup_multi_table_outer_db();
    assert_eq!(
        first_col_ints(&db, "SELECT t1.v FROM t1, t2 WHERE a IN (SELECT a FROM t3)"),
        vec![10, 10, 20, 20],
        "outer `a` is unique among {{t1, t2}} (t3 is inside the subquery)"
    );
}

/// NOT IN shares the ANTI-join path; still unambiguous. `a IN t3` matches t1
/// rows 1 and 2 (each × 2 t2 rows). NOT IN keeps t1 row 3 (× 2). sqlite3: `30,30`.
#[test]
fn test_multi_table_outer_not_in_subquery_unique_column_not_ambiguous() {
    let db = setup_multi_table_outer_db();
    assert_eq!(
        first_col_ints(&db, "SELECT t1.v FROM t1, t2 WHERE a NOT IN (SELECT a FROM t3)"),
        vec![30, 30],
        "outer `a` unique among outer tables; NOT IN keeps t1.a=3"
    );
}

/// Same multi-table shape on the row-oriented path (columnar join disabled).
#[test]
fn test_multi_table_outer_in_subquery_row_path() {
    std::env::set_var("VIBESQL_DISABLE_COLUMNAR_JOIN", "1");
    let db = setup_multi_table_outer_db();
    let in_vs = first_col_ints(&db, "SELECT t1.v FROM t1, t2 WHERE a IN (SELECT a FROM t3)");
    let not_in_vs =
        first_col_ints(&db, "SELECT t1.v FROM t1, t2 WHERE a NOT IN (SELECT a FROM t3)");
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR_JOIN");

    assert_eq!(in_vs, vec![10, 10, 20, 20], "row path: multi-table outer `a` unambiguous (IN)");
    assert_eq!(not_in_vs, vec![30, 30], "row path: multi-table outer `a` unambiguous (NOT IN)");
}

/// Genuine ambiguity AMONG the outer tables must still error: `a` on both `t1`
/// and `t2`. sqlite3 3.51.0: `Error: ambiguous column name: a`.
#[test]
fn test_multi_table_outer_genuinely_ambiguous_column_errors() {
    let db = setup_multi_table_outer_ambiguous_db();
    assert_ambiguous(&db, "SELECT t1.v FROM t1, t2 WHERE a IN (SELECT a FROM t3)", "a");
}

/// The genuine-ambiguity error must also fire on the row-oriented path.
#[test]
fn test_multi_table_outer_genuinely_ambiguous_column_errors_row_path() {
    std::env::set_var("VIBESQL_DISABLE_COLUMNAR_JOIN", "1");
    let db = setup_multi_table_outer_ambiguous_db();
    let select = parse_select("SELECT t1.v FROM t1, t2 WHERE a IN (SELECT a FROM t3)");
    let result = SelectExecutor::new(&db).execute(&select);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR_JOIN");
    match result {
        Err(ExecutorError::AmbiguousColumnName { column_name }) => {
            assert_eq!(column_name.to_lowercase(), "a");
        }
        other => panic!("expected AmbiguousColumnName on row path, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Issue #5926 regression (third cycle): CTE / VIEW outer FROM.
//
// The catalog-aware resolver resolved outer tables through `database.get_table()`
// only, which returns base tables. When the outer FROM is a CTE or a VIEW,
// `get_table()` misses, the resolver counts zero outer matches, leaves the outer
// column unqualified, and the #5870 guard over-errors on a query SQLite accepts.
//
// The fix extends outer-source resolution to consult views (`catalog.get_view` /
// derived from the view SELECT list) and enclosing CTEs (the query's
// `with_clause`, resolved via the CTE column list or its body's projection). The
// "exactly one outer source → qualify, two-or-more → leave for the guard" split
// is preserved for every source kind, matching SQLite's outer-scope-only
// semantics. Verified against sqlite3 3.51.0 on both the columnar (default) and
// row (`VIBESQL_DISABLE_COLUMNAR_JOIN=1`) paths.
// ---------------------------------------------------------------------------

/// Run a DDL/DML setup statement (CREATE TABLE / CREATE VIEW / INSERT) end to end.
fn exec_setup(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).unwrap() {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::CreateView(view) => {
            vibesql_executor::advanced_objects::execute_create_view(&view, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
        }
        other => panic!("unsupported setup statement: {other:?}"),
    }
}

/// VIEW `t1(a, v)` over base table `base`, plus subquery table `t3(a)` sharing
/// the `a` column name with the view. In the outer FROM only the view exposes
/// `a`, so it is UNAMBIGUOUS; `t3` lives inside the subquery. sqlite3: `10,20`.
fn setup_view_outer_db() -> Database {
    let mut db = Database::new();
    exec_setup(&mut db, "CREATE TABLE base(a INTEGER, v INTEGER)");
    exec_setup(&mut db, "INSERT INTO base VALUES(1,10),(2,20),(3,30)");
    exec_setup(&mut db, "CREATE TABLE t3(a INTEGER)");
    exec_setup(&mut db, "INSERT INTO t3 VALUES(1),(2)");
    exec_setup(&mut db, "CREATE VIEW t1 AS SELECT a, v FROM base");
    db
}

/// Base tables for the CTE cases. The CTE `t1` is declared inline in each query
/// via its WITH clause (parsed into the SELECT), sharing `a` with subquery `t3`.
fn setup_cte_outer_db() -> Database {
    let mut db = Database::new();
    exec_setup(&mut db, "CREATE TABLE t3(a INTEGER)");
    exec_setup(&mut db, "INSERT INTO t3 VALUES(1),(2)");
    db
}

/// CTE in the outer FROM sharing a column with the subquery table → returns rows.
/// `WITH t1(a,v) AS (VALUES...)`; only the CTE exposes `a` in the outer scope, so
/// it is unambiguous. sqlite3 3.51.0: `10,20`.
#[test]
fn test_cte_outer_in_subquery_shared_column_not_ambiguous() {
    let db = setup_cte_outer_db();
    assert_eq!(
        first_col_ints(
            &db,
            "WITH t1(a,v) AS (VALUES(1,10),(2,20),(3,30)) \
             SELECT t1.v FROM t1 WHERE a IN (SELECT a FROM t3)"
        ),
        vec![10, 20],
        "outer `a` resolves to the enclosing CTE t1 (t3 is inside the subquery)"
    );
}

/// VIEW in the outer FROM sharing a column with the subquery table → returns rows.
/// sqlite3 3.51.0: `10,20`.
#[test]
fn test_view_outer_in_subquery_shared_column_not_ambiguous() {
    let db = setup_view_outer_db();
    assert_eq!(
        first_col_ints(&db, "SELECT t1.v FROM t1 WHERE a IN (SELECT a FROM t3)"),
        vec![10, 20],
        "outer `a` resolves to the VIEW t1 (t3 is inside the subquery)"
    );
}

/// Both CTE and VIEW shapes must hold on the row-oriented path too.
#[test]
fn test_cte_and_view_outer_in_subquery_row_path() {
    std::env::set_var("VIBESQL_DISABLE_COLUMNAR_JOIN", "1");
    let cte_db = setup_cte_outer_db();
    let cte_vs = first_col_ints(
        &cte_db,
        "WITH t1(a,v) AS (VALUES(1,10),(2,20),(3,30)) \
         SELECT t1.v FROM t1 WHERE a IN (SELECT a FROM t3)",
    );
    let view_db = setup_view_outer_db();
    let view_vs = first_col_ints(&view_db, "SELECT t1.v FROM t1 WHERE a IN (SELECT a FROM t3)");
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR_JOIN");

    assert_eq!(cte_vs, vec![10, 20], "row path: CTE outer `a` unambiguous");
    assert_eq!(view_vs, vec![10, 20], "row path: VIEW outer `a` unambiguous");
}

/// Genuine ambiguity with a CTE column duplicated on a second outer table must
/// still error: `a` on both the CTE `t1` and base table `t2`. sqlite3 3.51.0:
/// `ambiguous column name: a`.
#[test]
fn test_cte_outer_genuinely_ambiguous_column_errors() {
    let mut db = setup_cte_outer_db();
    exec_setup(&mut db, "CREATE TABLE t2(a INTEGER, w INTEGER)");
    exec_setup(&mut db, "INSERT INTO t2 VALUES(1,100),(2,200)");
    assert_ambiguous(
        &db,
        "WITH t1(a,v) AS (VALUES(1,10),(2,20),(3,30)) \
         SELECT t1.v FROM t1, t2 WHERE a IN (SELECT a FROM t3)",
        "a",
    );
}

/// Genuine ambiguity with a VIEW column duplicated on a second outer table must
/// still error: `a` on both the VIEW `t1` and base table `t2`. sqlite3 3.51.0:
/// `ambiguous column name: a`.
#[test]
fn test_view_outer_genuinely_ambiguous_column_errors() {
    let mut db = setup_view_outer_db();
    exec_setup(&mut db, "CREATE TABLE t2(a INTEGER, w INTEGER)");
    exec_setup(&mut db, "INSERT INTO t2 VALUES(1,100),(2,200)");
    assert_ambiguous(&db, "SELECT t1.v FROM t1, t2 WHERE a IN (SELECT a FROM t3)", "a");
}
