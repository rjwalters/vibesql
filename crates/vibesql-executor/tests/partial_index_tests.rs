//! Tests for partial indexes (`CREATE INDEX ... WHERE predicate`).
//!
//! These tests verify the storage-level invariant that the index body only
//! contains rows whose WHERE predicate is truthy. See issue #5214.

use std::collections::BTreeMap;

use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::{database::indexes::IndexData, Database};
use vibesql_types::SqlValue;

/// Helper to execute one or more SQL statements separated by ';'.
fn execute_sql(db: &mut Database, sql: &str) {
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        execute_statement(&stmt, db);
    }
}

fn execute_statement(stmt: &vibesql_ast::Statement, db: &mut Database) {
    use vibesql_ast::Statement;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(s, db).expect("CREATE INDEX failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, s).expect("INSERT failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(s, db).expect("UPDATE failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(s, db).expect("DELETE failed");
        }
        _ => panic!("Unsupported statement type"),
    }
}

/// Collect the contents of an index body as a sorted (key, row_indices) map.
///
/// This bypasses the planner (which conservatively skips partial indexes)
/// and inspects the storage layer's index body directly so the test can
/// observe which rows are actually in the index.
fn index_body(db: &Database, index_name: &str) -> BTreeMap<Vec<SqlValue>, Vec<usize>> {
    let data = db.get_index_data(index_name).expect("index not found");
    match data {
        IndexData::InMemory { data, .. } => {
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        }
        _ => panic!("expected InMemory index body for this test"),
    }
}

/// Returns the row indices currently stored in the partial index, sorted.
fn index_row_indices(db: &Database, index_name: &str) -> Vec<usize> {
    let mut all: Vec<usize> = index_body(db, index_name).into_values().flatten().collect();
    all.sort_unstable();
    all
}

#[test]
fn create_partial_index_excludes_non_matching_rows_at_build_time() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        INSERT INTO orders VALUES (1, 0);
        INSERT INTO orders VALUES (2, 1);
        INSERT INTO orders VALUES (3, 1);
        INSERT INTO orders VALUES (4, 0);
        CREATE INDEX idx_open ON orders(id) WHERE status = 1;
        "#,
    );

    // Only rows with status = 1 (row indices 1 and 2) should be in the index.
    let entries = index_row_indices(&db, "idx_open");
    assert_eq!(entries, vec![1, 2], "partial index body should exclude non-matching rows");
}

#[test]
fn insert_into_partial_index_evaluates_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        CREATE INDEX idx_open ON orders(id) WHERE status = 1;
        "#,
    );

    // Empty so far.
    assert!(index_body(&db, "idx_open").is_empty());

    execute_sql(
        &mut db,
        r#"
        INSERT INTO orders VALUES (1, 0);
        INSERT INTO orders VALUES (2, 1);
        INSERT INTO orders VALUES (3, 0);
        INSERT INTO orders VALUES (4, 1);
        "#,
    );

    let entries = index_row_indices(&db, "idx_open");
    assert_eq!(entries, vec![1, 3], "only status=1 rows should be in the partial index");
}

#[test]
fn update_partial_index_handles_predicate_transition() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        INSERT INTO orders VALUES (1, 0);
        INSERT INTO orders VALUES (2, 1);
        INSERT INTO orders VALUES (3, 0);
        CREATE INDEX idx_open ON orders(id) WHERE status = 1;
        "#,
    );

    // Initially only row #1 (id=2) is in the index.
    assert_eq!(index_row_indices(&db, "idx_open"), vec![1]);

    // Out -> In: change status to 1 for id=1.
    execute_sql(&mut db, "UPDATE orders SET status = 1 WHERE id = 1");
    assert_eq!(index_row_indices(&db, "idx_open"), vec![0, 1]);

    // In -> Out: change status back to 0 for id=2.
    execute_sql(&mut db, "UPDATE orders SET status = 0 WHERE id = 2");
    assert_eq!(index_row_indices(&db, "idx_open"), vec![0]);

    // In -> In, key change: change id of an included row.
    execute_sql(&mut db, "UPDATE orders SET id = 99 WHERE id = 1");
    let body = index_body(&db, "idx_open");
    assert_eq!(body.len(), 1);
    let only_key = body.keys().next().unwrap();
    // The retained key should be 99, not 1 — the storage normalizes
    // integers to `Double` for consistent comparison, so accept either form.
    let key_is_99 = match only_key.first() {
        Some(SqlValue::Integer(n)) => *n == 99,
        Some(SqlValue::Bigint(n)) => *n == 99,
        Some(SqlValue::Smallint(n)) => *n == 99,
        Some(SqlValue::Double(n)) => (*n - 99.0).abs() < 1e-9,
        Some(SqlValue::Real(n)) => (*n - 99.0).abs() < 1e-9,
        Some(SqlValue::Float(n)) => (*n - 99.0).abs() < 1e-9,
        _ => false,
    };
    assert!(
        key_is_99,
        "after key change, partial-index entry should be re-keyed to 99 (saw {:?})",
        only_key
    );
}

#[test]
fn delete_removes_partial_index_entry_when_predicate_was_truthy() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        INSERT INTO orders VALUES (1, 1);
        INSERT INTO orders VALUES (2, 0);
        INSERT INTO orders VALUES (3, 1);
        CREATE INDEX idx_open ON orders(id) WHERE status = 1;
        "#,
    );

    assert_eq!(index_row_indices(&db, "idx_open"), vec![0, 2]);

    // Delete a row that was IN the index.
    execute_sql(&mut db, "DELETE FROM orders WHERE id = 1");
    assert_eq!(
        index_row_indices(&db, "idx_open"),
        vec![2],
        "deleting an included row should remove its index entry"
    );

    // Delete a row that was NOT in the index. With 2 of 3 original rows now
    // deleted (>50%), `delete_by_indices_batch` triggers compaction and all
    // surviving row indices renumber. The partial-index body must be
    // rebuilt so it still points at the right surviving row (id=3, now at
    // post-compaction row index 0) and must NOT retain the pre-compaction
    // pointer (row index 2).
    execute_sql(&mut db, "DELETE FROM orders WHERE id = 2");
    let body = index_body(&db, "idx_open");
    assert_eq!(body.len(), 1, "partial-index body must have exactly one entry after delete");
    let surviving_row_idx = *body.values().next().unwrap().first().unwrap();
    let surviving_row = &db.get_table("orders").unwrap().scan()[surviving_row_idx];
    // The surviving row must be the one whose predicate is truthy (id=3).
    let id_val = &surviving_row.values[0];
    let id_matches =
        matches!(id_val, SqlValue::Integer(3) | SqlValue::Bigint(3) | SqlValue::Smallint(3));
    assert!(
        id_matches,
        "partial-index body must reference the surviving predicate-truthy row (id=3); got {:?}",
        id_val
    );
}

#[test]
fn partial_unique_index_allows_duplicates_outside_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER, sku INTEGER);
        CREATE UNIQUE INDEX idx_open_sku ON orders(sku) WHERE status = 1;
        INSERT INTO orders VALUES (1, 0, 42);
        INSERT INTO orders VALUES (2, 0, 42);
        "#,
    );

    // Two status=0 rows with sku=42 are allowed because neither satisfies
    // the WHERE predicate; the partial UNIQUE index does not enforce
    // uniqueness over them.
    let table = db.get_table("orders").expect("table missing");
    assert_eq!(table.row_count(), 2);
    // Neither row should be in the index body.
    assert!(
        index_body(&db, "idx_open_sku").is_empty(),
        "partial index must be empty when no row matches the predicate"
    );
}

#[test]
fn partial_unique_index_rejects_duplicates_inside_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER, sku INTEGER);
        CREATE UNIQUE INDEX idx_open_sku ON orders(sku) WHERE status = 1;
        INSERT INTO orders VALUES (1, 1, 42);
        "#,
    );

    // The second insert satisfies the predicate AND collides on sku=42.
    let stmt = Parser::parse_sql("INSERT INTO orders VALUES (2, 1, 42)").unwrap();
    let result = match &stmt {
        vibesql_ast::Statement::Insert(s) => InsertExecutor::execute(&mut db, s),
        _ => unreachable!(),
    };
    assert!(
        result.is_err(),
        "partial UNIQUE index should reject duplicate keys within the predicate"
    );

    // The original row is still there; the conflicting one was not added.
    let table = db.get_table("orders").expect("table missing");
    assert_eq!(table.row_count(), 1);
}

#[test]
fn partial_index_does_not_index_predicate_falsy_rows_on_insert() {
    // Regression test: before issue #5214 the partial index body was
    // populated with every row regardless of the predicate.
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER);
        CREATE INDEX idx_flag ON t(id) WHERE flag = 1;
        INSERT INTO t VALUES (1, 0);
        INSERT INTO t VALUES (2, 1);
        INSERT INTO t VALUES (3, NULL);
        "#,
    );

    let entries = index_row_indices(&db, "idx_flag");
    assert_eq!(
        entries,
        vec![1],
        "only the row whose predicate evaluated to truthy should be indexed; \
         NULL (which is not truthy) and 0 (which is not equal to 1) must be excluded"
    );
}

#[test]
fn batch_insert_into_partial_index_evaluates_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER);
        CREATE INDEX idx_flag ON t(id) WHERE flag = 1;
        "#,
    );

    // Batch insert path (multiple rows, no triggers).
    execute_sql(&mut db, "INSERT INTO t VALUES (1, 1), (2, 0), (3, 1), (4, 0), (5, 1)");

    let entries = index_row_indices(&db, "idx_flag");
    // Row indices 0 (id=1), 2 (id=3), 4 (id=5) have flag=1.
    assert_eq!(entries, vec![0, 2, 4]);
}

// ============================================================================
// Compaction-rebuild regression tests
// ============================================================================
//
// `delete_by_indices_batch` compacts the table (renumbers all rows) when
// >50% of rows are deleted. Before the fix, the executor invoked
// `rebuild_indexes` (which skips partial indexes) but never rebuilt the
// partial-index body — so partial-index `row_index` entries pointed at the
// wrong table rows after compaction (silent corruption).
//
// The tests below trigger compaction with partial indexes present and verify
// that the index body still refers to the correct post-compaction rows.

/// Helper: assert the index body's row indices all refer to rows in the
/// post-compaction table whose row content matches the index's WHERE
/// predicate, and that *every* matching row in the table is represented.
fn assert_partial_index_consistent_after_compaction(
    db: &Database,
    table_name: &str,
    index_name: &str,
    expected_predicate_truthy_ids: &[i64],
    id_col: usize,
) {
    let table = db.get_table(table_name).expect("table missing");
    let rows: Vec<_> = table.scan().to_vec();

    // Collect the id values that the index currently points at.
    let mut indexed_ids: Vec<i64> = index_row_indices(db, index_name)
        .into_iter()
        .map(|row_idx| {
            let v = &rows[row_idx].values[id_col];
            match v {
                SqlValue::Integer(n) => *n,
                SqlValue::Bigint(n) => *n,
                SqlValue::Smallint(n) => *n as i64,
                SqlValue::Double(n) => *n as i64,
                SqlValue::Real(n) => *n as i64,
                SqlValue::Float(n) => *n as i64,
                other => panic!("unexpected id type in row: {:?}", other),
            }
        })
        .collect();
    indexed_ids.sort_unstable();

    let mut expected: Vec<i64> = expected_predicate_truthy_ids.to_vec();
    expected.sort_unstable();

    assert_eq!(
        indexed_ids, expected,
        "partial-index body after compaction must reference exactly the rows whose predicate is truthy"
    );
}

#[test]
fn partial_index_survives_table_compaction_after_bulk_delete() {
    // Build a table with enough rows that deleting >50% triggers compaction
    // in `delete_by_indices_batch`. Sprinkle predicate-truthy rows throughout
    // so the bug (stale row_index pointers after compaction) would surface
    // as the index referencing the wrong rows.
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        "#,
    );

    // 20 rows; predicate-truthy rows are at ids 3, 6, 9, 12, 15, 18 (status=1).
    // Other rows have status=0.
    let mut insert_sql = String::from("INSERT INTO orders VALUES ");
    for id in 1..=20 {
        let status = if id % 3 == 0 { 1 } else { 0 };
        if id > 1 {
            insert_sql.push_str(", ");
        }
        insert_sql.push_str(&format!("({}, {})", id, status));
    }
    execute_sql(&mut db, &insert_sql);

    execute_sql(&mut db, "CREATE INDEX idx_open ON orders(id) WHERE status = 1");

    // Before any delete, all 6 predicate-truthy rows should be indexed.
    let truthy_ids_before: Vec<i64> = vec![3, 6, 9, 12, 15, 18];
    assert_partial_index_consistent_after_compaction(
        &db,
        "orders",
        "idx_open",
        &truthy_ids_before,
        /* id col */ 0,
    );

    // Delete 11 rows (>50% of 20) to force compaction. Keep ids
    // {3, 6, 9, 12, 15, 18, 20} (all truthy ids plus one falsy survivor).
    execute_sql(&mut db, "DELETE FROM orders WHERE id NOT IN (3, 6, 9, 12, 15, 18, 20)");

    let truthy_ids_after: Vec<i64> = vec![3, 6, 9, 12, 15, 18];
    assert_partial_index_consistent_after_compaction(
        &db,
        "orders",
        "idx_open",
        &truthy_ids_after,
        /* id col */ 0,
    );

    // Also: the index must NOT reference any non-truthy row. The helper above
    // already checks the indexed ids equal the truthy set, so this is implied.
    // But also verify the table really compacted (row count dropped) so we
    // know the test actually exercised the compaction path.
    let table = db.get_table("orders").expect("table missing");
    assert_eq!(table.row_count(), 7, "compaction should have left 7 surviving rows");
}

#[test]
fn partial_unique_index_remains_enforceable_after_compaction() {
    // After compaction-rebuild, the partial UNIQUE index body must still
    // contain the correct surviving keys so subsequent inserts that would
    // collide with a surviving truthy row are still rejected.
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER, sku INTEGER);
        "#,
    );

    let mut insert_sql = String::from("INSERT INTO orders VALUES ");
    for id in 1..=20 {
        let status = if id % 3 == 0 { 1 } else { 0 };
        if id > 1 {
            insert_sql.push_str(", ");
        }
        // Unique sku per row.
        insert_sql.push_str(&format!("({}, {}, {})", id, status, 100 + id));
    }
    execute_sql(&mut db, &insert_sql);

    execute_sql(&mut db, "CREATE UNIQUE INDEX idx_open_sku ON orders(sku) WHERE status = 1");

    // Trigger compaction. Keep just the truthy rows + a couple falsy ones.
    execute_sql(&mut db, "DELETE FROM orders WHERE id NOT IN (3, 6, 9, 12, 15, 18, 1, 2)");

    // Surviving truthy rows have sku in {103, 106, 109, 112, 115, 118}.
    // An insert that re-uses one of those skus with status=1 must still be
    // rejected by the partial UNIQUE index. Before the fix, the index body
    // referenced *pre-compaction* row indices and so this insert could either
    // spuriously fail (if the stale pointer happened to land on a truthy
    // surviving row with a different sku) or pass (if it landed on a
    // non-truthy row). After the fix, the body has been rebuilt from current
    // rows so the duplicate is caught.
    let stmt = Parser::parse_sql("INSERT INTO orders VALUES (99, 1, 109)").unwrap();
    let result = match &stmt {
        vibesql_ast::Statement::Insert(s) => InsertExecutor::execute(&mut db, s),
        _ => unreachable!(),
    };
    assert!(
        result.is_err(),
        "partial UNIQUE index must still reject duplicates after table compaction"
    );

    // Conversely, an insert that does NOT collide must succeed.
    execute_sql(&mut db, "INSERT INTO orders VALUES (100, 1, 999)");

    // And an insert with a sku matching a *deleted* truthy row must succeed —
    // the body must NOT retain stale entries for the compacted-away rows.
    // (sku 121 belonged to id=21 which we never inserted; sku 103 belonged
    // to id=3 which we kept, so test with a deleted-truthy sku instead.)
    // We deleted id=21? No — only ids 1..=20 inserted. Deleted truthy ids:
    // none — we kept all truthy ids. So this case can't be exercised here;
    // the regression test above already proves the body doesn't contain
    // stale pointers (it would otherwise reject INSERT (100, 1, 999)).
}

#[test]
fn partial_index_body_empty_after_compaction_removes_all_truthy_rows() {
    // If every predicate-truthy row is deleted (and we cross the >50%
    // threshold), the partial-index body must end up empty — never
    // referencing the compacted-away rows.
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER);
        "#,
    );

    // 10 rows, only ids 4 and 7 truthy.
    execute_sql(
        &mut db,
        "INSERT INTO orders VALUES \
         (1, 0), (2, 0), (3, 0), (4, 1), (5, 0), \
         (6, 0), (7, 1), (8, 0), (9, 0), (10, 0)",
    );
    execute_sql(&mut db, "CREATE INDEX idx_open ON orders(id) WHERE status = 1");
    assert_eq!(index_row_indices(&db, "idx_open").len(), 2);

    // Delete 6 rows (>50%) including both truthy rows.
    execute_sql(&mut db, "DELETE FROM orders WHERE id IN (1, 2, 4, 5, 7, 8)");

    // No surviving row matches the predicate, body must be empty.
    assert!(
        index_body(&db, "idx_open").is_empty(),
        "after compaction removes all truthy rows, partial-index body must be empty"
    );

    let table = db.get_table("orders").expect("table missing");
    assert_eq!(table.row_count(), 4);
}

// ---------------------------------------------------------------------------
// Planner selection of partial indexes via structural predicate implication
// (issue #5325, date2-330)
// ---------------------------------------------------------------------------

/// Run EXPLAIN QUERY PLAN and return the text output.
fn explain_query_plan(db: &Database, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let stmt = Parser::parse_sql(&explain_sql).expect("Failed to parse EXPLAIN QUERY PLAN");
    if let vibesql_ast::Statement::Explain(explain_stmt) = stmt {
        vibesql_executor::ExplainExecutor::execute(&explain_stmt, db)
            .expect("EXPLAIN QUERY PLAN failed")
            .to_text()
    } else {
        panic!("Expected EXPLAIN statement");
    }
}

/// Execute a SELECT and return the first column of each row as integers.
fn select_first_column_ints(db: &Database, sql: &str) -> Vec<i64> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = vibesql_executor::SelectExecutor::new(db);
        let rows = executor.execute(&select_stmt).expect("SELECT failed");
        rows.iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(i) => *i,
                other => panic!("expected integer, got {:?}", other),
            })
            .collect()
    } else {
        panic!("Expected SELECT statement");
    }
}

/// date2-330 shape: the partial expression index is selected when the query
/// WHERE clause contains the index predicate verbatim as a conjunct.
#[test]
fn planner_selects_partial_expression_index_when_predicate_implied() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE t3 (a INTEGER PRIMARY KEY, b REAL);
        INSERT INTO t3 VALUES (1, 2457939.5);
        INSERT INTO t3 VALUES (2, 2457940.5);
        INSERT INTO t3 VALUES (3, 2457950.5);
        CREATE INDEX t3b1 ON t3(datetime(b)) WHERE typeof(b)='real'
        "#,
    );

    let plan = explain_query_plan(
        &db,
        "SELECT a FROM t3 WHERE typeof(b)='real' \
         AND datetime(b) BETWEEN '2017-07-04' AND '2017-07-08'",
    );
    assert!(
        plan.contains("USING INDEX t3b1"),
        "EXPLAIN QUERY PLAN must report USING INDEX t3b1, got:\n{}",
        plan
    );
}

/// Without the typeof(b)='real' conjunct the implication fails and the
/// partial index must NOT be selected.
#[test]
fn planner_skips_partial_expression_index_when_predicate_not_implied() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE t3 (a INTEGER PRIMARY KEY, b REAL);
        INSERT INTO t3 VALUES (1, 2457939.5);
        CREATE INDEX t3b1 ON t3(datetime(b)) WHERE typeof(b)='real'
        "#,
    );

    let plan = explain_query_plan(
        &db,
        "SELECT a FROM t3 WHERE datetime(b) BETWEEN '2017-07-04' AND '2017-07-08'",
    );
    assert!(
        !plan.contains("t3b1"),
        "partial index must not be selected without an implying WHERE conjunct, got:\n{}",
        plan
    );

    // OR at top level must not imply either.
    let plan_or = explain_query_plan(
        &db,
        "SELECT a FROM t3 WHERE typeof(b)='real' OR datetime(b) > '2017-07-04'",
    );
    assert!(
        !plan_or.contains("t3b1"),
        "top-level OR must not imply the index predicate, got:\n{}",
        plan_or
    );
}

/// Partial NON-expression indexes get the same treatment: selected when the
/// query repeats the predicate conjunct, skipped otherwise. Results must be
/// identical either way (full WHERE is re-applied as a post-filter).
#[test]
fn planner_selects_partial_non_expression_index_when_predicate_implied() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status INTEGER, sku INTEGER);
        INSERT INTO orders VALUES (1, 0, 100);
        INSERT INTO orders VALUES (2, 1, 200);
        INSERT INTO orders VALUES (3, 1, 300);
        INSERT INTO orders VALUES (4, 0, 300);
        CREATE INDEX idx_open_sku ON orders(sku) WHERE status = 1
        "#,
    );

    // Implied: predicate conjunct repeated in the query WHERE.
    let implied_sql = "SELECT id FROM orders WHERE status = 1 AND sku = 300";
    let plan = explain_query_plan(&db, implied_sql);
    assert!(
        plan.contains("USING INDEX idx_open_sku"),
        "partial non-expression index must be selected when implied, got:\n{}",
        plan
    );
    assert_eq!(select_first_column_ints(&db, implied_sql), vec![3]);

    // Not implied: same filter column but no status conjunct.
    let not_implied_sql = "SELECT id FROM orders WHERE sku = 300";
    let plan = explain_query_plan(&db, not_implied_sql);
    assert!(
        !plan.contains("idx_open_sku"),
        "partial index must not be selected without the status conjunct, got:\n{}",
        plan
    );
    let mut ids = select_first_column_ints(&db, not_implied_sql);
    ids.sort_unstable();
    assert_eq!(ids, vec![3, 4]);
}

/// Regression (PR #5331 review): `LIKE 'x!%y' ESCAPE '!'` matches only the
/// literal 'x%y', while `LIKE 'x!%y'` matches 'x!…y'. ExpressionHasher does
/// not hash the `escape` field, so a hash-only implication check falsely
/// claimed the escape-less query LIKE implied the index predicate, selected
/// the partial index, and silently dropped row 2 (which is not in the index
/// body). Structural equality must reject the implication and return the
/// correct rows via a full scan.
#[test]
fn like_escape_predicate_is_not_implied_by_escapeless_like() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        r#"
        CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO t VALUES (1, 'x%y');
        INSERT INTO t VALUES (2, 'x!zzy');
        CREATE INDEX idx_name ON t(name) WHERE name LIKE 'x!%y' ESCAPE '!'
        "#,
    );

    // Index body contains only row 1 ('x%y' is the sole escaped-LIKE match).
    assert_eq!(index_row_indices(&db, "idx_name"), vec![0]);

    // The escape-less LIKE matches both rows; the extra equality conjunct
    // narrows it to row 2 — which is NOT in the partial index body.
    let sql = "SELECT id FROM t WHERE name LIKE 'x!%y' AND name = 'x!zzy'";
    let plan = explain_query_plan(&db, sql);
    assert!(
        !plan.contains("idx_name"),
        "escape-less LIKE must not imply the LIKE ... ESCAPE index predicate, got:\n{}",
        plan
    );
    assert_eq!(select_first_column_ints(&db, sql), vec![2]);

    // Sanity: the structurally identical LIKE ... ESCAPE conjunct still
    // implies the predicate, and returns the right row.
    let implied_sql = "SELECT id FROM t WHERE name LIKE 'x!%y' ESCAPE '!' AND id = 1";
    assert_eq!(select_first_column_ints(&db, implied_sql), vec![1]);
}
