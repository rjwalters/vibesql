//! Integration tests for the per-table access-pattern signal (Phase 1 of #6199).
//!
//! These verify the end-to-end plumbing of the measurement-only access signal
//! through the real executor + storage paths:
//!   - analytical scans increment `scan_count` and record projection width,
//!   - point lookups increment `point_lookup_count`,
//!   - writes increment `write_count` (including for a native-columnar table,
//!     proving the increment fires before the `is_native_columnar` early-return),
//!   - two tables' signals stay independent and table names are case-insensitive,
//!   - the signal is observation-only (columnar-vs-row path is unchanged).
//!
//! All assertions are deterministic / counter-based — never wall-clock.

use vibesql_catalog::{ColumnSchema, StorageFormat, TableSchema};
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Execute one or more non-SELECT SQL statements separated by ';'.
fn execute_sql(db: &mut Database, sql: &str) {
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            }
            vibesql_ast::Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            other => panic!("Unsupported statement type: {:?}", other),
        }
    }
}

/// Execute a SELECT (driving the real scan path) and return the row count.
fn run_select(db: &Database, sql: &str) -> usize {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => panic!("Expected SELECT statement, got {:?}", other),
    };
    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).expect("SELECT execution failed").len()
}

fn setup_items(db: &mut Database) {
    execute_sql(
        db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
         INSERT INTO items VALUES (1, 'a', 10);
         INSERT INTO items VALUES (2, 'b', 20);
         INSERT INTO items VALUES (3, 'c', 30)",
    );
}

#[test]
fn test_scan_count_matches_number_of_analytical_scans() {
    let mut db = Database::new();
    setup_items(&mut db);

    // Ignore the writes from setup; measure only the scans below.
    db.reset_access_signals();

    // Run N analytical scans that go through the columnar-eligible scan path
    // (a full-coverage columnar predicate on a non-PK column).
    let n = 4;
    for _ in 0..n {
        run_select(&db, "SELECT id FROM items WHERE price >= 0");
    }

    let sig = db.table_access_signal("items").expect("items should have a signal");
    assert_eq!(sig.scan_count, n, "scan_count should equal number of analytical scans");
    // Projection width is the scan's output column count (all 3 columns of items).
    assert_eq!(sig.projection_width_n, n, "one width recorded per scan");
    assert_eq!(
        sig.avg_projection_width(),
        Some(3.0),
        "avg projection width = items column count (id, name, price)"
    );
    // Scans must not be miscounted as writes/lookups.
    assert_eq!(sig.point_lookup_count, 0);
    assert_eq!(sig.write_count, 0);
}

#[test]
fn test_point_lookup_count_matches_number_of_lookups() {
    let mut db = Database::new();
    setup_items(&mut db);
    db.reset_access_signals();

    let m = 5;
    for _ in 0..m {
        let row = db.get_row_by_pk("items", &SqlValue::Integer(1)).unwrap();
        assert!(row.is_some());
    }

    let sig = db.table_access_signal("items").expect("signal exists");
    assert_eq!(sig.point_lookup_count, m, "point_lookup_count should equal number of lookups");
    assert_eq!(sig.scan_count, 0);
}

#[test]
fn test_write_count_on_row_table() {
    let mut db = Database::new();
    setup_items(&mut db);
    db.reset_access_signals();

    // One INSERT, one UPDATE, one DELETE = three writes through the DML funnel.
    execute_sql(&mut db, "INSERT INTO items VALUES (4, 'd', 40)");

    let update = Parser::parse_sql("UPDATE items SET price = 99 WHERE id = 1").unwrap();
    if let vibesql_ast::Statement::Update(s) = update {
        vibesql_executor::UpdateExecutor::execute(&s, &mut db).expect("UPDATE failed");
    } else {
        panic!("expected UPDATE");
    }

    let delete = Parser::parse_sql("DELETE FROM items WHERE id = 2").unwrap();
    if let vibesql_ast::Statement::Delete(s) = delete {
        vibesql_executor::DeleteExecutor::execute(&s, &mut db).expect("DELETE failed");
    } else {
        panic!("expected DELETE");
    }

    let sig = db.table_access_signal("items").expect("signal exists");
    assert_eq!(sig.write_count, 3, "one write recorded per INSERT/UPDATE/DELETE");
}

#[test]
fn test_write_count_on_native_columnar_table() {
    // Proves record_write fires BEFORE the is_native_columnar early-return in
    // invalidate_columnar_cache: a native-columnar table must still be counted.
    let mut db = Database::new();

    let mut schema = TableSchema::with_primary_key(
        "cols".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("v".to_string(), DataType::Integer, true),
        ],
        vec!["id".to_string()],
    );
    schema.set_storage_format(StorageFormat::Columnar);
    db.create_table(schema).unwrap();
    assert!(
        db.get_table("cols").unwrap().is_native_columnar(),
        "table should be native-columnar for this test to be meaningful"
    );

    db.reset_access_signals();

    execute_sql(&mut db, "INSERT INTO cols VALUES (1, 100)");
    execute_sql(&mut db, "INSERT INTO cols VALUES (2, 200)");

    let sig = db.table_access_signal("cols").expect("signal exists for columnar table");
    assert_eq!(
        sig.write_count, 2,
        "writes to a native-columnar table must still be counted (increment precedes early-return)"
    );
}

#[test]
fn test_two_tables_independent_and_case_insensitive() {
    let mut db = Database::new();
    setup_items(&mut db);
    execute_sql(
        &mut db,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER);
         INSERT INTO orders VALUES (1, 500)",
    );
    db.reset_access_signals();

    // Mixed workload over two tables, using varied casing for `items`.
    run_select(&db, "SELECT id FROM items WHERE price >= 0");
    run_select(&db, "SELECT id FROM ITEMS WHERE price >= 0");
    db.get_row_by_pk("orders", &SqlValue::Integer(1)).unwrap();

    let items = db.table_access_signal("items").expect("items signal");
    let orders = db.table_access_signal("orders").expect("orders signal");

    // Case-insensitive: both `items` and `ITEMS` scans land on one entry.
    assert_eq!(items.scan_count, 2, "both casings map to the same signal entry");
    assert_eq!(items.point_lookup_count, 0);

    // No cross-contamination between tables.
    assert_eq!(orders.point_lookup_count, 1);
    assert_eq!(orders.scan_count, 0);

    // Case-insensitive accessor lookup.
    assert_eq!(db.table_access_signal("ITEMS"), db.table_access_signal("items"));
}

#[test]
fn test_unknown_table_and_reset() {
    let mut db = Database::new();
    setup_items(&mut db);

    assert_eq!(db.table_access_signal("does_not_exist"), None, "unknown table => None");

    db.get_row_by_pk("items", &SqlValue::Integer(1)).unwrap();
    assert!(db.table_access_signal("items").is_some());

    db.reset_access_signals();
    assert_eq!(db.table_access_signal("items"), None, "reset zeroes/clears all counters");
}

#[test]
fn test_scan_result_unchanged_by_signal() {
    // Regression: the signal is observation-only — query results are identical
    // to what they'd be without it. (Counters cannot alter the columnar-vs-row
    // path.) We assert the scan returns the expected rows while also recording.
    let mut db = Database::new();
    setup_items(&mut db);
    db.reset_access_signals();

    let count = run_select(&db, "SELECT id FROM items WHERE price >= 20");
    assert_eq!(count, 2, "WHERE price >= 20 matches rows 2 and 3");

    let sig = db.table_access_signal("items").expect("signal exists");
    assert_eq!(sig.scan_count, 1, "the observed scan was recorded");
}
