//! SQLite import/export roundtrip tests

use rusqlite::Connection;
use tempfile::TempDir;
use vibesql_types::SqlValue;

fn setup_test_db() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("source.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);
         INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
         INSERT INTO users VALUES (2, 'Bob', NULL);
         INSERT INTO users VALUES (3, 'Charlie', 'charlie@test.org');
         CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL, note TEXT, FOREIGN KEY (user_id) REFERENCES users(id));
         INSERT INTO orders VALUES (1, 1, 99.99, 'First order');
         INSERT INTO orders VALUES (2, 2, 49.50, NULL);
         INSERT INTO orders VALUES (3, 1, 25.00, 'Discount applied');
         CREATE INDEX idx_orders_user ON orders(user_id);",
    )
    .unwrap();
    conn.close().unwrap();
    (dir, path)
}

#[test]
fn test_import_tables_and_data() {
    let (_dir, db_path) = setup_test_db();
    let result = vibesql_cli::sqlite_io::import_sqlite(&db_path).unwrap();

    assert_eq!(result.tables_imported, 2);
    assert_eq!(result.tables_skipped, 0);
    assert_eq!(result.rows_imported, 6);
    assert_eq!(result.database.get_table("USERS").unwrap().row_count(), 3);
    assert_eq!(result.database.get_table("ORDERS").unwrap().row_count(), 3);
}

#[test]
fn test_import_preserves_null() {
    let (_dir, db_path) = setup_test_db();
    let result = vibesql_cli::sqlite_io::import_sqlite(&db_path).unwrap();

    let users = result.database.get_table("USERS").unwrap();
    let rows: Vec<vibesql_storage::Row> = users.scan_live().map(|(_, r)| r.clone()).collect();
    let bob =
        rows.iter().find(|r: &&vibesql_storage::Row| r.values[0] == SqlValue::Integer(2)).unwrap();
    assert_eq!(bob.values[2], SqlValue::Null);
}

#[test]
fn test_import_empty_database() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("empty.db");
    Connection::open(&path).unwrap().close().unwrap();

    let result = vibesql_cli::sqlite_io::import_sqlite(&path).unwrap();
    assert_eq!(result.tables_imported, 0);
    assert_eq!(result.rows_imported, 0);
}

#[test]
fn test_import_empty_table() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("empty_table.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=DELETE; CREATE TABLE t (id INTEGER, name TEXT);")
        .unwrap();
    conn.close().unwrap();

    let result = vibesql_cli::sqlite_io::import_sqlite(&path).unwrap();
    assert_eq!(result.tables_imported, 1);
    assert_eq!(result.rows_imported, 0);
}

#[test]
fn test_import_blob() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("blob.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=DELETE; CREATE TABLE bin (id INTEGER, data BLOB);")
        .unwrap();
    conn.execute("INSERT INTO bin VALUES (1, X'DEADBEEF');", []).unwrap();
    conn.close().unwrap();

    let result = vibesql_cli::sqlite_io::import_sqlite(&path).unwrap();
    let table = result.database.get_table("BIN").unwrap();
    let rows: Vec<vibesql_storage::Row> = table.scan_live().map(|(_, r)| r.clone()).collect();
    assert_eq!(rows[0].values[1], SqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[test]
fn test_import_skips_internal_tables() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("autoincr.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
         INSERT INTO users (name) VALUES ('Alice');",
    )
    .unwrap();
    conn.close().unwrap();

    let result = vibesql_cli::sqlite_io::import_sqlite(&path).unwrap();
    assert_eq!(result.tables_imported, 1);
    assert!(result.database.get_table("SQLITE_SEQUENCE").is_none());
}

#[test]
fn test_import_warns_on_triggers() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("trigger.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE t1 (id INTEGER);
         CREATE TRIGGER trg1 AFTER INSERT ON t1 BEGIN SELECT 1; END;",
    )
    .unwrap();
    conn.close().unwrap();

    let result = vibesql_cli::sqlite_io::import_sqlite(&path).unwrap();
    assert!(result.warnings.iter().any(|w: &String| w.contains("trigger")));
}

#[test]
fn test_export_tables_and_data() {
    let (_dir, src) = setup_test_db();
    let import_result = vibesql_cli::sqlite_io::import_sqlite(&src).unwrap();

    let out_dir = TempDir::new().unwrap();
    let out_path = out_dir.path().join("export.db");
    vibesql_cli::sqlite_io::export_sqlite(&import_result.database, &out_path).unwrap();

    let conn = Connection::open(&out_path).unwrap();
    let user_count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).unwrap();
    assert_eq!(user_count, 3);

    let alice_email: String =
        conn.query_row("SELECT email FROM users WHERE id = 1", [], |r| r.get(0)).unwrap();
    assert_eq!(alice_email, "alice@example.com");

    let bob_email: Option<String> =
        conn.query_row("SELECT email FROM users WHERE id = 2", [], |r| r.get(0)).unwrap();
    assert_eq!(bob_email, None);
}

#[test]
fn test_export_preserves_floats() {
    let (_dir, src) = setup_test_db();
    let import_result = vibesql_cli::sqlite_io::import_sqlite(&src).unwrap();

    let out_dir = TempDir::new().unwrap();
    let out_path = out_dir.path().join("float.db");
    vibesql_cli::sqlite_io::export_sqlite(&import_result.database, &out_path).unwrap();

    let conn = Connection::open(&out_path).unwrap();
    let amount: f64 =
        conn.query_row("SELECT amount FROM orders WHERE id = 1", [], |r| r.get(0)).unwrap();
    assert!((amount - 99.99).abs() < f64::EPSILON);
}

#[test]
fn test_roundtrip_sqlite_vibesql_sqlite() {
    let (_dir, orig) = setup_test_db();
    let import_result = vibesql_cli::sqlite_io::import_sqlite(&orig).unwrap();

    let out_dir = TempDir::new().unwrap();
    let out_path = out_dir.path().join("roundtrip.db");
    vibesql_cli::sqlite_io::export_sqlite(&import_result.database, &out_path).unwrap();

    let orig_conn = Connection::open(&orig).unwrap();
    let new_conn = Connection::open(&out_path).unwrap();

    // Compare table names (case-insensitive)
    let orig_tables: Vec<String> = orig_conn
        .prepare("SELECT LOWER(name) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    let new_tables: Vec<String> = new_conn
        .prepare("SELECT LOWER(name) FROM sqlite_master WHERE type='table' ORDER BY 1")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(orig_tables, new_tables);

    // Compare row counts
    for table in &new_tables {
        let orig_count: i64 = orig_conn
            .query_row(&format!("SELECT COUNT(*) FROM [{}]", table), [], |r| r.get(0))
            .unwrap();
        let new_count: i64 = new_conn
            .query_row(&format!("SELECT COUNT(*) FROM [{}]", table), [], |r| r.get(0))
            .unwrap();
        assert_eq!(orig_count, new_count, "Row count mismatch for '{}'", table);
    }
}

#[test]
fn test_roundtrip_vibesql_sqlite_vibesql() {
    let (_dir, src) = setup_test_db();
    let first = vibesql_cli::sqlite_io::import_sqlite(&src).unwrap();

    let mid_dir = TempDir::new().unwrap();
    let mid_path = mid_dir.path().join("mid.db");
    vibesql_cli::sqlite_io::export_sqlite(&first.database, &mid_path).unwrap();

    let second = vibesql_cli::sqlite_io::import_sqlite(&mid_path).unwrap();

    assert_eq!(first.tables_imported, second.tables_imported);
    assert_eq!(first.rows_imported, second.rows_imported);
    assert_eq!(
        first.database.get_table("USERS").unwrap().row_count(),
        second.database.get_table("USERS").unwrap().row_count()
    );
}

#[test]
fn test_roundtrip_special_characters() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("special.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE strings (id INTEGER, val TEXT);
         INSERT INTO strings VALUES (1, 'hello world');
         INSERT INTO strings VALUES (2, 'it''s a test');
         INSERT INTO strings VALUES (3, '');
         INSERT INTO strings VALUES (4, NULL);",
    )
    .unwrap();
    conn.close().unwrap();

    let import_result = vibesql_cli::sqlite_io::import_sqlite(&path).unwrap();

    let out_dir = TempDir::new().unwrap();
    let out_path = out_dir.path().join("special_out.db");
    vibesql_cli::sqlite_io::export_sqlite(&import_result.database, &out_path).unwrap();

    let conn = Connection::open(&out_path).unwrap();
    let val: String =
        conn.query_row("SELECT val FROM strings WHERE id = 2", [], |r| r.get(0)).unwrap();
    assert_eq!(val, "it's a test");

    let empty: String =
        conn.query_row("SELECT val FROM strings WHERE id = 3", [], |r| r.get(0)).unwrap();
    assert_eq!(empty, "");

    let null: Option<String> =
        conn.query_row("SELECT val FROM strings WHERE id = 4", [], |r| r.get(0)).unwrap();
    assert_eq!(null, None);
}
