// ============================================================================
// Binary Persistence Tests
// ============================================================================
//
// Tests for binary format serialization/deserialization, including:
// - TableIdentifier quoted flag persistence (SQL:1999 case-sensitivity)
// - Backward compatibility with older format versions
// - MVCC version field round-trip and v6 → v7 read compatibility (#5136 Phase 1a)

use vibesql_catalog::{ColumnSchema, TableIdentifier, TableSchema};
use vibesql_types::{DataType, SqlValue};

use crate::Database;

/// Test that the quoted flag for TableIdentifier is persisted correctly
/// through a save/load roundtrip.
#[test]
fn test_quoted_table_identifier_roundtrip() {
    let mut db = Database::new();

    // Create a table with a quoted identifier (case-sensitive)
    let schema = TableSchema::new(
        "MyTable".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    let identifier = TableIdentifier::quoted("MyTable");
    db.create_table_with_identifier(schema, identifier).unwrap();

    // Verify the identifier is marked as quoted
    let original_identifier = db.catalog.get_table_identifier("MyTable").unwrap();
    assert!(original_identifier.is_quoted(), "Original table should be quoted");
    assert_eq!(original_identifier.canonical(), "MyTable");

    // Save and load using binary format
    let path = "/tmp/test_quoted_identifier.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify the quoted flag was preserved
    let loaded_identifier = loaded_db.catalog.get_table_identifier("MyTable").unwrap();
    assert!(loaded_identifier.is_quoted(), "Loaded table should still be quoted after roundtrip");
    assert_eq!(loaded_identifier.canonical(), "MyTable");

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Test that unquoted tables remain unquoted through roundtrip
#[test]
fn test_unquoted_table_identifier_roundtrip() {
    let mut db = Database::new();

    // Create a table with an unquoted identifier (case-insensitive)
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    let identifier = TableIdentifier::unquoted("users");
    db.create_table_with_identifier(schema, identifier).unwrap();

    // Verify the identifier is NOT quoted
    let original_identifier = db.catalog.get_table_identifier("users").unwrap();
    assert!(!original_identifier.is_quoted(), "Original table should be unquoted");
    assert_eq!(original_identifier.canonical(), "users");

    // Save and load using binary format
    let path = "/tmp/test_unquoted_identifier.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify the unquoted flag was preserved
    let loaded_identifier = loaded_db.catalog.get_table_identifier("users").unwrap();
    assert!(
        !loaded_identifier.is_quoted(),
        "Loaded table should still be unquoted after roundtrip"
    );
    assert_eq!(loaded_identifier.canonical(), "users");

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Test that both quoted and unquoted tables can coexist and be persisted correctly
#[test]
fn test_mixed_quoted_unquoted_tables_roundtrip() {
    let mut db = Database::new();

    // Create an unquoted table (case-insensitive)
    let schema1 = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table_with_identifier(schema1, TableIdentifier::unquoted("users")).unwrap();

    // Create a quoted table (case-sensitive)
    let schema2 = TableSchema::new(
        "UserProfiles".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table_with_identifier(schema2, TableIdentifier::quoted("UserProfiles")).unwrap();

    // Save and load
    let path = "/tmp/test_mixed_identifiers.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify both tables preserved their quoted flags
    let users_id = loaded_db.catalog.get_table_identifier("users").unwrap();
    assert!(!users_id.is_quoted(), "users should remain unquoted");

    let profiles_id = loaded_db.catalog.get_table_identifier("UserProfiles").unwrap();
    assert!(profiles_id.is_quoted(), "UserProfiles should remain quoted");

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Test that tables with data preserve quoted flag
#[test]
fn test_quoted_table_with_data_roundtrip() {
    let mut db = Database::new();

    // Create a quoted table with data
    let schema = TableSchema::new(
        "CaseSensitiveTable".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table_with_identifier(schema, TableIdentifier::quoted("CaseSensitiveTable")).unwrap();

    // Insert data
    let table = db.get_table_mut("CaseSensitiveTable").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]))
        .unwrap();

    // Save and load
    let path = "/tmp/test_quoted_with_data.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify quoted flag preserved
    let identifier = loaded_db.catalog.get_table_identifier("CaseSensitiveTable").unwrap();
    assert!(identifier.is_quoted());

    // Verify data preserved
    let loaded_table = loaded_db.get_table("CaseSensitiveTable").unwrap();
    assert_eq!(loaded_table.row_count(), 2);

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Test legacy behavior: tables created without identifier use default (unquoted)
#[test]
fn test_legacy_table_creation_defaults_to_unquoted() {
    let mut db = Database::new();

    // Create table using legacy method (no explicit identifier)
    let schema = TableSchema::new(
        "legacy_table".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Should default to unquoted
    let identifier = db.catalog.get_table_identifier("legacy_table").unwrap();
    assert!(!identifier.is_quoted(), "Legacy tables should default to unquoted");

    // Save and load
    let path = "/tmp/test_legacy_table.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify still unquoted after load
    let loaded_id = loaded_db.catalog.get_table_identifier("legacy_table").unwrap();
    assert!(!loaded_id.is_quoted());

    // Cleanup
    std::fs::remove_file(path).ok();
}

// ============================================================================
// MVCC version field round-trip + v6 backwards-compatibility tests
// (#5136 Phase 1a)
// ============================================================================

/// Round-trip: a row with non-default `xmin`/`xmax` survives a v7 save/load.
///
/// Phase 1a does not stamp non-default version fields from any executor path,
/// but the on-disk format must still preserve them so Phase 1c (write-path
/// stamping) can rely on the persistence layer.
#[test]
fn test_mvcc_version_fields_roundtrip_v7() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "mvcc_versioned".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert three rows with distinct MVCC fingerprints.
    let table = db.get_table_mut("mvcc_versioned").unwrap();
    {
        let mut row = crate::Row::from_vec(vec![SqlValue::Integer(1)]);
        row.xmin = 100;
        row.xmax = None; // live row
        table.insert(row).unwrap();
    }
    {
        let mut row = crate::Row::from_vec(vec![SqlValue::Integer(2)]);
        row.xmin = 200;
        row.xmax = Some(300); // marked-deleted row
        table.insert(row).unwrap();
    }
    {
        let mut row = crate::Row::from_vec(vec![SqlValue::Integer(3)]);
        row.xmin = u64::MAX - 1; // edge value to catch byte-order bugs
        row.xmax = Some(u64::MAX);
        table.insert(row).unwrap();
    }

    let path = "/tmp/test_mvcc_roundtrip_v7.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    let loaded_table = loaded_db.get_table("mvcc_versioned").unwrap();
    assert_eq!(loaded_table.row_count(), 3);

    // Build a sorted view by `values[0]` for deterministic assertions.
    let rows: Vec<&crate::Row> = loaded_table.scan().iter().collect();
    let by_id = |id: i64| -> &crate::Row {
        rows.iter()
            .copied()
            .find(|r| matches!(r.values[0], SqlValue::Integer(v) if v == id))
            .unwrap_or_else(|| panic!("row with id={} not found after load", id))
    };

    let r1 = by_id(1);
    assert_eq!(r1.xmin, 100);
    assert_eq!(r1.xmax, None);

    let r2 = by_id(2);
    assert_eq!(r2.xmin, 200);
    assert_eq!(r2.xmax, Some(300));

    let r3 = by_id(3);
    assert_eq!(r3.xmin, u64::MAX - 1);
    assert_eq!(r3.xmax, Some(u64::MAX));

    std::fs::remove_file(path).ok();
}

/// Default-constructed rows (the path most code uses) save and load with the
/// pre-MVCC sentinel intact. This is what Phase 1a installs everywhere by
/// default and is what Phase 1b/1c will start to override.
#[test]
fn test_default_rows_carry_pre_mvcc_sentinel() {
    use crate::row::PRE_MVCC_TXN_ID;

    let mut db = Database::new();

    let schema = TableSchema::new(
        "sentinel_default".to_string(),
        vec![ColumnSchema::new("n".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("sentinel_default").unwrap();
    table.insert(crate::Row::new(vec![SqlValue::Integer(7)])).unwrap();
    table.insert(crate::Row::from_vec(vec![SqlValue::Integer(8)])).unwrap();

    let path = "/tmp/test_default_sentinel.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();
    let loaded_table = loaded_db.get_table("sentinel_default").unwrap();
    for row in loaded_table.scan() {
        assert_eq!(row.xmin, PRE_MVCC_TXN_ID);
        assert_eq!(row.xmax, None);
    }

    std::fs::remove_file(path).ok();
}

/// v6 → v7 read compatibility.
///
/// Constructs a synthetic v6-formatted byte sequence in memory by calling
/// the current writers and then patching:
///   1. the header `VERSION` byte at offset 5 from `7` back to `6`, and
///   2. stripping the per-row MVCC prefix (9 bytes: `xmin: u64` + `xmax_tag = 0`)
///      from the data section.
///
/// We then feed the bytes through `Database::load_binary` (which compresses
/// is bypassed because we use uncompressed `save_binary`). The expectation is
/// that the v6 reader path applies the pre-MVCC sentinel
/// (`xmin = PRE_MVCC_TXN_ID, xmax = None`) to every recovered row, exactly
/// as a real v6 file would.
#[test]
fn test_v6_to_v7_read_compatibility_via_synthesized_v6_file() {
    use std::io::Write;

    use crate::row::PRE_MVCC_TXN_ID;

    // 1) Build a v7 database with default-sentinel rows. We pick rows whose
    //    encoded-on-disk size is easy to predict so we can strip the prefix.
    let mut db = Database::new();
    let schema = TableSchema::new(
        "v6compat".to_string(),
        vec![ColumnSchema::new("n".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("v6compat").unwrap();
    table.insert(crate::Row::new(vec![SqlValue::Integer(11)])).unwrap();
    table.insert(crate::Row::new(vec![SqlValue::Integer(22)])).unwrap();

    let v7_path = "/tmp/test_v6_compat_source.vbsql";
    db.save_binary(v7_path).unwrap();

    // 2) Read the v7 file bytes.
    let v7_bytes = std::fs::read(v7_path).unwrap();
    std::fs::remove_file(v7_path).ok();

    // 3) Rewrite as a synthetic v6 file:
    //      - copy the 16-byte header but flip version byte (offset 5) to 6,
    //      - copy the catalog section verbatim,
    //      - strip the 9-byte MVCC prefix from each row in the data section.
    //
    //    We don't need to know where the catalog ends because the only
    //    difference between v6 and v7 row encodings is the per-row prefix; the
    //    catalog format itself is unchanged between v6 and v7. So the
    //    transformation we need is: walk the data section and drop 9 bytes
    //    before each row's column values. We can only do that if we know
    //    where the data section starts.
    //
    //    Approach: manually re-emit the file by re-running write_header /
    //    write_catalog / write_data on the in-memory db, but with our own
    //    v6-style data writer that omits the prefix. This is what a real v6
    //    build would have produced.
    let mut v6_bytes: Vec<u8> = Vec::new();
    {
        // Header
        crate::persistence::binary::write_header(&mut v6_bytes).unwrap();
        // Patch version byte from 7 back to 6 to claim v6 format.
        v6_bytes[5] = 6;

        // Catalog (unchanged between v6 and v7).
        crate::persistence::binary::catalog::write_catalog(&mut v6_bytes, &db).unwrap();

        // Data section, v6 layout: per-table {name, row_count, then for each
        // row: just the column values, NO MVCC prefix}.
        let table_names = db.catalog.list_tables();
        for table_name in table_names {
            if let Some(table) = db.get_table(&table_name) {
                crate::persistence::binary::io::write_string(&mut v6_bytes, &table_name).unwrap();
                crate::persistence::binary::io::write_u64(&mut v6_bytes, table.row_count() as u64)
                    .unwrap();
                for (_idx, row) in table.scan_live() {
                    for value in &row.values {
                        crate::persistence::binary::value::write_sql_value(&mut v6_bytes, value)
                            .unwrap();
                    }
                }
            }
        }
        // Note: we deliberately don't compare lengths against the real v7
        // bytes — the compressed/uncompressed sizes differ and we already
        // proved both paths in test_mvcc_version_fields_roundtrip_v7.
        let _ = v7_bytes; // silence unused warning if logic above changes
    }

    // 4) Write the synthetic v6 bytes to disk and load via the public API.
    let v6_path = "/tmp/test_v6_compat_synthetic.vbsql";
    {
        let mut f = std::fs::File::create(v6_path).unwrap();
        f.write_all(&v6_bytes).unwrap();
        f.flush().unwrap();
    }

    let loaded_db = Database::load_binary(v6_path).unwrap();
    let loaded_table = loaded_db.get_table("v6compat").unwrap();
    assert_eq!(loaded_table.row_count(), 2);

    // Every row recovered from a v6 file must carry the pre-MVCC sentinel.
    for row in loaded_table.scan() {
        assert_eq!(
            row.xmin, PRE_MVCC_TXN_ID,
            "v6 row should be stamped with PRE_MVCC_TXN_ID after upgrade-read"
        );
        assert_eq!(row.xmax, None, "v6 row should have no xmax");
    }

    // Sanity check: the column values came through correctly too.
    let mut found = std::collections::HashSet::new();
    for row in loaded_table.scan() {
        if let SqlValue::Integer(v) = row.values[0] {
            found.insert(v);
        }
    }
    assert!(found.contains(&11));
    assert!(found.contains(&22));

    std::fs::remove_file(v6_path).ok();
}

/// Sanity: confirm the public format constant has actually been bumped to v7.
/// If a future change accidentally reverts this, this test will catch it
/// before any silent on-disk regression.
#[test]
fn test_format_version_is_at_least_7() {
    assert!(
        crate::persistence::binary::format::VERSION >= 7,
        "binary format VERSION must be >= 7 after Phase 1a of #5136 (got {})",
        crate::persistence::binary::format::VERSION
    );
}
