// ============================================================================
// Binary Persistence Tests
// ============================================================================
//
// Tests for binary format serialization/deserialization, including:
// - TableIdentifier quoted flag persistence (preserved for display/echo;
//   canonical names are ASCII case-folded per SQLite — issue #5553)
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

    // The quoted flag is preserved, but SQLite case-folds the canonical key,
    // so the table is keyed/looked-up by its lowercase canonical form.
    let original_identifier = db.catalog.get_table_identifier("mytable").unwrap();
    assert!(original_identifier.is_quoted(), "Original table should be quoted");
    assert_eq!(original_identifier.canonical(), "mytable");

    // Save and load using binary format
    let path = "/tmp/test_quoted_identifier.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify the quoted flag was preserved
    let loaded_identifier = loaded_db.catalog.get_table_identifier("mytable").unwrap();
    assert!(loaded_identifier.is_quoted(), "Loaded table should still be quoted after roundtrip");
    assert_eq!(loaded_identifier.canonical(), "mytable");

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Issue #5619: the verbatim original `CREATE TABLE` source text must survive a
/// full file-level `save_binary` → `load_binary` round-trip (header + catalog +
/// data sections), not just the in-memory catalog encoder. This is the actual
/// cross-process path: one process writes the `.vbsql` file, another opens it
/// and answers `SELECT sql FROM sqlite_master`.
#[test]
fn test_binary_file_roundtrip_preserves_verbatim_sql_source() {
    let mut db = Database::new();

    let original_sql = "CREATE TABLE t1(\n  a INTEGER,\n  b TEXT\n)";
    let mut schema = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("b".to_string(), DataType::Varchar { max_length: None }, true),
        ],
    );
    schema.set_sql_source(original_sql);
    db.create_table_with_identifier(schema, TableIdentifier::new("t1", false)).unwrap();

    // Insert a row so the data section is exercised alongside the catalog.
    db.get_table_mut("t1")
        .unwrap()
        .insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("x".into())]))
        .unwrap();

    let path = "/tmp/test_sql_source_binary_roundtrip.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();
    let table = loaded_db.get_table("t1").expect("t1 must survive the binary file round-trip");
    assert_eq!(
        table.schema.sql_source.as_deref(),
        Some(original_sql),
        "verbatim multi-line CREATE TABLE source must survive a save_binary/load_binary cycle"
    );
    assert_eq!(table.row_count(), 1);

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

    // Verify both tables preserved their quoted flags (looked up by case-folded key)
    let users_id = loaded_db.catalog.get_table_identifier("users").unwrap();
    assert!(!users_id.is_quoted(), "users should remain unquoted");

    let profiles_id = loaded_db.catalog.get_table_identifier("userprofiles").unwrap();
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

    // Verify quoted flag preserved (looked up by case-folded canonical key)
    let identifier = loaded_db.catalog.get_table_identifier("casesensitivetable").unwrap();
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

        // Catalog: emitted by the current writer (latest catalog format). This
        // test only synthesizes the v6 *data* layout (the per-row MVCC prefix is
        // what differs at the v6/v7 boundary); the catalog section is read back
        // at the current version below so that catalog fields added in later
        // versions (e.g. v9 verbatim sql_source, #5619) stay byte-aligned. The
        // header still claims v6 to document the synthetic data layout, but the
        // sections are decoded with explicit per-section versions rather than a
        // single header-derived version.
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

    // 4) Decode the synthetic file directly, reading the catalog at the current
    //    version (it was written by the latest writer) and the data section at
    //    v6 (the layout we synthesized: no per-row MVCC prefix). This mirrors
    //    what `Database::load_binary` does, except it lets us mix the catalog
    //    and data versions — necessary because the single header version byte
    //    cannot express "latest catalog + v6 data".
    let mut reader = &v6_bytes[..];
    let header_version = crate::persistence::binary::read_header(&mut reader).unwrap();
    assert_eq!(header_version, 6, "synthetic file claims v6 in its header");
    let mut loaded_db = crate::persistence::binary::read_catalog_v(
        &mut reader,
        crate::persistence::binary::format::VERSION,
    )
    .unwrap();
    crate::persistence::binary::read_data(&mut reader, &mut loaded_db, 6).unwrap();

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

/// Binary-format write-side test for partial-index `WHERE` clause text (#5181).
///
/// The binary `save_binary` path serialises the catalog-side
/// `where_clause` after the index's column list (v8 schema). This test
/// inspects the raw bytes on disk and confirms the SQL form of the
/// predicate appears after the index column list. The matching
/// catalog-repopulation behaviour on load is exercised by the dedicated
/// round-trip tests below (issue #5215).
#[test]
fn test_partial_index_where_clause_written_to_binary_v8() {
    use vibesql_parser::arena_parser::parse_expression_to_owned;

    let mut db = Database::new();

    let schema = TableSchema::new(
        "p1".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, true),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    let ast_columns = vec![vibesql_ast::IndexColumn::new_column(
        "x".to_string(),
        vibesql_ast::OrderDirection::Asc,
    )];
    db.create_index("p1x".to_string(), "p1".to_string(), true, ast_columns).unwrap();
    let catalog_meta = vibesql_catalog::IndexMetadata::new(
        "p1x".to_string(),
        "p1".to_string(),
        vibesql_catalog::IndexType::BTree,
        vec![vibesql_catalog::IndexedColumn::new_column(
            "x".to_string(),
            vibesql_catalog::SortOrder::Ascending,
        )],
        true,
    );
    db.catalog.add_index(catalog_meta).unwrap();

    let predicate_expr = parse_expression_to_owned("y < 2").unwrap();
    let updated = db.catalog.set_index_where_clause("p1x", Some(predicate_expr));
    assert!(updated, "set_index_where_clause should find p1x");

    let path = "/tmp/test_partial_index_v8_write.vbsql";
    db.save_binary(path).unwrap();
    let bytes = std::fs::read(path).unwrap();
    std::fs::remove_file(path).ok();

    // The predicate is serialised by `to_sql()` and stored as a length-
    // prefixed UTF-8 string after the index column list. The exact
    // ToSql output for `y < 2` may add surrounding whitespace, so we look
    // for both halves substring-style in the raw bytes.
    let as_str = String::from_utf8_lossy(&bytes);
    assert!(
        as_str.contains("y") && as_str.contains("<") && as_str.contains("2"),
        "v8 binary should contain the partial-index WHERE expression text"
    );

    // And the file format version byte should be v8 or later.
    assert!(
        crate::persistence::binary::format::VERSION >= 8,
        "binary format VERSION must be >= 8 for partial-index WHERE support (got {})",
        crate::persistence::binary::format::VERSION
    );
}

/// SQL-dump emit test for partial-index `WHERE` clause (#5181).
///
/// The storage crate owns the dump-write path (`save_sql_dump`) but the
/// matching reload lives in the executor crate (`vibesql_executor::load_sql_dump`),
/// so this test only verifies the emit side: a partial UNIQUE INDEX must
/// produce `CREATE UNIQUE INDEX ... WHERE <expr>` in the dump. The
/// executor-side round-trip is exercised by integration tests in that
/// crate.
#[test]
fn test_partial_index_sql_dump_emits_where_clause() {
    use vibesql_parser::arena_parser::parse_expression_to_owned;

    let mut db = Database::new();

    let schema = TableSchema::new(
        "p1".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, true),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    let ast_columns = vec![vibesql_ast::IndexColumn::new_column(
        "x".to_string(),
        vibesql_ast::OrderDirection::Asc,
    )];
    // Storage index — owns the index body. Catalog metadata is added below.
    db.create_index("p1x".to_string(), "p1".to_string(), true, ast_columns).unwrap();
    // Catalog index — owns the `where_clause` / `is_partial()` flag. Mirrors
    // what `CreateIndexExecutor::execute` does in the executor crate (storage
    // alone does not add to the catalog).
    let catalog_meta = vibesql_catalog::IndexMetadata::new(
        "p1x".to_string(),
        "p1".to_string(),
        vibesql_catalog::IndexType::BTree,
        vec![vibesql_catalog::IndexedColumn::new_column(
            "x".to_string(),
            vibesql_catalog::SortOrder::Ascending,
        )],
        true,
    );
    db.catalog.add_index(catalog_meta).unwrap();

    let predicate_expr = parse_expression_to_owned("y < 2").unwrap();
    db.catalog.set_index_where_clause("p1x", Some(predicate_expr));

    let path = "/tmp/test_partial_index_dump_emit.sql";
    db.save_sql_dump(path).unwrap();
    let dump = std::fs::read_to_string(path).unwrap();
    std::fs::remove_file(path).ok();

    assert!(
        dump.contains("CREATE UNIQUE INDEX") && dump.contains("WHERE"),
        "SQL dump must emit WHERE for partial index; dump was:\n{}",
        dump
    );
}

// ============================================================================
// Issue #5215: Binary load path repopulates catalog index metadata
// ============================================================================

/// Full save → load → catalog-lookup round-trip for a partial UNIQUE index.
///
/// Before #5215, the binary load path called `db.create_index` (which only
/// touches the storage-side `IndexManager`) and then `set_index_where_clause`
/// (a silent no-op when no catalog entry exists). As a result, after a cold
/// load `Catalog::find_index_by_name` returned `None` and the partial-index
/// WHERE clause was lost, so `is_partial()` evaluated to `false` and the
/// planner/FK checks mis-classified the index as a full one.
///
/// This test creates a partial UNIQUE index, persists the database to a
/// binary file, reloads it, and asserts that the catalog still recognises
/// the index as partial with the original predicate intact.
#[test]
fn test_partial_index_round_trips_through_binary_load() {
    use vibesql_parser::arena_parser::parse_expression_to_owned;

    let mut db = Database::new();

    let schema = TableSchema::new(
        "p1".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, true),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    // Storage-side index body
    let ast_columns = vec![vibesql_ast::IndexColumn::new_column(
        "x".to_string(),
        vibesql_ast::OrderDirection::Asc,
    )];
    db.create_index("p1x".to_string(), "p1".to_string(), true, ast_columns).unwrap();

    // Catalog-side metadata with WHERE clause
    let catalog_meta = vibesql_catalog::IndexMetadata::new(
        "p1x".to_string(),
        "p1".to_string(),
        vibesql_catalog::IndexType::BTree,
        vec![vibesql_catalog::IndexedColumn::new_column(
            "x".to_string(),
            vibesql_catalog::SortOrder::Ascending,
        )],
        true,
    );
    db.catalog.add_index(catalog_meta).unwrap();
    let predicate_expr = parse_expression_to_owned("y < 2").unwrap();
    let updated = db.catalog.set_index_where_clause("p1x", Some(predicate_expr));
    assert!(updated, "set_index_where_clause should find p1x in the source catalog");

    // Sanity: source catalog reports the index as partial.
    let src_meta = db.catalog.find_index_by_name("p1x").expect("p1x in source catalog");
    assert!(src_meta.is_partial(), "source catalog should report p1x as partial");

    // Round-trip through the binary format.
    let path = "/tmp/test_partial_index_v8_roundtrip.vbsql";
    db.save_binary(path).unwrap();
    let loaded = Database::load_binary(path).unwrap();
    std::fs::remove_file(path).ok();

    // The loaded catalog must still know about the index AND know that it is
    // partial. Before #5215, `find_index_by_name` returned `None` here.
    let loaded_meta = loaded
        .catalog
        .find_index_by_name("p1x")
        .expect("loaded catalog must repopulate IndexMetadata for persisted indexes (#5215)");
    assert!(
        loaded_meta.is_partial(),
        "loaded catalog must preserve partial-index WHERE clause (#5215)"
    );
    assert_eq!(loaded_meta.name, "p1x");
    assert_eq!(loaded_meta.table_name, "p1");
    assert!(loaded_meta.is_unique);
}

/// Round-trip test that every persisted index (partial or not) shows up in
/// the catalog after binary load. Before #5215, the storage-side index was
/// created on load but the catalog never received the matching
/// `IndexMetadata`, so any code that consulted
/// `Catalog::find_index_by_name` post-load silently got `None`.
#[test]
fn test_load_binary_repopulates_catalog_for_all_indexes() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "t".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    // Plain index — storage + catalog (mirroring what the executor would do).
    db.create_index(
        "idx_a".to_string(),
        "t".to_string(),
        false,
        vec![vibesql_ast::IndexColumn::new_column(
            "a".to_string(),
            vibesql_ast::OrderDirection::Asc,
        )],
    )
    .unwrap();
    db.catalog
        .add_index(vibesql_catalog::IndexMetadata::new(
            "idx_a".to_string(),
            "t".to_string(),
            vibesql_catalog::IndexType::BTree,
            vec![vibesql_catalog::IndexedColumn::new_column(
                "a".to_string(),
                vibesql_catalog::SortOrder::Ascending,
            )],
            false,
        ))
        .unwrap();

    // Unique index — storage + catalog.
    db.create_index(
        "idx_b_unique".to_string(),
        "t".to_string(),
        true,
        vec![vibesql_ast::IndexColumn::new_column(
            "b".to_string(),
            vibesql_ast::OrderDirection::Asc,
        )],
    )
    .unwrap();
    db.catalog
        .add_index(vibesql_catalog::IndexMetadata::new(
            "idx_b_unique".to_string(),
            "t".to_string(),
            vibesql_catalog::IndexType::BTree,
            vec![vibesql_catalog::IndexedColumn::new_column(
                "b".to_string(),
                vibesql_catalog::SortOrder::Ascending,
            )],
            true,
        ))
        .unwrap();

    // Save + load.
    let path = "/tmp/test_repopulate_catalog_indexes.vbsql";
    db.save_binary(path).unwrap();
    let loaded = Database::load_binary(path).unwrap();
    std::fs::remove_file(path).ok();

    // Both indexes must be findable in the catalog after load.
    let m1 =
        loaded.catalog.find_index_by_name("idx_a").expect("idx_a must repopulate after load");
    assert!(!m1.is_unique);
    assert!(!m1.is_partial());
    assert_eq!(m1.table_name, "t");

    let m2 = loaded
        .catalog
        .find_index_by_name("idx_b_unique")
        .expect("idx_b_unique must repopulate after load");
    assert!(m2.is_unique);
    assert!(!m2.is_partial());
    assert_eq!(m2.table_name, "t");
}

// ---------------------------------------------------------------------------
// Per-row rowid persistence (format v13, issue #5835)
// ---------------------------------------------------------------------------

/// Round-trip: explicit rowids survive a save/load, and implicit rowids are
/// materialized from the row's physical position at save time. Before v13
/// every reloaded row had `row_id = None` and was silently renumbered by
/// physical position, so `WHERE rowid=N` targeted different rows across a
/// process restart.
#[test]
fn test_row_id_roundtrip_v13() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "rowid_rt".to_string(),
        vec![ColumnSchema::new("x".to_string(), DataType::Integer, true)],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("rowid_rt").unwrap();
    // Implicit rowid (physical position 0 → rowid 1).
    table.insert(crate::Row::new(vec![SqlValue::Integer(10)])).unwrap();
    // Explicit rowid well past the physical count.
    table.insert(crate::Row::with_row_id(vec![SqlValue::Integer(20)], 42)).unwrap();
    // Tombstoned row: dropped at save, but the following row's implicit
    // rowid (physical position 3 → rowid 4) must NOT shift down to 3.
    table.insert(crate::Row::new(vec![SqlValue::Integer(30)])).unwrap();
    table.insert(crate::Row::new(vec![SqlValue::Integer(40)])).unwrap();
    assert!(table.mark_deleted_inplace(2));

    let path = format!("/tmp/test_row_id_roundtrip_v13_{}.vbsql", std::process::id());
    db.save_binary(&path).unwrap();
    let loaded_db = Database::load_binary(&path).unwrap();
    std::fs::remove_file(&path).ok();

    let loaded = loaded_db.get_table("rowid_rt").unwrap();
    assert_eq!(loaded.row_count(), 3);

    let rowid_of = |x: i64| -> u64 {
        loaded
            .scan_live()
            .find(|(_, r)| matches!(r.values[0], SqlValue::Integer(v) if v == x))
            .and_then(|(_, r)| r.row_id)
            .unwrap_or_else(|| panic!("row x={x} must carry a persisted rowid after reload"))
    };
    assert_eq!(rowid_of(10), 1, "implicit rowid materialized from physical position");
    assert_eq!(rowid_of(20), 42, "explicit rowid preserved");
    assert_eq!(rowid_of(40), 4, "rowid must not shift when a tombstone is dropped at save");

    // Allocation continuity: the next rowid must exceed every persisted one.
    assert_eq!(loaded.next_rowid(), 43);
}

/// Negative rowids round-trip and never poison allocation (PR #5891 judge
/// review). Rowids are signed (SQLite model): `-1` is stored as the
/// two's-complement bit pattern `u64::MAX`. Before the fix, reload tracked
/// that bit pattern as an *unsigned* max, so the next `next_rowid()` call
/// computed `u64::MAX + 1` — a debug panic / a wrap to duplicate rowid 0 in
/// release builds.
#[test]
fn test_negative_row_id_roundtrip_does_not_poison_allocation() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "neg_rowid".to_string(),
        vec![ColumnSchema::new("x".to_string(), DataType::Integer, true)],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("neg_rowid").unwrap();
    table.insert(crate::Row::with_row_id(vec![SqlValue::Integer(5)], (-1i64) as u64)).unwrap();

    let path = format!("/tmp/test_negative_row_id_roundtrip_{}.vbsql", std::process::id());
    db.save_binary(&path).unwrap();
    let loaded_db = Database::load_binary(&path).unwrap();
    std::fs::remove_file(&path).ok();

    let loaded = loaded_db.get_table("neg_rowid").unwrap();

    // Bit-pattern-faithful round-trip: the reloaded row still carries -1.
    let rowids: Vec<i64> =
        loaded.scan_live().map(|(_, r)| r.row_id.expect("persisted rowid") as i64).collect();
    assert_eq!(rowids, vec![-1], "negative rowid must survive a binary reload");

    // Allocation is signed (sqlite3-verified): after rowid -1 the next
    // implicit rowid is 0 — NOT u64::MAX + 1 (panic) and NOT a positional
    // renumber.
    assert_eq!(loaded.max_rowid_signed(), Some(-1));
    assert_eq!(loaded.next_rowid_signed(), 0, "sqlite3: after rowid -1, next implicit rowid is 0");
}

/// A negative INTEGER PRIMARY KEY value is the rowid (alias) and is written
/// via `*v as u64`; reloading it must keep allocation sane so a REPLACE INTO
/// (which reserves `next_rowid()`) cannot hit the `u64::MAX + 1` panic path
/// (PR #5891 judge review, poisoning path 1).
#[test]
fn test_negative_ipk_alias_reload_keeps_next_rowid_sane() {
    let mut db = Database::new();

    let mut schema = TableSchema::with_primary_key(
        "neg_ipk".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, false),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
        ],
        vec!["a".to_string()],
    );
    schema.set_rowid_alias_column(Some(0));
    schema.set_sql_source("CREATE TABLE neg_ipk(a INTEGER PRIMARY KEY, b INTEGER)".to_string());
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("neg_ipk").unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(-5), SqlValue::Integer(50)]))
        .unwrap();

    let path = format!("/tmp/test_negative_ipk_reload_{}.vbsql", std::process::id());
    db.save_binary(&path).unwrap();
    let loaded_db = Database::load_binary(&path).unwrap();
    std::fs::remove_file(&path).ok();

    let loaded = loaded_db.get_table("neg_ipk").unwrap();

    // The persisted rowid is the (negative) IPK value, bit-pattern faithful.
    let rowids: Vec<i64> =
        loaded.scan_live().map(|(_, r)| r.row_id.expect("persisted rowid") as i64).collect();
    assert_eq!(rowids, vec![-5]);

    // Signed allocation: next rowid is -4 (sqlite3: signed max + 1), and in
    // particular next_rowid() is safe to call (no overflow panic).
    assert_eq!(loaded.max_rowid_signed(), Some(-5));
    assert_eq!(loaded.next_rowid_signed(), -4);
    let _ = loaded.next_rowid();
}

/// The rowid-alias INTEGER PRIMARY KEY column value IS the rowid; the v13
/// writer persists the alias value so the on-disk rowid stays meaningful.
#[test]
fn test_row_id_persists_ipk_alias_value() {
    let mut db = Database::new();

    let mut schema = TableSchema::with_primary_key(
        "ipk_alias".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, false),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
        ],
        vec!["a".to_string()],
    );
    schema.set_rowid_alias_column(Some(0));
    schema.set_sql_source("CREATE TABLE ipk_alias(a INTEGER PRIMARY KEY, b INTEGER)".to_string());
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("ipk_alias").unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(5), SqlValue::Integer(50)]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![SqlValue::Integer(7), SqlValue::Integer(70)]))
        .unwrap();

    let path = format!("/tmp/test_row_id_ipk_alias_{}.vbsql", std::process::id());
    db.save_binary(&path).unwrap();
    let loaded_db = Database::load_binary(&path).unwrap();
    std::fs::remove_file(&path).ok();

    let loaded = loaded_db.get_table("ipk_alias").unwrap();

    // The alias itself must be rehydrated from sql_source (issue #5835).
    assert_eq!(
        loaded.schema.rowid_alias_column,
        Some(0),
        "INTEGER PRIMARY KEY rowid alias must survive a binary reload"
    );

    // And each persisted rowid is the IPK value, not the physical position.
    let rowids: Vec<u64> =
        loaded.scan_live().map(|(_, r)| r.row_id.expect("persisted rowid")).collect();
    assert_eq!(rowids, vec![5, 7]);
}

/// "INT PRIMARY KEY" (not the exact keyword "INTEGER") is NOT a rowid alias
/// in SQLite; rehydration must not invent one.
#[test]
fn test_int_primary_key_is_not_rehydrated_as_rowid_alias() {
    let mut db = Database::new();

    let mut schema = TableSchema::with_primary_key(
        "int_pk".to_string(),
        vec![ColumnSchema::new("a".to_string(), DataType::Integer, false)],
        vec!["a".to_string()],
    );
    schema.set_sql_source("CREATE TABLE int_pk(a INT PRIMARY KEY)".to_string());
    db.create_table(schema).unwrap();

    let path = format!("/tmp/test_int_pk_no_alias_{}.vbsql", std::process::id());
    db.save_binary(&path).unwrap();
    let loaded_db = Database::load_binary(&path).unwrap();
    std::fs::remove_file(&path).ok();

    assert_eq!(
        loaded_db.get_table("int_pk").unwrap().schema.rowid_alias_column,
        None,
        "INT PRIMARY KEY must not become a rowid alias on reload"
    );
}
