mod common;

use vibesql_executor::InsertExecutor;

#[test]
fn test_character_varying_column_with_length() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE test_cv (id INT, description CHARACTER VARYING(100))
    let schema = vibesql_catalog::TableSchema::new(
        "test_cv".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "description".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO test_cv VALUES (1, 'Test description')
    let stmt = vibesql_ast::InsertStmt { schema_name: None, schema_quoted: false, table_quoted: false,
        table_name: "test_cv".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Test description"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify data was inserted correctly
    let table = db.get_table("test_cv").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_character_varying_column_without_length() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE test_cv_nolen (id INT, text CHARACTER VARYING)
    let schema = vibesql_catalog::TableSchema::new(
        "test_cv_nolen".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "text".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO test_cv_nolen VALUES (1, 'Unlimited length text')
    let stmt = vibesql_ast::InsertStmt { schema_name: None, schema_quoted: false, table_quoted: false,
        table_name: "test_cv_nolen".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Unlimited length text"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify data was inserted correctly
    let table = db.get_table("test_cv_nolen").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_with_default_value() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE users (id INT DEFAULT 999, name VARCHAR(50))
    let mut id_column = vibesql_catalog::ColumnSchema::new(
        "id".to_string(),
        vibesql_types::DataType::Integer,
        false,
    );
    id_column.default_value =
        Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(999)));

    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            id_column,
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice')
    let stmt = vibesql_ast::InsertStmt { schema_name: None, schema_quoted: false, table_quoted: false,
        table_name: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Default,
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Alice"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify default value was used
    let table = db.get_table("users").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(0), Some(&vibesql_types::SqlValue::Integer(999)));
    assert_eq!(row.get(1), Some(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
}

#[test]
fn test_insert_default_no_default_value_defined() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE users (id INT, name VARCHAR(50)) -- no default for id
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ), /* nullable */
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice')
    let stmt = vibesql_ast::InsertStmt { schema_name: None, schema_quoted: false, table_quoted: false,
        table_name: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Default,
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Alice"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify NULL was used when no default is defined
    let table = db.get_table("users").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(0), Some(&vibesql_types::SqlValue::Null));
    assert_eq!(row.get(1), Some(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
}

// ============================================================================
// INTEGER PRIMARY KEY NULL Auto-Generation Tests (SQLite Semantics)
// ============================================================================

/// Test that inserting NULL into an INTEGER PRIMARY KEY column auto-generates
/// the next value (max + 1, or 1 if table is empty).
#[test]
fn test_integer_primary_key_null_autogen_empty_table() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE track (tid INTEGER PRIMARY KEY, name TEXT)
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "track".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "tid".to_string(),
                vibesql_types::DataType::Integer,
                false, // NOT NULL (implicit for INTEGER PRIMARY KEY)
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                true,
            ),
        ],
        vec!["tid".to_string()],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO track VALUES (NULL, 'song1')
    let stmt = vibesql_ast::InsertStmt {
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "track".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("song1"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify auto-generated value is 1 (first row in empty table)
    let table = db.get_table("track").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(0), Some(&vibesql_types::SqlValue::Integer(1)));
    assert_eq!(row.get(1), Some(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("song1"))));
}

/// Test sequential auto-generation when inserting multiple NULLs
#[test]
fn test_integer_primary_key_null_autogen_sequential() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "items".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "val".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // INSERT first row with NULL → should get id=1
    let stmt1 = vibesql_ast::InsertStmt {
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "items".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("a"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // INSERT second row with NULL → should get id=2
    let stmt2 = vibesql_ast::InsertStmt {
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "items".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("b"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt2).unwrap();

    // Verify sequential ids
    let table = db.get_table("items").unwrap();
    assert_eq!(table.row_count(), 2);
    assert_eq!(table.scan()[0].get(0), Some(&vibesql_types::SqlValue::Integer(1)));
    assert_eq!(table.scan()[1].get(0), Some(&vibesql_types::SqlValue::Integer(2)));
}

/// Test that explicit values still work, and next NULL uses max + 1
#[test]
fn test_integer_primary_key_null_autogen_after_explicit() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "t".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "data".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // INSERT explicit value 100
    let stmt1 = vibesql_ast::InsertStmt {
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "t".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(100)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("explicit"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // INSERT NULL → should get id=101 (max + 1)
    let stmt2 = vibesql_ast::InsertStmt {
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "t".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("auto"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt2).unwrap();

    // Verify ids
    let table = db.get_table("t").unwrap();
    assert_eq!(table.row_count(), 2);

    // Find the auto-generated row
    let auto_row = table.scan().iter().find(|r| {
        r.get(1) == Some(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("auto")))
    });
    assert!(auto_row.is_some());
    assert_eq!(auto_row.unwrap().get(0), Some(&vibesql_types::SqlValue::Integer(101)));
}

/// Test that BIGINT PRIMARY KEY does NOT auto-generate (only INTEGER does)
#[test]
fn test_bigint_primary_key_no_autogen() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE t (id BIGINT PRIMARY KEY, data TEXT)
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "bigint_pk".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Bigint, // NOT Integer!
                true, // Must be nullable since we're inserting NULL
            ),
            vibesql_catalog::ColumnSchema::new(
                "data".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // INSERT NULL should stay as NULL (no auto-generation for BIGINT)
    let stmt = vibesql_ast::InsertStmt {
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "bigint_pk".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("test"),
            )),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Verify NULL is preserved
    let table = db.get_table("bigint_pk").unwrap();
    assert_eq!(table.scan()[0].get(0), Some(&vibesql_types::SqlValue::Null));
}

/// Test multi-row INSERT with all NULLs in INTEGER PRIMARY KEY
#[test]
fn test_integer_primary_key_null_autogen_multirow() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE track (tid INTEGER PRIMARY KEY, name TEXT)
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "track".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "tid".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                true,
            ),
        ],
        vec!["tid".to_string()],
    );
    db.create_table(schema).unwrap();

    // Multi-row INSERT with all NULLs (like SQLite's orderby1.test)
    // INSERT INTO track VALUES (NULL, 'one'), (NULL, 'two'), (NULL, 'three');
    let stmt = vibesql_ast::InsertStmt {
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "track".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                    arcstr::ArcStr::from("one"),
                )),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                    arcstr::ArcStr::from("two"),
                )),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                    arcstr::ArcStr::from("three"),
                )),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 3);

    // Verify all three rows have unique auto-generated ids
    let table = db.get_table("track").unwrap();
    assert_eq!(table.row_count(), 3);

    let ids: Vec<i64> = table
        .scan()
        .iter()
        .filter_map(|r| {
            if let Some(vibesql_types::SqlValue::Integer(id)) = r.get(0) {
                Some(*id)
            } else {
                None
            }
        })
        .collect();

    // All ids should be unique
    let mut sorted_ids = ids.clone();
    sorted_ids.sort();
    assert_eq!(sorted_ids, vec![1, 2, 3], "Expected sequential ids 1, 2, 3");
}

/// Test composite primary key does NOT auto-generate
#[test]
fn test_composite_primary_key_no_autogen() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY(a, b))
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "composite_pk".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "a".to_string(),
                vibesql_types::DataType::Integer,
                true, // Must be nullable
            ),
            vibesql_catalog::ColumnSchema::new(
                "b".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
        vec!["a".to_string(), "b".to_string()], // Composite PK
    );
    db.create_table(schema).unwrap();

    // INSERT NULL should stay as NULL (no auto-generation for composite PK)
    let stmt = vibesql_ast::InsertStmt {
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "composite_pk".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Verify NULL is preserved
    let table = db.get_table("composite_pk").unwrap();
    assert_eq!(table.scan()[0].get(0), Some(&vibesql_types::SqlValue::Null));
}
