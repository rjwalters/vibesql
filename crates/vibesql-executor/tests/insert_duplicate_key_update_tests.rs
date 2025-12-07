use vibesql_executor::InsertExecutor;
use vibesql_storage::Database;

fn setup_products_table(db: &mut Database) {
    // CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(50), stock INT)
    let schema = vibesql_catalog::TableSchema::with_primary_key(
        "products".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "stock".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
        vec!["id".to_string()], // id is PRIMARY KEY
    );
    db.create_table(schema).unwrap();
}

#[test]
fn test_on_duplicate_key_update_basic() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Initial INSERT: INSERT INTO products VALUES (1, 'Widget', 10)
    let initial_stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &initial_stmt).unwrap();
    assert_eq!(rows, 1);

    // Upsert: INSERT INTO products VALUES (1, 'Widget', 20) ON DUPLICATE KEY UPDATE stock = VALUES(stock)
    let upsert_stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: Some(vec![vibesql_ast::Assignment {
            column: "stock".to_string(),
            value: vibesql_ast::Expression::DuplicateKeyValue { column: "stock".to_string() },
        }]),
    };

    let rows = InsertExecutor::execute(&mut db, &upsert_stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify the update
    let table = db.get_table("products").unwrap();
    let result_rows = table.scan();
    assert_eq!(result_rows.len(), 1);

    let row = &result_rows[0];
    assert_eq!(row.values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(row.values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget")));
    assert_eq!(row.values[2], vibesql_types::SqlValue::Integer(20)); // Updated to 20
}

#[test]
fn test_on_duplicate_key_update_with_arithmetic() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // Initial INSERT
    let initial_stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    InsertExecutor::execute(&mut db, &initial_stmt).unwrap();

    // Upsert with arithmetic: stock = stock + VALUES(stock)
    let upsert_stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(30)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: Some(vec![vibesql_ast::Assignment {
            column: "stock".to_string(),
            value: vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Plus,
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "stock".to_string(),
                }),
                right: Box::new(vibesql_ast::Expression::DuplicateKeyValue {
                    column: "stock".to_string(),
                }),
            },
        }]),
    };

    InsertExecutor::execute(&mut db, &upsert_stmt).unwrap();

    // Verify: stock should be 10 + 30 = 40
    let table = db.get_table("products").unwrap();
    let result_rows = table.scan();
    assert_eq!(result_rows.len(), 1);

    let row = &result_rows[0];
    assert_eq!(row.values[2], vibesql_types::SqlValue::Integer(40));
}

#[test]
fn test_on_duplicate_key_update_no_conflict() {
    let mut db = Database::new();
    setup_products_table(&mut db);

    // INSERT with ON DUPLICATE KEY UPDATE - no conflict, should insert normally
    let upsert_stmt = vibesql_ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: Some(vec![vibesql_ast::Assignment {
            column: "stock".to_string(),
            value: vibesql_ast::Expression::DuplicateKeyValue { column: "stock".to_string() },
        }]),
    };

    let rows = InsertExecutor::execute(&mut db, &upsert_stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify the row was inserted (not updated)
    let table = db.get_table("products").unwrap();
    let result_rows = table.scan();
    assert_eq!(result_rows.len(), 1);

    let row = &result_rows[0];
    assert_eq!(row.values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(row.values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget")));
    assert_eq!(row.values[2], vibesql_types::SqlValue::Integer(10));
}
