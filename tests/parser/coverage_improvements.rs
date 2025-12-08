//! Integration tests to improve code coverage
//!
//! Target modules:
//! - predicates.rs: LIKE pattern matching
//! - subqueries.rs: EXISTS, ANY, ALL, SOME
//! - conversion.rs: CAST and type conversion functions

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// ========================================================================
// LIKE Pattern Matching Tests (predicates.rs)
// ========================================================================

fn create_products_schema() -> TableSchema {
    TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("CODE".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ],
    )
}

fn insert_sample_products(db: &mut Database) {
    let rows = vec![
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(StringValue::from("Apple iPhone")),
            SqlValue::Varchar(StringValue::from("APPL-001")),
        ]),
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(StringValue::from("Samsung Galaxy")),
            SqlValue::Varchar(StringValue::from("SAMS-002")),
        ]),
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(StringValue::from("Apple MacBook")),
            SqlValue::Varchar(StringValue::from("APPL-100")),
        ]),
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(StringValue::from("Microsoft Surface")),
            SqlValue::Varchar(StringValue::from("MSFT-050")),
        ]),
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(StringValue::from("Apple Watch")),
            SqlValue::Varchar(StringValue::from("APPL-200")),
        ]),
    ];

    for row in rows {
        db.insert_row("PRODUCTS", row).unwrap();
    }
}

#[test]
fn test_like_percent_wildcard() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM products WHERE name LIKE 'Apple%'").unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_like_underscore_wildcard() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results =
        execute_select(&db, "SELECT code FROM products WHERE code LIKE 'APPL-_0_'").unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_like_combined_wildcards() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results = execute_select(&db, "SELECT name FROM products WHERE name LIKE '%a%'").unwrap();
    assert!(results.len() >= 2);
}

#[test]
fn test_not_like() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM products WHERE name NOT LIKE 'Apple%'").unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_like_null_handling() {
    let schema = TableSchema::new(
        "TEST_NULLS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "VALUE".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("TEST_NULLS", Row::new(vec![SqlValue::Integer(1), SqlValue::Null])).unwrap();

    let results =
        execute_select(&db, "SELECT id FROM test_nulls WHERE value LIKE '%test%'").unwrap();
    assert_eq!(results.len(), 0);
}

// ========================================================================
// EXISTS Subquery Tests (subqueries.rs)
// ========================================================================

fn create_orders_schema() -> TableSchema {
    TableSchema::new(
        "ORDERS".to_string(),
        vec![
            ColumnSchema::new("ORDER_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("CUSTOMER_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("AMOUNT".to_string(), DataType::Integer, false),
        ],
    )
}

fn create_customers_schema() -> TableSchema {
    TableSchema::new(
        "CUSTOMERS".to_string(),
        vec![
            ColumnSchema::new("CUSTOMER_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    )
}

fn setup_customers_orders_db() -> Database {
    let mut db = Database::new();
    db.create_table(create_customers_schema()).unwrap();
    db.create_table(create_orders_schema()).unwrap();

    db.insert_row(
        "CUSTOMERS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(StringValue::from("Alice"))]),
    )
    .unwrap();

    db.insert_row(
        "CUSTOMERS",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(StringValue::from("Bob"))]),
    )
    .unwrap();

    db.insert_row(
        "CUSTOMERS",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(StringValue::from("Charlie"))]),
    )
    .unwrap();

    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(101), SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();

    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(102), SqlValue::Integer(1), SqlValue::Integer(200)]),
    )
    .unwrap();

    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(103), SqlValue::Integer(3), SqlValue::Integer(150)]),
    )
    .unwrap();

    db
}

#[test]
fn test_exists_predicate() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = 1)",
    )
    .unwrap();

    assert_eq!(results.len(), 3);
}

#[test]
fn test_not_exists_predicate() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE amount > 10000)",
    )
    .unwrap();

    assert_eq!(results.len(), 3);
}

// ========================================================================
// Quantified Comparison Tests (subqueries.rs)
// ========================================================================

#[test]
fn test_any_quantifier() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id = ANY (SELECT customer_id FROM orders WHERE amount > 150)"
    ).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Alice")));
}

#[test]
fn test_all_quantifier() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id <> ALL (SELECT customer_id FROM orders WHERE amount < 150)"
    ).unwrap();

    assert!(!results.is_empty());
}

#[test]
fn test_some_quantifier() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id = SOME (SELECT customer_id FROM orders)",
    )
    .unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_any_with_greater_than() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT order_id FROM orders WHERE amount > ANY (SELECT amount FROM orders WHERE customer_id = 1)"
    ).unwrap();

    assert!(!results.is_empty());
}

#[test]
fn test_all_with_less_than() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT order_id FROM orders WHERE amount < ALL (SELECT amount FROM orders WHERE customer_id = 1)"
    ).unwrap();

    assert_eq!(results.len(), 0);
}

// ========================================================================
// CAST Operations Tests (conversion.rs)
// ========================================================================

fn create_mixed_types_schema() -> TableSchema {
    TableSchema::new(
        "MIXED_DATA".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "TEXT_NUM".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
            ColumnSchema::new("INT_VAL".to_string(), DataType::Integer, true),
            ColumnSchema::new("FLOAT_VAL".to_string(), DataType::DoublePrecision, true),
        ],
    )
}

fn setup_mixed_types_db() -> Database {
    let mut db = Database::new();
    db.create_table(create_mixed_types_schema()).unwrap();

    db.insert_row(
        "MIXED_DATA",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(StringValue::from("123")),
            SqlValue::Integer(456),
            SqlValue::Double(78.9),
        ]),
    )
    .unwrap();

    db.insert_row(
        "MIXED_DATA",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(StringValue::from("999")),
            SqlValue::Integer(111),
            SqlValue::Double(22.3),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_cast_varchar_to_integer() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(text_num AS INTEGER) FROM mixed_data WHERE id = 1")
            .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(123));
}

#[test]
fn test_cast_integer_to_varchar() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS VARCHAR(50)) FROM mixed_data WHERE id = 1")
            .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("456")));
}

#[test]
fn test_cast_varchar_to_double() {
    let db = setup_mixed_types_db();

    let results = execute_select(
        &db,
        "SELECT CAST(text_num AS DOUBLE PRECISION) FROM mixed_data WHERE id = 1",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Double(123.0));
}

#[test]
fn test_cast_integer_to_double() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS DOUBLE) FROM mixed_data WHERE id = 1").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Double(456.0));
}

#[test]
fn test_cast_to_smallint() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS SMALLINT) FROM mixed_data WHERE id = 2")
            .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Smallint(111));
}

#[test]
fn test_cast_to_bigint() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS BIGINT) FROM mixed_data WHERE id = 1").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Bigint(456));
}

#[test]
fn test_cast_to_float() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS FLOAT) FROM mixed_data WHERE id = 1").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Float(456.0));
}

// ========================================================================
// Combined Tests - Complex Queries
// ========================================================================

#[test]
fn test_like_with_exists() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE name LIKE 'A%' AND EXISTS (SELECT 1 FROM orders WHERE amount > 100)"
    ).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Alice")));
}

#[test]
fn test_cast_in_where_clause() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT id FROM mixed_data WHERE CAST(text_num AS INTEGER) > 500")
            .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(2));
}

#[test]
fn test_multiple_predicates_combined() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results = execute_select(
        &db,
        "SELECT name FROM products WHERE name LIKE '%Apple%' AND code NOT LIKE 'APPL-2%'",
    )
    .unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_quantified_comparison_with_cast() {
    let db = setup_mixed_types_db();

    let results = execute_select(
        &db,
        "SELECT id FROM mixed_data WHERE CAST(text_num AS INTEGER) > ALL (SELECT int_val FROM mixed_data WHERE id > 100)"
    ).unwrap();

    assert_eq!(results.len(), 2);
}

// ========================================================================
// Edge Cases
// ========================================================================

#[test]
fn test_exists_empty_subquery() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE EXISTS (SELECT * FROM orders WHERE amount > 10000)",
    )
    .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_all_empty_subquery() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id > ALL (SELECT customer_id FROM orders WHERE amount > 10000)"
    ).unwrap();

    assert_eq!(results.len(), 3);
}

#[test]
fn test_any_empty_subquery() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id = ANY (SELECT customer_id FROM orders WHERE amount > 10000)"
    ).unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_like_exact_match() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM products WHERE name LIKE 'Apple iPhone'").unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_cast_null_value() {
    let schema = TableSchema::new(
        "NULL_TEST".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NULLABLE_VAL".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("NULL_TEST", Row::new(vec![SqlValue::Integer(1), SqlValue::Null])).unwrap();

    let results =
        execute_select(&db, "SELECT CAST(nullable_val AS INTEGER) FROM null_test").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}
