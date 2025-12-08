//! End-to-end integration tests for SQL predicates and pattern matching.
//!
//! Tests LIKE pattern matching, IN lists, and EXISTS predicates.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
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
// LIKE Pattern Matching Tests
// ========================================================================

#[test]
fn test_e2e_like_pattern_matching() {
    // Test LIKE pattern matching with wildcards
    let schema = TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(StringValue::from("Widget Pro"))]),
    )
    .unwrap();
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(StringValue::from("Gadget Plus"))]),
    )
    .unwrap();
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(StringValue::from("Widget Mini"))]),
    )
    .unwrap();
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar(StringValue::from("Super Gadget"))]),
    )
    .unwrap();

    // Test LIKE with 'starts with' pattern (Widget%)
    let results = execute_select(&db, "SELECT id FROM products WHERE name LIKE 'Widget%'").unwrap();
    assert_eq!(results.len(), 2, "Should match 'Widget%'");
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[1].values[0], SqlValue::Integer(3));

    // Test LIKE with 'ends with' pattern (%Plus)
    let results = execute_select(&db, "SELECT id FROM products WHERE name LIKE '%Plus'").unwrap();
    assert_eq!(results.len(), 1, "Should match '%Plus'");
    assert_eq!(results[0].values[0], SqlValue::Integer(2));

    // Test LIKE with 'contains' pattern (%Gadget%)
    let results =
        execute_select(&db, "SELECT id FROM products WHERE name LIKE '%Gadget%'").unwrap();
    assert_eq!(results.len(), 2, "Should match '%Gadget%'");
    assert_eq!(results[0].values[0], SqlValue::Integer(2));
    assert_eq!(results[1].values[0], SqlValue::Integer(4));

    // Test NOT LIKE
    let results =
        execute_select(&db, "SELECT id FROM products WHERE name NOT LIKE '%Gadget%'").unwrap();
    assert_eq!(results.len(), 2, "Should NOT match '%Gadget%'");
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[1].values[0], SqlValue::Integer(3));

    // Test LIKE with underscore wildcard (Widget ____)
    // 'Widget ____' = 11 chars, matches "Widget Mini" (11 chars) but not "Widget Pro" (10 chars)
    let results =
        execute_select(&db, "SELECT id FROM products WHERE name LIKE 'Widget ____'").unwrap();
    assert_eq!(results.len(), 1, "Should match 'Widget ____' (only Mini)");
    assert_eq!(results[0].values[0], SqlValue::Integer(3));

    // Test exact match
    let results =
        execute_select(&db, "SELECT id FROM products WHERE name LIKE 'Widget Pro'").unwrap();
    assert_eq!(results.len(), 1, "Should match exact 'Widget Pro'");
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
}

// ========================================================================
// IN List Predicate Tests
// ========================================================================

#[test]
fn test_e2e_in_list_predicate() {
    // Test IN predicate with value lists
    let schema = TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "CATEGORY".to_string(),
                DataType::Varchar { max_length: Some(20) },
                false,
            ),
            ColumnSchema::new("PRICE".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(StringValue::from("Widget")),
            SqlValue::Varchar(StringValue::from("electronics")),
            SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(StringValue::from("Gadget")),
            SqlValue::Varchar(StringValue::from("electronics")),
            SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(StringValue::from("Tool")),
            SqlValue::Varchar(StringValue::from("hardware")),
            SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(StringValue::from("Device")),
            SqlValue::Varchar(StringValue::from("electronics")),
            SqlValue::Integer(300),
        ]),
    )
    .unwrap();
    db.insert_row(
        "PRODUCTS",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(StringValue::from("Hammer")),
            SqlValue::Varchar(StringValue::from("hardware")),
            SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    // Test 1: IN with integer list
    let results = execute_select(&db, "SELECT name FROM products WHERE id IN (1, 3, 5)").unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Widget")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(StringValue::from("Tool")));
    assert_eq!(results[2].values[0], SqlValue::Varchar(StringValue::from("Hammer")));

    // Test 2: IN with string list
    let results =
        execute_select(&db, "SELECT name FROM products WHERE category IN ('hardware')").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Tool")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(StringValue::from("Hammer")));

    // Test 3: NOT IN
    let results = execute_select(&db, "SELECT name FROM products WHERE id NOT IN (2, 4)").unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Widget")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(StringValue::from("Tool")));
    assert_eq!(results[2].values[0], SqlValue::Varchar(StringValue::from("Hammer")));

    // Test 4: IN with single value
    let results = execute_select(&db, "SELECT name FROM products WHERE id IN (2)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Gadget")));

    // Test 5: IN with expressions
    let results =
        execute_select(&db, "SELECT name FROM products WHERE price IN (50, 100 + 100)").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Gadget")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(StringValue::from("Tool")));

    // Test 6: IN combined with AND
    let results = execute_select(
        &db,
        "SELECT name FROM products WHERE category IN ('electronics') AND price > 150",
    )
    .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Gadget")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(StringValue::from("Device")));

    // Test 7: Empty result
    let results = execute_select(&db, "SELECT name FROM products WHERE id IN (99, 100)").unwrap();
    assert_eq!(results.len(), 0);
}

// ========================================================================
// EXISTS Predicate Tests
// ========================================================================

#[test]
fn test_e2e_exists_predicate() {
    // Test EXISTS predicate with correlated and non-correlated subqueries
    let customers_schema = TableSchema::new(
        "CUSTOMERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    let orders_schema = TableSchema::new(
        "ORDERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("CUSTOMER_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("TOTAL".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(customers_schema).unwrap();
    db.create_table(orders_schema).unwrap();

    // Insert customers
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

    // Insert orders (only for customer 1 and 2)
    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(1), SqlValue::Integer(200)]),
    )
    .unwrap();
    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(2), SqlValue::Integer(150)]),
    )
    .unwrap();

    // Test 1: Simple EXISTS - check if there are any orders
    let results =
        execute_select(&db, "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders)")
            .unwrap();
    assert_eq!(results.len(), 3, "All customers should be returned when orders exist");

    // Test 2: NOT EXISTS with empty table - all customers should match
    let empty_schema = TableSchema::new(
        "EMPTY_TABLE".to_string(),
        vec![ColumnSchema::new("DUMMY".to_string(), DataType::Integer, false)],
    );
    db.create_table(empty_schema).unwrap();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM empty_table)",
    )
    .unwrap();
    assert_eq!(results.len(), 3, "All customers should match when NOT EXISTS on empty table");

    // Test 3: EXISTS with WHERE condition in subquery
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE total > 150)",
    )
    .unwrap();
    assert_eq!(results.len(), 3, "All customers returned when orders with total > 150 exist");

    // Test 4: NOT EXISTS with matching rows
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE total > 150)",
    )
    .unwrap();
    assert_eq!(results.len(), 0, "No customers when orders with total > 150 exist");

    // Test 5: EXISTS combined with AND
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE id > 1 AND EXISTS (SELECT 1 FROM orders)",
    )
    .unwrap();
    assert_eq!(results.len(), 2, "Should find customers with id > 1 when orders exist");
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Bob")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(StringValue::from("Charlie")));

    // Test 6: EXISTS combined with OR
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE id = 3 OR EXISTS (SELECT 1 FROM orders)",
    )
    .unwrap();
    assert_eq!(results.len(), 3, "Should find all customers (either id=3 or orders exist)");

    // Note: Correlated subqueries with table aliases (e.g., WHERE EXISTS (SELECT 1 FROM orders o
    // WHERE o.customer_id = c.id)) are not yet fully supported and will be added in a future
    // update.
}
