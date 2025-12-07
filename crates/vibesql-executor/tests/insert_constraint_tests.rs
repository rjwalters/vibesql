//! Tests for INSERT statement constraint enforcement
//!
//! This module tests that INSERT operations properly enforce:
//! - NOT NULL constraints
//! - PRIMARY KEY constraints (single and composite)
//! - UNIQUE constraints (single, composite, and multiple)
//! - CHECK constraints (simple, complex, and multi-column)

mod common;
use common::insert_constraint_fixtures::*;
use vibesql_executor::InsertExecutor;

// ============================================================================
// NOT NULL Constraint Tests
// ============================================================================

#[test]
fn test_insert_not_null_constraint_with_column_list() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE test (id INT NOT NULL, name VARCHAR(50) NOT NULL, age INT)
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO test (id, age) VALUES (1, 25)
    // name is NOT NULL but not provided, should fail
    let stmt = build_insert_columns(
        "test",
        vec!["id", "age"],
        vec![vibesql_types::SqlValue::Integer(1), vibesql_types::SqlValue::Integer(25)],
    );

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert_not_null_violation(result, "name", "test");
}

// ============================================================================
// PRIMARY KEY Constraint Tests
// ============================================================================

#[test]
fn test_insert_primary_key_duplicate_single_column() {
    let mut db = vibesql_storage::Database::new();
    create_single_pk_table(&mut db);

    // Insert first row
    let stmt1 = build_insert_values(
        "users",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert row with duplicate id
    let stmt2 = build_insert_values(
        "users",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ],
    );

    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert_primary_key_violation(result);
}

#[test]
fn test_insert_primary_key_duplicate_composite() {
    let mut db = vibesql_storage::Database::new();
    create_composite_pk_table(&mut db);

    // Insert first row (order_id=1, item_id=100)
    let stmt1 = build_insert_values(
        "order_items",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Integer(5),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Insert row with different combination (order_id=1, item_id=200) - should succeed
    let stmt2 = build_insert_values(
        "order_items",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(200),
            vibesql_types::SqlValue::Integer(3),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt2).unwrap();

    // Try to insert duplicate composite key (order_id=1, item_id=100)
    let stmt3 = build_insert_values(
        "order_items",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Integer(10),
        ],
    );

    let result = InsertExecutor::execute(&mut db, &stmt3);
    assert_primary_key_violation(result);
}

#[test]
fn test_insert_primary_key_unique_values() {
    let mut db = vibesql_storage::Database::new();
    create_single_pk_table(&mut db);

    // Insert rows with unique ids - should all succeed
    for i in 1..=3 {
        let stmt = build_insert_values(
            "users",
            vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("User{}", i))),
            ],
        );
        InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Verify all 3 rows inserted
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}

// ============================================================================
// UNIQUE Constraint Tests
// ============================================================================

#[test]
fn test_insert_unique_constraint_duplicate() {
    let mut db = vibesql_storage::Database::new();
    create_unique_email_table(&mut db);

    // Insert first row
    let stmt1 = build_insert_values(
        "users",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert row with duplicate email
    let stmt2 = build_insert_values(
        "users",
        vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
        ],
    );

    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert_unique_violation(result, "email");
}

#[test]
fn test_insert_unique_constraint_allows_null() {
    let mut db = vibesql_storage::Database::new();
    create_unique_email_table(&mut db);

    // Insert multiple rows with NULL email - should all succeed
    // (NULL != NULL in SQL, so multiple NULLs are allowed)
    for i in 1..=3 {
        let stmt = build_insert_values(
            "users",
            vec![vibesql_types::SqlValue::Integer(i), vibesql_types::SqlValue::Null],
        );
        InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Verify all 3 rows inserted
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}

#[test]
fn test_insert_unique_constraint_composite() {
    let mut db = vibesql_storage::Database::new();
    create_composite_unique_table(&mut db);

    // Insert first enrollment (student=1, course=101)
    let stmt1 = build_insert_values(
        "enrollments",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(101),
            vibesql_types::SqlValue::Integer(85),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Insert different combination (student=1, course=102) - should succeed
    let stmt2 = build_insert_values(
        "enrollments",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(102),
            vibesql_types::SqlValue::Integer(90),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt2).unwrap();

    // Try to insert duplicate combination (student=1, course=101)
    let stmt3 = build_insert_values(
        "enrollments",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(101),
            vibesql_types::SqlValue::Integer(95),
        ],
    );

    let result = InsertExecutor::execute(&mut db, &stmt3);
    assert_constraint_violation(result, &["UNIQUE"]);
}

#[test]
fn test_insert_multiple_unique_constraints() {
    let mut db = vibesql_storage::Database::new();
    create_multi_unique_table(&mut db);

    // Insert first user
    let stmt1 = build_insert_values(
        "users",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice")),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert with duplicate email (different username)
    let stmt2 = build_insert_values(
        "users",
        vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("bob")),
        ],
    );
    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert_unique_violation(result, "email");

    // Try to insert with duplicate username (different email)
    let stmt3 = build_insert_values(
        "users",
        vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("bob@example.com")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("alice")),
        ],
    );
    let result = InsertExecutor::execute(&mut db, &stmt3);
    assert_unique_violation(result, "username");
}

// ============================================================================
// CHECK Constraint Tests
// ============================================================================

#[test]
fn test_insert_check_constraint_passes() {
    let mut db = vibesql_storage::Database::new();
    create_check_price_table(&mut db, false);

    // Insert valid row (price >= 0)
    let stmt = build_insert_values(
        "products",
        vec![vibesql_types::SqlValue::Integer(1), vibesql_types::SqlValue::Integer(100)],
    );

    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Verify row inserted
    let table = db.get_table("products").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_check_constraint_violation() {
    let mut db = vibesql_storage::Database::new();
    create_check_price_table(&mut db, false);

    // Try to insert row with negative price (should fail)
    let stmt = build_insert_values(
        "products",
        vec![vibesql_types::SqlValue::Integer(1), vibesql_types::SqlValue::Integer(-10)],
    );

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert_check_violation(result, "price_positive");
}

#[test]
fn test_insert_check_constraint_with_null() {
    let mut db = vibesql_storage::Database::new();
    create_check_price_table(&mut db, true);

    // Insert row with NULL price - should succeed
    // (NULL in CHECK constraint evaluates to UNKNOWN, which is treated as TRUE)
    let stmt = build_insert_values(
        "products",
        vec![vibesql_types::SqlValue::Integer(1), vibesql_types::SqlValue::Null],
    );

    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Verify row inserted
    let table = db.get_table("products").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_multiple_check_constraints() {
    let mut db = vibesql_storage::Database::new();
    create_multi_check_table(&mut db);

    // Insert valid row
    let stmt1 = build_insert_values(
        "products",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Integer(50),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert row with negative price (violates first CHECK)
    let stmt2 = build_insert_values(
        "products",
        vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(-10),
            vibesql_types::SqlValue::Integer(50),
        ],
    );
    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert_check_violation(result, "price_positive");

    // Try to insert row with negative quantity (violates second CHECK)
    let stmt3 = build_insert_values(
        "products",
        vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Integer(-5),
        ],
    );
    let result = InsertExecutor::execute(&mut db, &stmt3);
    assert_check_violation(result, "quantity_positive");
}

#[test]
fn test_insert_check_constraint_with_expression() {
    let mut db = vibesql_storage::Database::new();
    create_check_comparison_table(&mut db);

    // Insert valid row (bonus < salary)
    let stmt1 = build_insert_values(
        "employees",
        vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(50000),
            vibesql_types::SqlValue::Integer(10000),
        ],
    );
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert row where bonus >= salary (should fail)
    let stmt2 = build_insert_values(
        "employees",
        vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(50000),
            vibesql_types::SqlValue::Integer(60000),
        ],
    );

    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert_check_violation(result, "bonus_less_than_salary");
}
