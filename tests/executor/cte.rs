use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

fn execute_query(db: &Database, query: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(query).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

fn create_test_database() -> Database {
    let mut db = Database::new();

    // Create users table
    let users_schema = TableSchema::new(
        "USERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("AGE".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(users_schema).unwrap();

    // Insert test data
    db.insert_row(
        "USERS",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(StringValue::from("Alice")),
            SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "USERS",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(StringValue::from("Bob")),
            SqlValue::Integer(25),
        ]),
    )
    .unwrap();
    db.insert_row(
        "USERS",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(StringValue::from("Charlie")),
            SqlValue::Integer(35),
        ]),
    )
    .unwrap();

    // Create orders table
    let orders_schema = TableSchema::new(
        "ORDERS".to_string(),
        vec![
            ColumnSchema::new("ORDER_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("USER_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("AMOUNT".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(orders_schema).unwrap();

    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(101), SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(102), SqlValue::Integer(2), SqlValue::Integer(200)]),
    )
    .unwrap();
    db.insert_row(
        "ORDERS",
        Row::new(vec![SqlValue::Integer(103), SqlValue::Integer(1), SqlValue::Integer(150)]),
    )
    .unwrap();

    db
}

#[test]
fn test_cte_basic() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH young_users AS (SELECT id, name FROM users WHERE age < 30) \
         SELECT * FROM young_users;",
    )
    .unwrap();

    assert_eq!(results.len(), 1); // Only Bob is under 30
    assert_eq!(results[0].values[0], SqlValue::Integer(2));
    assert_eq!(results[0].values[1], SqlValue::Varchar(StringValue::from("Bob")));
}

#[test]
fn test_cte_simple_select() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH all_users AS (SELECT id, name FROM users) \
         SELECT * FROM all_users;",
    )
    .unwrap();

    assert_eq!(results.len(), 3); // All 3 users
}

#[test]
fn test_cte_with_where_in_main_query() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH all_users AS (SELECT id, name, age FROM users) \
         SELECT name FROM all_users WHERE age > 25;",
    )
    .unwrap();

    assert_eq!(results.len(), 2); // Alice (30) and Charlie (35)
}

#[test]
fn test_cte_with_aggregates() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH user_spending AS (
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
         )
         SELECT * FROM user_spending WHERE total > 150;",
    )
    .unwrap();

    assert_eq!(results.len(), 2); // User 1 (250) and User 2 (200)
}

#[test]
fn test_cte_join_with_table() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH young_users AS (SELECT id FROM users WHERE age < 30) \
         SELECT o.order_id, o.amount
         FROM young_users y
         JOIN orders o ON y.id = o.user_id;",
    )
    .unwrap();

    assert_eq!(results.len(), 1); // Bob (user 2) has 1 order
    assert_eq!(results[0].values[0], SqlValue::Integer(102));
    assert_eq!(results[0].values[1], SqlValue::Integer(200));
}

#[test]
fn test_cte_multiple() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH
            young AS (SELECT id FROM users WHERE age < 30),
            old AS (SELECT id FROM users WHERE age >= 30)
         SELECT COUNT(*) AS young_count FROM young;",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
}

#[test]
fn test_cte_referencing_another() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH
            all_users AS (SELECT id, name, age FROM users),
            young_users AS (SELECT id, name FROM all_users WHERE age < 30)
         SELECT * FROM young_users;",
    )
    .unwrap();

    assert_eq!(results.len(), 1); // Only Bob
    assert_eq!(results[0].values[0], SqlValue::Integer(2));
}

#[test]
fn test_cte_with_column_aliases() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH user_info (user_id, user_name) AS (SELECT id, name FROM users) \
         SELECT user_id, user_name FROM user_info WHERE user_id = 1;",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[0].values[1], SqlValue::Varchar(StringValue::from("Alice")));
}

#[test]
fn test_cte_used_multiple_times() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH user_ids AS (SELECT id FROM users WHERE age > 25) \
         SELECT u1.id FROM user_ids u1 JOIN user_ids u2 ON u1.id != u2.id WHERE u1.id = 1;",
    )
    .unwrap();

    assert_eq!(results.len(), 1); // User 1 joined with user 3
}

/// Test for issue #3563: CTE visibility in IN subqueries
///
/// CTEs should be visible within IN subqueries in the WHERE clause.
/// Previously this failed with TableNotFound error because the CTE context
/// was not propagated to the predicate evaluation.
#[test]
fn test_cte_visible_in_in_subquery() {
    let db = create_test_database();

    // Query pattern similar to TPC-DS Q95:
    // WITH cte AS (...) SELECT ... WHERE col IN (SELECT col FROM cte)
    let results = execute_query(
        &db,
        "WITH high_orders AS (SELECT user_id FROM orders WHERE amount >= 150) \
         SELECT name FROM users WHERE id IN (SELECT user_id FROM high_orders);",
    )
    .unwrap();

    // Users 1 and 2 have orders >= 150 (101->100, 102->200, 103->150)
    // User 1: orders 101 (100) and 103 (150) - has order >= 150
    // User 2: order 102 (200) - has order >= 150
    assert_eq!(results.len(), 2);
}

/// Test CTE visibility in NOT IN subquery
#[test]
fn test_cte_visible_in_not_in_subquery() {
    let db = create_test_database();

    let results = execute_query(
        &db,
        "WITH big_spenders AS (SELECT user_id FROM orders WHERE amount >= 200) \
         SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM big_spenders);",
    )
    .unwrap();

    // User 2 has an order of 200, so they are a big spender
    // Users 1 and 3 should be returned (3 has no orders)
    assert_eq!(results.len(), 2);
}
