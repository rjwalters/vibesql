//! Tests for ALTER TABLE ADD/DROP PRIMARY KEY and FOREIGN KEY constraints (Phase 6 of #1388)

use vibesql_ast::Statement;
use vibesql_storage::Database;

/// Helper to parse and execute SQL statement
fn exec_sql(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
            .map_err(|e| e.to_string()),
        Statement::AlterTable(s) => {
            crate::alter::AlterTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::Delete(s) => crate::delete::DeleteExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) deleted", count))
            .map_err(|e| e.to_string()),
        Statement::Update(s) => crate::update::UpdateExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) updated", count))
            .map_err(|e| e.to_string()),
        _ => Err("Unsupported statement type".to_string()),
    }
}

#[test]
fn test_alter_table_add_primary_key_single_column() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    // Add primary key
    let result = exec_sql(&mut db, "ALTER TABLE users ADD PRIMARY KEY (id)").unwrap();
    assert!(result.contains("PRIMARY KEY"));

    // Verify duplicate insert fails
    let err = exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Charlie')").unwrap_err();
    assert!(err.contains("PRIMARY KEY") || err.contains("duplicate"));
}

#[test]
fn test_alter_table_add_primary_key_composite() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE order_items (order_id INT, item_id INT, quantity INT)")
        .unwrap();
    exec_sql(&mut db, "INSERT INTO order_items VALUES (1, 100, 5), (1, 101, 3), (2, 100, 2)")
        .unwrap();

    // Add composite primary key
    let result =
        exec_sql(&mut db, "ALTER TABLE order_items ADD PRIMARY KEY (order_id, item_id)").unwrap();
    assert!(result.contains("PRIMARY KEY"));

    // Verify duplicate insert fails
    let err = exec_sql(&mut db, "INSERT INTO order_items VALUES (1, 100, 10)").unwrap_err();
    assert!(err.contains("PRIMARY KEY") || err.contains("duplicate"));

    // Verify partial matches are allowed
    exec_sql(&mut db, "INSERT INTO order_items VALUES (2, 101, 7)").unwrap();
    exec_sql(&mut db, "INSERT INTO order_items VALUES (1, 102, 1)").unwrap();
}

#[test]
fn test_alter_table_add_primary_key_already_exists() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();

    // Adding second PK should fail
    let err = exec_sql(&mut db, "ALTER TABLE users ADD PRIMARY KEY (name)").unwrap_err();
    assert!(err.contains("already has a PRIMARY KEY"));
}

#[test]
fn test_alter_table_add_foreign_key_basic() {
    let mut db = Database::new();

    // Create parent and child tables
    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();

    // Insert test data
    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, 1), (2, 2)").unwrap();

    // Add foreign key
    let result =
        exec_sql(&mut db, "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)")
            .unwrap();
    assert!(result.contains("FOREIGN KEY"));

    // Verify orphan row is rejected
    let err = exec_sql(&mut db, "INSERT INTO orders VALUES (3, 999)").unwrap_err();
    assert!(err.contains("FOREIGN KEY"));

    // Verify valid row is accepted
    exec_sql(&mut db, "INSERT INTO orders VALUES (3, 1)").unwrap();
}

#[test]
fn test_alter_table_add_foreign_key_with_constraint_name() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();

    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice')").unwrap();
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, 1)").unwrap();

    // Add named foreign key
    let result = exec_sql(&mut db, "ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)").unwrap();
    assert!(result.contains("FOREIGN KEY"));
}

#[test]
fn test_alter_table_drop_foreign_key() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();

    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice')").unwrap();
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, 1)").unwrap();

    // Add named foreign key
    exec_sql(&mut db, "ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)").unwrap();

    // Verify FK is enforced
    let err = exec_sql(&mut db, "INSERT INTO orders VALUES (2, 999)").unwrap_err();
    assert!(err.contains("FOREIGN KEY"));

    // Drop foreign key
    let result = exec_sql(&mut db, "ALTER TABLE orders DROP CONSTRAINT fk_orders_user").unwrap();
    assert!(result.contains("FOREIGN KEY") && result.contains("dropped"));

    // Verify FK is no longer enforced
    exec_sql(&mut db, "INSERT INTO orders VALUES (2, 999)").unwrap();
}

#[test]
fn test_foreign_key_null_values_allowed() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();

    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice')").unwrap();

    // Add foreign key
    exec_sql(&mut db, "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)").unwrap();

    // NULL foreign keys should be allowed
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, NULL)").unwrap();
}

#[test]
fn test_foreign_key_cascade_delete() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();

    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)").unwrap();

    // Add foreign key with CASCADE delete
    exec_sql(
        &mut db,
        "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE",
    )
    .unwrap();

    // Delete parent row
    exec_sql(&mut db, "DELETE FROM users WHERE id = 1").unwrap();

    // Verify child rows were cascaded
    let table = db.get_table("orders").unwrap();
    assert_eq!(table.scan().len(), 1); // Only order 3 should remain
}

#[test]
fn test_foreign_key_restrict_delete() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();

    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, 1), (2, 2)").unwrap();

    // Add foreign key with NO ACTION (equivalent to RESTRICT)
    exec_sql(
        &mut db,
        "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE NO ACTION",
    )
    .unwrap();

    // Delete parent row should fail
    let err = exec_sql(&mut db, "DELETE FROM users WHERE id = 1").unwrap_err();
    assert!(err.contains("FOREIGN KEY") || err.contains("constraint"));

    // Verify parent row still exists
    let table = db.get_table("users").unwrap();
    assert_eq!(table.scan().len(), 2);
}

#[test]
fn test_foreign_key_set_null() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();

    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)").unwrap();

    // Add foreign key with SET NULL
    exec_sql(
        &mut db,
        "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL",
    )
    .unwrap();

    // Delete parent row
    exec_sql(&mut db, "DELETE FROM users WHERE id = 1").unwrap();

    // Verify child rows have NULL foreign key
    let table = db.get_table("orders").unwrap();
    let rows = table.scan();

    // Orders 1 and 2 should have NULL user_id
    assert!(rows[0].values[1].is_null() || rows[1].values[1].is_null());
}

#[test]
fn test_foreign_key_self_reference() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE employees (id INT PRIMARY KEY, manager_id INT)").unwrap();

    // Add self-referencing foreign key
    exec_sql(
        &mut db,
        "ALTER TABLE employees ADD FOREIGN KEY (manager_id) REFERENCES employees(id)",
    )
    .unwrap();

    // CEO with no manager (NULL)
    exec_sql(&mut db, "INSERT INTO employees VALUES (1, NULL)").unwrap();

    // Manager reporting to CEO
    exec_sql(&mut db, "INSERT INTO employees VALUES (2, 1)").unwrap();

    // Employee reporting to manager
    exec_sql(&mut db, "INSERT INTO employees VALUES (3, 2)").unwrap();

    // Verify invalid reference is rejected
    let err = exec_sql(&mut db, "INSERT INTO employees VALUES (4, 999)").unwrap_err();
    assert!(err.contains("FOREIGN KEY"));
}

#[test]
fn test_foreign_key_composite() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE courses (dept VARCHAR(10), course_num INT, title VARCHAR(100), PRIMARY KEY (dept, course_num))").unwrap();
    exec_sql(&mut db, "CREATE TABLE enrollments (student_id INT, dept VARCHAR(10), course_num INT, grade VARCHAR(2))").unwrap();

    exec_sql(
        &mut db,
        "INSERT INTO courses VALUES ('CS', 101, 'Intro to CS'), ('CS', 201, 'Data Structures')",
    )
    .unwrap();

    // Add composite foreign key
    exec_sql(&mut db, "ALTER TABLE enrollments ADD FOREIGN KEY (dept, course_num) REFERENCES courses(dept, course_num)").unwrap();

    // Valid enrollment
    exec_sql(&mut db, "INSERT INTO enrollments VALUES (1, 'CS', 101, 'A')").unwrap();

    // Invalid enrollment (course doesn't exist)
    let err = exec_sql(&mut db, "INSERT INTO enrollments VALUES (2, 'CS', 301, 'B')").unwrap_err();
    assert!(err.contains("FOREIGN KEY"));
}

#[test]
fn test_foreign_key_update_cascade() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();

    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice')").unwrap();
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, 1), (2, 1)").unwrap();

    // Add foreign key with CASCADE update
    exec_sql(
        &mut db,
        "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE CASCADE",
    )
    .unwrap();

    // Update parent key
    exec_sql(&mut db, "UPDATE users SET id = 100 WHERE id = 1").unwrap();

    // Verify child rows were updated
    let table = db.get_table("orders").unwrap();
    let rows = table.scan();

    // Both orders should have user_id = 100
    use vibesql_types::SqlValue;
    assert_eq!(rows[0].values[1], SqlValue::Integer(100));
    assert_eq!(rows[1].values[1], SqlValue::Integer(100));
}

#[test]
fn test_cascading_foreign_keys_multi_level() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();
    exec_sql(&mut db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)").unwrap();
    exec_sql(&mut db, "CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT)").unwrap();

    exec_sql(&mut db, "INSERT INTO users VALUES (1, 'Alice')").unwrap();
    exec_sql(&mut db, "INSERT INTO orders VALUES (1, 1), (2, 1)").unwrap();
    exec_sql(&mut db, "INSERT INTO order_items VALUES (1, 1), (2, 1), (3, 2)").unwrap();

    // Add cascading foreign keys
    exec_sql(
        &mut db,
        "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE",
    )
    .unwrap();
    exec_sql(&mut db, "ALTER TABLE order_items ADD FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE").unwrap();

    // Delete user - should cascade to orders and order_items
    exec_sql(&mut db, "DELETE FROM users WHERE id = 1").unwrap();

    // Verify all related data was deleted
    let orders = db.get_table("orders").unwrap();
    assert_eq!(orders.scan().len(), 0);

    let items = db.get_table("order_items").unwrap();
    assert_eq!(items.scan().len(), 0);
}
