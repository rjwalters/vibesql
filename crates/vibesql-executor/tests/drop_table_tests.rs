use vibesql_executor::{CreateTableExecutor, DropTableExecutor, ExecutorError, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_drop_table_basic() {
    let mut db = Database::new();

    // Create a table
    let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR(50));";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    assert!(db.catalog.table_exists("USERS"));

    // Drop the table
    let drop_sql = "DROP TABLE users;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'USERS' dropped successfully");
    }

    // Verify table is gone
    assert!(!db.catalog.table_exists("USERS"));
    assert!(db.get_table("USERS").is_none());
}

#[test]
fn test_drop_table_if_exists_when_exists() {
    let mut db = Database::new();

    // Create a table
    let create_sql = "CREATE TABLE products (id INTEGER);";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Drop with IF EXISTS
    let drop_sql = "DROP TABLE IF EXISTS products;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        assert!(stmt.if_exists);
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'PRODUCTS' dropped successfully");
    }

    assert!(!db.catalog.table_exists("products"));
}

#[test]
fn test_drop_table_if_exists_when_not_exists() {
    let mut db = Database::new();

    // Try to drop non-existent table with IF EXISTS
    let drop_sql = "DROP TABLE IF EXISTS nonexistent;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        assert!(stmt.if_exists);
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'NONEXISTENT' does not exist (IF EXISTS specified)");
    }
}

#[test]
fn test_drop_table_without_if_exists_error() {
    let mut db = Database::new();

    // Try to drop non-existent table without IF EXISTS
    let drop_sql = "DROP TABLE nonexistent;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        assert!(!stmt.if_exists);
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }
}

#[test]
fn test_drop_table_with_data() {
    let mut db = Database::new();

    // Create table
    let create_sql = "CREATE TABLE orders (id INTEGER, amount INTEGER);";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Insert data
    let insert_sql = "INSERT INTO orders VALUES (1, 100), (2, 200);";
    let insert_stmt = Parser::parse_sql(insert_sql).unwrap();

    if let vibesql_ast::Statement::Insert(stmt) = insert_stmt {
        InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Verify data exists
    assert_eq!(db.get_table("ORDERS").unwrap().row_count(), 2);

    // Drop table
    let drop_sql = "DROP TABLE orders;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
    }

    // Verify table and data are gone
    assert!(!db.catalog.table_exists("ORDERS"));
    assert!(db.get_table("ORDERS").is_none());
}

#[test]
fn test_drop_and_recreate_table() {
    let mut db = Database::new();

    // Create table (use "test_temp" to avoid conflict with TEMP keyword)
    let create_sql = "CREATE TABLE test_temp (id INTEGER);";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Insert data
    let insert_sql = "INSERT INTO test_temp VALUES (42);";
    let insert_stmt = Parser::parse_sql(insert_sql).unwrap();

    if let vibesql_ast::Statement::Insert(stmt) = insert_stmt {
        InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    assert_eq!(db.get_table("TEST_TEMP").unwrap().row_count(), 1);

    // Drop table
    let drop_sql = "DROP TABLE test_temp;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        DropTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Recreate table
    let create_sql2 = "CREATE TABLE test_temp (id INTEGER, name VARCHAR(50));";
    let create_stmt2 = Parser::parse_sql(create_sql2).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt2 {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // New table should be empty
    assert!(db.catalog.table_exists("TEST_TEMP"));
    assert_eq!(db.get_table("TEST_TEMP").unwrap().row_count(), 0);
    assert_eq!(db.get_table("TEST_TEMP").unwrap().schema.column_count(), 2);
}

#[test]
fn test_drop_multiple_tables_sequentially() {
    let mut db = Database::new();

    // Create three tables
    for name in &["table1", "table2", "table3"] {
        let create_sql = format!("CREATE TABLE {} (id INTEGER);", name);
        let create_stmt = Parser::parse_sql(&create_sql).unwrap();

        if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
    }

    assert_eq!(db.list_tables().len(), 3);

    // Drop them one by one
    for name in &["table1", "table2", "table3"] {
        let drop_sql = format!("DROP TABLE {};", name);
        let drop_stmt = Parser::parse_sql(&drop_sql).unwrap();

        if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
            let result = DropTableExecutor::execute(&stmt, &mut db);
            assert!(result.is_ok());
        }
    }

    assert_eq!(db.list_tables().len(), 0);
}

#[test]
fn test_drop_table_parser_case_insensitive() {
    let mut db = Database::new();

    // Create table
    let create_sql = "CREATE TABLE test (id INTEGER);";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Drop with mixed case keywords
    let drop_sql = "DrOp TaBlE test;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
    }

    assert!(!db.catalog.table_exists("test"));
}

#[test]
fn test_drop_table_without_semicolon() {
    let mut db = Database::new();

    // Create table
    let create_sql = "CREATE TABLE test (id INTEGER)";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Drop without semicolon
    let drop_sql = "DROP TABLE test";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
    }

    assert!(!db.catalog.table_exists("test"));
}

#[test]
fn test_drop_table_if_exists_parser() {
    // Test that parser correctly recognizes IF EXISTS clause
    let drop_sql = "DROP TABLE IF EXISTS mytable;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        assert_eq!(stmt.table_name, "MYTABLE");
        assert!(stmt.if_exists);
    } else {
        panic!("Expected DropTable statement");
    }
}

#[test]
fn test_drop_table_without_if_exists_parser() {
    // Test that parser correctly handles DROP TABLE without IF EXISTS
    let drop_sql = "DROP TABLE mytable;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        assert_eq!(stmt.table_name, "MYTABLE");
        assert!(!stmt.if_exists);
    } else {
        panic!("Expected DropTable statement");
    }
}

#[test]
fn test_drop_table_with_underscores() {
    let mut db = Database::new();

    // Create table with underscores
    let create_sql = "CREATE TABLE user_profiles (id INTEGER);";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Drop it
    let drop_sql = "DROP TABLE user_profiles;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        assert_eq!(stmt.table_name, "USER_PROFILES");
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
    }

    assert!(!db.catalog.table_exists("USER_PROFILES"));
}

#[test]
fn test_drop_table_integration_workflow() {
    let mut db = Database::new();

    // Complete workflow: CREATE -> INSERT -> SELECT count -> DROP -> verify gone

    // 1. Create table
    let create_sql = "CREATE TABLE customers (id INTEGER, name VARCHAR(100));";
    let create_stmt = Parser::parse_sql(create_sql).unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create_stmt {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // 2. Insert some data
    let insert_sql = "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');";
    let insert_stmt = Parser::parse_sql(insert_sql).unwrap();

    if let vibesql_ast::Statement::Insert(stmt) = insert_stmt {
        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 3);
    }

    // 3. Verify data
    assert_eq!(db.get_table("CUSTOMERS").unwrap().row_count(), 3);

    // 4. Drop table
    let drop_sql = "DROP TABLE customers;";
    let drop_stmt = Parser::parse_sql(drop_sql).unwrap();

    if let vibesql_ast::Statement::DropTable(stmt) = drop_stmt {
        let result = DropTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'CUSTOMERS' dropped successfully");
    }

    // 5. Verify table is completely gone
    assert!(!db.catalog.table_exists("CUSTOMERS"));
    assert!(db.get_table("CUSTOMERS").is_none());
    assert_eq!(db.list_tables().len(), 0);
}
