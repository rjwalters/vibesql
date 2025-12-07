// Integration tests for escaped quotes through WASM bindings
// Run with: cargo test --package wasm-bindings
//
// These tests use the internal vibesql_storage::Database and executor directly
// to bypass WASM-specific types and test the core parsing/execution path

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_insert_with_escaped_quotes() {
    let mut db = Database::new();

    // Create products table
    let create_sql = "CREATE TABLE products (
        product_id INTEGER,
        product_name VARCHAR(100),
        category_id INTEGER,
        unit_price FLOAT,
        units_in_stock INTEGER,
        units_on_order INTEGER
    );";

    let create_stmt = Parser::parse_sql(create_sql).expect("Failed to parse CREATE TABLE");
    match create_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).expect("Failed to create table");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    // Insert products with escaped quotes (products 1-5 from Northwind sample)
    let inserts = [
        "INSERT INTO products VALUES (1, 'Chai', 1, 18.0, 39, 0);",
        "INSERT INTO products VALUES (2, 'Chang', 1, 19.0, 17, 40);",
        "INSERT INTO products VALUES (3, 'Aniseed Syrup', 2, 10.0, 13, 70);",
        "INSERT INTO products VALUES (4, 'Chef Anton''s Cajun Seasoning', 2, 22.0, 53, 0);",
        "INSERT INTO products VALUES (5, 'Chef Anton''s Gumbo Mix', 2, 21.35, 0, 0);",
    ];

    for (i, insert_sql) in inserts.iter().enumerate() {
        let stmt = Parser::parse_sql(insert_sql)
            .unwrap_or_else(|_| panic!("Failed to parse INSERT #{}: {}", i + 1, insert_sql));

        match stmt {
            vibesql_ast::Statement::Insert(insert_stmt) => {
                let rows_inserted =
                    InsertExecutor::execute(&mut db, &insert_stmt).unwrap_or_else(|_| {
                        panic!("Failed to execute INSERT #{}: {}", i + 1, insert_sql)
                    });
                assert_eq!(rows_inserted, 1, "Expected 1 row inserted for product {}", i + 1);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    // Query all products
    let query_sql = "SELECT * FROM products LIMIT 5;";
    let stmt = Parser::parse_sql(query_sql).expect("Failed to parse SELECT");

    let rows = match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            executor.execute(&select_stmt).expect("Failed to execute SELECT")
        }
        _ => panic!("Expected SELECT statement"),
    };

    // Verify we got 5 rows
    assert_eq!(rows.len(), 5, "Expected 5 rows, got {}", rows.len());

    // Verify product #4 has the correct name with apostrophe
    let product_name_4 = &rows[3].values[1]; // product_name is column index 1
    match product_name_4 {
        vibesql_types::SqlValue::Varchar(name) => {
            assert_eq!(
                name.as_ref(), "Chef Anton's Cajun Seasoning",
                "Product #4 name should contain apostrophe"
            );
        }
        _ => panic!("Expected Varchar for product_name"),
    }

    // Verify product #5 has the correct name with apostrophe
    let product_name_5 = &rows[4].values[1];
    match product_name_5 {
        vibesql_types::SqlValue::Varchar(name) => {
            assert_eq!(name.as_ref(), "Chef Anton's Gumbo Mix", "Product #5 name should contain apostrophe");
        }
        _ => panic!("Expected Varchar for product_name"),
    }
}
