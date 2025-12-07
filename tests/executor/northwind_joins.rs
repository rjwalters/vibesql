//! Test that Northwind database JOIN examples work correctly

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

fn create_northwind_db() -> Database {
    let mut db = Database::new();

    // Create categories table
    let categories_schema = TableSchema::new(
        "CATEGORIES".to_string(),
        vec![
            ColumnSchema::new("CATEGORY_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "CATEGORY_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "DESCRIPTION".to_string(),
                DataType::Varchar { max_length: Some(200) },
                false,
            ),
        ],
    );
    db.create_table(categories_schema).unwrap();

    // Create products table
    let products_schema = TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("PRODUCT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "PRODUCT_NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("CATEGORY_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("UNIT_PRICE".to_string(), DataType::Float { precision: 53 }, false),
            ColumnSchema::new("units_in_stock".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(products_schema).unwrap();

    // Insert categories
    use vibesql_storage::Row;
    let categories_table = db.get_table_mut("CATEGORIES").unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(std::sync::Arc::from("Beverages")),
            SqlValue::Varchar(std::sync::Arc::from("Soft drinks, coffees, teas, beers, and ales")),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(std::sync::Arc::from("Condiments")),
            SqlValue::Varchar(std::sync::Arc::from("Sweet and savory sauces")),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(std::sync::Arc::from("Confections")),
            SqlValue::Varchar(std::sync::Arc::from("Desserts, candies, and sweet breads")),
        ]))
        .unwrap();

    // Insert products
    let products_table = db.get_table_mut("PRODUCTS").unwrap();
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(std::sync::Arc::from("Chai")),
            SqlValue::Integer(1),
            SqlValue::Float(18.0),
            SqlValue::Integer(39),
        ]))
        .unwrap();
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(std::sync::Arc::from("Chang")),
            SqlValue::Integer(1),
            SqlValue::Float(19.0),
            SqlValue::Integer(17),
        ]))
        .unwrap();
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(std::sync::Arc::from("Aniseed Syrup")),
            SqlValue::Integer(2),
            SqlValue::Float(10.0),
            SqlValue::Integer(13),
        ]))
        .unwrap();
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(16),
            SqlValue::Varchar(std::sync::Arc::from("Pavlova")),
            SqlValue::Integer(3),
            SqlValue::Float(17.45),
            SqlValue::Integer(29),
        ]))
        .unwrap();

    db
}

#[test]
fn test_inner_join_products_categories() {
    let db = create_northwind_db();
    let executor = SelectExecutor::new(&db);

    // Test the JOIN example from web demo
    let query = r#"
        SELECT
          p.product_name,
          c.category_name,
          p.unit_price
        FROM products p
        INNER JOIN categories c ON p.category_id = c.category_id
        ORDER BY c.category_name, p.product_name
    "#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 4 products
        assert_eq!(result.len(), 4);

        // All rows should have 3 columns: product_name, category_name, unit_price
        assert_eq!(result[0].values.len(), 3);

        // Verify first row (ordered by category_name, product_name)
        // Should be Beverages category with Chai or Chang
        if let SqlValue::Varchar(cat_name) = &result[0].values[1] {
            assert_eq!(cat_name.as_ref(), "Beverages");
        } else {
            panic!("Expected Varchar for category_name");
        }

        println!("✅ INNER JOIN products with categories works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_left_join_categories_products() {
    let db = create_northwind_db();
    let executor = SelectExecutor::new(&db);

    // Test LEFT JOIN to show all categories including those with no products
    // Note: ORDER BY with aggregates not yet supported, so we don't specify order
    let query = r#"
        SELECT
          c.category_name,
          COUNT(p.product_id) as product_count
        FROM categories c
        LEFT OUTER JOIN products p ON c.category_id = p.category_id
        GROUP BY c.category_name
    "#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 categories
        assert_eq!(result.len(), 3);

        // Each row should have 2 columns: category_name, product_count
        assert_eq!(result[0].values.len(), 2);

        println!("✅ LEFT JOIN categories with products aggregation works!");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_products_by_category_aggregate() {
    let db = create_northwind_db();
    let executor = SelectExecutor::new(&db);

    // Test aggregate by category
    // Note: ORDER BY with aggregates not yet supported, so we don't specify order
    let query = r#"
        SELECT
          c.category_name,
          COUNT(p.product_id) as product_count,
          AVG(p.unit_price) as avg_price
        FROM categories c
        INNER JOIN products p ON c.category_id = p.category_id
        GROUP BY c.category_name
    "#;

    let stmt = Parser::parse_sql(query).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should have 3 categories (all have products in our test data)
        assert_eq!(result.len(), 3);

        // Beverages should have 2 products
        let beverages_row = result
            .iter()
            .find(|row| {
                if let SqlValue::Varchar(name) = &row.values[0] {
                    name.as_ref() == "Beverages"
                } else {
                    false
                }
            })
            .expect("Should find Beverages category");

        assert_eq!(beverages_row.values[1], SqlValue::Integer(2));

        println!("✅ Aggregate JOIN query (COUNT, AVG) works!");
    } else {
        panic!("Expected SELECT statement");
    }
}
