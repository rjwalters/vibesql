use std::env;

use vibesql::catalog::{ColumnSchema, TableSchema};
/**
 * Query Runner for Web Demo Examples
 *
 * This tool executes SQL queries and formats the output as expected results
 * for web demo examples. It sets up the appropriate database, runs the query,
 * and outputs the result in the format expected by the test framework.
 *
 * Usage: cargo run --example query_runner
 */
use vibesql::executor::SelectExecutor;
use vibesql::parser::Parser;
use vibesql::storage::{Database, Row};
use vibesql::types::{DataType, SqlValue};

fn create_northwind_db() -> Database {
    let mut db = Database::new();

    // Create categories table
    let categories_schema = TableSchema::new(
        "categories".to_string(),
        vec![
            ColumnSchema::new("category_id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "category_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "description".to_string(),
                DataType::Varchar { max_length: Some(200) },
                false,
            ),
        ],
    );
    db.create_table(categories_schema).unwrap();

    // Create products table
    let products_schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("product_id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "product_name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("category_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("unit_price".to_string(), DataType::Float { precision: 53 }, false),
            ColumnSchema::new("units_in_stock".to_string(), DataType::Integer, false),
            ColumnSchema::new("units_on_order".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(products_schema).unwrap();

    // Insert categories
    let categories_table = db.get_table_mut("categories").unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Beverages".to_string()),
            SqlValue::Varchar("Soft drinks, coffees, teas, beers, and ales".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Condiments".to_string()),
            SqlValue::Varchar(
                "Sweet and savory sauces, relishes, spreads, and seasonings".to_string(),
            ),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Confections".to_string()),
            SqlValue::Varchar("Desserts, candies, and sweet breads".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Dairy Products".to_string()),
            SqlValue::Varchar("Cheeses".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Grains/Cereals".to_string()),
            SqlValue::Varchar("Breads, crackers, pasta, and cereal".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Varchar("Meat/Poultry".to_string()),
            SqlValue::Varchar("Prepared meats".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Varchar("Produce".to_string()),
            SqlValue::Varchar("Dried fruit and bean curd".to_string()),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Varchar("Seafood".to_string()),
            SqlValue::Varchar("Seaweed and fish".to_string()),
        ]))
        .unwrap();

    // Insert products
    let products_table = db.get_table_mut("products").unwrap();
    let products_data = vec![
        (1, "Chai", 1, 18.0, 39, 0),
        (2, "Chang", 1, 19.0, 17, 40),
        (3, "Aniseed Syrup", 2, 10.0, 13, 70),
        (4, "Chef Anton's Cajun Seasoning", 2, 22.0, 53, 0),
        (5, "Chef Anton's Gumbo Mix", 2, 21.35, 0, 0),
        (6, "Grandma's Boysenberry Spread", 2, 25.0, 120, 0),
        (7, "Uncle Bob's Organic Dried Pears", 7, 30.0, 15, 0),
        (8, "Northwoods Cranberry Sauce", 2, 40.0, 6, 0),
        (9, "Mishi Kobe Niku", 6, 97.0, 29, 0),
        (10, "Ikura", 8, 31.0, 31, 0),
        (11, "Queso Cabrales", 4, 21.0, 22, 30),
        (12, "Queso Manchego La Pastora", 4, 38.0, 86, 0),
        (13, "Konbu", 8, 6.0, 24, 0),
        (14, "Tofu", 7, 23.25, 35, 0),
        (15, "Genen Shouyu", 2, 15.5, 39, 0),
        (16, "Pavlova", 3, 17.45, 29, 0),
        (17, "Alice Mutton", 6, 39.0, 0, 0),
        (18, "Carnarvon Tigers", 8, 62.5, 42, 0),
        (19, "Teatime Chocolate Biscuits", 3, 9.2, 25, 0),
        (20, "Sir Rodney's Marmalade", 3, 81.0, 40, 0),
    ];

    for (id, name, cat_id, price, stock, on_order) in products_data {
        products_table
            .insert(Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Varchar(name.to_string()),
                SqlValue::Integer(cat_id),
                SqlValue::Float(price),
                SqlValue::Integer(stock),
                SqlValue::Integer(on_order),
            ]))
            .unwrap();
    }

    db
}

fn format_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Float(f) => {
            // Format float to remove unnecessary decimal places
            if f.fract() == 0.0 {
                format!("{:.1}", f)
            } else {
                f.to_string()
            }
        }
        SqlValue::Varchar(s) => s.clone(),
        SqlValue::Boolean(b) => b.to_string(),
        SqlValue::Null => "NULL".to_string(),
        _ => format!("{:?}", value),
    }
}

fn format_result_as_expected(result: &[Row], column_names: &[String]) -> String {
    if result.is_empty() {
        return "-- (0 rows)".to_string();
    }

    let mut output = String::new();

    // Calculate column widths
    let mut widths: Vec<usize> = column_names.iter().map(|n| n.len()).collect();
    for row in result {
        for (i, value) in row.values.iter().enumerate() {
            let formatted = format_value(value);
            widths[i] = widths[i].max(formatted.len());
        }
    }

    // Header row
    output.push_str("-- | ");
    for (i, name) in column_names.iter().enumerate() {
        output.push_str(name);
        output.push_str(&" ".repeat(widths[i].saturating_sub(name.len())));
        output.push_str(" | ");
    }
    output.push('\n');

    // Data rows
    for row in result {
        output.push_str("-- | ");
        for (i, value) in row.values.iter().enumerate() {
            let formatted = format_value(value);
            output.push_str(&formatted);
            output.push_str(&" ".repeat(widths[i].saturating_sub(formatted.len())));
            output.push_str(" | ");
        }
        output.push('\n');
    }

    // Row count
    output.push_str(&format!("-- ({} rows)", result.len()));

    output
}

fn run_query(db: &Database, query: &str) {
    println!("\n=== Running Query ===");
    println!("{}", query);
    println!("\n=== Expected Result ===");

    let stmt = match Parser::parse_sql(query) {
        Ok(stmt) => stmt,
        Err(e) => {
            println!("-- ⏭️ SKIP: Parse error - {:?}", e);
            return;
        }
    };

    if let vibesql::ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        match executor.execute(&select_stmt) {
            Ok(result) => {
                // Extract column names from the SELECT statement
                let column_names: Vec<String> = select_stmt
                    .select_list
                    .iter()
                    .enumerate()
                    .map(|(i, item)| match item {
                        vibesql::ast::SelectItem::Expression { alias: Some(alias), .. } => {
                            alias.clone()
                        }
                        vibesql::ast::SelectItem::Expression { alias: None, .. } => {
                            format!("column{}", i + 1)
                        }
                        vibesql::ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                            format!("{}.*", qualifier)
                        }
                        vibesql::ast::SelectItem::Wildcard { .. } => "*".to_string(),
                    })
                    .collect();

                let formatted = format_result_as_expected(&result, &column_names);
                println!("{}", formatted);
            }
            Err(e) => {
                println!("-- ⏭️ SKIP: Execution error - {:?}", e);
            }
        }
    } else {
        println!("-- ⏭️ SKIP: Not a SELECT statement");
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: cargo run --example query_runner <query>");
        eprintln!("Example: cargo run --example query_runner \"SELECT * FROM products LIMIT 5\"");
        std::process::exit(1);
    }

    let query = &args[1];
    let db = create_northwind_db();
    run_query(&db, query);
}
