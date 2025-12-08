use vibesql::catalog::{ColumnSchema, TableSchema};
/**
 * Batch Query Runner for Web Demo Examples
 *
 * Runs all 27 advanced example queries and generates expected results or SKIP comments.
 */
use vibesql::executor::SelectExecutor;
use vibesql::parser::Parser;
use vibesql::storage::{Database, Row};
use vibesql::types::{DataType, SqlValue, StringValue};

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
    let categories_data = vec![
        (1, "Beverages", "Soft drinks, coffees, teas, beers, and ales"),
        (2, "Condiments", "Sweet and savory sauces, relishes, spreads, and seasonings"),
        (3, "Confections", "Desserts, candies, and sweet breads"),
        (4, "Dairy Products", "Cheeses"),
        (5, "Grains/Cereals", "Breads, crackers, pasta, and cereal"),
        (6, "Meat/Poultry", "Prepared meats"),
        (7, "Produce", "Dried fruit and bean curd"),
        (8, "Seafood", "Seaweed and fish"),
    ];

    for (id, name, desc) in categories_data {
        categories_table
            .insert(Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Varchar(StringValue::from(name)),
                SqlValue::Varchar(StringValue::from(desc)),
            ]))
            .unwrap();
    }

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
                SqlValue::Varchar(StringValue::from(name)),
                SqlValue::Integer(cat_id),
                SqlValue::Float(price),
                SqlValue::Integer(stock),
                SqlValue::Integer(on_order),
            ]))
            .unwrap();
    }

    db
}

fn test_query(db: &Database, id: &str, title: &str, query: &str) {
    println!("\n========================================");
    println!("Testing: {} - {}", id, title);
    println!("========================================");

    let stmt = match Parser::parse_sql(query) {
        Ok(stmt) => stmt,
        Err(e) => {
            println!("❌ Parse error: {:?}", e);
            println!("Result: SKIP - Not implemented");
            return;
        }
    };

    if let vibesql::ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        match executor.execute(&select_stmt) {
            Ok(result) => {
                println!("✅ Success - {} rows", result.len());
                if !result.is_empty() && result.len() <= 10 {
                    for row in &result {
                        print!("   ");
                        for (i, val) in row.values.iter().enumerate() {
                            if i > 0 {
                                print!(" | ");
                            }
                            print!("{:?}", val);
                        }
                        println!();
                    }
                }
            }
            Err(e) => {
                println!("❌ Execution error: {:?}", e);
                println!("Result: SKIP - Not implemented or has errors");
            }
        }
    } else {
        println!("❌ Not a SELECT statement");
    }
}

fn main() {
    let db = create_northwind_db();

    let sep = "=".repeat(60);
    println!("{}", sep);
    println!("BATCH QUERY RUNNER - Testing Advanced SQL Features");
    println!("{}", sep);

    // Subqueries (3 examples)
    println!("\n### SUBQUERIES ###\n");

    test_query(
        &db,
        "sub-1",
        "Scalar subquery",
        r#"SELECT
  product_name,
  unit_price,
  (SELECT AVG(unit_price) FROM products) as avg_price
FROM products
WHERE unit_price > (SELECT AVG(unit_price) FROM products)
ORDER BY unit_price DESC"#,
    );

    test_query(
        &db,
        "sub-2",
        "IN subquery",
        r#"SELECT
  product_name,
  unit_price
FROM products
WHERE category_id IN (
  SELECT category_id
  FROM categories
  WHERE category_name IN ('Beverages', 'Condiments')
)
ORDER BY unit_price DESC"#,
    );

    test_query(
        &db,
        "sub-3",
        "Subquery in FROM clause",
        r#"SELECT
  department,
  avg_salary,
  CASE
    WHEN avg_salary > 100000 THEN 'High'
    WHEN avg_salary > 50000 THEN 'Medium'
    ELSE 'Low'
  END as salary_bracket
FROM (
  SELECT
    department,
    AVG(salary) as avg_salary
  FROM employees
  GROUP BY department
) dept_salaries
ORDER BY avg_salary DESC"#,
    );

    // CASE Expressions (3 examples)
    println!("\n### CASE EXPRESSIONS ###\n");

    test_query(
        &db,
        "case-1",
        "Simple CASE",
        r#"SELECT
  product_name,
  unit_price,
  CASE
    WHEN unit_price < 10 THEN 'Budget'
    WHEN unit_price < 50 THEN 'Standard'
    ELSE 'Premium'
  END as price_category
FROM products
ORDER BY unit_price"#,
    );

    test_query(
        &db,
        "case-2",
        "CASE in aggregation",
        r#"SELECT
  department,
  COUNT(*) as total_employees,
  COUNT(CASE WHEN salary > 100000 THEN 1 END) as high_earners,
  COUNT(CASE WHEN salary <= 100000 THEN 1 END) as other_earners
FROM employees
GROUP BY department
ORDER BY department"#,
    );

    test_query(
        &db,
        "case-3",
        "Multiple CASE expressions",
        r#"SELECT
  first_name || ' ' || last_name as employee,
  salary,
  CASE
    WHEN salary >= 200000 THEN 'Executive'
    WHEN salary >= 140000 THEN 'Senior Management'
    WHEN salary >= 100000 THEN 'Management'
    ELSE 'Staff'
  END as pay_grade,
  CASE
    WHEN department = 'Engineering' THEN 'Tech'
    WHEN department = 'Sales' THEN 'Revenue'
    ELSE 'Operations'
  END as division
FROM employees
ORDER BY salary DESC
LIMIT 10"#,
    );

    // Set Operations (3 examples)
    println!("\n### SET OPERATIONS ###\n");

    test_query(
        &db,
        "set-1",
        "UNION",
        r#"SELECT department FROM employees WHERE salary > 150000
UNION
SELECT department FROM employees WHERE title LIKE '%Director%'
ORDER BY department"#,
    );

    test_query(
        &db,
        "set-2",
        "UNION ALL",
        r#"SELECT 'Expensive' as category, product_name, unit_price
FROM products
WHERE unit_price > 50
UNION ALL
SELECT 'Cheap' as category, product_name, unit_price
FROM products
WHERE unit_price < 10
ORDER BY unit_price DESC"#,
    );

    test_query(
        &db,
        "set-3",
        "Column count in UNION",
        r#"SELECT category_name as name, 'category' as type
FROM categories
UNION
SELECT product_name as name, 'product' as type
FROM products
LIMIT 15"#,
    );

    // Recursive Queries (2 examples)
    println!("\n### RECURSIVE QUERIES ###\n");

    test_query(
        &db,
        "rec-1",
        "Employee hierarchy",
        r#"WITH RECURSIVE employee_hierarchy AS (
  SELECT
    employee_id,
    first_name,
    last_name,
    title,
    manager_id,
    1 as level,
    first_name || ' ' || last_name as path
  FROM employees
  WHERE manager_id IS NULL

  UNION ALL

  SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    e.title,
    e.manager_id,
    eh.level + 1,
    eh.path || ' > ' || e.first_name || ' ' || e.last_name
  FROM employees e
  INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT
  level,
  first_name || ' ' || last_name as employee,
  title,
  path as reporting_chain
FROM employee_hierarchy
ORDER BY level, last_name
LIMIT 15"#,
    );

    test_query(
        &db,
        "rec-2",
        "Count hierarchy levels",
        r#"WITH RECURSIVE hierarchy AS (
  SELECT
    employee_id,
    1 as level
  FROM employees
  WHERE manager_id IS NULL

  UNION ALL

  SELECT
    e.employee_id,
    h.level + 1
  FROM employees e
  INNER JOIN hierarchy h ON e.manager_id = h.employee_id
)
SELECT
  level,
  COUNT(*) as employee_count
FROM hierarchy
GROUP BY level
ORDER BY level"#,
    );

    println!("\n### WINDOW FUNCTIONS ###\n");
    test_query(
        &db,
        "window-1",
        "COUNT(*) OVER",
        r#"SELECT
  first_name || ' ' || last_name AS employee,
  department,
  salary,
  COUNT(*) OVER () AS total_employees
FROM employees
LIMIT 10"#,
    );

    println!("\n### NULL HANDLING ###\n");
    test_query(
        &db,
        "null-1",
        "COALESCE with Default Values",
        r#"SELECT
  product_name,
  unit_price,
  units_in_stock,
  COALESCE(units_in_stock, 0) AS stock_or_zero,
  COALESCE(units_on_order, 0) AS orders_or_zero
FROM products
LIMIT 10"#,
    );

    let sep = "=".repeat(60);
    println!("\n{}", sep);
    println!("Testing complete!");
    println!("{}", sep);
}
