use std::{env, fs};

use regex::Regex;
/**
 * Batch Results Generator for Web Demo Examples
 *
 * This tool processes ALL examples in examples.ts, runs those without expected
 * results, and generates formatted output that can be copied directly into the file.
 *
 * Usage: cargo run --example batch_results_generator [--filter category]
 */
use vibesql::catalog::{ColumnSchema, TableSchema};
use vibesql::parser::Parser;
use vibesql::storage::{Database, Row};
use vibesql::types::{DataType, SqlValue};

// Import database creation functions from test file
// (We'll inline them for now since we can't easily import from tests)

mod db_setup {
    use super::*;

    pub fn create_northwind_db() -> Database {
        // Copy implementation from test_web_demo_examples.rs
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
                ColumnSchema::new(
                    "unit_price".to_string(),
                    DataType::Float { precision: 53 },
                    false,
                ),
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
            (6, "Meat/Poultry", "Prepared meats"),
            (7, "Produce", "Dried fruit and bean curd"),
            (8, "Seafood", "Seaweed and fish"),
        ];

        for (id, name, desc) in categories_data {
            categories_table
                .insert(Row::new(vec![
                    SqlValue::Integer(id),
                    SqlValue::Varchar(name.to_string()),
                    SqlValue::Varchar(desc.to_string()),
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

    pub fn create_employees_db() -> Database {
        // Simplified version - full implementation in test file
        let mut db = Database::new();

        // Create employees table
        let employees_schema = TableSchema::new(
            "employees".to_string(),
            vec![
                ColumnSchema::new("employee_id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "first_name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new(
                    "last_name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new(
                    "department".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new("salary".to_string(), DataType::Float { precision: 53 }, false),
            ],
        );
        db.create_table(employees_schema).unwrap();

        db
    }

    pub fn create_university_db() -> Database {
        let mut db = Database::new();

        // Create students table
        let students_schema = TableSchema::new(
            "students".to_string(),
            vec![
                ColumnSchema::new("student_id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
                ColumnSchema::new(
                    "major".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new("gpa".to_string(), DataType::Float { precision: 53 }, false),
            ],
        );
        db.create_table(students_schema).unwrap();

        // Create courses table
        let courses_schema = TableSchema::new(
            "courses".to_string(),
            vec![
                ColumnSchema::new("course_id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "course_name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
                ColumnSchema::new(
                    "department".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new("credits".to_string(), DataType::Integer, false),
            ],
        );
        db.create_table(courses_schema).unwrap();

        // Create enrollments table
        let enrollments_schema = TableSchema::new(
            "enrollments".to_string(),
            vec![
                ColumnSchema::new("student_id".to_string(), DataType::Integer, false),
                ColumnSchema::new("course_id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "grade".to_string(),
                    DataType::Varchar { max_length: Some(2) },
                    true,
                ),
                ColumnSchema::new(
                    "semester".to_string(),
                    DataType::Varchar { max_length: Some(20) },
                    false,
                ),
            ],
        );
        db.create_table(enrollments_schema).unwrap();

        // Insert students
        let students_table = db.get_table_mut("students").unwrap();
        for i in 1..=20 {
            let (name, major, gpa) = match i {
                1 => ("Alice Johnson", "Computer Science", 3.8_f32),
                2 => ("Bob Smith", "Mathematics", 3.5_f32),
                3 => ("Carol White", "Computer Science", 3.9_f32),
                4 => ("David Brown", "Physics", 3.2_f32),
                5 => ("Eve Martinez", "Computer Science", 3.7_f32),
                6 => ("Frank Wilson", "Mathematics", 3.4_f32),
                7 => ("Grace Taylor", "Physics", 3.6_f32),
                8 => ("Henry Anderson", "Computer Science", 3.3_f32),
                9 => ("Iris Chen", "Mathematics", 3.8_f32),
                10 => ("Jack Robinson", "Physics", 3.1_f32),
                _ => ("Student", "Computer Science", 3.0_f32 + (i as f32 % 10.0) / 10.0),
            };
            students_table
                .insert(Row::new(vec![
                    SqlValue::Integer(i),
                    SqlValue::Varchar(name.to_string()),
                    SqlValue::Varchar(major.to_string()),
                    SqlValue::Float(gpa),
                ]))
                .unwrap();
        }

        // Insert courses
        let courses_table = db.get_table_mut("courses").unwrap();
        let course_data = vec![
            (101, "Introduction to Programming", "Computer Science", 4),
            (102, "Data Structures", "Computer Science", 4),
            (103, "Algorithms", "Computer Science", 4),
            (201, "Calculus I", "Mathematics", 4),
            (202, "Linear Algebra", "Mathematics", 3),
            (203, "Statistics", "Mathematics", 3),
            (301, "Classical Mechanics", "Physics", 4),
            (302, "Electromagnetism", "Physics", 4),
            (303, "Quantum Mechanics", "Physics", 3),
        ];

        for (id, name, dept, credits) in course_data {
            courses_table
                .insert(Row::new(vec![
                    SqlValue::Integer(id),
                    SqlValue::Varchar(name.to_string()),
                    SqlValue::Varchar(dept.to_string()),
                    SqlValue::Integer(credits),
                ]))
                .unwrap();
        }

        // Insert enrollments
        let enrollments_table = db.get_table_mut("enrollments").unwrap();
        let grade_distribution = vec![("A", 30), ("B", 27), ("C", 18), ("D", 6), ("F", 2)];

        let mut enrollment_id = 0;
        for (grade, count) in grade_distribution {
            for _ in 0..count {
                let student_id = (enrollment_id % 20) + 1;
                let course_id = match enrollment_id % 9 {
                    0 => 101,
                    1 => 102,
                    2 => 103,
                    3 => 201,
                    4 => 202,
                    5 => 203,
                    6 => 301,
                    7 => 302,
                    _ => 303,
                };

                enrollments_table
                    .insert(Row::new(vec![
                        SqlValue::Integer(student_id),
                        SqlValue::Integer(course_id),
                        SqlValue::Varchar(grade.to_string()),
                        SqlValue::Varchar("Fall 2024".to_string()),
                    ]))
                    .unwrap();

                enrollment_id += 1;
            }
        }

        // Add some NULL grades (in progress courses)
        for i in 0..10 {
            let student_id = (i % 20) + 1;
            let course_id = 101 + (i % 3);
            enrollments_table
                .insert(Row::new(vec![
                    SqlValue::Integer(student_id),
                    SqlValue::Integer(course_id),
                    SqlValue::Null,
                    SqlValue::Varchar("Spring 2025".to_string()),
                ]))
                .unwrap();
        }

        db
    }

    pub fn create_empty_db() -> Database {
        Database::new()
    }

    pub fn load_database(db_name: &str) -> Option<Database> {
        match db_name {
            "northwind" => Some(create_northwind_db()),
            "employees" | "company" => Some(create_employees_db()),
            "university" => Some(create_university_db()),
            "empty" => Some(create_empty_db()),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct Example {
    id: String,
    title: String,
    database: String,
    sql: String,
    has_expected: bool,
}

fn parse_examples(content: &str) -> Vec<Example> {
    let mut examples = Vec::new();

    let example_pattern = Regex::new(
        r#"(?s)\{\s*id:\s*['"]([^'"]+)['"],\s*title:\s*['"]([^'"]+)['"],\s*database:\s*['"]([^'"]+)['"],\s*sql:\s*`([^`]+)`"#
    ).unwrap();

    for cap in example_pattern.captures_iter(content) {
        let id = cap.get(1).unwrap().as_str().to_string();
        let title = cap.get(2).unwrap().as_str().to_string();
        let database = cap.get(3).unwrap().as_str().to_string();
        let sql = cap.get(4).unwrap().as_str().to_string();
        let has_expected = sql.contains("-- EXPECTED:");

        examples.push(Example { id, title, database, sql, has_expected });
    }

    examples
}

fn extract_query(sql: &str) -> String {
    sql.lines()
        .take_while(|line| !line.trim().contains("-- EXPECTED"))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

fn format_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Float(f) => {
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
    output.push_str("-- EXPECTED:\n");

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

    // Data rows (limit to first 10 for readability)
    for row in result.iter().take(10) {
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

fn main() {
    let args: Vec<String> = env::args().collect();
    let filter_category = args.get(2).map(|s| s.as_str());

    println!("\n🔧 Batch Results Generator");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Read examples.ts
    let content =
        fs::read_to_string("web-demo/src/data/examples.ts").expect("Failed to read examples.ts");

    let examples = parse_examples(&content);
    let missing: Vec<_> = examples.iter().filter(|ex| !ex.has_expected).collect();

    println!("📊 Total examples: {}", examples.len());
    println!("✅ With expected results: {}", examples.len() - missing.len());
    println!("❌ Missing expected results: {}\n", missing.len());

    if missing.is_empty() {
        println!("✨ All examples have expected results!");
        return;
    }

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let mut generated = 0;
    let mut skipped = 0;

    for example in &missing {
        // Apply category filter if provided
        if let Some(filter) = filter_category {
            if !example.id.starts_with(filter) {
                continue;
            }
        }

        println!("\n/// Example: {} - {}", example.id, example.title);
        println!("/// Database: {}\n", example.database);

        // Load database
        let db = match db_setup::load_database(&example.database) {
            Some(db) => db,
            None => {
                println!("⏭️  SKIP: Database '{}' not supported\n", example.database);
                skipped += 1;
                continue;
            }
        };

        // Extract query
        let query = extract_query(&example.sql);

        // Parse SQL
        let stmt = match Parser::parse_sql(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                println!("⏭️  SKIP: Parse error - {:?}\n", e);
                skipped += 1;
                continue;
            }
        };

        // Execute based on statement type
        let result = match stmt {
            vibesql::ast::Statement::Select(ref select_stmt) => {
                let executor = vibesql::executor::SelectExecutor::new(&db);
                match executor.execute(select_stmt) {
                    Ok(rows) => {
                        // Extract column names
                        let column_names: Vec<String> = select_stmt
                            .select_list
                            .iter()
                            .enumerate()
                            .map(|(i, item)| match item {
                                vibesql::ast::SelectItem::Expression {
                                    alias: Some(alias), ..
                                } => alias.clone(),
                                vibesql::ast::SelectItem::Expression { alias: None, expr } => {
                                    // Try to extract column name from expression
                                    match expr {
                                        vibesql::ast::Expression::ColumnRef { column, .. } => {
                                            column.clone()
                                        }
                                        _ => format!("col{}", i + 1),
                                    }
                                }
                                vibesql::ast::SelectItem::QualifiedWildcard {
                                    qualifier,
                                    alias: _,
                                } => {
                                    format!("{}.*", qualifier)
                                }
                                vibesql::ast::SelectItem::Wildcard { alias: _ } => "*".to_string(),
                            })
                            .collect();

                        Some((rows, column_names))
                    }
                    Err(e) => {
                        println!("⏭️  SKIP: Execution error - {:?}\n", e);
                        skipped += 1;
                        continue;
                    }
                }
            }
            _ => {
                println!("⏭️  SKIP: Not a SELECT statement\n");
                skipped += 1;
                continue;
            }
        };

        if let Some((rows, column_names)) = result {
            let formatted = format_result_as_expected(&rows, &column_names);
            println!("{}\n", formatted);
            generated += 1;
        }
    }

    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 Summary");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("✅ Generated: {}", generated);
    println!("⏭️  Skipped: {}", skipped);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
}
