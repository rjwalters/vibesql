//! Database fixtures for web demo SQL tests
//!
//! This module provides pre-configured test databases including:
//! - Northwind: Categories and products
//! - Employees: Company structure with departments, projects, and employees
//! - University: Students, courses, and enrollments
//! - Empty: Minimal empty database for testing

#![allow(dead_code)]

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Create a Northwind database for testing
pub fn create_northwind_db() -> Database {
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
            ColumnSchema::new("UNITS_IN_STOCK".to_string(), DataType::Integer, false),
            ColumnSchema::new("UNITS_ON_ORDER".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(products_schema).unwrap();

    // Insert sample data
    let categories_table = db.get_table_mut("CATEGORIES").unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Beverages")),
            SqlValue::Varchar(arcstr::ArcStr::from("Soft drinks, coffees, teas, beers, and ales")),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Condiments")),
            SqlValue::Varchar(arcstr::ArcStr::from("Sweet and savory sauces")),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Confections")),
            SqlValue::Varchar(arcstr::ArcStr::from("Desserts and candies")),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Dairy Products")),
            SqlValue::Varchar(arcstr::ArcStr::from("Cheeses")),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Varchar(arcstr::ArcStr::from("Meat/Poultry")),
            SqlValue::Varchar(arcstr::ArcStr::from("Prepared meats")),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Varchar(arcstr::ArcStr::from("Produce")),
            SqlValue::Varchar(arcstr::ArcStr::from("Dried fruit and bean curd")),
        ]))
        .unwrap();
    categories_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Varchar(arcstr::ArcStr::from("Seafood")),
            SqlValue::Varchar(arcstr::ArcStr::from("Seaweed and fish")),
        ]))
        .unwrap();

    let products_table = db.get_table_mut("PRODUCTS").unwrap();

    // Insert products - matching the expected results in examples
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Chai")),
            SqlValue::Integer(1),
            SqlValue::Float(18.0),
            SqlValue::Integer(39),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Chang")),
            SqlValue::Integer(1),
            SqlValue::Float(19.0),
            SqlValue::Integer(17),
            SqlValue::Integer(40),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Aniseed Syrup")),
            SqlValue::Integer(2),
            SqlValue::Float(10.0),
            SqlValue::Integer(13),
            SqlValue::Integer(70),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Chef Anton's Cajun Seasoning")),
            SqlValue::Integer(2),
            SqlValue::Float(22.0),
            SqlValue::Integer(53),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("Chef Anton's Gumbo Mix")),
            SqlValue::Integer(2),
            SqlValue::Float(21.35),
            SqlValue::Integer(0),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    // Add more products for WHERE clause examples
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Varchar(arcstr::ArcStr::from("Mishi Kobe Niku")),
            SqlValue::Integer(3),
            SqlValue::Float(97.0),
            SqlValue::Integer(29),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Varchar(arcstr::ArcStr::from("Sir Rodney's Marmalade")),
            SqlValue::Integer(3),
            SqlValue::Float(81.0),
            SqlValue::Integer(40),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Varchar(arcstr::ArcStr::from("Carnarvon Tigers")),
            SqlValue::Integer(3),
            SqlValue::Float(62.5),
            SqlValue::Integer(42),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(9),
            SqlValue::Varchar(arcstr::ArcStr::from("Northwoods Cranberry Sauce")),
            SqlValue::Integer(2),
            SqlValue::Float(40.0),
            SqlValue::Integer(6),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(10),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice Mutton")),
            SqlValue::Integer(3),
            SqlValue::Float(39.0),
            SqlValue::Integer(0),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(11),
            SqlValue::Varchar(arcstr::ArcStr::from("Queso Manchego La Pastora")),
            SqlValue::Integer(2),
            SqlValue::Float(38.0),
            SqlValue::Integer(86),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(12),
            SqlValue::Varchar(arcstr::ArcStr::from("Ikura")),
            SqlValue::Integer(3),
            SqlValue::Float(31.0),
            SqlValue::Integer(31),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(13),
            SqlValue::Varchar(arcstr::ArcStr::from("Grandma's Boysenberry Spread")),
            SqlValue::Integer(2),
            SqlValue::Float(25.0),
            SqlValue::Integer(120),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(14),
            SqlValue::Varchar(arcstr::ArcStr::from("Tofu")),
            SqlValue::Integer(1),
            SqlValue::Float(23.25),
            SqlValue::Integer(35),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(15),
            SqlValue::Varchar(arcstr::ArcStr::from("Queso Cabrales")),
            SqlValue::Integer(2),
            SqlValue::Float(21.0),
            SqlValue::Integer(22),
            SqlValue::Integer(30),
        ]))
        .unwrap();

    // Add products for categories 4, 6, 7, 8 to ensure DISTINCT returns 7 categories
    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(16),
            SqlValue::Varchar(arcstr::ArcStr::from("Pavlova")),
            SqlValue::Integer(3),
            SqlValue::Float(17.45),
            SqlValue::Integer(29),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(17),
            SqlValue::Varchar(arcstr::ArcStr::from("Raclette Courdavault")),
            SqlValue::Integer(4),
            SqlValue::Float(15.0),
            SqlValue::Integer(79),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(18),
            SqlValue::Varchar(arcstr::ArcStr::from("Perth Pasties")),
            SqlValue::Integer(6),
            SqlValue::Float(12.8),
            SqlValue::Integer(0),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(19),
            SqlValue::Varchar(arcstr::ArcStr::from("Manjimup Dried Apples")),
            SqlValue::Integer(7),
            SqlValue::Float(18.0),
            SqlValue::Integer(20),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    products_table
        .insert(Row::new(vec![
            SqlValue::Integer(20),
            SqlValue::Varchar(arcstr::ArcStr::from("Inlagd Sill")),
            SqlValue::Integer(8),
            SqlValue::Float(19.0),
            SqlValue::Integer(112),
            SqlValue::Integer(0),
        ]))
        .unwrap();

    db
}

/// Create an Employees database for testing
pub fn create_employees_db() -> Database {
    let mut db = Database::new();

    // Create departments table for company examples
    let departments_schema = TableSchema::new(
        "DEPARTMENTS".to_string(),
        vec![
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "DEPT_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LOCATION".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(departments_schema).unwrap();

    // Create projects table for company examples
    let projects_schema = TableSchema::new(
        "PROJECTS".to_string(),
        vec![
            ColumnSchema::new("PROJECT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "PROJECT_NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, true),
            ColumnSchema::new("BUDGET".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(projects_schema).unwrap();

    // Create employees table
    let employees_schema = TableSchema::new(
        "EMPLOYEES".to_string(),
        vec![
            ColumnSchema::new("EMPLOYEE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("EMP_ID".to_string(), DataType::Integer, false), /* alias for company examples */
            ColumnSchema::new(
                "FIRST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "LAST_NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ), // full name for company examples
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, true), /* for company
                                                                                * examples */
            ColumnSchema::new(
                "TITLE".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("SALARY".to_string(), DataType::Float { precision: 53 }, false),
            ColumnSchema::new(
                "HIRE_DATE".to_string(),
                DataType::Varchar { max_length: Some(20) },
                true,
            ), // for datetime examples
            ColumnSchema::new("MANAGER_ID".to_string(), DataType::Integer, true), // nullable
        ],
    );
    db.create_table(employees_schema).unwrap();

    // Insert departments
    let departments_table = db.get_table_mut("DEPARTMENTS").unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Varchar(arcstr::ArcStr::from("San Francisco")),
        ]))
        .unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Varchar(arcstr::ArcStr::from("New York")),
        ]))
        .unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Marketing")),
            SqlValue::Varchar(arcstr::ArcStr::from("Los Angeles")),
        ]))
        .unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Human Resources")),
            SqlValue::Varchar(arcstr::ArcStr::from("Chicago")),
        ]))
        .unwrap();
    departments_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("Operations")),
            SqlValue::Varchar(arcstr::ArcStr::from("Seattle")),
        ]))
        .unwrap();

    // Insert projects
    let projects_table = db.get_table_mut("PROJECTS").unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Cloud Migration")),
            SqlValue::Integer(1),
            SqlValue::Integer(500000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Mobile App")),
            SqlValue::Integer(1),
            SqlValue::Integer(350000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Data Analytics Platform")),
            SqlValue::Integer(1),
            SqlValue::Integer(220000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Marketing Campaign")),
            SqlValue::Integer(3),
            SqlValue::Integer(150000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("Brand Redesign")),
            SqlValue::Integer(3),
            SqlValue::Integer(125000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Varchar(arcstr::ArcStr::from("CRM Implementation")),
            SqlValue::Integer(2),
            SqlValue::Integer(150000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Varchar(arcstr::ArcStr::from("HR System Upgrade")),
            SqlValue::Integer(4),
            SqlValue::Integer(75000),
        ]))
        .unwrap();
    projects_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Varchar(arcstr::ArcStr::from("Warehouse Automation")),
            SqlValue::Integer(5),
            SqlValue::Integer(410000),
        ]))
        .unwrap();

    let employees_table = db.get_table_mut("EMPLOYEES").unwrap();

    // Insert employees data matching expected results
    // From string-2 expected: Alice Johnson, Bob Smith, Carol White, David Brown, Eve Martinez,
    // Frank Wilson, Grace Taylor, Henry Anderson
    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            SqlValue::Varchar(arcstr::ArcStr::from("Johnson")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice Johnson")),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Senior Engineer")),
            SqlValue::Float(95000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2020-01-15")),
            SqlValue::Null,
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            SqlValue::Varchar(arcstr::ArcStr::from("Smith")),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob Smith")),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales Manager")),
            SqlValue::Float(85000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2019-03-22")),
            SqlValue::Null,
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Carol")),
            SqlValue::Varchar(arcstr::ArcStr::from("White")),
            SqlValue::Varchar(arcstr::ArcStr::from("Carol White")),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineer")),
            SqlValue::Float(75000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2021-06-10")),
            SqlValue::Integer(1),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("David")),
            SqlValue::Varchar(arcstr::ArcStr::from("Brown")),
            SqlValue::Varchar(arcstr::ArcStr::from("David Brown")),
            SqlValue::Varchar(arcstr::ArcStr::from("Marketing")),
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Marketing Specialist")),
            SqlValue::Float(65000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2022-01-05")),
            SqlValue::Integer(7),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("Eve")),
            SqlValue::Varchar(arcstr::ArcStr::from("Martinez")),
            SqlValue::Varchar(arcstr::ArcStr::from("Eve Martinez")),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Senior Engineer")),
            SqlValue::Float(110000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2018-09-12")),
            SqlValue::Null,
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Integer(6),
            SqlValue::Varchar(arcstr::ArcStr::from("Frank")),
            SqlValue::Varchar(arcstr::ArcStr::from("Wilson")),
            SqlValue::Varchar(arcstr::ArcStr::from("Frank Wilson")),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales Representative")),
            SqlValue::Float(55000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2021-11-20")),
            SqlValue::Integer(2),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Integer(7),
            SqlValue::Varchar(arcstr::ArcStr::from("Grace")),
            SqlValue::Varchar(arcstr::ArcStr::from("Taylor")),
            SqlValue::Varchar(arcstr::ArcStr::from("Grace Taylor")),
            SqlValue::Varchar(arcstr::ArcStr::from("Marketing")),
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Marketing Manager")),
            SqlValue::Float(90000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2019-07-08")),
            SqlValue::Null,
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Integer(8),
            SqlValue::Varchar(arcstr::ArcStr::from("Henry")),
            SqlValue::Varchar(arcstr::ArcStr::from("Anderson")),
            SqlValue::Varchar(arcstr::ArcStr::from("Henry Anderson")),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineer")),
            SqlValue::Float(80000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2020-12-01")),
            SqlValue::Integer(1),
        ]))
        .unwrap();

    // Add additional employees with 'a' in first_name for string-7
    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(9),
            SqlValue::Integer(9),
            SqlValue::Varchar(arcstr::ArcStr::from("Maria")),
            SqlValue::Varchar(arcstr::ArcStr::from("Clark")),
            SqlValue::Varchar(arcstr::ArcStr::from("Maria Clark")),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales Representative")),
            SqlValue::Float(58000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2022-04-15")),
            SqlValue::Integer(2),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(10),
            SqlValue::Integer(10),
            SqlValue::Varchar(arcstr::ArcStr::from("Nathan")),
            SqlValue::Varchar(arcstr::ArcStr::from("Lewis")),
            SqlValue::Varchar(arcstr::ArcStr::from("Nathan Lewis")),
            SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Senior Engineer")),
            SqlValue::Float(105000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2019-05-18")),
            SqlValue::Integer(5),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(11),
            SqlValue::Integer(11),
            SqlValue::Varchar(arcstr::ArcStr::from("Olivia")),
            SqlValue::Varchar(arcstr::ArcStr::from("Walker")),
            SqlValue::Varchar(arcstr::ArcStr::from("Olivia Walker")),
            SqlValue::Varchar(arcstr::ArcStr::from("Marketing")),
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Marketing Specialist")),
            SqlValue::Float(67000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2021-08-22")),
            SqlValue::Integer(7),
        ]))
        .unwrap();

    employees_table
        .insert(Row::new(vec![
            SqlValue::Integer(12),
            SqlValue::Integer(12),
            SqlValue::Varchar(arcstr::ArcStr::from("Paul")),
            SqlValue::Varchar(arcstr::ArcStr::from("Hall")),
            SqlValue::Varchar(arcstr::ArcStr::from("Paul Hall")),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Sales Representative")),
            SqlValue::Float(56000.0),
            SqlValue::Varchar(arcstr::ArcStr::from("2023-02-10")),
            SqlValue::Integer(2),
        ]))
        .unwrap();

    db
}

/// Create a University database for testing
pub fn create_university_db() -> Database {
    let mut db = Database::new();

    // Create students table
    let students_schema = TableSchema::new(
        "STUDENTS".to_string(),
        vec![
            ColumnSchema::new("STUDENT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new(
                "MAJOR".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("GPA".to_string(), DataType::Float { precision: 53 }, false),
        ],
    );
    db.create_table(students_schema).unwrap();

    // Create courses table
    let courses_schema = TableSchema::new(
        "COURSES".to_string(),
        vec![
            ColumnSchema::new("COURSE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "COURSE_NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new(
                "DEPARTMENT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("CREDITS".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(courses_schema).unwrap();

    // Create enrollments table
    let enrollments_schema = TableSchema::new(
        "ENROLLMENTS".to_string(),
        vec![
            ColumnSchema::new("STUDENT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("COURSE_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("GRADE".to_string(), DataType::Varchar { max_length: Some(2) }, true), /* nullable */
            ColumnSchema::new(
                "SEMESTER".to_string(),
                DataType::Varchar { max_length: Some(20) },
                false,
            ),
        ],
    );
    db.create_table(enrollments_schema).unwrap();

    // Insert students
    let students_table = db.get_table_mut("STUDENTS").unwrap();
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
                SqlValue::Varchar(arcstr::ArcStr::from(name)),
                SqlValue::Varchar(arcstr::ArcStr::from(major)),
                SqlValue::Float(gpa),
            ]))
            .unwrap();
    }

    // Insert courses
    let courses_table = db.get_table_mut("COURSES").unwrap();
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
                SqlValue::Varchar(arcstr::ArcStr::from(name)),
                SqlValue::Varchar(arcstr::ArcStr::from(dept)),
                SqlValue::Integer(credits),
            ]))
            .unwrap();
    }

    // Insert enrollments - matching grade distribution from uni-4:
    // A: 30, B: 27, C: 18, D: 6, F: 2 (total: 83 non-NULL grades)
    let enrollments_table = db.get_table_mut("ENROLLMENTS").unwrap();

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
                    SqlValue::Varchar(arcstr::ArcStr::from(grade)),
                    SqlValue::Varchar(arcstr::ArcStr::from("Fall 2024")),
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
                SqlValue::Varchar(arcstr::ArcStr::from("Spring 2025")),
            ]))
            .unwrap();
    }

    db
}

/// Create an empty database for testing
pub fn create_empty_db() -> Database {
    Database::new()
}

/// Load the appropriate database for a given example
pub fn load_database(db_name: &str) -> Option<Database> {
    match db_name {
        "northwind" => Some(create_northwind_db()),
        "employees" | "company" => Some(create_employees_db()),
        "university" => Some(create_university_db()),
        "empty" => Some(create_empty_db()),
        _ => None,
    }
}
