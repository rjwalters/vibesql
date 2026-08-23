/**
 * Sample Database Definitions
 *
 * Pre-configured sample databases for Core SQL:1999 feature demonstration.
 * Each database includes schema definitions and sample data initialization.
 */

import type { Database } from '../db/types'

export interface SampleTable {
  name: string
  createSql: string
  insertSql: string[]
}

export interface SampleDatabase {
  id: string
  name: string
  description: string
  tables: SampleTable[]
}

/**
 * Employees Sample Database
 * Purpose: Basic DML, JOINs, simple aggregates, string functions
 */
export const employeesDatabase: SampleDatabase = {
  id: 'employees',
  name: 'Employees',
  description: 'Sample employee data for basic queries, JOINs, and string functions',
  tables: [
    {
      name: 'employees',
      createSql: `CREATE TABLE employees (
        id INTEGER,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        department VARCHAR(50),
        title VARCHAR(50),
        salary INTEGER,
        hire_date VARCHAR(20)
      );`,
      insertSql: [
        "INSERT INTO employees VALUES (1, 'Alice', 'Johnson', 'Engineering', 'Senior Software Engineer', 95000, '2020-01-15');",
        "INSERT INTO employees VALUES (2, 'Bob', 'Smith', 'Engineering', 'Software Engineer', 87000, '2021-03-22');",
        "INSERT INTO employees VALUES (3, 'Carol', 'White', 'Sales', 'Sales Representative', 72000, '2022-06-10');",
        "INSERT INTO employees VALUES (4, 'David', 'Brown', 'Sales', 'Sales Representative', 68000, '2019-11-05');",
        "INSERT INTO employees VALUES (5, 'Eve', 'Martinez', 'HR', 'HR Manager', 65000, '2020-08-17');",
        "INSERT INTO employees VALUES (6, 'Frank', 'Wilson', 'Engineering', 'Senior Software Engineer', 92000, '2018-02-28');",
        "INSERT INTO employees VALUES (7, 'Grace', 'Taylor', 'Engineering', 'Software Engineer', 88000, '2021-04-12');",
        "INSERT INTO employees VALUES (8, 'Henry', 'Anderson', 'Sales', 'Sales Director', 74000, '2022-01-20');",
        "INSERT INTO employees VALUES (9, 'Iris', 'Thomas', 'HR', 'Recruiter', 71000, '2020-09-15');",
        "INSERT INTO employees VALUES (10, 'Jack', 'Moore', 'Marketing', 'Marketing Manager', 79000, '2021-07-03');",
        "INSERT INTO employees VALUES (11, 'Karen', 'Jackson', 'Marketing', 'Content Specialist', 82000, '2019-12-18');",
        "INSERT INTO employees VALUES (12, 'Leo', 'Harris', 'Engineering', 'Senior Software Engineer', 91000, '2020-05-09');",
        "INSERT INTO employees VALUES (13, 'Maria', 'Clark', 'Sales', 'Sales Representative', 76000, '2022-03-15');",
        "INSERT INTO employees VALUES (14, 'Nathan', 'Lewis', 'HR', 'Recruiter', 69000, '2021-11-22');",
        "INSERT INTO employees VALUES (15, 'Olivia', 'Walker', 'Engineering', 'Engineering Director', 93000, '2019-08-07');",
        "INSERT INTO employees VALUES (16, 'Paul', 'Hall', 'Marketing', 'Content Specialist', 77000, '2020-10-30');",
        "INSERT INTO employees VALUES (17, 'Quinn', 'Allen', 'Sales', 'Sales Representative', 73000, '2022-02-14');",
        "INSERT INTO employees VALUES (18, 'Rachel', 'Young', 'Engineering', 'Software Engineer', 89000, '2021-06-19');",
        "INSERT INTO employees VALUES (19, 'Sam', 'King', 'HR', 'Recruiter', 70000, '2020-12-05');",
        "INSERT INTO employees VALUES (20, 'Tina', 'Wright', 'Marketing', 'Marketing Manager', 80000, '2019-09-28');",
      ],
    },
  ],
}

/**
 * Northwind Sample Database (Simplified)
 * Purpose: Complex JOINs, aggregates, subqueries
 *
 * NOTE ON DATA DIVERGENCE (#6518): This `products` table is seeded
 * independently from `tests/common/web_demo_helpers/database_fixtures.rs`'s
 * `create_northwind_db()`, which is a separate, hand-authored northwind
 * fixture used only by the Rust `web_demo_*` integration tests. From
 * `product_id` 6 onward the two datasets intentionally differ in row
 * ordering, membership, and column values (and the Rust fixture omits
 * `customers`/`orders` entirely) — they are NOT meant to be kept in sync.
 * `web-demo/src/data/examples/*.json`'s `expectedRows` blocks are ground-
 * truthed against the **Rust fixture**, not this file, because the Rust
 * side is the only consumer that does a strict, ordered, cell-by-cell
 * comparison (`validate_results()` in
 * `tests/common/web_demo_helpers/mod.rs`, exercised by
 * `test_basic_sql_examples`). Do NOT recompute `expectedRows` against the
 * data in this file — see the mirrored note on `create_northwind_db()` for
 * the other half of this contract.
 */
export const northwindDatabase: SampleDatabase = {
  id: 'northwind',
  name: 'Northwind',
  description: 'E-commerce database with products, orders, and customers',
  tables: [
    {
      name: 'categories',
      createSql: `CREATE TABLE categories (
        category_id INTEGER,
        category_name VARCHAR(50),
        description VARCHAR(200)
      );`,
      insertSql: [
        "INSERT INTO categories VALUES (1, 'Beverages', 'Soft drinks, coffees, teas, beers, and ales');",
        "INSERT INTO categories VALUES (2, 'Condiments', 'Sweet and savory sauces, relishes, spreads, and seasonings');",
        "INSERT INTO categories VALUES (3, 'Confections', 'Desserts, candies, and sweet breads');",
        "INSERT INTO categories VALUES (4, 'Dairy Products', 'Cheeses');",
        "INSERT INTO categories VALUES (5, 'Grains/Cereals', 'Breads, crackers, pasta, and cereal');",
        "INSERT INTO categories VALUES (6, 'Meat/Poultry', 'Prepared meats');",
        "INSERT INTO categories VALUES (7, 'Produce', 'Dried fruit and bean curd');",
        "INSERT INTO categories VALUES (8, 'Seafood', 'Seaweed and fish');",
      ],
    },
    {
      name: 'customers',
      createSql: `CREATE TABLE customers (
        customer_id INTEGER,
        company_name VARCHAR(100),
        contact_name VARCHAR(50),
        city VARCHAR(50),
        country VARCHAR(50)
      );`,
      insertSql: [
        "INSERT INTO customers VALUES (1, 'Alfreds Futterkiste', 'Maria Anders', 'Berlin', 'Germany');",
        "INSERT INTO customers VALUES (2, 'Ana Trujillo', 'Ana Trujillo', 'México D.F.', 'Mexico');",
        "INSERT INTO customers VALUES (3, 'Antonio Moreno', 'Antonio Moreno', 'México D.F.', 'Mexico');",
        "INSERT INTO customers VALUES (4, 'Around the Horn', 'Thomas Hardy', 'London', 'UK');",
        "INSERT INTO customers VALUES (5, 'Berglunds snabbköp', 'Christina Berglund', 'Luleå', 'Sweden');",
      ],
    },
    {
      name: 'products',
      createSql: `CREATE TABLE products (
        product_id INTEGER,
        product_name VARCHAR(100),
        category_id INTEGER,
        unit_price FLOAT,
        units_in_stock INTEGER,
        units_on_order INTEGER
      );`,
      insertSql: [
        "INSERT INTO products VALUES (1, 'Chai', 1, 18.0, 39, 0);",
        "INSERT INTO products VALUES (2, 'Chang', 1, 19.0, 17, 40);",
        "INSERT INTO products VALUES (3, 'Aniseed Syrup', 2, 10.0, 13, 70);",
        "INSERT INTO products VALUES (4, 'Chef Anton''s Cajun Seasoning', 2, 22.0, 53, 0);",
        "INSERT INTO products VALUES (5, 'Chef Anton''s Gumbo Mix', 2, 21.35, 0, 0);",
        "INSERT INTO products VALUES (6, 'Grandma''s Boysenberry Spread', 2, 25.0, 120, 0);",
        "INSERT INTO products VALUES (7, 'Uncle Bob''s Organic Dried Pears', 7, 30.0, 15, 0);",
        "INSERT INTO products VALUES (8, 'Northwoods Cranberry Sauce', 2, 40.0, 6, 0);",
        "INSERT INTO products VALUES (9, 'Mishi Kobe Niku', 6, 97.0, 29, 0);",
        "INSERT INTO products VALUES (10, 'Ikura', 8, 31.0, 31, 0);",
        "INSERT INTO products VALUES (11, 'Queso Cabrales', 4, 21.0, 22, 30);",
        "INSERT INTO products VALUES (12, 'Queso Manchego La Pastora', 4, 38.0, 86, 0);",
        "INSERT INTO products VALUES (13, 'Konbu', 8, 6.0, 24, 0);",
        "INSERT INTO products VALUES (14, 'Tofu', 7, 23.25, 35, 0);",
        "INSERT INTO products VALUES (15, 'Genen Shouyu', 2, 15.5, 39, 0);",
        "INSERT INTO products VALUES (16, 'Pavlova', 3, 17.45, 29, 0);",
        "INSERT INTO products VALUES (17, 'Alice Mutton', 6, 39.0, 0, 0);",
        "INSERT INTO products VALUES (18, 'Carnarvon Tigers', 8, 62.5, 42, 0);",
        "INSERT INTO products VALUES (19, 'Teatime Chocolate Biscuits', 3, 9.2, 25, 0);",
        "INSERT INTO products VALUES (20, 'Sir Rodney''s Marmalade', 3, 81.0, 40, 0);",
      ],
    },
    {
      name: 'orders',
      createSql: `CREATE TABLE orders (
        order_id INTEGER,
        customer_id INTEGER,
        order_date VARCHAR(20),
        total_amount FLOAT
      );`,
      insertSql: [
        "INSERT INTO orders VALUES (1, 1, '2025-01-15', 145.50);",
        "INSERT INTO orders VALUES (2, 2, '2025-01-16', 89.00);",
        "INSERT INTO orders VALUES (3, 1, '2025-01-17', 220.75);",
        "INSERT INTO orders VALUES (4, 3, '2025-01-18', 156.30);",
        "INSERT INTO orders VALUES (5, 4, '2025-01-19', 98.00);",
        "INSERT INTO orders VALUES (6, 2, '2025-01-20', 305.25);",
      ],
    },
  ],
}

/**
 * Company Sample Database
 * Purpose: Multi-table JOINs and business analytics
 */
export const companyDatabase: SampleDatabase = {
  id: 'company',
  name: 'Company',
  description: 'Corporate database with departments, employees, and projects',
  tables: [
    {
      name: 'departments',
      createSql: `CREATE TABLE departments (
        dept_id INTEGER,
        dept_name VARCHAR(100),
        location VARCHAR(100)
      );`,
      insertSql: [
        "INSERT INTO departments VALUES (1, 'Engineering', 'San Francisco');",
        "INSERT INTO departments VALUES (2, 'Sales', 'New York');",
        "INSERT INTO departments VALUES (3, 'Marketing', 'Los Angeles');",
        "INSERT INTO departments VALUES (4, 'Human Resources', 'Chicago');",
        "INSERT INTO departments VALUES (5, 'Operations', 'Seattle');",
      ],
    },
    {
      name: 'employees',
      createSql: `CREATE TABLE employees (
        emp_id INTEGER,
        name VARCHAR(100),
        dept_id INTEGER,
        salary INTEGER,
        hire_date VARCHAR(20)
      );`,
      insertSql: [
        "INSERT INTO employees VALUES (1, 'Alice Chen', 1, 125000, '2020-01-15');",
        "INSERT INTO employees VALUES (2, 'Bob Martinez', 1, 110000, '2021-03-22');",
        "INSERT INTO employees VALUES (3, 'Carol Williams', 1, 95000, '2022-06-10');",
        "INSERT INTO employees VALUES (4, 'David Thompson', 2, 85000, '2019-11-05');",
        "INSERT INTO employees VALUES (5, 'Emma Davis', 2, 92000, '2020-08-17');",
        "INSERT INTO employees VALUES (6, 'Frank Lee', 2, 78000, '2023-02-28');",
        "INSERT INTO employees VALUES (7, 'Grace Park', 3, 88000, '2021-04-12');",
        "INSERT INTO employees VALUES (8, 'Henry Wilson', 3, 82000, '2022-01-20');",
        "INSERT INTO employees VALUES (9, 'Iris Brown', 4, 75000, '2020-09-15');",
        "INSERT INTO employees VALUES (10, 'Jack Robinson', 4, 71000, '2022-11-08');",
        "INSERT INTO employees VALUES (11, 'Karen Miller', 5, 68000, '2021-07-03');",
        "INSERT INTO employees VALUES (12, 'Leo Garcia', 5, 72000, '2022-05-18');",
        "INSERT INTO employees VALUES (13, 'Maria Rodriguez', 1, 118000, '2019-02-10');",
        "INSERT INTO employees VALUES (14, 'Nathan Taylor', 2, 89000, '2020-12-01');",
        "INSERT INTO employees VALUES (15, 'Olivia Johnson', 3, 91000, '2021-10-25');",
        "INSERT INTO employees VALUES (16, 'Paul Anderson', 5, 70000, '2023-03-14');",
        "INSERT INTO employees VALUES (17, 'Quinn Moore', NULL, 95000, '2023-01-09');",
        "INSERT INTO employees VALUES (18, 'Rachel White', NULL, 88000, '2023-04-20');",
        "INSERT INTO employees VALUES (19, 'Sam Harris', 1, NULL, '2023-06-05');",
        "INSERT INTO employees VALUES (20, 'Tina Clark', 2, NULL, '2023-07-12');",
      ],
    },
    {
      name: 'projects',
      createSql: `CREATE TABLE projects (
        project_id INTEGER,
        project_name VARCHAR(100),
        dept_id INTEGER,
        budget INTEGER
      );`,
      insertSql: [
        "INSERT INTO projects VALUES (1, 'Cloud Migration', 1, 500000);",
        "INSERT INTO projects VALUES (2, 'Mobile App Redesign', 1, 250000);",
        "INSERT INTO projects VALUES (3, 'Q4 Sales Campaign', 2, 150000);",
        "INSERT INTO projects VALUES (4, 'Customer Portal', 1, 320000);",
        "INSERT INTO projects VALUES (5, 'Brand Refresh', 3, 180000);",
        "INSERT INTO projects VALUES (6, 'Market Research Initiative', 3, 95000);",
        "INSERT INTO projects VALUES (7, 'Employee Wellness Program', 4, 75000);",
        "INSERT INTO projects VALUES (8, 'Supply Chain Optimization', 5, 410000);",
        "INSERT INTO projects VALUES (9, 'Innovation Lab', NULL, 200000);",
        "INSERT INTO projects VALUES (10, 'Security Audit', NULL, NULL);",
      ],
    },
  ],
}

/**
 * University Sample Database
 * Purpose: Complex relationships, correlated subqueries, aggregates
 */
export const universityDatabase: SampleDatabase = {
  id: 'university',
  name: 'University',
  description: 'Academic database with students, courses, enrollments, and professors',
  tables: [
    {
      name: 'students',
      createSql: `CREATE TABLE students (
        student_id INTEGER,
        name VARCHAR(100),
        major VARCHAR(50),
        gpa FLOAT
      );`,
      insertSql: [
        // Computer Science majors
        "INSERT INTO students VALUES (1001, 'Emma Wilson', 'Computer Science', 3.8);",
        "INSERT INTO students VALUES (1002, 'Liam Chen', 'Computer Science', 3.6);",
        "INSERT INTO students VALUES (1003, 'Olivia Rodriguez', 'Computer Science', 3.9);",
        "INSERT INTO students VALUES (1004, 'Noah Anderson', 'Computer Science', 3.4);",
        "INSERT INTO students VALUES (1005, 'Ava Martinez', 'Computer Science', 3.7);",
        "INSERT INTO students VALUES (1006, 'Ethan Brown', 'Computer Science', 2.9);",
        "INSERT INTO students VALUES (1007, 'Sophia Taylor', 'Computer Science', 3.5);",
        "INSERT INTO students VALUES (1008, 'Mason Garcia', 'Computer Science', NULL);", // Probationary
        // Mathematics majors
        "INSERT INTO students VALUES (1009, 'Isabella Lee', 'Mathematics', 3.9);",
        "INSERT INTO students VALUES (1010, 'James White', 'Mathematics', 3.6);",
        "INSERT INTO students VALUES (1011, 'Charlotte Kim', 'Mathematics', 3.8);",
        "INSERT INTO students VALUES (1012, 'Benjamin Clark', 'Mathematics', 3.3);",
        "INSERT INTO students VALUES (1013, 'Amelia Lewis', 'Mathematics', 3.7);",
        "INSERT INTO students VALUES (1014, 'Lucas Hall', 'Mathematics', 2.8);",
        "INSERT INTO students VALUES (1015, 'Mia Young', 'Mathematics', 3.4);",
        // Physics majors
        "INSERT INTO students VALUES (1016, 'Henry Allen', 'Physics', 3.5);",
        "INSERT INTO students VALUES (1017, 'Harper Scott', 'Physics', 3.9);",
        "INSERT INTO students VALUES (1018, 'Alexander King', 'Physics', 3.2);",
        "INSERT INTO students VALUES (1019, 'Evelyn Wright', 'Physics', 3.6);",
        "INSERT INTO students VALUES (1020, 'Daniel Green', 'Physics', 3.1);",
        "INSERT INTO students VALUES (1021, 'Abigail Hill', 'Physics', 3.7);",
        "INSERT INTO students VALUES (1022, 'Michael Adams', 'Physics', NULL);", // Probationary
        // Biology majors
        "INSERT INTO students VALUES (1023, 'Emily Baker', 'Biology', 3.4);",
        "INSERT INTO students VALUES (1024, 'William Nelson', 'Biology', 3.8);",
        "INSERT INTO students VALUES (1025, 'Madison Mitchell', 'Biology', 3.2);",
        "INSERT INTO students VALUES (1026, 'Jacob Turner', 'Biology', 3.6);",
        "INSERT INTO students VALUES (1027, 'Ella Parker', 'Biology', 3.9);",
        "INSERT INTO students VALUES (1028, 'Joseph Edwards', 'Biology', 2.7);",
        "INSERT INTO students VALUES (1029, 'Avery Collins', 'Biology', 3.5);",
        // English majors
        "INSERT INTO students VALUES (1030, 'David Stewart', 'English', 3.3);",
        "INSERT INTO students VALUES (1031, 'Sofia Morris', 'English', 3.7);",
        "INSERT INTO students VALUES (1032, 'Matthew Rogers', 'English', 3.1);",
        "INSERT INTO students VALUES (1033, 'Grace Reed', 'English', 3.8);",
        "INSERT INTO students VALUES (1034, 'Samuel Cook', 'English', 2.9);",
        "INSERT INTO students VALUES (1035, 'Chloe Morgan', 'English', 3.4);",
        "INSERT INTO students VALUES (1036, 'Christopher Bell', 'English', 3.6);",
        // History majors
        "INSERT INTO students VALUES (1037, 'Victoria Murphy', 'History', 3.2);",
        "INSERT INTO students VALUES (1038, 'Andrew Bailey', 'History', 3.5);",
        "INSERT INTO students VALUES (1039, 'Zoe Rivera', 'History', 3.9);",
        "INSERT INTO students VALUES (1040, 'Joshua Cooper', 'History', 3.0);",
        "INSERT INTO students VALUES (1041, 'Lily Richardson', 'History', 3.7);",
        "INSERT INTO students VALUES (1042, 'Ryan Cox', 'History', 2.6);",
        "INSERT INTO students VALUES (1043, 'Hannah Howard', 'History', 3.4);",
        "INSERT INTO students VALUES (1044, 'Nathan Ward', 'History', 3.8);",
        // Mixed majors (remaining)
        "INSERT INTO students VALUES (1045, 'Aubrey Torres', 'Computer Science', 3.2);",
        "INSERT INTO students VALUES (1046, 'Sebastian Peterson', 'Mathematics', 3.5);",
        "INSERT INTO students VALUES (1047, 'Zoey Gray', 'Physics', 3.6);",
        "INSERT INTO students VALUES (1048, 'Jack Ramirez', 'Biology', 3.3);",
        "INSERT INTO students VALUES (1049, 'Layla James', 'English', 3.8);",
        "INSERT INTO students VALUES (1050, 'Owen Watson', 'History', 3.1);",
      ],
    },
    {
      name: 'courses',
      createSql: `CREATE TABLE courses (
        course_id INTEGER,
        course_name VARCHAR(100),
        department VARCHAR(50),
        credits INTEGER
      );`,
      insertSql: [
        // Computer Science courses
        "INSERT INTO courses VALUES (101, 'Intro to Programming', 'CS', 3);",
        "INSERT INTO courses VALUES (102, 'Data Structures', 'CS', 4);",
        "INSERT INTO courses VALUES (103, 'Database Systems', 'CS', 3);",
        "INSERT INTO courses VALUES (104, 'Operating Systems', 'CS', 4);",
        // Mathematics courses
        "INSERT INTO courses VALUES (105, 'Calculus I', 'MATH', 4);",
        "INSERT INTO courses VALUES (106, 'Calculus II', 'MATH', 4);",
        "INSERT INTO courses VALUES (107, 'Linear Algebra', 'MATH', 3);",
        "INSERT INTO courses VALUES (108, 'Discrete Mathematics', 'MATH', 3);",
        // Physics courses
        "INSERT INTO courses VALUES (109, 'General Physics I', 'PHYS', 4);",
        "INSERT INTO courses VALUES (110, 'General Physics II', 'PHYS', 4);",
        "INSERT INTO courses VALUES (111, 'Modern Physics', 'PHYS', 3);",
        // Biology courses
        "INSERT INTO courses VALUES (112, 'General Biology', 'BIO', 4);",
        "INSERT INTO courses VALUES (113, 'Molecular Biology', 'BIO', 3);",
        "INSERT INTO courses VALUES (114, 'Genetics', 'BIO', 3);",
        // English courses
        "INSERT INTO courses VALUES (115, 'English Composition', 'ENG', 3);",
        "INSERT INTO courses VALUES (116, 'American Literature', 'ENG', 3);",
        "INSERT INTO courses VALUES (117, 'Creative Writing', 'ENG', 3);",
        // History courses
        "INSERT INTO courses VALUES (118, 'World History I', 'HIST', 3);",
        "INSERT INTO courses VALUES (119, 'World History II', 'HIST', 3);",
        "INSERT INTO courses VALUES (120, 'US History', 'HIST', 3);",
      ],
    },
    {
      name: 'enrollments',
      createSql: `CREATE TABLE enrollments (
        student_id INTEGER,
        course_id INTEGER,
        semester VARCHAR(20),
        grade VARCHAR(2)
      );`,
      insertSql: [
        // Fall 2024 enrollments
        "INSERT INTO enrollments VALUES (1001, 101, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1001, 105, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1002, 101, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1002, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1003, 102, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1003, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1004, 101, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1004, 105, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1005, 102, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1005, 108, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1006, 101, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1006, 115, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1007, 103, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1007, 107, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1009, 105, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1009, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1010, 106, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1010, 108, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1011, 105, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1011, 115, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1012, 105, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1012, 118, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1013, 106, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1013, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1014, 105, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1014, 115, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1015, 108, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1015, 116, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1016, 109, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1016, 105, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1017, 109, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1017, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1018, 109, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1018, 115, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1019, 110, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1019, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1020, 109, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1020, 118, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1021, 110, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1021, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1023, 112, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1023, 105, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1024, 112, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1024, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1025, 112, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1025, 115, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1026, 113, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1026, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1027, 112, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1027, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1028, 112, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1028, 115, 'Fall2024', 'F');",
        "INSERT INTO enrollments VALUES (1029, 113, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1029, 108, 'Fall2024', 'C');",
        // Spring 2025 enrollments
        "INSERT INTO enrollments VALUES (1030, 115, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1030, 118, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1031, 116, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1031, 119, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1032, 115, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1032, 105, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1033, 117, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1033, 119, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1034, 115, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1034, 118, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1035, 116, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1035, 107, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1036, 117, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1036, 120, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1037, 118, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1037, 105, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1038, 119, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1038, 106, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1039, 120, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1039, 107, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1040, 118, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1040, 115, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1041, 119, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1041, 116, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1042, 118, 'Spring2025', 'F');",
        "INSERT INTO enrollments VALUES (1042, 115, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1043, 120, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1043, 117, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1044, 119, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1044, 116, 'Spring2025', 'A');",
        // Fall 2025 enrollments (some in progress with NULL grades)
        "INSERT INTO enrollments VALUES (1001, 103, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1001, 109, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1002, 104, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1002, 110, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1003, 103, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1004, 102, 'Fall2025', 'C');",
        "INSERT INTO enrollments VALUES (1005, 104, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1009, 106, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1010, 107, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1016, 110, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1017, 111, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1024, 113, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1027, 114, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1031, 117, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1039, 120, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1045, 101, 'Fall2025', 'C');",
        "INSERT INTO enrollments VALUES (1046, 108, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1047, 109, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1048, 112, 'Fall2025', 'C');",
        "INSERT INTO enrollments VALUES (1049, 116, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1050, 119, 'Fall2025', NULL);", // In progress
      ],
    },
    {
      name: 'professors',
      createSql: `CREATE TABLE professors (
        prof_id INTEGER,
        name VARCHAR(100),
        department VARCHAR(50),
        salary INTEGER
      );`,
      insertSql: [
        "INSERT INTO professors VALUES (201, 'Dr. Sarah Johnson', 'CS', 95000);",
        "INSERT INTO professors VALUES (202, 'Dr. Robert Chen', 'CS', 98000);",
        "INSERT INTO professors VALUES (203, 'Dr. Maria Garcia', 'MATH', 92000);",
        "INSERT INTO professors VALUES (204, 'Dr. James Wilson', 'MATH', 94000);",
        "INSERT INTO professors VALUES (205, 'Dr. Patricia Brown', 'PHYS', 97000);",
        "INSERT INTO professors VALUES (206, 'Dr. Michael Davis', 'PHYS', NULL);", // Visiting prof
        "INSERT INTO professors VALUES (207, 'Dr. Jennifer Martinez', 'BIO', 90000);",
        "INSERT INTO professors VALUES (208, 'Dr. William Lee', 'ENG', 85000);",
        "INSERT INTO professors VALUES (209, 'Dr. Elizabeth Taylor', 'ENG', 87000);",
        "INSERT INTO professors VALUES (210, 'Dr. David Anderson', 'HIST', NULL);", // Visiting prof (Add University sample database with 200 rows)
      ],
    },
  ],
}

/**
 * SQLLogicTest Results Database
 * Purpose: Real-world dogfooding - explore VibeSQL's own conformance test results
 * Note: Data loaded from SQL dump file at runtime
 */
export const sqlLogicTestDatabase: SampleDatabase = {
  id: 'sqllogictest',
  name: 'SQLLogicTest Results',
  description: 'Live conformance test results - real production data from CI runs',
  tables: [
    {
      name: 'test_files',
      createSql: `CREATE TABLE test_files (
        file_path VARCHAR(500),
        category VARCHAR(50),
        subcategory VARCHAR(50),
        status VARCHAR(20),
        last_tested VARCHAR(30),
        last_passed VARCHAR(30)
      );`,
      insertSql: [], // Loaded from SQL dump
    },
    {
      name: 'test_runs',
      createSql: `CREATE TABLE test_runs (
        run_id INTEGER,
        started_at VARCHAR(30),
        completed_at VARCHAR(30),
        total_files INTEGER,
        passed INTEGER,
        failed INTEGER,
        untested INTEGER,
        git_commit VARCHAR(40),
        ci_run_id VARCHAR(100)
      );`,
      insertSql: [], // Loaded from SQL dump
    },
    {
      name: 'test_results',
      createSql: `CREATE TABLE test_results (
        result_id INTEGER,
        run_id INTEGER,
        file_path VARCHAR(500),
        status VARCHAR(20),
        tested_at VARCHAR(30),
        duration_ms INTEGER,
        error_message VARCHAR(2000)
      );`,
      insertSql: [], // Loaded from SQL dump
    },
  ],
}

/**
 * Empty Database
 * Purpose: Clean slate for user experimentation
 */
export const emptyDatabase: SampleDatabase = {
  id: 'empty',
  name: 'Empty',
  description: 'Start with a blank database',
  tables: [],
}

/**
 * All available sample databases
 */
export const sampleDatabases = [
  employeesDatabase,
  northwindDatabase,
  companyDatabase,
  universityDatabase,
  sqlLogicTestDatabase,
  emptyDatabase,
]

/**
 * Load a sample database into the WASM database instance
 *
 * @param database - WASM database instance
 * @param sampleDb - Sample database to load
 * @returns Success or error
 */
export function loadSampleDatabase(database: Database, sampleDb: SampleDatabase): void {
  // Drop all existing tables first
  const existingTables = database.list_tables()
  for (const table of existingTables) {
    try {
      database.execute(`DROP TABLE ${table};`)
    } catch (error) {
      console.warn(`Failed to drop table ${table}:`, error)
    }
  }

  // Create tables and insert data
  for (const table of sampleDb.tables) {
    // Create table
    database.execute(table.createSql)

    // Insert rows
    for (const insertSql of table.insertSql) {
      database.execute(insertSql)
    }
  }
}

/**
 * Get sample database by ID
 *
 * @param id - Database ID
 * @returns Sample database or undefined
 */
export function getSampleDatabase(id: string): SampleDatabase | undefined {
  return sampleDatabases.find(db => db.id === id)
}

/**
 * Load SQL dump file and execute statements
 *
 * @param database - WASM database instance
 * @param sqlDumpPath - Path to SQL dump file (relative to public directory)
 * @returns Promise that resolves when SQL dump is loaded
 */
export async function loadSqlDump(database: Database, sqlDumpPath: string): Promise<void> {
  try {
    const response = await fetch(sqlDumpPath)
    if (!response.ok) {
      throw new Error(`Failed to fetch SQL dump: ${response.statusText}`)
    }

    const sqlDump = await response.text()

    // Split SQL dump into individual statements
    // Handle both semicolon-terminated and newline-separated statements
    const statements = sqlDump
      .split(';')
      .map(stmt => stmt.trim())
      .filter(stmt => stmt.length > 0 && !stmt.startsWith('--'))

    // Execute each statement
    for (const statement of statements) {
      if (statement.trim()) {
        database.execute(statement + ';')
      }
    }
  } catch (error) {
    console.error('Failed to load SQL dump:', error)
    throw error
  }
}
