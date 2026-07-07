//! Tests for column name derivation in SELECT queries

use vibesql_executor::SelectExecutor;

fn create_test_database() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    // Set full_column_names=ON to test table.column format (the legacy default)
    // New default is short_column_names=ON which uses just column name
    db.set_full_column_names(true);

    // Create employees table
    let create_stmt = vibesql_parser::Parser::parse_sql(
        "CREATE TABLE employees (
            id INTEGER,
            name VARCHAR(100),
            department VARCHAR(50),
            salary INTEGER
        )",
    )
    .unwrap();

    if let vibesql_ast::Statement::CreateTable(create_table) = create_stmt {
        vibesql_executor::CreateTableExecutor::execute(&create_table, &mut db).unwrap();
    }

    // Insert some test data
    let insert_stmt = vibesql_parser::Parser::parse_sql(
        "INSERT INTO employees (id, name, department, salary) VALUES
        (1, 'John Smith', 'Engineering', 75000),
        (2, 'Jane Doe', 'Sales', 82000),
        (3, 'Bob Wilson', 'Engineering', 68000)",
    )
    .unwrap();

    if let vibesql_ast::Statement::Insert(insert) = insert_stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert).unwrap();
    }

    db
}

#[test]
fn test_column_names_simple_select() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT id, name FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // Column names include table prefix (full_column_names format)
        assert_eq!(result.columns, vec!["employees.id", "employees.name"]);
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_with_alias() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT name AS employee_name, salary AS annual_salary FROM employees",
    )
    .unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // SQL:1999 normalizes unquoted identifiers to lowercase
        assert_eq!(result.columns, vec!["employee_name", "annual_salary"]);
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_star_expansion() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT * FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // With full_column_names=ON, SELECT * prefixes every column with its source
        // table name, even for a single-table query (colname.test section 4.1).
        assert_eq!(
            result.columns,
            vec!["employees.id", "employees.name", "employees.department", "employees.salary"]
        );
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_functions() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT COUNT(*), AVG(salary) FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        eprintln!("DEBUG: columns = {:?}", result.columns);
        assert_eq!(result.columns.len(), 2);
        // Function names preserve original case from SQL
        // Note: COUNT(*) becomes count(*) in the column name (case-insensitive SQL)
        assert!(
            result.columns[0].to_uppercase().contains("COUNT"),
            "Expected column[0] to contain COUNT, got: {}",
            result.columns[0]
        );
        assert!(
            result.columns[1].to_uppercase().contains("AVG"),
            "Expected column[1] to contain AVG, got: {}",
            result.columns[1]
        );
        assert_eq!(result.rows.len(), 1);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_expressions() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT salary * 12 FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns.len(), 1);
        // Expression should have a generated name
        assert!(result.columns[0].contains("salary") || result.columns[0].contains("*"));
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_mixed() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT id, name AS emp_name, salary * 12 FROM employees",
    )
    .unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns.len(), 3);
        // Column names include table prefix (full_column_names format)
        assert_eq!(result.columns[0], "employees.id");
        assert_eq!(result.columns[1], "emp_name");
        // Third column is an expression
        assert!(result.columns[2].contains("salary") || result.columns[2].contains("*"));
        assert_eq!(result.rows.len(), 3);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_column_names_function_with_alias() {
    let db = create_test_database();
    let executor = SelectExecutor::new(&db);

    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT COUNT(*) AS total_employees FROM employees")
            .unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // SQL:1999 normalizes unquoted identifiers to lowercase
        assert_eq!(result.columns, vec!["total_employees"]);
        assert_eq!(result.rows.len(), 1);
    } else {
        panic!("Expected SELECT statement");
    }
}

// ===========================================================================
// Tests for PRAGMA short_column_names and full_column_names (#4419)
// ===========================================================================

fn create_test_database_default_settings() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    // Create employees table
    let create_stmt = vibesql_parser::Parser::parse_sql(
        "CREATE TABLE employees (
            id INTEGER,
            name VARCHAR(100)
        )",
    )
    .unwrap();

    if let vibesql_ast::Statement::CreateTable(create_table) = create_stmt {
        vibesql_executor::CreateTableExecutor::execute(&create_table, &mut db).unwrap();
    }

    // Insert some test data
    let insert_stmt =
        vibesql_parser::Parser::parse_sql("INSERT INTO employees (id, name) VALUES (1, 'John')")
            .unwrap();

    if let vibesql_ast::Statement::Insert(insert) = insert_stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert).unwrap();
    }

    db
}

#[test]
fn test_pragma_default_settings() {
    let db = create_test_database_default_settings();

    // Default: short_column_names=ON, full_column_names=OFF
    assert!(db.short_column_names());
    assert!(!db.full_column_names());
}

#[test]
fn test_pragma_short_column_names_default() {
    // By default, short_column_names=ON means columns are just the column name
    let db = create_test_database_default_settings();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT id, name FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // Default is short column names - just the column name without table prefix
        assert_eq!(result.columns, vec!["id", "name"]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_pragma_full_column_names_on() {
    let mut db = create_test_database_default_settings();
    db.set_full_column_names(true);

    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT id, name FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // full_column_names=ON means table.column format
        assert_eq!(result.columns, vec!["employees.id", "employees.name"]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_pragma_wildcard_short_column_names() {
    let db = create_test_database_default_settings();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT * FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // short_column_names=ON: wildcards use just column names
        assert_eq!(result.columns, vec!["id", "name"]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_pragma_wildcard_full_column_names() {
    let mut db = create_test_database_default_settings();
    db.set_full_column_names(true);

    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_parser::Parser::parse_sql("SELECT * FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // full_column_names=ON: SELECT * uses table.column form (colname.test section 4.1)
        assert_eq!(result.columns, vec!["employees.id", "employees.name"]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_pragma_alias_takes_precedence() {
    let db = create_test_database_default_settings();
    let executor = SelectExecutor::new(&db);

    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT id AS employee_id FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // Alias always takes precedence regardless of PRAGMA settings
        assert_eq!(result.columns, vec!["employee_id"]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_pragma_alias_takes_precedence_with_full_column_names() {
    let mut db = create_test_database_default_settings();
    db.set_full_column_names(true);

    let executor = SelectExecutor::new(&db);

    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT id AS employee_id FROM employees").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        // Alias always takes precedence even with full_column_names=ON
        assert_eq!(result.columns, vec!["employee_id"]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_pragma_set_and_get() {
    let mut db = create_test_database_default_settings();

    // Default values
    assert!(db.short_column_names());
    assert!(!db.full_column_names());

    // Set full_column_names ON
    db.set_full_column_names(true);
    assert!(db.full_column_names());

    // Set full_column_names OFF
    db.set_full_column_names(false);
    assert!(!db.full_column_names());

    // Set short_column_names OFF
    db.set_short_column_names(false);
    assert!(!db.short_column_names());

    // Set short_column_names ON
    db.set_short_column_names(true);
    assert!(db.short_column_names());
}

// ---------------------------------------------------------------------------
// full_column_names=ON table-prefix behavior for wildcards (issue #5842 item 3c,
// colname.test section 4). SQLite prefixes every wildcard-expanded column with
// its source table name when short_column_names=OFF and full_column_names=ON.
// ---------------------------------------------------------------------------

fn create_two_table_database_full_names() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    db.set_short_column_names(false);
    db.set_full_column_names(true);

    for ddl in ["CREATE TABLE tabc(a, b, c)", "CREATE TABLE txyz(x, y, z)"] {
        let stmt = vibesql_parser::Parser::parse_sql(ddl).unwrap();
        if let vibesql_ast::Statement::CreateTable(create_table) = stmt {
            vibesql_executor::CreateTableExecutor::execute(&create_table, &mut db).unwrap();
        }
    }
    for dml in ["INSERT INTO tabc VALUES(1, 2, 3)", "INSERT INTO txyz VALUES(4, 5, 6)"] {
        let stmt = vibesql_parser::Parser::parse_sql(dml).unwrap();
        if let vibesql_ast::Statement::Insert(insert) = stmt {
            vibesql_executor::InsertExecutor::execute(&mut db, &insert).unwrap();
        }
    }
    db
}

#[test]
fn test_full_column_names_single_table_star() {
    // colname-4.1: SELECT * FROM tabc -> tabc.a, tabc.b, tabc.c
    let db = create_two_table_database_full_names();
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_parser::Parser::parse_sql("SELECT * FROM tabc").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns, vec!["tabc.a", "tabc.b", "tabc.c"]);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_full_column_names_qualified_wildcards() {
    // colname-4.6: SELECT tabc.*, txyz.* -> tabc.a..tabc.c, txyz.x..txyz.z
    let db = create_two_table_database_full_names();
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_parser::Parser::parse_sql("SELECT tabc.*, txyz.* FROM tabc, txyz").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(
            result.columns,
            vec!["tabc.a", "tabc.b", "tabc.c", "txyz.x", "txyz.y", "txyz.z"]
        );
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_full_column_names_multi_table_star() {
    // colname-4.5 tail: SELECT * FROM tabc, txyz -> all columns table-qualified
    let db = create_two_table_database_full_names();
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_parser::Parser::parse_sql("SELECT * FROM tabc, txyz").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(
            result.columns,
            vec!["tabc.a", "tabc.b", "tabc.c", "txyz.x", "txyz.y", "txyz.z"]
        );
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_short_column_names_star_no_prefix() {
    // Regression guard: with short_column_names=ON (default), SELECT * stays unprefixed
    // even across multiple tables.
    let mut db = create_two_table_database_full_names();
    db.set_full_column_names(false);
    db.set_short_column_names(true);
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_parser::Parser::parse_sql("SELECT tabc.*, txyz.* FROM tabc, txyz").unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute_with_columns(&select_stmt).unwrap();
        assert_eq!(result.columns, vec!["a", "b", "c", "x", "y", "z"]);
    } else {
        panic!("Expected SELECT statement");
    }
}

// ---------------------------------------------------------------------------
// COLLATE wrapper naming in derived tables (issue #5314, select1-18.1 Bug A)
//
// SQLite's sqlite3ColumnsFromExprList() skips TK_COLLATE wrappers when deriving
// result-column names, so `SELECT x COLLATE rtrim` in a derived table produces
// a column named `x` that the outer query can resolve.
// ---------------------------------------------------------------------------

fn create_collate_test_database() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    let create_stmt =
        vibesql_parser::Parser::parse_sql("CREATE TABLE t2 (x INTEGER, y INTEGER)").unwrap();
    if let vibesql_ast::Statement::CreateTable(create_table) = create_stmt {
        vibesql_executor::CreateTableExecutor::execute(&create_table, &mut db).unwrap();
    }

    let insert_stmt = vibesql_parser::Parser::parse_sql("INSERT INTO t2 (x) VALUES (123)").unwrap();
    if let vibesql_ast::Statement::Insert(insert) = insert_stmt {
        vibesql_executor::InsertExecutor::execute(&mut db, &insert).unwrap();
    }

    db
}

fn run_select(
    db: &vibesql_storage::Database,
    sql: &str,
) -> Result<vibesql_executor::SelectResult, vibesql_executor::ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        SelectExecutor::new(db).execute_with_columns(&select_stmt)
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_derived_table_collate_column_named_after_inner_expr() {
    let db = create_collate_test_database();

    // The derived column must be named `x`, not `?column?`
    let result = run_select(&db, "SELECT * FROM (SELECT x COLLATE rtrim FROM t2)").unwrap();
    assert_eq!(result.columns, vec!["x"]);
    assert_eq!(result.rows.len(), 1);

    // The outer query can therefore resolve `x`
    let result = run_select(&db, "SELECT x FROM (SELECT x COLLATE rtrim FROM t2)").unwrap();
    assert_eq!(result.columns, vec!["x"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Integer(123));
}

#[test]
fn test_derived_table_nested_collate_column_name() {
    let db = create_collate_test_database();

    // Nested COLLATE wrappers are peeled recursively
    let result =
        run_select(&db, "SELECT x FROM (SELECT x COLLATE binary COLLATE rtrim FROM t2)").unwrap();
    assert_eq!(result.columns, vec!["x"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Integer(123));
}

#[test]
fn test_derived_table_collate_alias_takes_precedence() {
    let db = create_collate_test_database();

    // An explicit alias wins over the derived name
    let result = run_select(&db, "SELECT y FROM (SELECT x COLLATE rtrim AS y FROM t2)").unwrap();
    assert_eq!(result.columns, vec!["y"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], vibesql_types::SqlValue::Integer(123));
}
