use super::super::*;

// ========================================================================
// CREATE TABLE Statement Tests
// ========================================================================

#[test]
fn test_parse_create_table_basic() {
    let result = Parser::parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR(100));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "users");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "id");
            assert_eq!(create.columns[1].name, "name");
            match create.columns[0].data_type {
                vibesql_types::DataType::Integer => {} // Success
                _ => panic!("Expected Integer data type"),
            }
            match create.columns[1].data_type {
                vibesql_types::DataType::Varchar { max_length: Some(100) } => {} // Success
                _ => panic!("Expected VARCHAR(100) data type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_various_types() {
    let result =
        Parser::parse_sql("CREATE TABLE test (id INT, flag BOOLEAN, birth DATE, code CHAR(5));");
    if let Err(ref e) = result {
        eprintln!("Parse error: {}", e);
    }
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "test");
            assert_eq!(create.columns.len(), 4);
            match create.columns[0].data_type {
                vibesql_types::DataType::Integer => {} // Success
                _ => panic!("Expected Integer"),
            }
            match create.columns[1].data_type {
                vibesql_types::DataType::Boolean => {} // Success
                _ => panic!("Expected Boolean"),
            }
            match create.columns[2].data_type {
                vibesql_types::DataType::Date => {} // Success
                _ => panic!("Expected Date"),
            }
            match create.columns[3].data_type {
                vibesql_types::DataType::Character { length: 5 } => {} // Success
                _ => panic!("Expected CHAR(5)"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_without_oids() {
    let result = Parser::parse_sql("CREATE TABLE t1 (id INT) WITHOUT OIDS;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t1");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "id");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_oids() {
    let result = Parser::parse_sql("CREATE TABLE t2 (id INT) WITH OIDS;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t2");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "id");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_no_oids_clause() {
    // Ensure tables without OIDS clause still work
    let result = Parser::parse_sql("CREATE TABLE t3 (id INT);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t3");
            assert_eq!(create.columns.len(), 1);
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// Backtick Identifier Tests (MySQL-style)
// ========================================================================

#[test]
fn test_parse_create_table_with_backtick_table_name() {
    let result = Parser::parse_sql("CREATE TABLE `user_table` (id INTEGER);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            // Backtick identifiers preserve case
            assert_eq!(create.table_name, "user_table");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "id");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_backtick_column_names() {
    let result =
        Parser::parse_sql("CREATE TABLE users (`user_id` INTEGER, `user_name` VARCHAR(100));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "users");
            assert_eq!(create.columns.len(), 2);
            // Backtick identifiers preserve case
            assert_eq!(create.columns[0].name, "user_id");
            assert_eq!(create.columns[1].name, "user_name");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_backtick_reserved_word() {
    // Reserved words can be used as identifiers when backtick-quoted
    let result = Parser::parse_sql("CREATE TABLE `select` (`from` INTEGER, `where` VARCHAR(50));");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "select");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "from");
            assert_eq!(create.columns[1].name, "where");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_backtick_spaces() {
    // Backtick identifiers can contain spaces
    let result = Parser::parse_sql(
        "CREATE TABLE `my table` (`first name` INTEGER, `last name` VARCHAR(100));",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "my table");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "first name");
            assert_eq!(create.columns[1].name, "last name");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_mixed_backtick_and_regular() {
    // Mix backtick and regular identifiers
    let result = Parser::parse_sql(
        "CREATE TABLE `MyTable` (id INTEGER, `userName` VARCHAR(100), status INT);",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "MyTable");
            assert_eq!(create.columns.len(), 3);
            assert_eq!(create.columns[0].name, "id"); // Regular identifier - uppercased
            assert_eq!(create.columns[1].name, "userName"); // Backtick - preserves case
            assert_eq!(create.columns[2].name, "status"); // Regular identifier - uppercased
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_generated_column_short_form() {
    // Test generated column with short form: AS (expression)
    let sql = "CREATE TABLE t1 (\n  a REAL,\n  b BLOB AS (a * 2)\n)";
    let result = Parser::parse_sql(sql);
    if let Err(ref e) = result {
        eprintln!("Parse error: {}", e);
    }
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t1");
            assert_eq!(create.columns.len(), 2);

            // First column - not generated
            assert_eq!(create.columns[0].name, "a");
            assert!(
                create.columns[0].generated_expr.is_none(),
                "First column should not be generated"
            );

            // Second column - should be generated
            assert_eq!(create.columns[1].name, "b");
            assert!(
                create.columns[1].generated_expr.is_some(),
                "Expected generated_expr to be Some, but it was None"
            );
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// The parser captures the *verbatim* source spelling of the table-name token
/// (including quotes/brackets/backticks and casing) in `name_source`, so the
/// executor can echo it in the "table ... already exists" error to match
/// sqlite3 3.51.0. Mirrors the CREATE TRIGGER mechanism from #5538 (issue #5544).
#[test]
fn test_parse_create_table_captures_name_source() {
    let cases = [
        ("CREATE TABLE tbl1 (a)", "tbl1", "tbl1"),
        ("CREATE TABLE \"tbl1\" (a)", "tbl1", "\"tbl1\""),
        ("CREATE TABLE [tbl1] (a)", "tbl1", "[tbl1]"),
        ("CREATE TABLE `tbl1` (a)", "tbl1", "`tbl1`"),
        ("CREATE TABLE \"TBL1\" (a)", "TBL1", "\"TBL1\""),
        // schema-qualified: name_source is the table-name token (no schema prefix).
        ("CREATE TABLE main.\"tbl1\" (a)", "main.tbl1", "\"tbl1\""),
    ];

    for (sql, expected_name, expected_source) in cases {
        match Parser::parse_sql(sql).expect("parse failed") {
            vibesql_ast::Statement::CreateTable(create) => {
                assert_eq!(create.table_name, expected_name, "table_name for: {sql}");
                assert_eq!(
                    create.name_source.as_deref(),
                    Some(expected_source),
                    "name_source for: {sql}"
                );
            }
            _ => panic!("Expected CREATE TABLE statement for: {sql}"),
        }
    }
}

/// CREATE TABLE ... AS SELECT also captures the table-name source spelling.
#[test]
fn test_parse_create_table_as_select_captures_name_source() {
    match Parser::parse_sql("CREATE TABLE \"tbl1\" AS SELECT 1").expect("parse failed") {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "tbl1");
            assert_eq!(create.name_source.as_deref(), Some("\"tbl1\""));
            assert!(create.as_query.is_some());
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// SQLite accepts otherwise-reserved keywords as unquoted column names in a
/// CREATE TABLE column list. Table-constraint keywords (PRIMARY, FOREIGN, ...)
/// are consumed earlier, so any keyword in column-name position is an identifier.
/// Regression test for issue #5661 (table.test table-7.x: `no such table: weird`).
#[test]
fn test_parse_create_table_keyword_column_names() {
    // desc, asc, key, begin, end are all keywords but valid SQLite column names.
    let sql = "CREATE TABLE weird(desc text, asc text, key int, begin blob, end clob)";
    match Parser::parse_sql(sql).expect("keyword column names should parse") {
        vibesql_ast::Statement::CreateTable(create) => {
            let names: Vec<&str> = create.columns.iter().map(|c| c.name.as_str()).collect();
            assert_eq!(names, vec!["desc", "asc", "key", "begin", "end"]);
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// SQLite accepts `release` (a keyword) as both a column name and a SET target.
/// Regression test for issue #5661 (table.test table-7.3: `CREATE TABLE
/// savepoint(release)` followed by INSERT/UPDATE on `release`).
#[test]
fn test_parse_keyword_column_in_insert_and_update() {
    Parser::parse_sql("CREATE TABLE savepoint(release)")
        .expect("keyword table and column names should parse");
    Parser::parse_sql("INSERT INTO savepoint(release) VALUES(10)")
        .expect("keyword column name in INSERT list should parse");
    Parser::parse_sql("UPDATE savepoint SET release = 5")
        .expect("keyword column name in SET clause should parse");
}

/// SQLite/Oracle-style `NUMBER(precision, scale)` (with an unrecognized type
/// name carrying a two-argument size specifier) must parse. Regression test for
/// issue #5661 (table.test table-11.x: `b number(5,10)` blocked `t7` creation).
#[test]
fn test_parse_create_table_number_precision_scale() {
    let sql = "CREATE TABLE t7(a integer primary key, b number(5,10))";
    match Parser::parse_sql(sql).expect("NUMBER(p,s) should parse") {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[1].name, "b");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_true_false_as_column_names() {
    // SQLite treats TRUE/FALSE as fallback keywords usable as identifiers.
    let result = Parser::parse_sql("CREATE TABLE t(\"true\" INTEGER, \"false\" INTEGER)");
    assert!(result.is_ok(), "quoted true/false columns: {:?}", result);

    let result = Parser::parse_sql("CREATE TABLE t(true INTEGER, false INTEGER)");
    assert!(result.is_ok(), "bare true/false columns: {:?}", result);
}

#[test]
fn test_true_false_level_as_table_names() {
    for name in ["true", "false", "level"] {
        let sql = format!("CREATE TABLE {name}(a INTEGER)");
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "{sql} should parse: {:?}", result);
    }
}
