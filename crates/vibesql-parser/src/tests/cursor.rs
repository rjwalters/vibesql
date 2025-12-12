//! Tests for DECLARE CURSOR statement parsing (SQL:1999 Feature E121)

use vibesql_ast::{CursorUpdatability, Statement};

use crate::parser::Parser;

#[test]
fn test_declare_cursor_basic() {
    let sql = "DECLARE my_cursor CURSOR FOR SELECT a FROM table1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse basic DECLARE CURSOR: {:?}", result.err());

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(!stmt.insensitive);
        assert!(!stmt.scroll);
        assert_eq!(stmt.hold, None);
        assert!(matches!(stmt.updatability, CursorUpdatability::Unspecified));
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_with_insensitive() {
    let sql = "DECLARE my_cursor INSENSITIVE CURSOR FOR SELECT a FROM table1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse DECLARE CURSOR with INSENSITIVE: {:?}", result.err());

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(stmt.insensitive);
        assert!(!stmt.scroll);
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_with_scroll() {
    let sql = "DECLARE my_cursor SCROLL CURSOR FOR SELECT a FROM table1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse DECLARE CURSOR with SCROLL: {:?}", result.err());

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(!stmt.insensitive);
        assert!(stmt.scroll);
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_insensitive_scroll() {
    let sql = "DECLARE my_cursor INSENSITIVE SCROLL CURSOR FOR SELECT a FROM table1";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse DECLARE CURSOR with INSENSITIVE SCROLL: {:?}",
        result.err()
    );

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(stmt.insensitive);
        assert!(stmt.scroll);
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_with_hold() {
    let sql = "DECLARE my_cursor CURSOR WITH HOLD FOR SELECT a FROM table1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse DECLARE CURSOR with WITH HOLD: {:?}", result.err());

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert_eq!(stmt.hold, Some(true));
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_without_hold() {
    let sql = "DECLARE my_cursor CURSOR WITHOUT HOLD FOR SELECT a FROM table1";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse DECLARE CURSOR with WITHOUT HOLD: {:?}", result.err());

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert_eq!(stmt.hold, Some(false));
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_for_read_only() {
    let sql = "DECLARE my_cursor CURSOR FOR SELECT a FROM table1 FOR READ ONLY";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse DECLARE CURSOR with FOR READ ONLY: {:?}",
        result.err()
    );

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.updatability, CursorUpdatability::ReadOnly));
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_for_update() {
    let sql = "DECLARE my_cursor CURSOR FOR SELECT a FROM table1 FOR UPDATE";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse DECLARE CURSOR with FOR UPDATE: {:?}", result.err());

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        if let CursorUpdatability::Update { columns } = stmt.updatability {
            assert_eq!(columns, None);
        } else {
            panic!("Expected Update updatability");
        }
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_for_update_of_columns() {
    let sql = "DECLARE my_cursor CURSOR FOR SELECT a, b, c FROM table1 FOR UPDATE OF a, b";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse DECLARE CURSOR with FOR UPDATE OF: {:?}",
        result.err()
    );

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        if let CursorUpdatability::Update { columns } = stmt.updatability {
            assert_eq!(columns, Some(vec!["a".to_string(), "b".to_string()]));
        } else {
            panic!("Expected Update updatability with columns");
        }
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_full_syntax() {
    let sql = "DECLARE my_cursor INSENSITIVE SCROLL CURSOR WITH HOLD FOR SELECT a FROM table1 FOR READ ONLY";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse full DECLARE CURSOR syntax: {:?}", result.err());

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(stmt.insensitive);
        assert!(stmt.scroll);
        assert_eq!(stmt.hold, Some(true));
        assert!(matches!(stmt.updatability, CursorUpdatability::ReadOnly));
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_declare_cursor_complex_query() {
    let sql = "DECLARE c1 CURSOR FOR SELECT e.emp_name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.salary > 50000";
    let result = Parser::parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse DECLARE CURSOR with complex query: {:?}",
        result.err()
    );

    if let Ok(Statement::DeclareCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "c1");
    } else {
        panic!("Expected DeclareCursor statement");
    }
}

#[test]
fn test_open_cursor_statement() {
    let sql = "OPEN my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse OPEN CURSOR: {:?}", result.err());

    if let Ok(Statement::OpenCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
    } else {
        panic!("Expected OpenCursor statement");
    }
}

#[test]
fn test_close_cursor_statement() {
    let sql = "CLOSE my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse CLOSE CURSOR: {:?}", result.err());

    if let Ok(Statement::CloseCursor(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
    } else {
        panic!("Expected CloseCursor statement");
    }
}

#[test]
fn test_fetch_statement_next() {
    let sql = "FETCH NEXT FROM my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse FETCH NEXT: {:?}", result.err());

    if let Ok(Statement::Fetch(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.orientation, vibesql_ast::FetchOrientation::Next));
        assert_eq!(stmt.into_variables, None);
    } else {
        panic!("Expected Fetch statement");
    }
}

#[test]
fn test_fetch_statement_prior() {
    let sql = "FETCH PRIOR FROM my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse FETCH PRIOR: {:?}", result.err());

    if let Ok(Statement::Fetch(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.orientation, vibesql_ast::FetchOrientation::Prior));
        assert_eq!(stmt.into_variables, None);
    } else {
        panic!("Expected Fetch statement");
    }
}

#[test]
fn test_fetch_statement_first() {
    let sql = "FETCH FIRST FROM my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse FETCH FIRST: {:?}", result.err());

    if let Ok(Statement::Fetch(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.orientation, vibesql_ast::FetchOrientation::First));
        assert_eq!(stmt.into_variables, None);
    } else {
        panic!("Expected Fetch statement");
    }
}

#[test]
fn test_fetch_statement_last() {
    let sql = "FETCH LAST FROM my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse FETCH LAST: {:?}", result.err());

    if let Ok(Statement::Fetch(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.orientation, vibesql_ast::FetchOrientation::Last));
        assert_eq!(stmt.into_variables, None);
    } else {
        panic!("Expected Fetch statement");
    }
}

#[test]
fn test_fetch_statement_absolute() {
    let sql = "FETCH ABSOLUTE 5 FROM my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse FETCH ABSOLUTE: {:?}", result.err());

    if let Ok(Statement::Fetch(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.orientation, vibesql_ast::FetchOrientation::Absolute(5)));
        assert_eq!(stmt.into_variables, None);
    } else {
        panic!("Expected Fetch statement");
    }
}

#[test]
fn test_fetch_statement_relative() {
    let sql = "FETCH RELATIVE 3 FROM my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse FETCH RELATIVE: {:?}", result.err());

    if let Ok(Statement::Fetch(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.orientation, vibesql_ast::FetchOrientation::Relative(3)));
        assert_eq!(stmt.into_variables, None);
    } else {
        panic!("Expected Fetch statement");
    }
}

#[test]
fn test_fetch_statement_with_into() {
    let sql = "FETCH NEXT FROM my_cursor INTO var1, var2";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse FETCH with INTO: {:?}", result.err());

    if let Ok(Statement::Fetch(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.orientation, vibesql_ast::FetchOrientation::Next));
        assert_eq!(stmt.into_variables, Some(vec!["var1".to_string(), "var2".to_string()]));
    } else {
        panic!("Expected Fetch statement");
    }
}

#[test]
fn test_fetch_statement_default_orientation() {
    let sql = "FETCH FROM my_cursor";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse FETCH with default orientation: {:?}", result.err());

    if let Ok(Statement::Fetch(stmt)) = result {
        assert_eq!(stmt.cursor_name, "my_cursor");
        assert!(matches!(stmt.orientation, vibesql_ast::FetchOrientation::Next)); // default
        assert_eq!(stmt.into_variables, None);
    } else {
        panic!("Expected Fetch statement");
    }
}
