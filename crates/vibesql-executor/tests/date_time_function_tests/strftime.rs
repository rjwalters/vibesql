//! Integration tests for the SQLite-compatible STRFTIME function
//!
//! Conformance values are taken from SQLite's date.test section 3
//! (docs/reference/sqlite/test/date.test), which uses the reference value
//! '2003-10-31 12:34:56.432'.

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn execute_query(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Execute `SELECT <expr>` and return the single result value
fn eval_scalar(sql_expr: &str) -> SqlValue {
    let db = Database::new();
    let rows = execute_query(&db, &format!("SELECT {}", sql_expr));
    assert_eq!(rows.len(), 1, "expected a single row for {}", sql_expr);
    rows[0].values[0].clone()
}

fn assert_text(sql_expr: &str, expected: &str) {
    match eval_scalar(sql_expr) {
        SqlValue::Varchar(s) => assert_eq!(s.as_str(), expected, "for {}", sql_expr),
        other => panic!("{} should return TEXT '{}', got {:?}", sql_expr, expected, other),
    }
}

// ==================== SQLite date.test section 3 conformance ====================

#[test]
fn test_strftime_sqlite_date_test_section_3() {
    let reference = "'2003-10-31 12:34:56.432'";
    let cases = [
        ("%d", "31"),                   // 3.1
        ("pre%fpost", "pre56.432post"), // 3.2.1
        ("%H", "12"),                   // 3.3
        ("%j", "304"),                  // 3.4
        ("%J", "2452944.024264259"),    // 3.5
        ("%m", "10"),                   // 3.6
        ("%M", "34"),                   // 3.7
        ("%s", "1067603696"),           // 3.8.1
        ("%S", "56"),                   // 3.9
        ("%w", "5"),                    // 3.10
        ("%W", "43"),                   // 3.11.1
        ("%Y", "2003"),                 // 3.12
        ("%%", "%"),                    // 3.13
    ];
    for (format, expected) in cases {
        assert_text(&format!("strftime('{}', {})", format, reference), expected);
    }

    // 3.2.2: fractional seconds truncate, not round
    assert_text("strftime('%f', '2003-10-31 12:34:59.9999999')", "59.999");
    // 3.14: unknown specifier returns NULL
    assert_eq!(eval_scalar("strftime('%_', '2003-10-31 12:34:56.432')"), SqlValue::Null);
    // 3.15: multi-specifier format
    assert_text("strftime('%Y-%m-%d', '2003-10-31')", "2003-10-31");
}

#[test]
fn test_strftime_returns_text_type() {
    // SQLite's strftime returns TEXT (matters for cross-engine comparison: '1995' vs 1995)
    assert_text("strftime('%Y', '1995-03-15')", "1995");
    assert_text("typeof(strftime('%Y', '1995-03-15'))", "text");
}

#[test]
fn test_strftime_with_modifiers() {
    assert_text("strftime('%Y', '1995-03-15', '+1 year')", "1996");
    assert_text("strftime('%Y-%m-%d', '2003-10-31', 'start of month')", "2003-10-01");
    assert_text("strftime('%Y-%m-%d', '2003-10-31', '+2 days')", "2003-11-02");
    assert_text("strftime('%Y-%m-%d %H:%M:%S', 1067603696, 'unixepoch')", "2003-10-31 12:34:56");
}

#[test]
fn test_strftime_null_and_invalid_inputs() {
    assert_eq!(eval_scalar("strftime(NULL, '2003-10-31')"), SqlValue::Null);
    assert_eq!(eval_scalar("strftime('%Y', NULL)"), SqlValue::Null);
    assert_eq!(eval_scalar("strftime('%Y', 'not-a-date')"), SqlValue::Null);
    assert_eq!(eval_scalar("strftime('%Y', '2003-10-31', 'bogus modifier')"), SqlValue::Null);
}

// ==================== Use with table columns (TPC-H Q7/Q8/Q9 pattern) ====================

fn setup_orders_db() -> Database {
    let mut db = Database::new();

    let create = Parser::parse_sql("CREATE TABLE orders (id INTEGER, o_orderdate DATE)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    let inserts = [
        "INSERT INTO orders VALUES (1, '1995-03-15')",
        "INSERT INTO orders VALUES (2, '1995-11-30')",
        "INSERT INTO orders VALUES (3, '1996-01-02')",
        "INSERT INTO orders VALUES (4, '1996-12-01')",
    ];
    for sql in inserts {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
    }

    db
}

#[test]
fn test_strftime_on_date_column() {
    let db = setup_orders_db();
    let rows = execute_query(&db, "SELECT strftime('%Y', o_orderdate) FROM orders ORDER BY id");
    let years: Vec<String> = rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected TEXT, got {:?}", other),
        })
        .collect();
    assert_eq!(years, vec!["1995", "1995", "1996", "1996"]);
}

#[test]
fn test_strftime_in_group_by() {
    // The TPC-H Q7/Q8/Q9 pattern: GROUP BY strftime('%Y', date_col)
    let db = setup_orders_db();
    let rows = execute_query(
        &db,
        "SELECT strftime('%Y', o_orderdate) AS o_year, COUNT(*) \
         FROM orders GROUP BY o_year ORDER BY o_year",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Varchar("1995".into()));
    assert_eq!(rows[0].values[1], SqlValue::Integer(2));
    assert_eq!(rows[1].values[0], SqlValue::Varchar("1996".into()));
    assert_eq!(rows[1].values[1], SqlValue::Integer(2));
}

#[test]
fn test_strftime_in_where_clause() {
    let db = setup_orders_db();
    let rows = execute_query(
        &db,
        "SELECT id FROM orders WHERE strftime('%Y', o_orderdate) = '1995' ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
}
