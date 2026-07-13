//! Tests for GLOB in the simple (scalar) evaluator with non-literal operands (Issue #6070)
//!
//! The scalar "simple evaluator" used inside the aggregation path
//! (`select::executor::aggregation::evaluation::simple`) previously had no
//! `Glob` arm, so any GLOB expression that reached it — even one with plain
//! literal operands — failed with:
//!
//! ```text
//! Unsupported expression: Unexpected expression in simple evaluator: Glob { ... }
//! ```
//!
//! This is the #5884/#5892 class: GLOB works on the row/columnar scalar path
//! but blew up on the aggregation simple-evaluator path. These tests drive
//! GLOB (and NOT GLOB) with literal, column, expression, subquery, CAST, IN,
//! IS NULL, function, NULL, and numeric operands through a GROUP BY query so
//! the expression is routed through the simple evaluator.

use vibesql_executor::SelectExecutor;

/// Build a one-row table so a GROUP BY query routes non-aggregate SELECT-list
/// expressions through the aggregation simple evaluator.
fn setup_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    let schema = vibesql_catalog::TableSchema::new(
        "T".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "G".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "NAME".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(64) },
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "PAT".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(64) },
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "NUM".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "T",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("abc")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("a*")),
            vibesql_types::SqlValue::Integer(12),
        ]),
    )
    .unwrap();

    db
}

/// Run a single-column, single-row SELECT and return the scalar result.
fn eval_scalar(db: &vibesql_storage::Database, sql: &str) -> vibesql_types::SqlValue {
    let executor = SelectExecutor::new(db);
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement for `{sql}`");
    };
    let rows = executor
        .execute(&select_stmt)
        .unwrap_or_else(|e| panic!("execute failed for `{sql}`: {e:?}"));
    assert_eq!(rows.len(), 1, "expected exactly one row for `{sql}`");
    rows[0].values[0].clone()
}

/// SQLite renders GLOB match results as integers 0/1; VibeSQL uses Boolean.
/// Accept either representation of a truthy/falsy scalar.
fn assert_bool(value: &vibesql_types::SqlValue, expected: bool, sql: &str) {
    match value {
        vibesql_types::SqlValue::Boolean(b) => {
            assert_eq!(*b, expected, "`{sql}` -> {value:?}, expected {expected}")
        }
        vibesql_types::SqlValue::Integer(i) => {
            assert_eq!(*i, expected as i64, "`{sql}` -> {value:?}, expected {expected}")
        }
        other => panic!("`{sql}` -> {other:?}, expected boolean {expected}"),
    }
}

fn assert_null(value: &vibesql_types::SqlValue, sql: &str) {
    assert!(matches!(value, vibesql_types::SqlValue::Null), "`{sql}` -> {value:?}, expected NULL");
}

/// Literal operands on both sides (regression: previously errored even for
/// pure literals — mirrors fuzz-4.2.4455 `Glob { Literal(Null), Literal }`).
#[test]
fn glob_literal_operands() {
    let db = setup_db();

    assert_bool(
        &eval_scalar(&db, "SELECT 'abc' GLOB 'a*' FROM t GROUP BY g"),
        true,
        "literal match",
    );
    assert_bool(
        &eval_scalar(&db, "SELECT 'abc' GLOB 'x*' FROM t GROUP BY g"),
        false,
        "literal non-match",
    );
    assert_bool(
        &eval_scalar(&db, "SELECT 'abc' NOT GLOB 'x*' FROM t GROUP BY g"),
        true,
        "literal NOT GLOB",
    );
    // NULL literal on the left yields NULL (fuzz-4.2.4455 shape).
    assert_null(&eval_scalar(&db, "SELECT NULL GLOB 'abc' FROM t GROUP BY g"), "NULL GLOB literal");
    // NULL pattern yields NULL.
    assert_null(&eval_scalar(&db, "SELECT 'abc' GLOB NULL FROM t GROUP BY g"), "literal GLOB NULL");
}

/// GLOB is case-sensitive (unlike default LIKE).
#[test]
fn glob_is_case_sensitive() {
    let db = setup_db();
    assert_bool(
        &eval_scalar(&db, "SELECT 'ABC' GLOB 'abc' FROM t GROUP BY g"),
        false,
        "GLOB case sensitivity",
    );
    assert_bool(
        &eval_scalar(&db, "SELECT 'ABC' GLOB 'ABC' FROM t GROUP BY g"),
        true,
        "GLOB exact case",
    );
}

/// Column operands on both the value and pattern side.
#[test]
fn glob_column_operands() {
    let db = setup_db();
    assert_bool(
        &eval_scalar(&db, "SELECT name GLOB pat FROM t GROUP BY g"),
        true,
        "column GLOB column ('abc' GLOB 'a*')",
    );
    assert_bool(
        &eval_scalar(&db, "SELECT name GLOB 'x*' FROM t GROUP BY g"),
        false,
        "column GLOB literal (non-match)",
    );
    assert_bool(
        &eval_scalar(&db, "SELECT 'abc' GLOB pat FROM t GROUP BY g"),
        true,
        "literal GLOB column",
    );
}

/// Arbitrary expression operands (function, CAST, IN, IS NULL, NULL, numeric)
/// — the exact non-literal shapes enumerated in the issue.
#[test]
fn glob_expression_operands() {
    let db = setup_db();

    // Function operand: upper('abc') -> 'ABC'
    assert_bool(
        &eval_scalar(&db, "SELECT upper(name) GLOB 'ABC' FROM t GROUP BY g"),
        true,
        "function operand",
    );

    // CAST operand: numeric coerced to text for GLOB.
    assert_bool(
        &eval_scalar(&db, "SELECT CAST(num AS TEXT) GLOB '1*' FROM t GROUP BY g"),
        true,
        "CAST operand ('12' GLOB '1*')",
    );

    // Numeric literal coerced to text on both sides.
    assert_bool(
        &eval_scalar(&db, "SELECT 12 GLOB '1*' FROM t GROUP BY g"),
        true,
        "numeric value coerced to text",
    );

    // IN expression as operand (boolean 1/0 rendered as text).
    assert_bool(
        &eval_scalar(&db, "SELECT (g IN (1,2)) GLOB '1' FROM t GROUP BY g"),
        true,
        "IN result GLOB '1'",
    );

    // IS NULL expression as operand.
    assert_bool(
        &eval_scalar(&db, "SELECT (name IS NULL) GLOB '0' FROM t GROUP BY g"),
        true,
        "IS NULL result GLOB '0'",
    );

    // Scalar subquery as pattern operand.
    assert_bool(
        &eval_scalar(&db, "SELECT name GLOB (SELECT pat FROM t) FROM t GROUP BY g"),
        true,
        "subquery pattern",
    );

    // NOT GLOB with an expression operand.
    assert_bool(
        &eval_scalar(&db, "SELECT upper(name) NOT GLOB 'zzz' FROM t GROUP BY g"),
        true,
        "NOT GLOB expression operand",
    );
}
