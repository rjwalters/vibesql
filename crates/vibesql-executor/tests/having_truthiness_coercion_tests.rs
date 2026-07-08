//! Differential tests vs sqlite3 for the grouped/HAVING fuzz class (#5856).
//!
//! The fuzz.test corpus (deterministic, srand(0)) generates grouped queries of
//! the shape `SELECT ... FROM (...) GROUP BY <expr> HAVING <expr>`, frequently
//! wrapped in expressions that SQLite evaluates via numeric coercion because
//! it has no boolean storage class. Every expected value in this file was
//! verified against sqlite3.
//!
//! The failing shapes this file locks in:
//! - unary `+`/`-` on a Boolean (e.g. `- EXISTS (SELECT ... HAVING ...)`)
//! - arithmetic with Boolean or BLOB operands (bytes → text → number)
//! - BLOB operands of AND/OR (boolean context)
//! - non-boolean CASE WHEN conditions in the grouped-aggregation evaluator

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

/// abc(a, b, c) with the fuzz-4.1 data: (1,2,3), (4,5,6), (7,8,9)
fn setup_abc() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "ABC".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "A".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "B".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "C".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    for (a, b, c) in [(1, 2, 3), (4, 5, 6), (7, 8, 9)] {
        db.insert_row(
            "ABC",
            vibesql_storage::Row::new(vec![
                SqlValue::Integer(a),
                SqlValue::Integer(b),
                SqlValue::Integer(c),
            ]),
        )
        .unwrap();
    }
    db
}

fn run_query(db: &vibesql_storage::Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let executor = SelectExecutor::new(db);
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("parse failed for {sql}: {e:?}"));
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement: {sql}");
    };
    executor.execute(&select_stmt).unwrap_or_else(|e| panic!("execute failed for {sql}: {e:?}"))
}

fn single_value(db: &vibesql_storage::Database, sql: &str) -> SqlValue {
    let rows = run_query(db, sql);
    assert_eq!(rows.len(), 1, "expected 1 row for {sql}");
    rows[0].values[0].clone()
}

fn assert_i64(db: &vibesql_storage::Database, sql: &str, expected: i64) {
    let v = single_value(db, sql);
    let got = match v {
        SqlValue::Integer(n) => n,
        SqlValue::Smallint(n) => n as i64,
        SqlValue::Bigint(n) => n,
        SqlValue::Boolean(b) => b as i64,
        other => panic!("expected integer {expected} for {sql}, got {other:?}"),
    };
    assert_eq!(got, expected, "wrong value for {sql}");
}

// ---------------------------------------------------------------------------
// Unary +/- on Boolean (sqlite3: SELECT - EXISTS (SELECT 1) → -1)
// ---------------------------------------------------------------------------

#[test]
fn test_unary_minus_on_exists() {
    let db = setup_abc();
    assert_i64(&db, "SELECT - EXISTS (SELECT 1)", -1);
    assert_i64(&db, "SELECT - EXISTS (SELECT 1 WHERE 0)", 0);
}

#[test]
fn test_unary_plus_on_exists_and_comparison() {
    let db = setup_abc();
    assert_i64(&db, "SELECT + EXISTS (SELECT 1)", 1);
    assert_i64(&db, "SELECT - (5 > 3)", -1);
    assert_i64(&db, "SELECT + (5 > 3)", 1);
}

/// The dominant fuzz shape: unary minus on an EXISTS whose subquery is a
/// grouped query with a non-boolean HAVING. sqlite3:
///   SELECT - EXISTS (SELECT b FROM (SELECT 1 AS b) GROUP BY b HAVING 'first' NOT IN (SELECT -1))
///   → -1
#[test]
fn test_unary_minus_exists_grouped_having() {
    let db = setup_abc();
    assert_i64(
        &db,
        "SELECT - EXISTS ( SELECT a FROM abc GROUP BY a HAVING 'first' NOT IN (SELECT -1) )",
        -1,
    );
    // HAVING NULL filters every group → EXISTS is false → 0
    assert_i64(&db, "SELECT - EXISTS ( SELECT a FROM abc GROUP BY a HAVING NULL )", 0);
}

// ---------------------------------------------------------------------------
// Arithmetic coercion with Boolean / BLOB operands
// (sqlite3: x'313233' + 1 → 124; (1>0) + 'injection' → 1; (1>0) + 0.5 → 1.5)
// ---------------------------------------------------------------------------

#[test]
fn test_boolean_plus_text() {
    let db = setup_abc();
    // 'injection' coerces to 0; (1>0) coerces to 1
    assert_i64(&db, "SELECT (1 > 0) + 'injection'", 1);
    assert_i64(&db, "SELECT 'injection' - (1 > 0)", -1);
    assert_i64(&db, "SELECT (1 > 0) * 'hardware'", 0);
    // '7The' coerces to 7; (1=2) coerces to 0; 0 % 7 = 0
    assert_i64(&db, "SELECT (1 = 2) % '7The'", 0);
}

#[test]
fn test_boolean_plus_float_promotes() {
    let db = setup_abc();
    let v = single_value(&db, "SELECT (1 > 0) + 0.5");
    let got = match v {
        SqlValue::Real(f) => f,
        SqlValue::Double(f) => f,
        SqlValue::Float(f) => f as f64,
        SqlValue::Numeric(f) => f,
        other => panic!("expected float 1.5, got {other:?}"),
    };
    assert!((got - 1.5).abs() < 1e-9, "expected 1.5, got {got}");
}

#[test]
fn test_blob_arithmetic() {
    let db = setup_abc();
    // x'313233' is the bytes "123"
    assert_i64(&db, "SELECT x'313233' + 1", 124);
    assert_i64(&db, "SELECT x'32' * 3", 6);
    assert_i64(&db, "SELECT x'' - 5", -5);
    // non-numeric bytes coerce to 0
    assert_i64(&db, "SELECT 1 + zeroblob(16)", 1);
    assert_i64(&db, "SELECT 1 % x'32'", 1);
}

// ---------------------------------------------------------------------------
// BLOB in boolean context (AND/OR) — sqlite3: x'31' AND 1 → 1; zeroblob(4) AND 1 → 0
// ---------------------------------------------------------------------------

#[test]
fn test_blob_in_logical_operators() {
    let db = setup_abc();
    assert_i64(&db, "SELECT x'31' AND 1", 1);
    assert_i64(&db, "SELECT zeroblob(4) AND 1", 0);
    assert_i64(&db, "SELECT x'00' OR 0", 0);
    assert_i64(&db, "SELECT x'31' OR 0", 1);
}

/// Blob AND/OR inside a HAVING clause of a grouped query.
/// sqlite3: SELECT count(*) FROM abc GROUP BY a HAVING x'31' AND 1 → three rows of 1
#[test]
fn test_blob_logical_in_having() {
    let db = setup_abc();
    let rows = run_query(&db, "SELECT count(*) FROM abc GROUP BY a HAVING x'31' AND 1");
    assert_eq!(rows.len(), 3);
    let rows = run_query(&db, "SELECT count(*) FROM abc GROUP BY a HAVING zeroblob(4) AND 1");
    assert_eq!(rows.len(), 0);
}

// ---------------------------------------------------------------------------
// CASE WHEN condition truthiness in the grouped-aggregation evaluator.
// sqlite3 evaluates any CASE condition for truthiness; the aggregation path
// used to error with "CASE condition must evaluate to boolean".
// ---------------------------------------------------------------------------

#[test]
fn test_case_text_condition_with_aggregates() {
    let db = setup_abc();
    // Text condition, aggregate in THEN — forces the aggregation CASE path.
    // '1x' parses to 1 (truthy); 'x' parses to 0 (falsy). Verified in sqlite3.
    let rows =
        run_query(&db, "SELECT CASE WHEN '1x' THEN count(*) ELSE -1 END FROM abc GROUP BY a");
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(row.values[0], SqlValue::Integer(1));
    }

    let rows = run_query(&db, "SELECT CASE WHEN 'x' THEN count(*) ELSE -1 END FROM abc GROUP BY a");
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(row.values[0], SqlValue::Integer(-1));
    }
}

#[test]
fn test_case_numeric_and_blob_condition_with_aggregates() {
    let db = setup_abc();
    // Numeric (real) condition — previously unhandled in the aggregation CASE match
    let rows =
        run_query(&db, "SELECT CASE WHEN 56.1 THEN count(*) ELSE -1 END FROM abc GROUP BY a");
    for row in &rows {
        assert_eq!(row.values[0], SqlValue::Integer(1));
    }
    // Blob condition: x'31' ("1") is truthy in sqlite3
    let rows =
        run_query(&db, "SELECT CASE WHEN x'31' THEN count(*) ELSE -1 END FROM abc GROUP BY a");
    for row in &rows {
        assert_eq!(row.values[0], SqlValue::Integer(1));
    }
}

#[test]
fn test_case_text_condition_in_having() {
    let db = setup_abc();
    // sqlite3: HAVING CASE WHEN 'text' … END with aggregates
    let rows = run_query(
        &db,
        "SELECT a FROM abc GROUP BY a HAVING CASE WHEN 'first' THEN count(*) ELSE 0 END",
    );
    assert_eq!(rows.len(), 0);
    let rows = run_query(
        &db,
        "SELECT a FROM abc GROUP BY a HAVING CASE WHEN '1st' THEN count(*) ELSE 0 END",
    );
    assert_eq!(rows.len(), 3);
}

// ---------------------------------------------------------------------------
// Non-boolean HAVING acceptance in grouped queries (regression guard for the
// paths unified by #5829; sqlite3 accepts any expression in HAVING)
// ---------------------------------------------------------------------------

#[test]
fn test_having_nonboolean_shapes() {
    let db = setup_abc();
    // HAVING 'text' → falsy for all groups
    assert_eq!(run_query(&db, "SELECT count(*) FROM abc GROUP BY a HAVING 'text'").len(), 0);
    // HAVING 123 → truthy
    assert_eq!(run_query(&db, "SELECT count(*) FROM abc GROUP BY a HAVING 123").len(), 3);
    // HAVING x'31' → truthy blob
    assert_eq!(run_query(&db, "SELECT count(*) FROM abc GROUP BY a HAVING x'31'").len(), 3);
    // HAVING sum(b) → numeric truthiness
    assert_eq!(run_query(&db, "SELECT count(*) FROM abc GROUP BY a HAVING sum(b)").len(), 3);
    // HAVING NULL → falsy
    assert_eq!(run_query(&db, "SELECT count(*) FROM abc GROUP BY a HAVING NULL").len(), 0);
}
