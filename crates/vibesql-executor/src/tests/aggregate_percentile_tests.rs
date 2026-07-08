//! PERCENTILE family aggregate tests: median(Y), percentile(Y,P),
//! percentile_cont(Y,F), percentile_disc(Y,F)
//!
//! Semantics follow SQLite's ext/misc/percentile.c extension (built into
//! modern SQLite by default). Expected values below were verified against
//! sqlite3 3.51.0 (see issue #5818).

use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use super::super::*;
use crate::select::grouping::AggregateAccumulator;

/// Execute a SQL statement; errors are mapped through Display so that
/// SQLite-exact messages (SqliteCompatError et al) can be asserted verbatim.
fn execute_sql(db: &mut Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| e.message)?;

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt).map_err(|e| e.to_string())
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db).map_err(|e| e.to_string())?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt).map_err(|e| e.to_string())?;
            Ok(vec![])
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

/// Build the canonical percentile.test table:
/// t1(x) = 1,4,6,7,8,9,11,11,11
fn setup_t1(db: &mut Database) {
    execute_sql(db, "CREATE TABLE t1(x REAL)").unwrap();
    execute_sql(db, "INSERT INTO t1 VALUES(1),(4),(6),(7),(8),(9),(11),(11),(11)").unwrap();
}

/// Run a single-value aggregate query and return the Display form of the
/// result (SQLite REAL formatting, e.g. "8.0").
fn query_display(db: &mut Database, sql: &str) -> String {
    let rows = execute_sql(db, sql).unwrap();
    assert_eq!(rows.len(), 1, "expected a single row from {sql}");
    rows[0].values[0].to_string()
}

#[test]
fn test_percentile_basic_values() {
    // sqlite3-verified: percentile(x,0) -> 1.0, percentile(x,15) -> 4.4,
    // percentile(x,20) -> 5.2, percentile(x,50) -> 8.0, percentile(x,100) -> 11.0
    let mut db = Database::new();
    setup_t1(&mut db);

    assert_eq!(query_display(&mut db, "SELECT percentile(x,0) FROM t1"), "1.0");
    assert_eq!(query_display(&mut db, "SELECT percentile(x,15) FROM t1"), "4.4");
    assert_eq!(query_display(&mut db, "SELECT percentile(x,20) FROM t1"), "5.2");
    assert_eq!(query_display(&mut db, "SELECT percentile(x,50) FROM t1"), "8.0");
    assert_eq!(query_display(&mut db, "SELECT percentile(x,100) FROM t1"), "11.0");
    // The 11,11,11 tail: percentile(x,80) -> 11.0
    assert_eq!(query_display(&mut db, "SELECT percentile(x,80) FROM t1"), "11.0");
    assert_eq!(query_display(&mut db, "SELECT percentile(x,12.5) FROM t1"), "4.0");
}

#[test]
fn test_percentile_cont_disc_median() {
    let mut db = Database::new();
    setup_t1(&mut db);

    assert_eq!(query_display(&mut db, "SELECT percentile_cont(x,0.15) FROM t1"), "4.4");
    assert_eq!(query_display(&mut db, "SELECT percentile_disc(x,0.15) FROM t1"), "4.0");
    assert_eq!(query_display(&mut db, "SELECT percentile_disc(x,0.5) FROM t1"), "8.0");
    // sqlite3-verified: median(x) -> 8.0
    assert_eq!(query_display(&mut db, "SELECT median(x) FROM t1"), "8.0");
}

#[test]
fn test_percentile_nulls_ignored() {
    // sqlite3-verified after adding two NULLs: percentile(x,50) -> 8.0,
    // percentile_cont(x,0.125) -> 4.0, percentile_disc(x,0.89) -> 11.0
    let mut db = Database::new();
    setup_t1(&mut db);
    execute_sql(&mut db, "INSERT INTO t1 VALUES(NULL),(NULL)").unwrap();

    assert_eq!(query_display(&mut db, "SELECT percentile(x,50) FROM t1"), "8.0");
    assert_eq!(query_display(&mut db, "SELECT percentile_cont(x,0.125) FROM t1"), "4.0");
    assert_eq!(query_display(&mut db, "SELECT percentile_disc(x,0.89) FROM t1"), "11.0");
    assert_eq!(query_display(&mut db, "SELECT median(x) FROM t1"), "8.0");
}

#[test]
fn test_percentile_empty_and_all_null_returns_null() {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(x REAL)").unwrap();

    // Empty table -> NULL (requirement 8)
    let rows = execute_sql(&mut db, "SELECT percentile(x,50) FROM t1").unwrap();
    assert_eq!(rows[0].values[0], SqlValue::Null);

    // All-NULL input -> NULL
    execute_sql(&mut db, "INSERT INTO t1 VALUES(NULL),(NULL)").unwrap();
    let rows = execute_sql(&mut db, "SELECT median(x) FROM t1").unwrap();
    assert_eq!(rows[0].values[0], SqlValue::Null);
}

#[test]
fn test_percentile_single_value_always_real() {
    // Requirement 9 + 11: one non-NULL value returns that value, always REAL
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(x)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(NULL),(12345),(NULL)").unwrap();

    assert_eq!(query_display(&mut db, "SELECT percentile(x,0) FROM t1"), "12345.0");
    assert_eq!(query_display(&mut db, "SELECT percentile(x,50) FROM t1"), "12345.0");
    assert_eq!(query_display(&mut db, "SELECT percentile(x,100) FROM t1"), "12345.0");
}

#[test]
fn test_percentile_integer_fraction_argument() {
    // Fraction supplied as INTEGER must work (percentile(x,50), cont(x,1))
    let mut db = Database::new();
    setup_t1(&mut db);

    assert_eq!(query_display(&mut db, "SELECT percentile_cont(x,1) FROM t1"), "11.0");
    assert_eq!(query_display(&mut db, "SELECT percentile_cont(x,0) FROM t1"), "1.0");
    assert_eq!(query_display(&mut db, "SELECT percentile_disc(x,1) FROM t1"), "11.0");
}

#[test]
fn test_percentile_fraction_out_of_range_errors() {
    // sqlite3-verified error strings (tests percentile-1.10 through 1.14.3)
    let mut db = Database::new();
    setup_t1(&mut db);

    for sql in [
        "SELECT percentile(x,null) FROM t1",
        "SELECT percentile(x,'fifty') FROM t1",
        "SELECT percentile(x,-0.0000001) FROM t1",
        "SELECT percentile(x,100.0000001) FROM t1",
    ] {
        let err = execute_sql(&mut db, sql).unwrap_err();
        assert_eq!(
            err, "the fraction argument to percentile() is not between 0.0 and 100.0",
            "wrong error for {sql}"
        );
    }

    let err = execute_sql(&mut db, "SELECT percentile_cont(x,1.0000001) FROM t1").unwrap_err();
    assert_eq!(err, "the fraction argument to percentile_cont() is not between 0.0 and 1.0");
    let err = execute_sql(&mut db, "SELECT percentile_disc(x,1.0000001) FROM t1").unwrap_err();
    assert_eq!(err, "the fraction argument to percentile_disc() is not between 0.0 and 1.0");
}

#[test]
fn test_percentile_fraction_consistency() {
    // percentile-1.4/1.5: P may vary by at most 0.001 (normalized) vs the
    // FIRST row. 15+0.000001*rowid passes; 15+0.1*rowid errors.
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(id INTEGER, x REAL)").unwrap();
    execute_sql(
        &mut db,
        "INSERT INTO t1 VALUES(1,1),(2,4),(3,6),(4,7),(5,8),(6,9),(7,11),(8,11),(9,11)",
    )
    .unwrap();

    let rows =
        execute_sql(&mut db, "SELECT round(percentile(x, 15+0.000001*id),1) FROM t1").unwrap();
    assert_eq!(rows[0].values[0].to_string(), "4.4");

    let err = execute_sql(&mut db, "SELECT percentile(x, 15+0.1*id) FROM t1").unwrap_err();
    assert_eq!(err, "the fraction argument to percentile() is not the same for all input rows");
    let err =
        execute_sql(&mut db, "SELECT percentile_cont(x, (15+0.1*id)*0.01) FROM t1").unwrap_err();
    assert_eq!(
        err,
        "the fraction argument to percentile_cont() is not the same for all input rows"
    );
}

#[test]
fn test_percentile_tolerance_boundary_direct() {
    // percentSameValue: |diff| <= 0.001 (on the NORMALIZED fraction P/100)
    // is "same". All boundary expectations below are sqlite3 3.51.0-verified
    // with the identical double arithmetic:
    //   15.0 vs 15.0999 -> diff  -0.000999.. -> accepted
    //   15.0 vs 15.1    -> diff  -0.0010000000000000009 (float rounding
    //                      lands just OUTSIDE 0.001)   -> error
    use vibesql_types::SqlValue as V;

    // Just inside the tolerance: OK
    let mut acc = AggregateAccumulator::new("PERCENTILE", false).unwrap();
    acc.accumulate_percentile(&V::Double(1.0), Some(&V::Double(15.0)));
    acc.accumulate_percentile(&V::Double(2.0), Some(&V::Double(15.0999)));
    assert!(acc.finalize().is_ok(), "normalized diff just below 0.001 must be accepted");

    // 15.0 vs 15.1: sqlite3-verified error (float diff exceeds 0.001)
    let mut acc = AggregateAccumulator::new("PERCENTILE", false).unwrap();
    acc.accumulate_percentile(&V::Double(1.0), Some(&V::Double(15.0)));
    acc.accumulate_percentile(&V::Double(2.0), Some(&V::Double(15.1)));
    let err = acc.finalize().unwrap_err().to_string();
    assert_eq!(err, "the fraction argument to percentile() is not the same for all input rows");

    // Clearly outside: error
    let mut acc = AggregateAccumulator::new("PERCENTILE", false).unwrap();
    acc.accumulate_percentile(&V::Double(1.0), Some(&V::Double(15.0)));
    acc.accumulate_percentile(&V::Double(2.0), Some(&V::Double(15.2)));
    assert!(acc.finalize().is_err());

    // Comparison is anchored to the FIRST row's fraction, not the previous
    // row's: 15.0, 15.0999, 15.1998 errors on the third row even though each
    // adjacent step is within tolerance.
    let mut acc = AggregateAccumulator::new("PERCENTILE", false).unwrap();
    acc.accumulate_percentile(&V::Double(1.0), Some(&V::Double(15.0)));
    acc.accumulate_percentile(&V::Double(2.0), Some(&V::Double(15.0999)));
    acc.accumulate_percentile(&V::Double(3.0), Some(&V::Double(15.1998)));
    assert!(acc.finalize().is_err(), "tolerance must be anchored to the first row");
}

#[test]
fn test_percentile_non_numeric_input_errors() {
    // Requirement 4: non-NULL non-numeric Y errors by TYPE - no coercion.
    // TEXT '50' is an error even though it looks numeric (percentile-1.15).
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t2(x)").unwrap();
    execute_sql(&mut db, "INSERT INTO t2 VALUES('50'),(1)").unwrap();

    let err = execute_sql(&mut db, "SELECT median(x) FROM t2").unwrap_err();
    assert_eq!(err, "input to median() is not numeric");
    let err = execute_sql(&mut db, "SELECT percentile(x, 50) FROM t2").unwrap_err();
    assert_eq!(err, "input to percentile() is not numeric");
    let err = execute_sql(&mut db, "SELECT percentile_cont(x, 0.5) FROM t2").unwrap_err();
    assert_eq!(err, "input to percentile_cont() is not numeric");
    let err = execute_sql(&mut db, "SELECT percentile_disc(x, 0.5) FROM t2").unwrap_err();
    assert_eq!(err, "input to percentile_disc() is not numeric");
}

#[test]
fn test_percentile_infinity_errors() {
    // Requirement 5: +/-Inf input errors (percentile-1.20/1.21)
    let mut db = Database::new();
    setup_t1(&mut db);
    execute_sql(&mut db, "INSERT INTO t1 VALUES(1.0e300*1.0e300)").unwrap();

    let err = execute_sql(&mut db, "SELECT percentile(x,50) FROM t1").unwrap_err();
    assert_eq!(err, "Inf input to percentile()");
    let err = execute_sql(&mut db, "SELECT median(x) FROM t1").unwrap_err();
    assert_eq!(err, "Inf input to median()");

    let mut db = Database::new();
    setup_t1(&mut db);
    execute_sql(&mut db, "INSERT INTO t1 VALUES(-1.0e300*1.0e300)").unwrap();
    let err = execute_sql(&mut db, "SELECT percentile(x,50) FROM t1").unwrap_err();
    assert_eq!(err, "Inf input to percentile()");
}

#[test]
fn test_percentile_wrong_number_of_arguments() {
    // percentile-1.8/1.9: SQLite-exact arg-count errors
    let mut db = Database::new();
    setup_t1(&mut db);

    for (sql, func) in [
        ("SELECT percentile(x,0,1) FROM t1", "percentile"),
        ("SELECT percentile_cont(x,0,1) FROM t1", "percentile_cont"),
        ("SELECT percentile_disc(x,0,1) FROM t1", "percentile_disc"),
        ("SELECT median(x,0) FROM t1", "median"),
        ("SELECT percentile(x) FROM t1", "percentile"),
        ("SELECT percentile_cont(x) FROM t1", "percentile_cont"),
        ("SELECT percentile_disc(x) FROM t1", "percentile_disc"),
        ("SELECT median() FROM t1", "median"),
    ] {
        let err = execute_sql(&mut db, sql).unwrap_err();
        assert_eq!(
            err,
            format!("wrong number of arguments to function {func}()"),
            "wrong error for {sql}"
        );
    }
}

#[test]
fn test_percentile_random_input_order() {
    // percentile-1.6/1.7: input order must not matter
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t2(x REAL)").unwrap();
    execute_sql(&mut db, "INSERT INTO t2 VALUES(11),(8),(1),(11),(9),(4),(11),(7),(6)").unwrap();

    assert_eq!(query_display(&mut db, "SELECT percentile(x,50) FROM t2"), "8.0");
    assert_eq!(query_display(&mut db, "SELECT percentile(x,15) FROM t2"), "4.4");
    assert_eq!(query_display(&mut db, "SELECT percentile_disc(x,0.2) FROM t2"), "4.0");
}

#[test]
fn test_median_group_by() {
    // percentile-4.2: median with GROUP BY
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE products(vendorId INT, price REAL)").unwrap();
    execute_sql(
        &mut db,
        "INSERT INTO products VALUES (1001,25.99),(1001,25.99),(1001,14.75),(1001,11.99),\
         (1002,33.49),(1003,245.00),(1003,55.99),(1003,11.01),(1004,12.45),(1004,9.99)",
    )
    .unwrap();

    let rows =
        execute_sql(&mut db, "SELECT vendorId, median(price) FROM products GROUP BY 1 ORDER BY 1")
            .unwrap();
    assert_eq!(rows.len(), 4);
    let got: Vec<(String, String)> =
        rows.iter().map(|r| (r.values[0].to_string(), r.values[1].to_string())).collect();
    assert_eq!(
        got,
        vec![
            ("1001".to_string(), "20.37".to_string()),
            ("1002".to_string(), "33.49".to_string()),
            ("1003".to_string(), "55.99".to_string()),
            ("1004".to_string(), "11.22".to_string()),
        ]
    );
}

#[test]
fn test_median_window_function() {
    // percentile-4.1 (abridged): median() as a window function with
    // PARTITION BY
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE products(vendorId INT, price REAL)").unwrap();
    execute_sql(
        &mut db,
        "INSERT INTO products VALUES (1001,25.99),(1001,25.99),(1001,14.75),(1001,11.99),\
         (1002,33.49)",
    )
    .unwrap();

    let rows = execute_sql(
        &mut db,
        "SELECT vendorId, median(price) OVER (PARTITION BY vendorId) FROM products \
         ORDER BY vendorId",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    for row in &rows[0..4] {
        assert_eq!(row.values[1].to_string(), "20.37");
    }
    assert_eq!(rows[4].values[1].to_string(), "33.49");
}

#[test]
fn test_percentile_window_function_with_fraction() {
    // percentile-5.1 (abridged): percentile(cost, P) OVER (PARTITION BY class)
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE user(name TEXT, class TEXT, cost REAL)").unwrap();
    execute_sql(
        &mut db,
        "INSERT INTO user VALUES ('Hank','W',24.99),('Irma','W',24.99),('Mia','W',59.53),\
         ('Nate','W',23.50)",
    )
    .unwrap();

    let rows = execute_sql(
        &mut db,
        "SELECT name, percentile(cost, 25) OVER (PARTITION BY class) FROM user ORDER BY name",
    )
    .unwrap();
    assert_eq!(rows.len(), 4);
    // sqlite3-verified: P25 of {23.5, 24.99, 24.99, 59.53} is 24.6175
    for row in &rows {
        assert_eq!(row.values[1].to_string(), "24.6175");
    }
}

#[test]
fn test_median_fuzzer_regression() {
    // percentile-6.0 fuzzer find, sqlite3-verified -> 0.55
    let mut db = Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<12) \
         SELECT median(iif(n%2,0.1,1.0)) FROM c",
    )
    .unwrap();
    assert_eq!(rows[0].values[0].to_string(), "0.55");
}
