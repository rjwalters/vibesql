//! Test for issue #4493: column resolution in nested correlated subqueries.
//!
//! An inner subquery must be able to resolve a column reference against *any*
//! enclosing query level, not just its immediate parent. With three levels of
//! nesting (outer -> middle -> innermost), a column that only exists in an
//! outer level's FROM clause must still be visible from the innermost subquery.
//!
//! Promoted from the manual repro fixtures in `tests/issue-4493/`
//! (test_minimal.sql, test_nested_simple.sql, test_nested_subquery.sql).

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

/// Helper to execute SELECT and return rows
fn select_rows(db: &Database, sql: &str) -> Vec<Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

fn text(s: &str) -> SqlValue {
    SqlValue::Varchar(StringValue::from(s))
}

/// Create tables t1(c TEXT) and t2(x TEXT) and insert one row 'a' into each
fn create_minimal_tables(db: &mut Database) {
    for (table, col) in [("t1", "c"), ("t2", "x")] {
        let schema = TableSchema::new(
            table.to_string(),
            vec![ColumnSchema::new(
                col.to_string(),
                DataType::Varchar { max_length: None },
                true,
            )],
        );
        db.create_table(schema).unwrap();
        let t = db.get_table_mut(table).unwrap();
        t.insert(Row::new(vec![text("a")])).unwrap();
    }
}

/// Create tables t1(c TEXT) and t2(x TEXT) with rows 'a','b','c' in each
fn create_three_row_tables(db: &mut Database) {
    for (table, col) in [("t1", "c"), ("t2", "x")] {
        let schema = TableSchema::new(
            table.to_string(),
            vec![ColumnSchema::new(
                col.to_string(),
                DataType::Varchar { max_length: None },
                true,
            )],
        );
        db.create_table(schema).unwrap();
        let t = db.get_table_mut(table).unwrap();
        for v in ["a", "b", "c"] {
            t.insert(Row::new(vec![text(v)])).unwrap();
        }
    }
}

/// Collect the first column of every row as sorted strings (join order is not
/// guaranteed, so assertions compare order-insensitively)
fn sorted_first_column(rows: &[Row]) -> Vec<String> {
    let mut vals: Vec<String> = rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected text value, got {other:?}"),
        })
        .collect();
    vals.sort();
    vals
}

#[test]
fn test_correlated_column_from_outer_cross_join() {
    // From test_minimal.sql: the inner subquery's `x` must resolve to the outer
    // query's t2.x (the inner FROM only provides t1.c)
    let mut db = Database::new();
    create_minimal_tables(&mut db);

    let rows = select_rows(&db, "SELECT x FROM t2, t1 WHERE c IN (SELECT c FROM t1 WHERE c = x)");
    assert_eq!(rows.len(), 1, "outer row should match via correlated inner subquery");
    assert_eq!(rows[0].values[0], text("a"));
}

#[test]
fn test_two_level_uncorrelated_baseline() {
    // From test_nested_simple.sql: simple 2-level IN subquery (baseline)
    let mut db = Database::new();
    create_minimal_tables(&mut db);

    let rows = select_rows(&db, "SELECT * FROM t1 WHERE c IN (SELECT c FROM t1 WHERE c = 'a')");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], text("a"));
}

#[test]
fn test_three_level_scalar_subquery_correlation() {
    // From test_nested_simple.sql: 3 levels — the middle subquery's `x` must
    // resolve to the outer t2.x, and the innermost scalar subquery has its own t2
    let mut db = Database::new();
    create_minimal_tables(&mut db);

    let rows = select_rows(
        &db,
        "SELECT * FROM t2, t1 WHERE x IN (SELECT x FROM t1 WHERE x = (SELECT x FROM t2 WHERE x = 'a'))",
    );
    assert_eq!(rows.len(), 1, "3-level nesting should resolve outer column x");
    // SELECT * over t2, t1 -> (x, c) = ('a', 'a')
    assert_eq!(rows[0].values.len(), 2);
    assert_eq!(rows[0].values[0], text("a"));
    assert_eq!(rows[0].values[1], text("a"));
}

#[test]
fn test_two_level_nested_in_subquery() {
    // From test_nested_subquery.sql: 2 levels, inner subquery has its own t2, t1
    // and `x = c` refers to the inner tables. Inner yields {a, b, c}, so every
    // outer cross-join row (3x3) matches.
    let mut db = Database::new();
    create_three_row_tables(&mut db);

    let rows = select_rows(
        &db,
        "SELECT x FROM t2, t1 WHERE x IN (SELECT x FROM t2, t1 WHERE x = c)",
    );
    assert_eq!(rows.len(), 9);
    assert_eq!(
        sorted_first_column(&rows),
        vec!["a", "a", "a", "b", "b", "b", "c", "c", "c"]
    );
}

#[test]
fn test_three_level_nested_in_subquery() {
    // From test_nested_subquery.sql: 3 levels. The innermost subquery's FROM only
    // provides t1.c, so its `x` must resolve through the middle level's t2.x.
    // Innermost yields the middle x whenever some t1.c equals it (always true),
    // so the middle and outer IN predicates are satisfied for every row.
    let mut db = Database::new();
    create_three_row_tables(&mut db);

    let rows = select_rows(
        &db,
        "SELECT x FROM t2, t1 WHERE x IN (
            SELECT x FROM t2, t1 WHERE x IN (
                SELECT x FROM t1 WHERE x = c
            )
        )",
    );
    assert_eq!(rows.len(), 9, "3-level nested correlated subquery should match all rows");
    assert_eq!(
        sorted_first_column(&rows),
        vec!["a", "a", "a", "b", "b", "b", "c", "c", "c"]
    );
}
