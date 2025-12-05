//! ROLLUP, CUBE, and GROUPING SETS integration tests
//!
//! Tests SQL:1999 OLAP extensions for multi-dimensional aggregation.

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn setup_db() -> Database {
    let mut db = Database::new();

    // Create sales table for testing
    let create = Parser::parse_sql(
        r#"CREATE TABLE sales (
            year INTEGER,
            quarter INTEGER,
            region TEXT,
            product TEXT,
            amount INTEGER
        )"#,
    )
    .unwrap();

    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Insert test data
    let inserts = [
        "INSERT INTO sales VALUES (2023, 1, 'East', 'Widget', 100)",
        "INSERT INTO sales VALUES (2023, 1, 'West', 'Widget', 150)",
        "INSERT INTO sales VALUES (2023, 2, 'East', 'Widget', 120)",
        "INSERT INTO sales VALUES (2023, 2, 'West', 'Gadget', 200)",
        "INSERT INTO sales VALUES (2024, 1, 'East', 'Gadget', 180)",
        "INSERT INTO sales VALUES (2024, 1, 'West', 'Widget', 140)",
    ];

    for sql in inserts {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
    }

    db
}

fn execute_query(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

fn get_i64(val: &SqlValue) -> i64 {
    match val {
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Null => -999999, // Sentinel for NULL
        _ => panic!("Expected integer, got {:?}", val),
    }
}

fn is_null(val: &SqlValue) -> bool {
    matches!(val, SqlValue::Null)
}

// ============================================================================
// ROLLUP Tests
// ============================================================================

#[test]
fn test_rollup_single_column() {
    let db = setup_db();

    // ROLLUP(year) produces: (year), ()
    let rows = execute_query(&db, "SELECT year, SUM(amount) FROM sales GROUP BY ROLLUP(year)");

    // Should have 3 rows: 2023, 2024, and grand total
    assert_eq!(rows.len(), 3, "Expected 3 rows, got {}", rows.len());

    // Check we have both year groups and grand total
    let mut found_2023 = false;
    let mut found_2024 = false;
    let mut found_grand_total = false;

    for row in &rows {
        if is_null(&row.values[0]) {
            // Grand total
            assert_eq!(get_i64(&row.values[1]), 890, "Grand total should be 890");
            found_grand_total = true;
        } else if get_i64(&row.values[0]) == 2023 {
            assert_eq!(get_i64(&row.values[1]), 570, "2023 total should be 570");
            found_2023 = true;
        } else if get_i64(&row.values[0]) == 2024 {
            assert_eq!(get_i64(&row.values[1]), 320, "2024 total should be 320");
            found_2024 = true;
        }
    }

    assert!(found_2023, "Missing 2023 row");
    assert!(found_2024, "Missing 2024 row");
    assert!(found_grand_total, "Missing grand total row");
}

#[test]
fn test_rollup_two_columns() {
    let db = setup_db();

    // ROLLUP(year, quarter) produces: (year, quarter), (year), ()
    let rows = execute_query(
        &db,
        "SELECT year, quarter, SUM(amount) FROM sales GROUP BY ROLLUP(year, quarter)",
    );

    // Should have 6 rows:
    // 2023 Q1, 2023 Q2, 2023 total, 2024 Q1, 2024 total, grand total
    assert_eq!(rows.len(), 6);

    // Verify grand total
    let grand_total_row = rows.iter().find(|r| is_null(&r.values[0]) && is_null(&r.values[1]));
    assert!(grand_total_row.is_some(), "Missing grand total row");
    assert_eq!(get_i64(&grand_total_row.unwrap().values[2]), 890);

    // Verify 2023 subtotal
    let subtotal_2023 =
        rows.iter().find(|r| get_i64(&r.values[0]) == 2023 && is_null(&r.values[1]));
    assert!(subtotal_2023.is_some(), "Missing 2023 subtotal row");
    assert_eq!(get_i64(&subtotal_2023.unwrap().values[2]), 570);
}

// ============================================================================
// CUBE Tests
// ============================================================================

#[test]
fn test_cube_single_column() {
    let db = setup_db();

    // CUBE(year) - same as ROLLUP for single column
    let rows = execute_query(&db, "SELECT year, SUM(amount) FROM sales GROUP BY CUBE(year)");

    assert_eq!(rows.len(), 3);
}

#[test]
fn test_cube_two_columns() {
    let db = setup_db();

    // CUBE(year, quarter) produces: (year, quarter), (year), (quarter), ()
    let rows = execute_query(
        &db,
        "SELECT year, quarter, SUM(amount) FROM sales GROUP BY CUBE(year, quarter)",
    );

    // Should have 8 rows:
    // 2023 Q1, 2023 Q2, 2024 Q1, (2023, NULL), (2024, NULL), (NULL, Q1), (NULL, Q2), (NULL, NULL)
    assert_eq!(rows.len(), 8);

    // Verify quarter-only subtotals exist (distinguishes CUBE from ROLLUP)
    let q1_subtotal = rows.iter().find(|r| is_null(&r.values[0]) && get_i64(&r.values[1]) == 1);
    assert!(q1_subtotal.is_some(), "Missing Q1-only subtotal (CUBE feature)");
    assert_eq!(get_i64(&q1_subtotal.unwrap().values[2]), 570); // 2023Q1 + 2024Q1 = 250 + 320 = 570

    let q2_subtotal = rows.iter().find(|r| is_null(&r.values[0]) && get_i64(&r.values[1]) == 2);
    assert!(q2_subtotal.is_some(), "Missing Q2-only subtotal (CUBE feature)");
    assert_eq!(get_i64(&q2_subtotal.unwrap().values[2]), 320); // 2023Q2 only
}

// ============================================================================
// GROUPING SETS Tests
// ============================================================================

#[test]
fn test_grouping_sets_basic() {
    let db = setup_db();

    // GROUPING SETS((year, quarter), (year), ())
    let rows = execute_query(
        &db,
        "SELECT year, quarter, SUM(amount) FROM sales GROUP BY GROUPING SETS((year, quarter), (year), ())",
    );

    // Same as ROLLUP(year, quarter) - 6 rows
    assert_eq!(rows.len(), 6);
}

#[test]
fn test_grouping_sets_grand_total_only() {
    let db = setup_db();

    // GROUPING SETS(()) - just grand total
    let rows = execute_query(&db, "SELECT SUM(amount) FROM sales GROUP BY GROUPING SETS(())");

    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0].values[0]), 890);
}

#[test]
fn test_grouping_sets_specific_combinations() {
    let db = setup_db();

    // Explicitly specify only certain combinations
    let rows = execute_query(
        &db,
        "SELECT region, year, SUM(amount) FROM sales GROUP BY GROUPING SETS((region), (year))",
    );

    // Should have 4 rows: East, West, 2023, 2024
    assert_eq!(rows.len(), 4);
}

// ============================================================================
// GROUPING() Function Tests
// ============================================================================

#[test]
fn test_grouping_function_with_rollup() {
    let db = setup_db();

    // GROUPING() returns 1 if column is aggregated (NULL from ROLLUP), 0 otherwise
    let rows = execute_query(
        &db,
        "SELECT year, GROUPING(year), SUM(amount) FROM sales GROUP BY ROLLUP(year)",
    );

    assert_eq!(rows.len(), 3);

    // Find grand total row
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]));
    assert!(grand_total.is_some());
    assert_eq!(get_i64(&grand_total.unwrap().values[1]), 1); // GROUPING(year) = 1 for grand total

    // Find 2023 row
    let row_2023 = rows.iter().find(|r| get_i64(&r.values[0]) == 2023);
    assert!(row_2023.is_some());
    assert_eq!(get_i64(&row_2023.unwrap().values[1]), 0); // GROUPING(year) = 0 for regular group
}

#[test]
fn test_grouping_function_multiple_columns() {
    let db = setup_db();

    let rows = execute_query(
        &db,
        "SELECT year, quarter, GROUPING(year), GROUPING(quarter), SUM(amount)
         FROM sales GROUP BY ROLLUP(year, quarter)",
    );

    assert_eq!(rows.len(), 6);

    // Grand total: GROUPING(year)=1, GROUPING(quarter)=1
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]) && is_null(&r.values[1]));
    assert!(grand_total.is_some());
    assert_eq!(get_i64(&grand_total.unwrap().values[2]), 1); // GROUPING(year)
    assert_eq!(get_i64(&grand_total.unwrap().values[3]), 1); // GROUPING(quarter)

    // Year subtotal: GROUPING(year)=0, GROUPING(quarter)=1
    let year_subtotal =
        rows.iter().find(|r| get_i64(&r.values[0]) == 2023 && is_null(&r.values[1]));
    assert!(year_subtotal.is_some());
    assert_eq!(get_i64(&year_subtotal.unwrap().values[2]), 0); // GROUPING(year)
    assert_eq!(get_i64(&year_subtotal.unwrap().values[3]), 1); // GROUPING(quarter)

    // Detail row: GROUPING(year)=0, GROUPING(quarter)=0
    let detail = rows.iter().find(|r| get_i64(&r.values[0]) == 2023 && get_i64(&r.values[1]) == 1);
    assert!(detail.is_some());
    assert_eq!(get_i64(&detail.unwrap().values[2]), 0); // GROUPING(year)
    assert_eq!(get_i64(&detail.unwrap().values[3]), 0); // GROUPING(quarter)
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_rollup_with_having() {
    let db = setup_db();

    let rows = execute_query(
        &db,
        "SELECT year, quarter, SUM(amount) FROM sales GROUP BY ROLLUP(year, quarter) HAVING SUM(amount) > 300",
    );

    // Only rows with SUM > 300
    for row in &rows {
        assert!(get_i64(&row.values[2]) > 300);
    }
}

#[test]
fn test_rollup_with_order_by() {
    let db = setup_db();

    let rows = execute_query(
        &db,
        "SELECT year, quarter, SUM(amount) AS total FROM sales GROUP BY ROLLUP(year, quarter) ORDER BY total DESC",
    );

    // Verify ordering
    for i in 1..rows.len() {
        let prev = get_i64(&rows[i - 1].values[2]);
        let curr = get_i64(&rows[i].values[2]);
        assert!(prev >= curr, "Rows not properly ordered by total DESC");
    }
}

#[test]
fn test_rollup_composite_element() {
    let db = setup_db();

    // ROLLUP((year, quarter), region) - (year, quarter) treated as single unit
    // Produces: ((year, quarter), region), ((year, quarter)), ()
    let rows = execute_query(
        &db,
        "SELECT year, quarter, region, SUM(amount) FROM sales GROUP BY ROLLUP((year, quarter), region)",
    );

    // Verify we have the expected grouping structure
    // Should have: detail rows, (year,quarter) subtotals, and grand total
    let grand_total = rows
        .iter()
        .find(|r| is_null(&r.values[0]) && is_null(&r.values[1]) && is_null(&r.values[2]));
    assert!(grand_total.is_some(), "Missing grand total");
    assert_eq!(get_i64(&grand_total.unwrap().values[3]), 890);
}

#[test]
fn test_cube_with_count_distinct() {
    let db = setup_db();

    let rows = execute_query(
        &db,
        "SELECT region, COUNT(DISTINCT product) FROM sales GROUP BY CUBE(region)",
    );

    // Verify we get the grand total with distinct count
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]));
    assert!(grand_total.is_some());
    assert_eq!(get_i64(&grand_total.unwrap().values[1]), 2); // Widget and Gadget
}

// ============================================================================
// GROUPING_ID() Function Tests
// ============================================================================

#[test]
fn test_grouping_id_with_rollup_two_columns() {
    let db = setup_db();

    // GROUPING_ID(year, quarter) returns a bitmap:
    // - (year, quarter): 0 (binary 00) - both present
    // - (year, NULL): 1 (binary 01) - quarter rolled up
    // - (NULL, NULL): 3 (binary 11) - both rolled up
    let rows = execute_query(
        &db,
        "SELECT year, quarter, GROUPING_ID(year, quarter), SUM(amount)
         FROM sales GROUP BY ROLLUP(year, quarter)",
    );

    assert_eq!(rows.len(), 6);

    // Grand total: GROUPING_ID = 3 (binary 11)
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]) && is_null(&r.values[1]));
    assert!(grand_total.is_some());
    assert_eq!(get_i64(&grand_total.unwrap().values[2]), 3); // GROUPING_ID(year, quarter) = 3

    // Year subtotal: GROUPING_ID = 1 (binary 01)
    let year_subtotal =
        rows.iter().find(|r| get_i64(&r.values[0]) == 2023 && is_null(&r.values[1]));
    assert!(year_subtotal.is_some());
    assert_eq!(get_i64(&year_subtotal.unwrap().values[2]), 1); // GROUPING_ID(year, quarter) = 1

    // Detail row: GROUPING_ID = 0 (binary 00)
    let detail = rows.iter().find(|r| get_i64(&r.values[0]) == 2023 && get_i64(&r.values[1]) == 1);
    assert!(detail.is_some());
    assert_eq!(get_i64(&detail.unwrap().values[2]), 0); // GROUPING_ID(year, quarter) = 0
}

#[test]
fn test_grouping_id_with_cube_three_columns() {
    let db = setup_db();

    // CUBE(year, quarter, region) produces 2^3 = 8 grouping combinations
    // GROUPING_ID shows which columns are rolled up as a bitmap
    let rows = execute_query(
        &db,
        "SELECT year, quarter, region, GROUPING_ID(year, quarter, region), SUM(amount)
         FROM sales GROUP BY CUBE(year, quarter, region)",
    );

    // For CUBE(year, quarter, region), all 8 combinations should be present
    // Grand total: GROUPING_ID = 7 (binary 111)
    let grand_total = rows
        .iter()
        .find(|r| is_null(&r.values[0]) && is_null(&r.values[1]) && is_null(&r.values[2]));
    assert!(grand_total.is_some());
    assert_eq!(get_i64(&grand_total.unwrap().values[3]), 7); // All rolled up

    // region-only subtotal: GROUPING_ID = 6 (binary 110) - year and quarter rolled up
    let region_only = rows
        .iter()
        .find(|r| is_null(&r.values[0]) && is_null(&r.values[1]) && !is_null(&r.values[2]));
    assert!(region_only.is_some());
    assert_eq!(get_i64(&region_only.unwrap().values[3]), 6);

    // Detail row: GROUPING_ID = 0 (binary 000)
    let detail = rows
        .iter()
        .find(|r| !is_null(&r.values[0]) && !is_null(&r.values[1]) && !is_null(&r.values[2]));
    assert!(detail.is_some());
    assert_eq!(get_i64(&detail.unwrap().values[3]), 0);
}

#[test]
fn test_grouping_id_single_column() {
    let db = setup_db();

    // GROUPING_ID with single column - same as GROUPING()
    let rows = execute_query(
        &db,
        "SELECT year, GROUPING_ID(year), SUM(amount) FROM sales GROUP BY ROLLUP(year)",
    );

    assert_eq!(rows.len(), 3);

    // Grand total: GROUPING_ID(year) = 1
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]));
    assert!(grand_total.is_some());
    assert_eq!(get_i64(&grand_total.unwrap().values[1]), 1);

    // Regular row: GROUPING_ID(year) = 0
    let regular = rows.iter().find(|r| get_i64(&r.values[0]) == 2023);
    assert!(regular.is_some());
    assert_eq!(get_i64(&regular.unwrap().values[1]), 0);
}

#[test]
fn test_grouping_id_with_grouping_sets() {
    let db = setup_db();

    // GROUPING SETS allows explicit grouping specification
    let rows = execute_query(
        &db,
        "SELECT year, quarter, GROUPING_ID(year, quarter), SUM(amount)
         FROM sales GROUP BY GROUPING SETS((year, quarter), (year), ())",
    );

    assert_eq!(rows.len(), 6);

    // Verify GROUPING_ID values match the grouping sets
    // Grand total (): GROUPING_ID = 3
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]) && is_null(&r.values[1]));
    assert!(grand_total.is_some());
    assert_eq!(get_i64(&grand_total.unwrap().values[2]), 3);

    // Year only (year): GROUPING_ID = 1
    let year_only = rows.iter().find(|r| get_i64(&r.values[0]) == 2023 && is_null(&r.values[1]));
    assert!(year_only.is_some());
    assert_eq!(get_i64(&year_only.unwrap().values[2]), 1);

    // Detail (year, quarter): GROUPING_ID = 0
    let detail = rows.iter().find(|r| get_i64(&r.values[0]) == 2023 && get_i64(&r.values[1]) == 1);
    assert!(detail.is_some());
    assert_eq!(get_i64(&detail.unwrap().values[2]), 0);
}

#[test]
fn test_grouping_id_subset_of_groupby_columns() {
    let db = setup_db();

    // GROUPING_ID can be called with a subset of GROUP BY columns
    let rows = execute_query(
        &db,
        "SELECT year, quarter, region, GROUPING_ID(year, region), SUM(amount)
         FROM sales GROUP BY ROLLUP(year, quarter, region)",
    );

    // Find the row where only year is present (quarter and region are rolled up)
    // GROUPING_ID(year, region) for this row should be:
    // year present (0), region rolled up (1) = binary 01 = 1
    let year_only = rows
        .iter()
        .find(|r| !is_null(&r.values[0]) && is_null(&r.values[1]) && is_null(&r.values[2]));
    assert!(year_only.is_some());
    assert_eq!(get_i64(&year_only.unwrap().values[3]), 1);

    // Grand total: GROUPING_ID(year, region) = 3 (both rolled up)
    let grand_total = rows
        .iter()
        .find(|r| is_null(&r.values[0]) && is_null(&r.values[1]) && is_null(&r.values[2]));
    assert!(grand_total.is_some());
    assert_eq!(get_i64(&grand_total.unwrap().values[3]), 3);
}

#[test]
fn test_grouping_in_order_by_tpcds_q70_pattern() {
    // This test replicates the TPC-DS Q70 pattern that was failing:
    // ORDER BY with a CASE expression containing GROUPING() in the condition
    let db = setup_db();

    // Q70-style query: GROUPING() sum in SELECT list, CASE with GROUPING() in ORDER BY
    let rows = execute_query(
        &db,
        "SELECT
            SUM(amount) AS total,
            year,
            quarter,
            GROUPING(year) + GROUPING(quarter) AS lochierarchy
         FROM sales
         GROUP BY ROLLUP(year, quarter)
         ORDER BY
            lochierarchy DESC,
            CASE WHEN GROUPING(year) + GROUPING(quarter) = 0 THEN year END
         LIMIT 10",
    );

    // Should have at least 1 row
    assert!(!rows.is_empty(), "Expected at least one row");

    // Verify grand total row is first (lochierarchy = 2)
    assert_eq!(get_i64(&rows[0].values[3]), 2, "First row should be grand total (lochierarchy=2)");
}
