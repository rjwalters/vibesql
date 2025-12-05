//! TPC-H Subquery Validation Tests
//!
//! Tests to validate correctness of TPC-H queries with subqueries:
//! - Q11: Scalar subquery in HAVING clause
//! - Q13: Subquery in FROM with LEFT OUTER JOIN
//! - Q20: Nested IN subqueries with correlation
//! - Q21: Correlated EXISTS subqueries
//!
//! These tests execute on SF 0.01 dataset and validate:
//! 1. Queries execute without errors
//! 2. Results are non-empty (where expected)
//! 3. Basic result count validation
//!
//! Related to issue #2426

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

// Import TPC-H infrastructure
#[path = "../benches/tpch/mod.rs"]
mod tpch;
use tpch::queries::*;
use tpch::schema::load_vibesql;

/// Helper to execute a TPC-H query and return row count
fn execute_tpch_query(db: &Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute(&select).map_err(|e| format!("Execution error: {:?}", e))?;
        Ok(result.len())
    } else {
        Err("Not a SELECT statement".to_string())
    }
}

#[test]
fn test_q2_correlated_scalar_subquery() {
    // Q2: Minimum Cost Supplier
    // Tests correlated scalar subquery in WHERE clause
    // Pattern: Correlated on p_partkey - subquery references outer query column
    // Performance: Previously timed out (>30s), should now complete with caching
    // This test validates issue #2452 is fixed

    let db = load_vibesql(0.01);

    let result = execute_tpch_query(&db, TPCH_Q2);

    match result {
        Ok(count) => {
            println!("Q2 executed successfully: {} rows", count);
            // Q2 has LIMIT 100, so should return at most 100 rows
            assert!(count <= 100, "Q2 has LIMIT 100");
            // Should return at least some suppliers (exact count varies by data)
            println!("Q2 completed without timeout (row count: {})", count);
        }
        Err(e) => {
            panic!("Q2 failed to execute: {}", e);
        }
    }
}

#[test]
fn test_q11_scalar_subquery_having() {
    // Q11: Important Stock Identification
    // Tests scalar subquery in HAVING clause
    // Pattern: Uncorrelated scalar subquery for threshold computation
    // Performance: Fast (~3s on SF 0.01) with scalar subquery caching (#2431)

    let db = load_vibesql(0.01);

    let result = execute_tpch_query(&db, TPCH_Q11);

    match result {
        Ok(count) => {
            println!("Q11 executed successfully: {} rows", count);
            // Q11 should return some parts that meet the threshold
            // Exact count depends on data, but should be non-zero
            assert!(count > 0, "Q11 should return at least one row");
        }
        Err(e) => {
            panic!("Q11 failed to execute: {}", e);
        }
    }
}

#[test]
fn test_q13_complex_join_subquery() {
    // Q13: Customer Distribution
    // Tests subquery in FROM with LEFT OUTER JOIN and aggregate
    // Pattern: No decorrelation needed, uses hash join for LEFT OUTER JOIN
    // Performance: Fast (~2s on SF 0.01) thanks to hash join optimization (#2465)

    let db = load_vibesql(0.01);

    let result = execute_tpch_query(&db, TPCH_Q13);

    match result {
        Ok(count) => {
            println!("Q13 executed successfully: {} rows", count);
            // Q13 returns distribution of customer order counts
            // Should have multiple groups
            assert!(count > 0, "Q13 should return at least one row");
        }
        Err(e) => {
            panic!("Q13 failed to execute: {}", e);
        }
    }
}

#[test]
fn test_q20_nested_in_subqueries() {
    // Q20: Potential Part Promotion
    // Tests nested IN subqueries with correlated scalar subquery
    // Pattern: IN with nested IN, plus correlated scalar subquery
    // Performance: Fast (~3s on SF 0.01) with semi-join optimization (#2405, #2475)

    let db = load_vibesql(0.01);

    let result = execute_tpch_query(&db, TPCH_Q20);

    match result {
        Ok(count) => {
            println!("Q20 executed successfully: {} rows", count);
            // Q20 may return 0 rows on small dataset if no suppliers meet criteria
            // Just verify it doesn't crash
            println!("Q20 completed without errors (row count: {})", count);
        }
        Err(e) => {
            panic!("Q20 failed to execute: {}", e);
        }
    }
}

#[test]
fn test_q21_correlated_exists() {
    // Q21: Suppliers Who Kept Orders Waiting
    // Tests EXISTS with correlated predicate and inequality
    // Pattern: EXISTS with complex correlation
    // Performance: Moderate (~36s on SF 0.01) with EXISTS to join conversion (#2481, #2473)

    let db = load_vibesql(0.01);

    let result = execute_tpch_query(&db, TPCH_Q21);

    match result {
        Ok(count) => {
            println!("Q21 executed successfully: {} rows", count);
            // Q21 is limited to 100 rows by the query itself
            assert!(count <= 100, "Q21 has LIMIT 100");
            // May return 0 rows on small dataset if no suppliers meet criteria
            println!("Q21 completed without errors (row count: {})", count);
        }
        Err(e) => {
            panic!("Q21 failed to execute: {}", e);
        }
    }
}

#[test]
fn test_q11_q13_batch() {
    // Run Q11 and Q13 together to validate both work
    // Performance: Both queries now complete quickly
    // Q11: ~3s with scalar subquery caching (#2431)
    // Q13: ~3s with hash join optimization (#2472)

    let db = load_vibesql(0.01);

    // Test Q11
    let q11_result = execute_tpch_query(&db, TPCH_Q11);
    assert!(q11_result.is_ok(), "Q11 should execute: {:?}", q11_result);
    println!("Q11: {} rows", q11_result.unwrap());

    // Test Q13
    let q13_result = execute_tpch_query(&db, TPCH_Q13);
    assert!(q13_result.is_ok(), "Q13 should execute: {:?}", q13_result);
    println!("Q13: {} rows", q13_result.unwrap());
}

#[test]
fn test_q18_in_subquery_with_having() {
    // Q18: Large Volume Customer
    // Tests IN subquery with GROUP BY and HAVING clause
    // Pattern: o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)
    // Issue #2898: Query was crashing/returning 0 rows

    let db = load_vibesql(0.01);

    // First, test the inner subquery alone
    let subquery = r#"
        SELECT l_orderkey, SUM(l_quantity) as total_qty
        FROM lineitem
        GROUP BY l_orderkey
        HAVING SUM(l_quantity) > 300
    "#;

    println!("\n=== Q18 Inner Subquery Test ===");
    let subquery_result = execute_tpch_query(&db, subquery);
    println!("Subquery result: {:?}", subquery_result);

    // At SF 0.01, there might not be orders with > 300 quantity
    // But the query should execute without errors
    assert!(subquery_result.is_ok(), "Q18 subquery should execute: {:?}", subquery_result);
    let subquery_count = subquery_result.unwrap();
    println!("Q18 subquery returned {} rows (may be 0 at small scale factor)", subquery_count);

    // Check the max quantity per order to understand the data
    let max_qty_query = r#"
        SELECT l_orderkey, SUM(l_quantity) as total_qty
        FROM lineitem
        GROUP BY l_orderkey
        ORDER BY total_qty DESC
        LIMIT 5
    "#;

    println!("\n=== Top 5 Orders by Quantity ===");
    if let Ok(vibesql_ast::Statement::Select(select)) = Parser::parse_sql(max_qty_query) {
        let executor = SelectExecutor::new(&db);
        if let Ok(rows) = executor.execute(&select) {
            for (i, row) in rows.iter().enumerate() {
                println!("  Row {}: {:?}", i, row);
            }
        }
    }

    // Now test full Q18
    println!("\n=== Full Q18 Test ===");
    let q18_result = execute_tpch_query(&db, TPCH_Q18);
    assert!(q18_result.is_ok(), "Q18 should execute: {:?}", q18_result);
    let q18_count = q18_result.unwrap();
    println!("Q18 returned {} rows", q18_count);

    // Q18 has LIMIT 100, so should return at most 100 rows
    assert!(q18_count <= 100, "Q18 has LIMIT 100");

    // At SF 0.01, if no orders have quantity > 300, this will return 0 rows
    // That's correct behavior - not a bug
    if subquery_count == 0 {
        assert_eq!(q18_count, 0, "Q18 should return 0 rows if subquery returns 0");
        println!(
            "Q18 correctly returns 0 rows because no orders have SUM(l_quantity) > 300 at SF 0.01"
        );
    } else {
        assert!(q18_count > 0, "Q18 should return rows if subquery found matching orders");
    }
}

#[test]
fn test_q7_volume_shipping() {
    // Q7: Volume Shipping
    // Tests SUBSTR(l_shipdate, 1, 4) to extract year from DATE column
    // Issue #2955: This query was failing because SUBSTRING didn't support Date type
    // Pattern: 6-table JOIN with SUBSTR and date filtering

    let db = load_vibesql(0.01);

    let result = execute_tpch_query(&db, TPCH_Q7);

    match result {
        Ok(count) => {
            println!("Q7 executed successfully: {} rows", count);
            // Q7 may return 0 rows on small dataset if no orders match criteria
            // The important thing is that it doesn't fail with "SUBSTRING requires string argument"
            println!("Q7 completed without errors (row count: {})", count);
        }
        Err(e) => {
            // Specifically check that the error is NOT about SUBSTRING type mismatch
            if e.contains("SUBSTRING requires string argument") {
                panic!("Q7 failed with SUBSTRING type error - Issue #2955 not fixed: {}", e);
            }
            panic!("Q7 failed to execute: {}", e);
        }
    }
}

#[test]
fn test_q9_product_type_profit() {
    // Q9: Product Type Profit Measure
    // Tests SUBSTR(o_orderdate, 1, 4) to extract year from DATE column
    // Issue #2955: This query was failing because SUBSTRING didn't support Date type
    // Pattern: 6-table JOIN with LIKE and SUBSTR on date

    let db = load_vibesql(0.01);

    let result = execute_tpch_query(&db, TPCH_Q9);

    match result {
        Ok(count) => {
            println!("Q9 executed successfully: {} rows", count);
            // Q9 may return 0 rows on small dataset if no parts match LIKE '%green%'
            // The important thing is that it doesn't fail with "SUBSTRING requires string argument"
            println!("Q9 completed without errors (row count: {})", count);
        }
        Err(e) => {
            // Specifically check that the error is NOT about SUBSTRING type mismatch
            if e.contains("SUBSTRING requires string argument") {
                panic!("Q9 failed with SUBSTRING type error - Issue #2955 not fixed: {}", e);
            }
            panic!("Q9 failed to execute: {}", e);
        }
    }
}
