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
        let result = executor.execute(&select)
            .map_err(|e| format!("Execution error: {:?}", e))?;
        Ok(result.len())
    } else {
        Err("Not a SELECT statement".to_string())
    }
}

#[test]
#[ignore] // SLOW: Takes >60s on SF 0.01 (needs scalar subquery caching #2425)
fn test_q11_scalar_subquery_having() {
    // Q11: Important Stock Identification
    // Tests scalar subquery in HAVING clause
    // Pattern: Uncorrelated scalar subquery for threshold computation
    // Performance: SLOW - needs optimization (#2425)

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
#[ignore] // SLOW: Takes >60s on SF 0.01 (needs join optimization)
fn test_q13_complex_join_subquery() {
    // Q13: Customer Distribution
    // Tests subquery in FROM with LEFT OUTER JOIN and aggregate
    // Pattern: No decorrelation needed, just complex join
    // Performance: SLOW - needs optimization

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
#[ignore] // May timeout without semi-join optimization
fn test_q20_nested_in_subqueries() {
    // Q20: Potential Part Promotion
    // Tests nested IN subqueries with correlated scalar subquery
    // Pattern: IN with nested IN, plus correlated scalar subquery
    // Expected: Works but may be slow without semi-join optimization (#2424)

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
#[ignore] // May timeout without semi-join optimization
fn test_q21_correlated_exists() {
    // Q21: Suppliers Who Kept Orders Waiting
    // Tests EXISTS with correlated predicate and inequality
    // Pattern: EXISTS with complex correlation
    // Expected: Works but may be slow without semi-join optimization (#2424)

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
#[ignore] // VERY SLOW: Both queries take >60s each on SF 0.01
fn test_q11_q13_batch() {
    // Run Q11 and Q13 together to validate both work
    // FINDINGS: Both queries work but are slower than expected
    // Q11 needs scalar subquery caching (#2425)
    // Q13 needs join optimization

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
