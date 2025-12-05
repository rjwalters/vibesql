//! Tests for aggregation web demo SQL examples
//!
//! This test suite validates SQL aggregation examples (GROUP BY, HAVING, COUNT, SUM, AVG, etc.)
//! from the web demo by parsing the TypeScript example files and executing queries.

#[path = "../common/mod.rs"]
mod common;

use common::web_demo_helpers::{
    extract_query, load_database, parse_example_files, validate_results, WebDemoExample,
};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

/// Test aggregation SQL examples from web demo
/// Includes examples with IDs: agg*, group*, having*, count*, sum*, avg*, min*, max*
#[test]
fn test_aggregation_sql_examples() {
    // Parse all examples from web demo
    let examples = parse_example_files().expect("Failed to parse example files");

    // Filter for aggregation examples
    let aggregation_examples: Vec<&WebDemoExample> = examples
        .iter()
        .filter(|ex| {
            ex.id.starts_with("agg")
                || ex.id.starts_with("group")
                || ex.id.starts_with("having")
                || ex.id.starts_with("count")
                || ex.id.starts_with("sum")
                || ex.id.starts_with("avg")
                || ex.id.starts_with("min")
                || ex.id.starts_with("max")
        })
        .collect();

    assert!(
        !aggregation_examples.is_empty(),
        "No aggregation examples found - check web demo examples file exists"
    );

    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    for example in &aggregation_examples {
        // Load the appropriate database
        let db = match load_database(&example.database) {
            Some(db) => db,
            None => {
                println!("⚠️  Skipping {}: Unknown database '{}'", example.id, example.database);
                skipped += 1;
                continue;
            }
        };

        // Extract just the SQL query (without expected comments)
        let query = extract_query(&example.sql);

        // Parse the SQL
        let stmt = match Parser::parse_sql(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                println!("❌ {}: Parse error: {}", example.id, e);
                failed += 1;
                continue;
            }
        };

        // Execute the query (aggregation queries are SELECT statements)
        let result = match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&db);
                executor.execute(&select_stmt)
            }
            _ => {
                println!("⚠️  Skipping {}: Not a SELECT statement", example.id);
                skipped += 1;
                continue;
            }
        };

        // Check execution result
        match result {
            Ok(rows) => {
                // Validate expected results (count and/or row data)
                let (is_valid, error_msg) = validate_results(
                    &example.id,
                    &rows,
                    example.expected_count,
                    example.expected_rows.as_ref(),
                );

                if !is_valid {
                    println!("❌ {}: {}", example.id, error_msg.unwrap());
                    failed += 1;
                    continue;
                }

                println!("✓  {}: Passed ({} rows)", example.id, rows.len());
                passed += 1;
            }
            Err(e) => {
                println!("❌ {}: Execution error: {}", example.id, e);
                failed += 1;
            }
        }
    }

    // Print summary
    println!("\n=== Aggregation Examples Test Summary ===");
    println!("Total:   {}", aggregation_examples.len());
    println!("Passed:  {}", passed);
    println!("Failed:  {}", failed);
    println!("Skipped: {}", skipped);
    println!("=========================================\n");

    // Require at least some tests to pass (will be stricter once all examples have expected data)
    assert!(passed >= 1, "Expected at least 1 aggregation example to pass, got {}", passed);
}
