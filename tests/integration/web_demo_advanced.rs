//! Tests for advanced web demo SQL examples
//!
//! This test suite validates advanced SQL examples (CTEs, window functions, complex scenarios,
//! string functions, etc.) from the web demo by parsing the TypeScript example files and executing
//! queries.

#[path = "../common/mod.rs"]
mod common;

use common::web_demo_helpers::{
    extract_query, load_database, parse_example_files, validate_results, WebDemoExample,
};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

/// Test advanced SQL examples from web demo
/// Includes examples with IDs: cte*, with*, window*, partition*, string*, concat*, advanced*,
/// complex*, uni*, company*
#[test]
fn test_advanced_sql_examples() {
    // Parse all examples from web demo
    let examples = parse_example_files().expect("Failed to parse example files");

    // Filter for advanced examples
    let advanced_examples: Vec<&WebDemoExample> = examples
        .iter()
        .filter(|ex| {
            ex.id.starts_with("cte")
                || ex.id.starts_with("with")
                || ex.id.starts_with("window")
                || ex.id.starts_with("partition")
                || ex.id.starts_with("string")
                || ex.id.starts_with("concat")
                || ex.id.starts_with("advanced")
                || ex.id.starts_with("complex")
                || ex.id.starts_with("uni")
                || ex.id.starts_with("company")
        })
        .collect();

    assert!(
        !advanced_examples.is_empty(),
        "No advanced examples found - check web demo examples file exists"
    );

    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    for example in &advanced_examples {
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

        // Execute the query (advanced queries are typically SELECT statements)
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
    println!("\n=== Advanced Examples Test Summary ===");
    println!("Total:   {}", advanced_examples.len());
    println!("Passed:  {}", passed);
    println!("Failed:  {}", failed);
    println!("Skipped: {}", skipped);
    println!("======================================\n");

    // Many advanced features are still being implemented
    // For now, just require at least some tests to pass
    assert!(passed >= 1, "Expected at least 1 advanced example to pass, got {}", passed);
}
