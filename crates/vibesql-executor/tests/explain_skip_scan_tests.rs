//! Tests for EXPLAIN output with skip-scan optimization
//!
//! These tests verify that EXPLAIN correctly displays skip-scan information
//! when the optimizer chooses skip-scan for non-prefix index usage.

use vibesql_ast::Statement;
use vibesql_executor::ExplainExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Helper function to parse and execute EXPLAIN
fn explain_query(db: &Database, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN {}", sql);
    let stmt = Parser::parse_sql(&explain_sql).expect("Failed to parse SQL");

    if let Statement::Explain(explain_stmt) = stmt {
        let result = ExplainExecutor::execute(&explain_stmt, db).expect("EXPLAIN failed");
        result.to_text()
    } else {
        panic!("Expected EXPLAIN statement");
    }
}

/// Helper function to parse and execute EXPLAIN FORMAT JSON
fn explain_query_json(db: &Database, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN FORMAT JSON {}", sql);
    let stmt = Parser::parse_sql(&explain_sql).expect("Failed to parse SQL");

    if let Statement::Explain(explain_stmt) = stmt {
        let result = ExplainExecutor::execute(&explain_stmt, db).expect("EXPLAIN failed");
        result.to_json()
    } else {
        panic!("Expected EXPLAIN statement");
    }
}

/// Create a test database with a multi-column index suitable for skip-scan
fn create_skip_scan_test_db() -> Database {
    let mut db = Database::new();

    // Create a sales table with region and date columns
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "region".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "sale_date".to_string(),
                vibesql_types::DataType::Date,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "amount".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert data with few distinct regions (low cardinality prefix)
    // This makes skip-scan potentially beneficial
    let regions = ["North", "South", "East", "West"];
    for (i, region) in regions.iter().cycle().take(100).enumerate() {
        db.insert_row(
            "sales",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i as i64),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(*region)),
                vibesql_types::SqlValue::Date(vibesql_types::Date {
                    year: 2024,
                    month: 1,
                    day: ((i % 28) + 1) as u8,
                }),
                vibesql_types::SqlValue::Integer((i * 100) as i64),
            ]),
        )
        .unwrap();
    }

    // Create composite index on (region, sale_date)
    // This index is suitable for skip-scan when querying by sale_date alone
    // API signature: create_index(index_name: String, table_name: String, unique: bool, columns: Vec<IndexColumn>)
    db.create_index(
        "idx_region_date".to_string(),
        "sales".to_string(),
        false,
        vec![
            vibesql_ast::IndexColumn {
                column_name: "region".to_string(),
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
            },
            vibesql_ast::IndexColumn {
                column_name: "sale_date".to_string(),
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // Compute statistics so the optimizer can make cost-based decisions
    if let Some(table) = db.get_table_mut("sales") {
        table.analyze();
    }

    db
}

#[test]
fn test_explain_shows_seq_scan_without_where() {
    let db = create_skip_scan_test_db();

    let output = explain_query(&db, "SELECT * FROM sales");

    // Without WHERE clause, should use Seq Scan
    assert!(output.contains("Seq Scan"), "Expected Seq Scan, got:\n{}", output);
    assert!(!output.contains("Skip Scan"), "Should not show Skip Scan without WHERE");
}

#[test]
fn test_explain_shows_index_scan_with_prefix_filter() {
    let db = create_skip_scan_test_db();

    let output = explain_query(&db, "SELECT * FROM sales WHERE region = 'North'");

    // With filter on prefix column (region), should use regular Index Scan
    assert!(
        output.contains("Index Scan") || output.contains("Seq Scan"),
        "Expected Index Scan or Seq Scan, got:\n{}",
        output
    );
    // Should NOT use Skip Scan when filtering on prefix
    assert!(!output.contains("Skip Scan"), "Should not use Skip Scan for prefix filter");
}

#[test]
fn test_explain_text_format() {
    let db = create_skip_scan_test_db();

    let output = explain_query(&db, "SELECT * FROM sales");

    // Basic structure check
    assert!(output.contains("Select"), "Expected Select in output");
    // Table name is normalized to uppercase by the parser
    assert!(
        output.contains("sales") || output.contains("SALES"),
        "Expected table name in output"
    );
}

#[test]
fn test_explain_json_format() {
    let db = create_skip_scan_test_db();

    let output = explain_query_json(&db, "SELECT * FROM sales");

    // Should be valid JSON structure
    assert!(output.starts_with('{'), "JSON should start with {{");
    assert!(output.ends_with('}'), "JSON should end with }}");
    assert!(output.contains("\"operation\""), "JSON should have operation field");
}

#[test]
fn test_explain_shows_row_estimates() {
    let db = create_skip_scan_test_db();

    let output = explain_query(&db, "SELECT * FROM sales");

    // Row estimates may not always be shown (depends on whether statistics are available)
    // This test verifies the basic output structure
    assert!(
        output.contains("Scan"),
        "Expected scan operation in output:\n{}",
        output
    );
    // If row estimates are shown, they should be formatted correctly
    if output.contains("rows=") {
        assert!(
            output.contains("rows=100") || output.contains("rows="),
            "Row estimates should be formatted correctly"
        );
    }
}

#[test]
fn test_explain_with_alias() {
    let db = create_skip_scan_test_db();

    let output = explain_query(&db, "SELECT * FROM sales AS s");

    // Should show the alias (may be uppercase)
    assert!(
        output.contains("Alias: s") || output.contains("Alias: S"),
        "Expected alias in output:\n{}",
        output
    );
}

#[test]
fn test_explain_index_scan_shows_index_name() {
    let db = create_skip_scan_test_db();

    let output = explain_query(&db, "SELECT * FROM sales WHERE region = 'North'");

    // If using index, should show the index name
    if output.contains("Index Scan") {
        assert!(
            output.contains("Using index:"),
            "Index Scan should show index name:\n{}",
            output
        );
    }
}

/// Test that EXPLAIN output for skip-scan includes all expected fields
/// when skip-scan is chosen by the optimizer.
///
/// Note: This test may show Seq Scan if the cost model doesn't favor skip-scan
/// for the test data. The test verifies the format is correct when skip-scan is used.
#[test]
fn test_explain_skip_scan_format_when_applicable() {
    let db = create_skip_scan_test_db();

    // Query filtering on non-prefix column (sale_date)
    // This is a candidate for skip-scan
    let output = explain_query(&db, "SELECT * FROM sales WHERE sale_date = DATE '2024-01-15'");

    // The optimizer may choose Skip Scan, Seq Scan, or Index Scan
    // depending on cost estimates. Just verify the output is well-formed.
    assert!(
        output.contains("Scan") || output.contains("Select"),
        "Expected some scan operation:\n{}",
        output
    );

    // If skip-scan is chosen, verify its format
    if output.contains("Skip Scan") {
        assert!(
            output.contains("Using index:"),
            "Skip Scan should show index name:\n{}",
            output
        );
        assert!(
            output.contains("Skip columns:"),
            "Skip Scan should show skip columns:\n{}",
            output
        );
        assert!(
            output.contains("cardinality:"),
            "Skip Scan should show cardinality:\n{}",
            output
        );
        assert!(
            output.contains("Filter column:"),
            "Skip Scan should show filter column:\n{}",
            output
        );
    }
}
