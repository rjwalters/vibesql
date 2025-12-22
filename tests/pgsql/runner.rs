//! PostgreSQL-style regression test runner for VibeSQL.
//!
//! This runner executes SQL test files and validates results against expected output.
//! The test format is inspired by PostgreSQL's regression tests but adapted for VibeSQL.

use std::{
    fs,
    path::{Path, PathBuf},
};

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

use super::stats::{FileStats, PgTestStats, TestStatus};

/// A single test case extracted from a test file
#[derive(Debug, Clone)]
pub struct TestCase {
    pub name: String,
    pub sql: String,
    pub expected: Option<ExpectedResult>,
    pub skip_reason: Option<String>,
    #[allow(dead_code)]
    pub line_number: usize,
}

/// Expected result for a test case
#[derive(Debug, Clone)]
pub enum ExpectedResult {
    /// Expected rows in pipe-delimited format
    Rows(Vec<String>),
    /// Expected row count
    Count(usize),
    /// Expected error message (or partial match)
    Error(String),
    /// Statement should succeed with no result check
    Ok,
}

/// Parser for test files
pub struct TestFileParser;

impl TestFileParser {
    /// Parse a test file into test cases
    pub fn parse(content: &str) -> Vec<TestCase> {
        let mut cases = Vec::new();
        let mut current_sql = String::new();
        let mut current_name = String::new();
        let mut current_expected: Option<ExpectedResult> = None;
        let mut current_skip: Option<String> = None;
        let mut start_line = 0;
        let mut in_multi_line_expect = false;
        let mut expect_rows: Vec<String> = Vec::new();
        let mut begin_depth = 0; // Track BEGIN...END nesting for triggers

        for (line_num, line) in content.lines().enumerate() {
            let line_number = line_num + 1;
            let trimmed = line.trim();

            // Skip empty lines at the start
            if trimmed.is_empty() && current_sql.is_empty() {
                continue;
            }

            // Handle multi-line EXPECT blocks
            if in_multi_line_expect {
                if trimmed.starts_with("-- ") && !trimmed.starts_with("-- EXPECT") {
                    // Continue collecting expected rows
                    let row = trimmed.strip_prefix("-- ").unwrap_or(trimmed);
                    if !row.is_empty() {
                        expect_rows.push(row.to_string());
                    }
                    continue;
                } else {
                    // End of EXPECT block
                    in_multi_line_expect = false;
                    current_expected = Some(ExpectedResult::Rows(std::mem::take(&mut expect_rows)));
                }
            }

            // Handle comment directives
            if trimmed.starts_with("--") {
                let comment = trimmed.strip_prefix("--").unwrap_or("").trim();

                // Test name/description
                if comment.starts_with("TEST:") {
                    // Save previous test case if exists
                    if !current_sql.is_empty() {
                        cases.push(TestCase {
                            name: if current_name.is_empty() {
                                format!("line_{}", start_line)
                            } else {
                                std::mem::take(&mut current_name)
                            },
                            sql: std::mem::take(&mut current_sql),
                            expected: current_expected.take(),
                            skip_reason: current_skip.take(),
                            line_number: start_line,
                        });
                        begin_depth = 0;
                    }
                    current_name = comment.strip_prefix("TEST:").unwrap_or("").trim().to_string();
                    start_line = line_number;
                    continue;
                }

                // Skip directive
                if comment.starts_with("SKIP:") {
                    current_skip =
                        Some(comment.strip_prefix("SKIP:").unwrap_or("").trim().to_string());
                    continue;
                }

                // Expected result directives
                if comment.starts_with("EXPECT:") {
                    let expect_content = comment.strip_prefix("EXPECT:").unwrap_or("").trim();
                    if expect_content.is_empty() {
                        // Multi-line expect block
                        in_multi_line_expect = true;
                        expect_rows.clear();
                    } else {
                        // Single line expect
                        current_expected =
                            Some(ExpectedResult::Rows(vec![expect_content.to_string()]));
                    }
                    continue;
                }

                if comment.starts_with("EXPECT_COUNT:") {
                    let count_str = comment.strip_prefix("EXPECT_COUNT:").unwrap_or("").trim();
                    if let Ok(count) = count_str.parse::<usize>() {
                        current_expected = Some(ExpectedResult::Count(count));
                    }
                    continue;
                }

                if comment.starts_with("EXPECT_ERROR:") {
                    let error = comment.strip_prefix("EXPECT_ERROR:").unwrap_or("").trim();
                    current_expected = Some(ExpectedResult::Error(error.to_string()));
                    continue;
                }

                if comment.starts_with("EXPECT_OK") {
                    current_expected = Some(ExpectedResult::Ok);
                    continue;
                }

                // Regular comment - skip
                continue;
            }

            // SQL statement
            if start_line == 0 {
                start_line = line_number;
            }
            if !current_sql.is_empty() {
                current_sql.push('\n');
            }
            current_sql.push_str(line);

            // Track BEGIN...END blocks for multi-statement triggers
            let upper = trimmed.to_uppercase();
            if upper == "BEGIN" {
                begin_depth += 1;
            }
            // END; or END closes a block
            if upper == "END;" || upper == "END" {
                if begin_depth > 0 {
                    begin_depth -= 1;
                }
            }

            // Check for statement terminator (but only if not inside a BEGIN...END block)
            if trimmed.ends_with(';') && begin_depth == 0 {
                cases.push(TestCase {
                    name: if current_name.is_empty() {
                        format!("line_{}", start_line)
                    } else {
                        std::mem::take(&mut current_name)
                    },
                    sql: std::mem::take(&mut current_sql),
                    expected: current_expected.take(),
                    skip_reason: current_skip.take(),
                    line_number: start_line,
                });
                start_line = 0;
            }
        }

        // Handle any remaining SQL
        if !current_sql.is_empty() {
            cases.push(TestCase {
                name: if current_name.is_empty() {
                    format!("line_{}", start_line)
                } else {
                    current_name
                },
                sql: current_sql,
                expected: current_expected,
                skip_reason: current_skip,
                line_number: start_line,
            });
        }

        cases
    }
}

/// Execute a single test case against VibeSQL
pub fn execute_test_case(db: &mut Database, case: &TestCase) -> (TestStatus, Option<String>) {
    // Handle skip
    if case.skip_reason.is_some() {
        return (TestStatus::Skipped, None);
    }

    // Parse the SQL
    let parse_result = Parser::parse_sql(&case.sql);
    let stmt = match parse_result {
        Ok(stmt) => stmt,
        Err(e) => {
            // Check if we expected an error
            if let Some(ExpectedResult::Error(expected_err)) = &case.expected {
                let err_msg = format!("{:?}", e);
                if err_msg.to_lowercase().contains(&expected_err.to_lowercase()) {
                    return (TestStatus::Passed, None);
                } else {
                    return (
                        TestStatus::Failed,
                        Some(format!("Expected error '{}' but got '{}'", expected_err, err_msg)),
                    );
                }
            }
            return (TestStatus::Error, Some(format!("Parse error: {:?}", e)));
        }
    };

    // Execute the statement
    let result = execute_statement(db, stmt);

    match result {
        Ok(output) => {
            // Validate against expected result
            match &case.expected {
                Some(ExpectedResult::Rows(expected_rows)) => {
                    if output == *expected_rows {
                        (TestStatus::Passed, None)
                    } else {
                        (
                            TestStatus::Failed,
                            Some(format!(
                                "Row mismatch:\n  Expected: {:?}\n  Got: {:?}",
                                expected_rows, output
                            )),
                        )
                    }
                }
                Some(ExpectedResult::Count(expected_count)) => {
                    if output.len() == *expected_count {
                        (TestStatus::Passed, None)
                    } else {
                        (
                            TestStatus::Failed,
                            Some(format!(
                                "Count mismatch: expected {} rows, got {}",
                                expected_count,
                                output.len()
                            )),
                        )
                    }
                }
                Some(ExpectedResult::Error(_)) => {
                    (TestStatus::Failed, Some("Expected error but statement succeeded".to_string()))
                }
                Some(ExpectedResult::Ok) | None => (TestStatus::Passed, None),
            }
        }
        Err(e) => {
            if let Some(ExpectedResult::Error(expected_err)) = &case.expected {
                if e.to_lowercase().contains(&expected_err.to_lowercase()) {
                    (TestStatus::Passed, None)
                } else {
                    (
                        TestStatus::Failed,
                        Some(format!("Expected error '{}' but got '{}'", expected_err, e)),
                    )
                }
            } else {
                (TestStatus::Error, Some(e))
            }
        }
    }
}

/// Execute a parsed statement and return formatted output
fn execute_statement(
    db: &mut Database,
    stmt: vibesql_ast::Statement,
) -> Result<Vec<String>, String> {
    use vibesql_ast::Statement;

    match stmt {
        Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            let rows = executor.execute(&select_stmt).map_err(|e| format!("{:?}", e))?;
            Ok(rows
                .iter()
                .map(|row| row.values.iter().map(|v| format_value(v)).collect::<Vec<_>>().join("|"))
                .collect())
        }
        Statement::CreateTable(create_stmt) => {
            vibesql_executor::CreateTableExecutor::execute(&create_stmt, db)
                .map_err(|e| format!("{:?}", e))?;
            Ok(vec![])
        }
        Statement::Insert(insert_stmt) => {
            let rows_affected = vibesql_executor::InsertExecutor::execute(db, &insert_stmt)
                .map_err(|e| format!("{:?}", e))?;
            // Track changes count for changes() and total_changes() functions
            db.set_last_changes_count(rows_affected);
            db.increment_total_changes_count(rows_affected);
            Ok(vec![format!("{} rows inserted", rows_affected)])
        }
        Statement::Update(update_stmt) => {
            let rows_affected = vibesql_executor::UpdateExecutor::execute(&update_stmt, db)
                .map_err(|e| format!("{:?}", e))?;
            // Track changes count for changes() and total_changes() functions
            db.set_last_changes_count(rows_affected);
            db.increment_total_changes_count(rows_affected);
            Ok(vec![format!("{} rows updated", rows_affected)])
        }
        Statement::Delete(delete_stmt) => {
            let rows_deleted = vibesql_executor::DeleteExecutor::execute(&delete_stmt, db)
                .map_err(|e| format!("{:?}", e))?;
            // Track changes count for changes() and total_changes() functions
            db.set_last_changes_count(rows_deleted);
            db.increment_total_changes_count(rows_deleted);
            Ok(vec![format!("{} rows deleted", rows_deleted)])
        }
        Statement::CreateTrigger(trigger_stmt) => {
            vibesql_executor::TriggerExecutor::create_trigger(db, &trigger_stmt)
                .map_err(|e| format!("{:?}", e))?;
            Ok(vec![])
        }
        Statement::DropTrigger(drop_stmt) => {
            vibesql_executor::TriggerExecutor::drop_trigger(db, &drop_stmt)
                .map_err(|e| format!("{:?}", e))?;
            Ok(vec![])
        }
        Statement::DropTable(drop_stmt) => {
            vibesql_executor::DropTableExecutor::execute(&drop_stmt, db)
                .map_err(|e| format!("{:?}", e))?;
            Ok(vec![])
        }
        Statement::CreateView(create_view) => {
            vibesql_executor::ViewExecutor::execute_create_view(&create_view, db)
                .map_err(|e| format!("{:?}", e))?;
            Ok(vec![])
        }
        Statement::DropView(drop_view) => {
            vibesql_executor::ViewExecutor::execute_drop_view(&drop_view, db)
                .map_err(|e| format!("{:?}", e))?;
            Ok(vec![])
        }
        Statement::CreateIndex(create_index) => {
            vibesql_executor::CreateIndexExecutor::execute(&create_index, db)
                .map_err(|e| format!("{:?}", e))?;
            Ok(vec![])
        }
        Statement::DropIndex(drop_index) => {
            vibesql_executor::DropIndexExecutor::execute(&drop_index, db)
                .map_err(|e| format!("{:?}", e))?;
            Ok(vec![])
        }
        _ => Err(format!("Unsupported statement type: {:?}", stmt)),
    }
}

/// Format a SQL value for output comparison
fn format_value(value: &vibesql_types::SqlValue) -> String {
    use vibesql_types::SqlValue;
    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Smallint(i) => i.to_string(),
        SqlValue::Bigint(i) => i.to_string(),
        SqlValue::Unsigned(u) => u.to_string(),
        SqlValue::Numeric(n) => {
            if n.fract() == 0.0 {
                format!("{:.0}", n)
            } else {
                n.to_string()
            }
        }
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Real(r) => r.to_string(),
        SqlValue::Double(d) => d.to_string(),
        SqlValue::Character(s) | SqlValue::Varchar(s) => s.to_string(),
        SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Date(d) => format!("{}", d),
        SqlValue::Time(t) => format!("{}", t),
        SqlValue::Timestamp(ts) => format!("{}", ts),
        SqlValue::Interval(i) => format!("{}", i),
        SqlValue::Vector(v) => format!("{:?}", v),
        SqlValue::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            format!("X'{}'", hex)
        }
    }
}

/// Run all tests in a single file
pub fn run_test_file(path: &Path) -> FileStats {
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            let mut stats = FileStats::default();
            stats.add_result(TestStatus::Error, Some(format!("Failed to read file: {}", e)));
            return stats;
        }
    };

    let cases = TestFileParser::parse(&content);
    let mut stats = FileStats::default();
    let mut db = Database::new();

    for case in cases {
        let (status, error) = execute_test_case(&mut db, &case);
        stats.add_result(status, error);
    }

    stats
}

/// Run all test files in a directory
pub fn run_test_suite(test_dir: &Path) -> PgTestStats {
    let mut stats = PgTestStats::new();

    // Find all .sql files
    let pattern = format!("{}/**/*.sql", test_dir.display());
    let files: Vec<PathBuf> =
        glob::glob(&pattern).expect("Failed to read test pattern").filter_map(Result::ok).collect();

    println!("\n=== PostgreSQL Regression Test Suite ===");
    println!("Test directory: {}", test_dir.display());
    println!("Found {} test files\n", files.len());

    for file in files {
        let relative_path =
            file.strip_prefix(test_dir).unwrap_or(&file).to_string_lossy().to_string();

        // Extract category from path (e.g., "triggers" from "triggers.sql")
        let category = file
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        print!("Running {}... ", relative_path);
        let file_stats = run_test_file(&file);

        let status_emoji =
            if file_stats.failed == 0 && file_stats.errors == 0 { "✓" } else { "✗" };
        println!(
            "{} {}/{} passed ({:.1}%)",
            status_emoji,
            file_stats.passed,
            file_stats.total,
            file_stats.pass_rate()
        );

        stats.add_file_result(&category, &relative_path, file_stats);
    }

    // Print summary
    let total = stats.total_stats();
    println!("\n=== Summary ===");
    println!(
        "Total: {} tests, {} passed, {} failed, {} skipped, {} errors",
        total.total, total.passed, total.failed, total.skipped, total.errors
    );
    println!("Pass rate: {:.2}%", total.pass_rate());

    println!("\nBy category:");
    for (cat, cat_stats) in &stats.by_category {
        println!(
            "  {}: {}/{} ({:.1}%)",
            cat,
            cat_stats.passed,
            cat_stats.total,
            cat_stats.pass_rate()
        );
    }

    stats
}

/// Get the default test directory
#[allow(dead_code)]
pub fn get_test_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("tests")
        .join("pgsql")
        .join("sql")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_test_file() {
        let content = r#"
-- TEST: Create table
CREATE TABLE test (id INTEGER);

-- TEST: Insert data
-- EXPECT_OK
INSERT INTO test VALUES (1);

-- TEST: Query data
-- EXPECT: 1
SELECT id FROM test;
"#;

        let cases = TestFileParser::parse(content);
        assert_eq!(cases.len(), 3);
        assert_eq!(cases[0].name, "Create table");
        assert_eq!(cases[1].name, "Insert data");
        assert!(matches!(cases[1].expected, Some(ExpectedResult::Ok)));
        assert_eq!(cases[2].name, "Query data");
    }

    #[test]
    fn test_parse_multiline_expect() {
        let content = r#"
-- TEST: Multi-row result
-- EXPECT:
-- 1|hello
-- 2|world
SELECT * FROM test;
"#;

        let cases = TestFileParser::parse(content);
        assert_eq!(cases.len(), 1);
        if let Some(ExpectedResult::Rows(rows)) = &cases[0].expected {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], "1|hello");
            assert_eq!(rows[1], "2|world");
        } else {
            panic!("Expected Rows result");
        }
    }

    #[test]
    fn test_parse_skip_directive() {
        let content = r#"
-- TEST: Skipped test
-- SKIP: Not implemented yet
SELECT * FROM nonexistent;
"#;

        let cases = TestFileParser::parse(content);
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].skip_reason, Some("Not implemented yet".to_string()));
    }

    #[test]
    fn test_parse_trigger_with_begin_end() {
        let content = r#"
-- TEST: Create trigger
CREATE TRIGGER my_trigger
BEFORE INSERT ON my_table
FOR EACH ROW
BEGIN
    INSERT INTO audit (msg) VALUES ('test');
END;

-- TEST: Another statement
SELECT 1;
"#;

        let cases = TestFileParser::parse(content);
        assert_eq!(cases.len(), 2, "Should have 2 test cases");
        assert_eq!(cases[0].name, "Create trigger");
        assert!(
            cases[0].sql.contains("BEGIN") && cases[0].sql.contains("END;"),
            "Trigger SQL should contain full BEGIN...END block"
        );
        assert_eq!(cases[1].name, "Another statement");
    }
}
