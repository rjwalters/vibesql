//! Example parsing for web demo SQL tests
//!
//! This module handles discovery and parsing of SQL example files, including:
//! - JSON file discovery and loading
//! - Example object extraction
//! - Expected results parsing from SQL comments
//! - Query extraction and database detection

#![allow(dead_code)]

use std::{fs, path::Path};

use regex::Regex;
use serde_json;

/// Represents a parsed SQL example from the web demo
#[derive(Debug, Clone)]
pub struct WebDemoExample {
    pub id: String,
    #[allow(dead_code)]
    pub title: String,
    pub database: String,
    pub sql: String,
    pub expected_rows: Option<Vec<Vec<String>>>,
    pub expected_count: Option<usize>,
}

/// Parse all JSON example files and extract SQL examples
pub fn parse_example_files() -> Result<Vec<WebDemoExample>, Box<dyn std::error::Error>> {
    let mut examples = Vec::new();

    // Look for JSON files in the examples directory
    let examples_dir = Path::new("web-demo/src/data/examples");
    if examples_dir.exists() {
        for entry in fs::read_dir(examples_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Some(file_name) = path.file_stem().and_then(|s| s.to_str()) {
                    // Skip non-category files
                    if file_name == "types" || file_name == "loader" {
                        continue;
                    }
                    let content = fs::read_to_string(&path)?;
                    examples.extend(parse_json_examples(&content, file_name)?);
                }
            }
        }
    }

    Ok(examples)
}

/// Parse JSON content to extract example objects
fn parse_json_examples(
    content: &str,
    _category_name: &str,
) -> Result<Vec<WebDemoExample>, Box<dyn std::error::Error>> {
    let mut examples = Vec::new();

    // Parse the JSON
    let json: serde_json::Value = serde_json::from_str(content)?;
    let root_obj = json.as_object().ok_or("JSON root must be an object")?;

    // Get the examples section
    let examples_value = match root_obj.get("examples") {
        Some(v) => v,
        None => return Ok(examples),
    };

    // Handle two formats:
    // 1. Standard format: "examples" is an object with example IDs as keys
    // 2. Queries format: "examples" has a "queries" array (e.g., sqllogictest.json)
    if let Some(examples_obj) = examples_value.as_object() {
        // Check if this is the queries array format
        if let Some(queries_array) = examples_obj.get("queries").and_then(|v| v.as_array()) {
            // Format 2: queries array
            for query_value in queries_array {
                if let Some(example) = parse_example_object(query_value, None) {
                    examples.push(example);
                }
            }
        } else {
            // Format 1: standard object with example IDs as keys
            for (example_id, example_value) in examples_obj {
                if let Some(example) = parse_example_object(example_value, Some(example_id)) {
                    examples.push(example);
                }
            }
        }
    }

    Ok(examples)
}

/// Parse a single example object into a WebDemoExample
/// Returns None if the object doesn't have required fields (like sql)
fn parse_example_object(
    value: &serde_json::Value,
    fallback_id: Option<&String>,
) -> Option<WebDemoExample> {
    let obj = value.as_object()?;

    // sql is required
    let sql = obj.get("sql").and_then(|v| v.as_str())?.to_string();

    // id can come from the object or the fallback (key name)
    let id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| fallback_id.cloned())?;

    let title = obj
        .get("title")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("Example {}", id));

    let database = obj
        .get("database")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "northwind".to_string());

    // Parse expected results from JSON
    let expected_rows = obj.get("expectedRows").and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|row| row.as_array())
            .map(|row| {
                row.iter()
                    .filter_map(|cell| cell.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .collect()
    });

    let expected_count = obj.get("expectedCount").and_then(|v| v.as_u64()).map(|n| n as usize);

    Some(WebDemoExample {
        id,
        title,
        database,
        sql,
        expected_rows,
        expected_count,
    })
}

/// Parse expected results from SQL comment blocks
/// Returns (expected_rows, expected_count)
pub fn parse_expected_results(sql: &str) -> (Option<Vec<Vec<String>>>, Option<usize>) {
    let lines: Vec<&str> = sql.lines().collect();
    let mut in_expected_block = false;
    let mut expected_rows = Vec::new();
    let mut expected_count = None;

    // Compile regex once outside the loop
    let row_count_re = Regex::new(r"--\s*\((\d+)\s+rows?\)").ok();

    for line in lines {
        let trimmed = line.trim();

        // Check for EXPECTED block start
        if trimmed.contains("-- EXPECTED:") {
            in_expected_block = true;
            continue;
        }

        // Check for row count pattern: "-- (N rows)"
        if let Some(ref re) = row_count_re {
            if let Some(cap) = re.captures(trimmed) {
                if let Ok(count) = cap.get(1).unwrap().as_str().parse::<usize>() {
                    expected_count = Some(count);
                }
                in_expected_block = false;
                continue;
            }
        }

        // Parse table rows in expected block
        if in_expected_block && trimmed.starts_with("--") {
            let row_content = trimmed.trim_start_matches("--").trim();

            // Skip header separator lines
            if row_content.starts_with("|") && !row_content.contains("----") {
                // Parse table row: | col1 | col2 | col3 |
                let values: Vec<String> = row_content
                    .split('|')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if !values.is_empty() {
                    expected_rows.push(values);
                }
            }
        }

        // Stop parsing if we hit a line that's not a comment
        if in_expected_block && !trimmed.starts_with("--") {
            in_expected_block = false;
        }
    }

    let rows = if expected_rows.len() > 1 {
        // Skip header row (first row)
        Some(expected_rows[1..].to_vec())
    } else {
        None
    };

    (rows, expected_count)
}

/// Extract just the SQL query without expected result comments
pub fn extract_query(sql: &str) -> String {
    sql.lines()
        .take_while(|line| !line.trim().contains("-- EXPECTED"))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Extract database name from SQL by looking for table references
/// This is a heuristic since we don't have the metadata in JSON
#[allow(dead_code)]
fn extract_database_from_sql(sql: &str) -> Option<String> {
    // Look for common database indicators in the SQL
    let sql_lower = sql.to_lowercase();
    if sql_lower.contains("employees")
        || sql_lower.contains("dept_id")
        || sql_lower.contains("manager_id")
    {
        Some("employees".to_string())
    } else if sql_lower.contains("students")
        || sql_lower.contains("courses")
        || sql_lower.contains("enrollments")
    {
        Some("university".to_string())
    } else if sql_lower.contains("departments") || sql_lower.contains("projects") {
        Some("company".to_string())
    } else {
        // Default to northwind
        Some("northwind".to_string())
    }
}
