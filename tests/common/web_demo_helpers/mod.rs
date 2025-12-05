//! Common helpers for web demo SQL example tests
//!
//! This module provides shared functionality for testing web demo SQL examples,
//! broken down into focused submodules for better organization and maintainability.

use vibesql_storage::Row;

// Public submodules
pub mod database_fixtures;
pub mod example_parsing;

// Re-export commonly used types and functions to maintain backward compatibility
#[allow(unused_imports)]
pub use database_fixtures::{
    create_employees_db, create_empty_db, create_northwind_db, create_university_db, load_database,
};
#[allow(unused_imports)]
pub use example_parsing::{
    extract_query, parse_example_files, parse_expected_results, WebDemoExample,
};

/// Validate query results against expected data
///
/// Returns (passed, error_message) tuple
#[allow(dead_code)]
pub fn validate_results(
    _example_id: &str,
    actual_rows: &[Row],
    expected_count: Option<usize>,
    expected_rows: Option<&Vec<Vec<String>>>,
) -> (bool, Option<String>) {
    // Validate expected row count if specified
    if let Some(expected) = expected_count {
        if actual_rows.len() != expected {
            return (false, Some(format!("Expected {} rows, got {}", expected, actual_rows.len())));
        }
    }

    // Validate expected row data if specified
    if let Some(expected) = expected_rows {
        if actual_rows.len() != expected.len() {
            return (
                false,
                Some(format!("Expected {} rows, got {} rows", expected.len(), actual_rows.len())),
            );
        }

        // Validate each row
        for (i, (actual_row, expected_row)) in actual_rows.iter().zip(expected.iter()).enumerate() {
            // Convert actual row to strings for comparison
            let actual_values: Vec<String> =
                actual_row.values.iter().map(|v| v.to_string()).collect();

            if actual_values.len() != expected_row.len() {
                return (
                    false,
                    Some(format!(
                        "Row {}: Expected {} columns, got {} columns",
                        i,
                        expected_row.len(),
                        actual_values.len()
                    )),
                );
            }

            for (j, (actual_val, expected_val)) in
                actual_values.iter().zip(expected_row.iter()).enumerate()
            {
                // Try numeric comparison first (handles "18" vs "18.0")
                let values_match = if let (Ok(actual_num), Ok(expected_num)) =
                    (actual_val.parse::<f64>(), expected_val.parse::<f64>())
                {
                    // For numeric values, compare as floats with small epsilon
                    // Using 1e-5 to handle reasonable floating point precision errors
                    (actual_num - expected_num).abs() < 1e-5
                } else {
                    // For non-numeric values, do string comparison
                    actual_val == expected_val
                };

                if !values_match {
                    return (
                        false,
                        Some(format!(
                            "Row {}, Column {}: Expected '{}', got '{}'",
                            i, j, expected_val, actual_val
                        )),
                    );
                }
            }
        }
    }

    (true, None)
}
