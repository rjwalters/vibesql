use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use vibesql_storage::Database;

/// Validate table name to prevent SQL injection
/// Returns an error if the table doesn't exist in the database
pub fn validate_table_name(db: &Database, table_name: &str) -> anyhow::Result<()> {
    // Check if table exists in the database
    if db.get_table(table_name).is_none() {
        return Err(anyhow::anyhow!("Table '{}' does not exist", table_name));
    }
    Ok(())
}

/// Validate CSV column names against table schema to prevent SQL injection
/// Returns an error if columns don't match the table schema
pub fn validate_csv_columns(
    db: &Database,
    file_path: &str,
    table_name: &str,
) -> anyhow::Result<()> {
    // Get table schema
    let table = db
        .get_table(table_name)
        .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", table_name))?;

    // Read CSV header
    let file = File::open(file_path)
        .map_err(|e| anyhow::anyhow!("Failed to open file '{}': {}", file_path, e))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header_line = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("CSV file is empty"))?
        .map_err(|e| anyhow::anyhow!("Failed to read header: {}", e))?;

    let csv_columns: Vec<&str> = header_line.split(',').map(|s| s.trim()).collect();

    // Validate each column name
    for col_name in &csv_columns {
        // Check for SQL injection characters
        if col_name.contains(';')
            || col_name.contains('\'')
            || col_name.contains('"')
            || col_name.contains('(')
            || col_name.contains(')')
        {
            return Err(anyhow::anyhow!(
                "Invalid column name '{}': contains forbidden characters",
                col_name
            ));
        }

        // Check if column exists in table schema
        let column_exists =
            table.schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col_name));

        if !column_exists {
            return Err(anyhow::anyhow!(
                "Column '{}' does not exist in table '{}'",
                col_name,
                table_name
            ));
        }
    }

    Ok(())
}

/// Validate JSON column names against table schema to prevent SQL injection
/// Returns an error if columns don't match the table schema
pub fn validate_json_columns(
    db: &Database,
    file_path: &str,
    table_name: &str,
) -> anyhow::Result<()> {
    use std::fs;

    // Get table schema
    let table = db
        .get_table(table_name)
        .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", table_name))?;

    // Read JSON file
    let json_content = fs::read_to_string(file_path)
        .map_err(|e| anyhow::anyhow!("Failed to read file '{}': {}", file_path, e))?;

    // Parse as array of objects
    let json_array: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_str(&json_content)
            .map_err(|e| anyhow::anyhow!("Invalid JSON format: {}", e))?;

    if json_array.is_empty() {
        return Err(anyhow::anyhow!("JSON file contains no data"));
    }

    // Extract column names from first object
    let first_obj = &json_array[0];
    let json_columns: Vec<&str> = first_obj.keys().map(|s| s.as_str()).collect();

    // Validate each column name
    for col_name in &json_columns {
        // Check for SQL injection characters
        if col_name.contains(';')
            || col_name.contains('\'')
            || col_name.contains('"')
            || col_name.contains('(')
            || col_name.contains(')')
        {
            return Err(anyhow::anyhow!(
                "Invalid column name '{}': contains forbidden characters",
                col_name
            ));
        }

        // Check if column exists in table schema
        let column_exists =
            table.schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col_name));

        if !column_exists {
            return Err(anyhow::anyhow!(
                "Column '{}' does not exist in table '{}'",
                col_name,
                table_name
            ));
        }
    }

    Ok(())
}
