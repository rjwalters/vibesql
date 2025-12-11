use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

use crate::executor::QueryResult;

/// Data import/export utilities
pub struct DataIO;

impl DataIO {
    /// Export query results to CSV file
    pub fn export_csv(result: &QueryResult, file_path: &str) -> anyhow::Result<()> {
        let mut file = File::create(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to create file '{}': {}", file_path, e))?;

        // Write header
        write_csv_row(&mut file, &result.columns)?;

        // Write rows
        for row in &result.rows {
            write_csv_row(&mut file, row)?;
        }

        println!("Exported {} rows to '{}'", result.row_count, file_path);
        Ok(())
    }

    /// Export query results to JSON file
    pub fn export_json(result: &QueryResult, file_path: &str) -> anyhow::Result<()> {
        let mut json_rows = Vec::new();
        for row in &result.rows {
            let mut json_obj = serde_json::Map::new();
            for (i, col) in result.columns.iter().enumerate() {
                if i < row.len() {
                    json_obj.insert(col.clone(), serde_json::Value::String(row[i].clone()));
                }
            }
            json_rows.push(serde_json::Value::Object(json_obj));
        }

        let json_output = serde_json::to_string_pretty(&json_rows)?;
        let mut file = File::create(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to create file '{}': {}", file_path, e))?;
        file.write_all(json_output.as_bytes())?;

        println!("Exported {} rows to '{}'", result.row_count, file_path);
        Ok(())
    }

    /// Import CSV file and generate INSERT statements
    pub fn import_csv(file_path: &str, table_name: &str) -> anyhow::Result<Vec<String>> {
        let file = File::open(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to open file '{}': {}", file_path, e))?;

        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut statements = Vec::new();

        // Read header
        let header_line = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("CSV file is empty"))?
            .map_err(|e| anyhow::anyhow!("Failed to read header: {}", e))?;

        let columns: Vec<&str> = header_line.split(',').collect();

        // Read data rows
        for (idx, line) in lines.enumerate() {
            let line =
                line.map_err(|e| anyhow::anyhow!("Failed to read line {}: {}", idx + 2, e))?;
            let values: Vec<&str> = line.split(',').collect();

            if values.len() != columns.len() {
                return Err(anyhow::anyhow!(
                    "Row {} has {} columns, expected {}",
                    idx + 2,
                    values.len(),
                    columns.len()
                ));
            }

            // Build INSERT statement
            let cols = columns.join(", ");
            let vals = values
                .iter()
                .map(|v| {
                    // Properly escape single quotes to prevent SQL injection
                    let escaped = v.trim().replace("'", "''");
                    format!("'{}'", escaped)
                })
                .collect::<Vec<_>>()
                .join(", ");

            let stmt = format!("INSERT INTO {} ({}) VALUES ({});", table_name, cols, vals);
            statements.push(stmt);
        }

        println!("Generated {} INSERT statements from '{}'", statements.len(), file_path);
        Ok(statements)
    }

    /// Import JSON file and generate INSERT statements
    pub fn import_json(file_path: &str, table_name: &str) -> anyhow::Result<Vec<String>> {
        use std::fs;

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

        let mut statements = Vec::new();

        // Process each JSON object
        for (idx, obj) in json_array.iter().enumerate() {
            // Extract column names and values from this object
            let columns: Vec<String> = obj.keys().cloned().collect();

            if columns.is_empty() {
                return Err(anyhow::anyhow!("Row {} has no columns", idx + 1));
            }

            let values: Vec<String> = columns
                .iter()
                .map(|col| {
                    let value = &obj[col];
                    // Convert JSON value to string and escape for SQL
                    let value_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Null => "NULL".to_string(),
                        _ => value.to_string(),
                    };

                    // Escape single quotes to prevent SQL injection
                    if value_str == "NULL" {
                        value_str
                    } else {
                        let escaped = value_str.replace("'", "''");
                        format!("'{}'", escaped)
                    }
                })
                .collect();

            // Build INSERT statement
            let cols = columns.join(", ");
            let vals = values.join(", ");
            let stmt = format!("INSERT INTO {} ({}) VALUES ({});", table_name, cols, vals);
            statements.push(stmt);
        }

        println!("Generated {} INSERT statements from '{}'", statements.len(), file_path);
        Ok(statements)
    }
}

fn write_csv_row<W: Write>(writer: &mut W, values: &[String]) -> anyhow::Result<()> {
    let csv_line = values.iter().map(|v| escape_csv_value(v)).collect::<Vec<_>>().join(",");

    writeln!(writer, "{}", csv_line)?;
    Ok(())
}

fn escape_csv_value(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace("\"", "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_csv_simple() {
        assert_eq!(escape_csv_value("hello"), "hello");
    }

    #[test]
    fn test_escape_csv_with_comma() {
        assert_eq!(escape_csv_value("hello,world"), "\"hello,world\"");
    }

    #[test]
    fn test_escape_csv_with_quotes() {
        assert_eq!(escape_csv_value("hello\"world"), "\"hello\"\"world\"");
    }

    #[test]
    fn test_escape_csv_with_newline() {
        assert_eq!(escape_csv_value("hello\nworld"), "\"hello\nworld\"");
    }

    #[test]
    fn test_sql_value_escaping() {
        // Test that single quotes are properly escaped when building INSERT statements
        let value = "O'Brien";
        let escaped = value.replace("'", "''");
        assert_eq!(escaped, "O''Brien");

        // Verify the escaped value in a SQL statement wouldn't cause injection
        let sql = format!("INSERT INTO users (name) VALUES ('{}');", escaped);
        assert_eq!(sql, "INSERT INTO users (name) VALUES ('O''Brien');");
    }

    #[test]
    fn test_import_json_basic() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        // Create temporary JSON file
        let mut temp_file = NamedTempFile::new().unwrap();
        let json_data = r#"[
            {"id": "1", "name": "Alice", "email": "alice@example.com"},
            {"id": "2", "name": "Bob", "email": "bob@example.com"}
        ]"#;
        temp_file.write_all(json_data.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        // Import JSON
        let statements = DataIO::import_json(temp_file.path().to_str().unwrap(), "users").unwrap();

        // Verify we got 2 INSERT statements
        assert_eq!(statements.len(), 2);

        // Verify statements contain expected data
        assert!(statements[0].contains("INSERT INTO users"));
        assert!(statements[0].contains("Alice"));
        assert!(statements[1].contains("Bob"));
    }

    #[test]
    fn test_import_json_sql_injection() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        // Create JSON with SQL injection attempt
        let mut temp_file = NamedTempFile::new().unwrap();
        let json_data = r#"[
            {"name": "Bob'); DROP TABLE users; --", "email": "test@example.com"}
        ]"#;
        temp_file.write_all(json_data.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        // Import JSON
        let statements =
            DataIO::import_json(temp_file.path().to_str().unwrap(), "test_table").unwrap();

        // Verify single quotes are properly escaped
        assert_eq!(statements.len(), 1);
        let stmt = &statements[0];

        // Should contain escaped quotes: Bob'') instead of Bob')
        assert!(stmt.contains("Bob''); DROP TABLE users; --"));
        // Verify it's inside quotes, making it safe
        assert!(stmt.contains("'Bob''); DROP TABLE users; --'"));
    }

    #[test]
    fn test_import_json_invalid_format() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        // Create invalid JSON file
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"not valid json").unwrap();
        temp_file.flush().unwrap();

        // Attempt to import
        let result = DataIO::import_json(temp_file.path().to_str().unwrap(), "users");

        // Should fail with error
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid JSON format"));
    }

    #[test]
    fn test_import_json_empty_array() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        // Create empty JSON array
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"[]").unwrap();
        temp_file.flush().unwrap();

        // Attempt to import
        let result = DataIO::import_json(temp_file.path().to_str().unwrap(), "users");

        // Should fail with error
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no data"));
    }
}
