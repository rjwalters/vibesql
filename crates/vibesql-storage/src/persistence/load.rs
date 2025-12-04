// ============================================================================
// SQL Dump Loading Utilities (Load Operations)
// ============================================================================
//
// Provides utilities for parsing and loading SQL dump files.
// Actual execution of statements happens at the CLI layer via the parser
// and executor, but the parsing logic lives here for reusability.

use std::{fs, path::Path};

use crate::StorageError;

/// Read SQL dump content from file
///
/// # Errors
/// Returns `StorageError::NotImplemented` if the file cannot be read or is not a text file
pub fn read_sql_dump<P: AsRef<Path>>(path: P) -> Result<String, StorageError> {
    let path_ref = path.as_ref();
    if !path_ref.exists() {
        return Err(StorageError::NotImplemented(format!("File does not exist: {:?}", path_ref)));
    }

    // Try to read the file as text
    match fs::read_to_string(path_ref) {
        Ok(content) => Ok(content),
        Err(e) => {
            // Check if this might be a binary database file (like SQLite)
            if let Ok(bytes) = fs::read(path_ref).map(|b| b.get(0..16).unwrap_or(&[]).to_vec()) {
                // Check for SQLite file signature
                if bytes.starts_with(b"SQLite format") {
                    return Err(StorageError::NotImplemented(format!(
                        "File appears to be a binary SQLite database. vibesql uses SQL dump format (text). \
                         To import, export from SQLite as SQL: sqlite3 {} .dump > {}.sql",
                        path_ref.display(),
                        path_ref.file_stem().and_then(|s| s.to_str()).unwrap_or("database")
                    )));
                }
                // Check for other binary file indicators (null bytes in first 512 bytes)
                if let Ok(sample) =
                    fs::read(path_ref).map(|b| b.get(0..512).unwrap_or(&[]).to_vec())
                {
                    if sample.contains(&0) {
                        return Err(StorageError::NotImplemented(
                            "File appears to be a binary database format. vibesql uses SQL dump format (text files). \
                             Please export your database as SQL text format.".to_string()
                        ));
                    }
                }
            }

            // Generic error for other read failures
            Err(StorageError::NotImplemented(format!("Failed to read file: {}", e)))
        }
    }
}

/// Parse SQL dump content into individual statements
///
/// Handles:
/// - Comments (lines starting with -- or inline -- comments)
/// - Multi-line statements
/// - Statement termination by semicolon
/// - String literals (preserves content within quotes)
///
/// # Returns
/// A vector of SQL statement strings, trimmed and ready to parse
pub fn parse_sql_statements(content: &str) -> Result<Vec<String>, StorageError> {
    let mut statements = Vec::new();
    let mut current_statement = String::new();
    let mut in_string = false;
    let mut string_char = ' ';
    let mut escape_next = false;

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip full-line comments and empty lines
        if trimmed.starts_with("--") || trimmed.is_empty() {
            continue;
        }

        // Process line character by character to handle string literals and inline comments
        let chars: Vec<char> = line.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let ch = chars[i];

            if escape_next {
                current_statement.push(ch);
                escape_next = false;
                i += 1;
                continue;
            }

            // Check for inline comment (-- outside of string)
            if !in_string && ch == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
                // Skip rest of line (inline comment)
                break;
            }

            match ch {
                '\\' if in_string && string_char == '\'' => {
                    current_statement.push(ch);
                    escape_next = true;
                }
                '\'' | '"' if !in_string => {
                    in_string = true;
                    string_char = ch;
                    current_statement.push(ch);
                }
                c if in_string && c == string_char => {
                    in_string = false;
                    current_statement.push(ch);
                }
                ';' if !in_string => {
                    current_statement.push(ch);
                    // Statement complete
                    if !current_statement.trim().is_empty() {
                        statements.push(current_statement.trim_end_matches(';').to_string());
                    }
                    current_statement.clear();
                }
                _ => {
                    current_statement.push(ch);
                }
            }
            i += 1;
        }

        // Add space between lines (preserves SQL readability)
        if !in_string {
            current_statement.push(' ');
        }
    }

    // Handle any remaining statement
    if !current_statement.trim().is_empty() {
        statements.push(current_statement.trim().to_string());
    }

    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_statements() {
        let content = r#"
            -- Comment
            CREATE TABLE users (id INTEGER);
            INSERT INTO users VALUES (1);
        "#;

        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("CREATE TABLE"));
        assert!(statements[1].contains("INSERT INTO"));
    }

    #[test]
    fn test_parse_with_string_literals() {
        let content = r#"INSERT INTO users VALUES (1, 'John; Doe');"#;

        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("John; Doe"));
    }

    #[test]
    fn test_skip_comments() {
        let content = r#"
            -- This is a comment
            CREATE TABLE users (id INTEGER);
            -- Another comment
        "#;

        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_multiline_statements() {
        let content = r#"
            CREATE TABLE users (
                id INTEGER,
                name VARCHAR(100)
            );
        "#;

        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("id INTEGER"));
        assert!(statements[0].contains("name VARCHAR"));
    }

    #[test]
    fn test_inline_comments() {
        let content = r#"
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,  -- 'PASS', 'FAIL', 'TIMEOUT', 'UNTESTED'
                last_tested TIMESTAMP
            );
        "#;

        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("file_path VARCHAR(500) PRIMARY KEY"));
        assert!(statements[0].contains("status VARCHAR(20) NOT NULL"));
        // The inline comment should NOT be included
        assert!(!statements[0].contains("PASS"));
        assert!(!statements[0].contains("FAIL"));
    }

    #[test]
    fn test_inline_comments_preserve_strings_with_dashes() {
        // Ensure -- inside string literals is NOT treated as a comment
        let content = r#"INSERT INTO users VALUES (1, 'test--value');"#;

        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("test--value"));
    }

    #[test]
    fn test_inline_comment_after_insert() {
        let content = r#"INSERT INTO users VALUES (1, 'Alice'); -- Add first user"#;

        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("'Alice'"));
        assert!(!statements[0].contains("Add first user"));
    }
}
