// ============================================================================
// SQL Dump Loading Utilities (Load Operations)
// ============================================================================
//
// Provides utilities for parsing and loading SQL dump files.
// Actual execution of statements happens at the CLI layer via the parser
// and executor, but the parsing logic lives here for reusability.

use std::{fs, path::Path};

use crate::StorageError;

/// Marker that indicates a complete SQL dump file
pub const SQL_DUMP_END_MARKER: &str = "-- End of dump";

/// Read SQL dump content from file
///
/// # Errors
/// Returns `StorageError::NotImplemented` if:
/// - The file cannot be read or is not a text file
/// - The file appears to be truncated (missing end marker)
pub fn read_sql_dump<P: AsRef<Path>>(path: P) -> Result<String, StorageError> {
    let path_ref = path.as_ref();
    if !path_ref.exists() {
        return Err(StorageError::NotImplemented(format!("File does not exist: {:?}", path_ref)));
    }

    // Try to read the file as text
    match fs::read_to_string(path_ref) {
        Ok(content) => {
            // Verify the dump is complete by checking for the end marker
            // This detects truncated/corrupted files from interrupted writes
            if content.contains(SQL_DUMP_END_MARKER) {
                Ok(content)
            } else if content.trim().is_empty() {
                // Empty file is valid for a new database
                Ok(content)
            } else if content.starts_with("-- VibeSQL Database Dump") {
                // File has our header but is missing the end marker - truncated
                Err(StorageError::NotImplemented(format!(
                    "Database file {:?} appears to be truncated (missing end marker). \
                     This may be caused by an interrupted write. \
                     If you have a backup, please restore it.",
                    path_ref
                )))
            } else {
                // Not a VibeSQL dump file - might be plain SQL, allow it
                Ok(content)
            }
        }
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
/// - `CREATE TRIGGER ... BEGIN ... END;` blocks where embedded semicolons inside
///   the trigger body must NOT terminate the outer statement. We track BEGIN/END
///   nesting depth (matched as whole-word identifiers, case-insensitively) and
///   only honor `;` as a terminator at depth 0.
///
/// # Returns
/// A vector of SQL statement strings, trimmed and ready to parse
pub fn parse_sql_statements(content: &str) -> Result<Vec<String>, StorageError> {
    let mut statements = Vec::new();
    let mut current_statement = String::new();
    let mut in_string = false;
    let mut string_char = ' ';
    let mut escape_next = false;
    // BEGIN/END nesting depth. While > 0, semicolons inside the trigger body do
    // not terminate the outer statement.
    let mut begin_depth: u32 = 0;
    // CASE...END nesting depth *within* a trigger body. A `CASE ... END`
    // expression in a trigger action (e.g.
    // `SELECT CASE WHEN new.a = 4 THEN RAISE(IGNORE) END`) introduces an inner
    // `END` that must NOT be counted as the trigger's terminating `END`.
    // Without this, reloading a dumped `CREATE TRIGGER ... BEGIN ... CASE ...
    // END ... END;` is split at the CASE's `END`, truncating the trigger body
    // (issue #5468). Only tracked while inside a trigger body
    // (`begin_depth > 0`); top-level CASE expressions split on their `;`.
    let mut case_depth: u32 = 0;

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

            // Detect BEGIN/END as whole-word, case-insensitive identifiers when
            // not inside a string literal. Track nesting depth so `;` inside a
            // trigger body doesn't prematurely terminate the CREATE TRIGGER.
            if !in_string && ch.is_ascii_alphabetic() {
                let prev_is_ident = i > 0
                    && (chars[i - 1].is_ascii_alphanumeric() || chars[i - 1] == '_');
                if !prev_is_ident {
                    if let Some(consumed) = match_keyword(&chars, i, "BEGIN") {
                        begin_depth += 1;
                        current_statement.push_str("BEGIN");
                        i += consumed;
                        continue;
                    }
                    // A CASE expression inside a trigger body opens a block that
                    // its own END closes; track it so END does not prematurely
                    // close the trigger body (issue #5468).
                    if begin_depth > 0 {
                        if let Some(consumed) = match_keyword(&chars, i, "CASE") {
                            case_depth += 1;
                            current_statement.push_str("CASE");
                            i += consumed;
                            continue;
                        }
                    }
                    if let Some(consumed) = match_keyword(&chars, i, "END") {
                        if case_depth > 0 {
                            // Closes an inner CASE expression, not the body.
                            case_depth -= 1;
                        } else if begin_depth > 0 {
                            begin_depth -= 1;
                        }
                        current_statement.push_str("END");
                        i += consumed;
                        continue;
                    }
                }
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
                ';' if !in_string && begin_depth == 0 => {
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

/// Match a keyword (case-insensitively) at position `i` in `chars`, ensuring that the
/// character after the keyword is not part of an identifier (so e.g. "BEGINNING" does
/// not match "BEGIN"). Returns the number of characters consumed if matched.
fn match_keyword(chars: &[char], i: usize, keyword: &str) -> Option<usize> {
    let kw_chars: Vec<char> = keyword.chars().collect();
    if i + kw_chars.len() > chars.len() {
        return None;
    }
    for (k, kc) in kw_chars.iter().enumerate() {
        if chars[i + k].to_ascii_uppercase() != *kc {
            return None;
        }
    }
    // Ensure the next character is not part of an identifier
    let next_idx = i + kw_chars.len();
    if next_idx < chars.len() {
        let nc = chars[next_idx];
        if nc.is_ascii_alphanumeric() || nc == '_' {
            return None;
        }
    }
    Some(kw_chars.len())
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

    #[test]
    fn test_parse_create_trigger_body_with_semicolons() {
        // Trigger bodies contain BEGIN ... END; with embedded semicolons that must
        // NOT split the outer statement.
        let content = r#"
CREATE TABLE t(a, b);
CREATE TRIGGER tr AFTER INSERT ON t BEGIN
  INSERT INTO t VALUES(99, 99);
  UPDATE t SET b = 1 WHERE a = 0;
END;
INSERT INTO t VALUES(1, 1);
"#;
        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 3, "got: {:#?}", statements);
        assert!(statements[0].contains("CREATE TABLE"));
        assert!(statements[1].contains("CREATE TRIGGER"));
        assert!(statements[1].contains("BEGIN"));
        assert!(statements[1].contains("END"));
        // Both inner statements must remain inside the trigger body
        assert!(statements[1].contains("INSERT INTO t VALUES(99, 99)"));
        assert!(statements[1].contains("UPDATE t SET b = 1"));
        assert!(statements[2].contains("INSERT INTO t VALUES(1, 1)"));
    }

    #[test]
    fn test_parse_nested_begin_end() {
        // Nested BEGIN/END must balance correctly.
        let content = r#"
CREATE TRIGGER tr AFTER INSERT ON t BEGIN
  BEGIN
    INSERT INTO t VALUES(1);
  END;
  INSERT INTO t VALUES(2);
END;
SELECT 1;
"#;
        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("CREATE TRIGGER"));
        assert!(statements[1].trim().starts_with("SELECT 1"));
    }

    #[test]
    fn test_parse_trigger_body_with_case_end() {
        // Issue #5468: a CASE...END expression inside a trigger body must not
        // have its END counted as the trigger's terminating END when a dump is
        // reloaded.
        let content = r#"
CREATE TABLE tbl(a, b, c);
CREATE TRIGGER before_tbl_insert BEFORE INSERT ON tbl BEGIN SELECT CASE WHEN (new.a = 4) THEN RAISE(IGNORE) END; END;
INSERT INTO tbl VALUES(1, 2, 3);
"#;
        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 3, "got: {:#?}", statements);
        assert!(statements[0].contains("CREATE TABLE"));
        assert!(statements[1].contains("CREATE TRIGGER"));
        assert!(statements[1].contains("CASE WHEN"));
        assert!(statements[1].contains("RAISE(IGNORE)"));
        // The trigger body's terminating END must be captured; without the
        // CASE-depth fix the statement is truncated at the CASE's END and the
        // trailing `END;` becomes a separate fragment.
        assert!(
            statements[1].contains("RAISE(IGNORE) END; END"),
            "trigger body truncated at CASE END: {:?}",
            statements[1]
        );
        assert!(statements[2].contains("INSERT INTO tbl VALUES(1, 2, 3)"));
    }

    #[test]
    fn test_parse_trigger_body_with_nested_case() {
        // Multiple / nested CASE...END expressions inside one trigger body.
        let content = r#"
CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN
  SELECT CASE WHEN a = 1 THEN CASE WHEN b = 2 THEN RAISE(ABORT, 'x') END END;
  SELECT CASE WHEN c = 3 THEN RAISE(IGNORE) END;
END;
SELECT 1;
"#;
        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 2, "got: {:#?}", statements);
        assert!(statements[0].contains("CREATE TRIGGER"));
        assert!(statements[1].trim().starts_with("SELECT 1"));
    }

    #[test]
    fn test_parse_begin_keyword_does_not_match_beginning() {
        // "BEGINNING" is an identifier, not a BEGIN keyword.
        let content = r#"INSERT INTO t VALUES('BEGINNING');
SELECT 1;"#;
        let statements = parse_sql_statements(content).unwrap();
        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn test_truncation_detection() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Test 1: Complete dump file should succeed
        let mut complete_file = NamedTempFile::new().unwrap();
        writeln!(complete_file, "-- VibeSQL Database Dump\nCREATE TABLE t(x INT);\n-- End of dump")
            .unwrap();
        assert!(read_sql_dump(complete_file.path()).is_ok());

        // Test 2: Truncated dump file should fail
        let mut truncated_file = NamedTempFile::new().unwrap();
        writeln!(
            truncated_file,
            "-- VibeSQL Database Dump\nCREATE TABLE t(x INT);\nINSERT INTO t VALUES ("
        )
        .unwrap();
        let result = read_sql_dump(truncated_file.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("truncated"));

        // Test 3: Empty file should succeed (valid for new database)
        let empty_file = NamedTempFile::new().unwrap();
        assert!(read_sql_dump(empty_file.path()).is_ok());

        // Test 4: Plain SQL (not a VibeSQL dump) should succeed
        let mut plain_sql = NamedTempFile::new().unwrap();
        writeln!(plain_sql, "CREATE TABLE t(x INT);").unwrap();
        assert!(read_sql_dump(plain_sql.path()).is_ok());
    }
}
