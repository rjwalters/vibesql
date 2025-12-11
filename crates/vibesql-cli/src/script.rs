use std::{
    fs,
    io::{self, Read},
};

use vibesql_l10n::vibe_msg;

use crate::{
    commands::MetaCommand,
    executor::SqlExecutor,
    formatter::{OutputFormat, ResultFormatter},
};

/// Script executor - runs multiple SQL statements from files or stdin
pub struct ScriptExecutor {
    executor: SqlExecutor,
    formatter: ResultFormatter,
    verbose: bool,
    database_path: Option<String>,
}

impl ScriptExecutor {
    pub fn new(
        database: Option<String>,
        verbose: bool,
        format: Option<OutputFormat>,
    ) -> anyhow::Result<Self> {
        let database_path = database.clone();
        let executor = SqlExecutor::new(database)?;
        let mut formatter = ResultFormatter::new();

        if let Some(fmt) = format {
            formatter.set_format(fmt);
        }

        Ok(ScriptExecutor { executor, formatter, verbose, database_path })
    }

    /// Execute SQL from a file
    pub fn execute_file(&mut self, file_path: &str) -> anyhow::Result<()> {
        let contents = fs::read_to_string(file_path).map_err(|e| {
            anyhow::anyhow!(
                "{}",
                vibe_msg!("file-read-error", path = file_path, error = e.to_string())
            )
        })?;

        self.execute_script(&contents)
    }

    /// Execute SQL from stdin
    pub fn execute_stdin(&mut self) -> anyhow::Result<()> {
        let mut contents = String::new();
        io::stdin().read_to_string(&mut contents).map_err(|e| {
            anyhow::anyhow!("{}", vibe_msg!("stdin-read-error", error = e.to_string()))
        })?;

        self.execute_script(&contents)
    }

    /// Execute a script (multiple SQL statements)
    pub fn execute_script(&mut self, script: &str) -> anyhow::Result<()> {
        // Split script into individual statements
        // Simple approach: split by semicolon (doesn't handle all edge cases)
        let statements = parse_statements(script);

        if statements.is_empty() {
            if self.verbose {
                println!("{}", vibe_msg!("script-no-statements"));
            }
            return Ok(());
        }

        let mut success_count = 0;
        let mut error_count = 0;

        for (idx, stmt) in statements.iter().enumerate() {
            if self.verbose {
                println!(
                    "{}",
                    vibe_msg!(
                        "script-executing",
                        current = (idx + 1) as i64,
                        total = statements.len() as i64
                    )
                );
            }

            // Check if this is a meta-command (dot or backslash command)
            if let Some(meta_cmd) = MetaCommand::parse(stmt) {
                match self.handle_meta_command(meta_cmd) {
                    Ok(should_exit) => {
                        if should_exit {
                            // .quit/.exit in script mode - stop processing
                            return Ok(());
                        }
                        success_count += 1;
                    }
                    Err(e) => {
                        eprintln!(
                            "{}",
                            vibe_msg!(
                                "script-error",
                                index = (idx + 1) as i64,
                                error = e.to_string()
                            )
                        );
                        error_count += 1;
                    }
                }
                continue;
            }

            match self.executor.execute(stmt) {
                Ok(result) => {
                    self.formatter.print_result(&result);
                    success_count += 1;

                    // Auto-save after modification statements if database path is provided
                    if let Some(ref path) = self.database_path {
                        if is_modification_statement(stmt) {
                            if let Err(e) = self.executor.save_database(path) {
                                eprintln!(
                                    "{}",
                                    vibe_msg!("warning-auto-save-failed", error = e.to_string())
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "{}",
                        vibe_msg!("script-error", index = (idx + 1) as i64, error = e.to_string())
                    );
                    error_count += 1;
                    // Continue executing remaining statements
                }
            }
        }

        // Summary
        if self.verbose || error_count > 0 {
            println!("\n{}", vibe_msg!("script-summary-title"));
            println!("{}", vibe_msg!("script-total", count = statements.len() as i64));
            println!("{}", vibe_msg!("script-successful", count = success_count as i64));
            println!("{}", vibe_msg!("script-failed", count = error_count as i64));
        }

        if error_count > 0 {
            Err(anyhow::anyhow!("{}", vibe_msg!("script-failed-error", count = error_count as i64)))
        } else {
            Ok(())
        }
    }

    /// Handle a meta-command. Returns Ok(true) if the script should exit.
    fn handle_meta_command(&mut self, cmd: MetaCommand) -> anyhow::Result<bool> {
        match cmd {
            MetaCommand::Quit => {
                return Ok(true);
            }
            MetaCommand::Help => {
                self.print_help();
            }
            MetaCommand::DescribeTable(table_name) => {
                self.executor.describe_table(&table_name)?;
            }
            MetaCommand::ListTables => {
                self.executor.list_tables()?;
            }
            MetaCommand::ListSchemas => {
                self.executor.list_schemas()?;
            }
            MetaCommand::ListIndexes => {
                self.executor.list_indexes()?;
            }
            MetaCommand::ListRoles => {
                self.executor.list_roles()?;
            }
            MetaCommand::SetFormat(format) => {
                self.formatter.set_format(format);
            }
            MetaCommand::Timing => {
                self.executor.toggle_timing();
            }
            MetaCommand::Copy { table, file_path, direction, format } => {
                self.executor.handle_copy(&table, &file_path, direction, format)?;
            }
            MetaCommand::Save(path) => {
                let save_path = path.or_else(|| self.database_path.clone());
                match save_path {
                    Some(ref p) => {
                        self.executor.save_database(p)?;
                        println!("{}", vibe_msg!("database-saved", path = p.as_str()));
                    }
                    None => {
                        eprintln!("{}", vibe_msg!("no-database-file"));
                    }
                }
            }
            MetaCommand::Errors => {
                // No error history in script mode - just skip
            }
        }
        Ok(false)
    }

    fn print_help(&self) {
        println!(
            "
Meta-commands (PostgreSQL-style):
  \\d [table]      - Describe table or list all tables
  \\dt             - List tables
  \\ds             - List schemas
  \\di             - List indexes
  \\f <format>     - Set output format
  \\timing         - Toggle query timing
  \\copy           - Import/export data
  \\save [file]    - Save database
  \\q, \\quit      - Exit

Dot-commands (SQLite-style):
  .tables         - List tables
  .schema [table] - Show CREATE statement or list tables
  .indexes        - List indexes
  .databases      - List schemas
  .mode <format>  - Set output format (table, json, csv, markdown, html)
  .timer          - Toggle query timing
  .import FILE TABLE - Import data from file
  .save [file]    - Save database
  .quit, .exit    - Exit
"
        );
    }
}

/// Check if a SQL statement is a modification (DDL/DML) that should trigger auto-save
fn is_modification_statement(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("CREATE ")
        || upper.starts_with("DROP ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("INSERT ")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("DELETE ")
}

/// Parse SQL script into individual statements
///
/// This implementation:
/// 1. Removes single-line comments (lines starting with --)
/// 2. Removes multi-line comments (/* ... */)
/// 3. Splits on semicolons, but respects string literals and comments
/// 4. Handles escaped quotes within strings ('' for SQL)
/// 5. Treats newlines as delimiters for dot-commands (SQLite compatibility)
fn parse_statements(script: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current_statement = String::new();
    let mut in_string = false;
    let mut in_multiline_comment = false;
    let mut chars = script.chars().peekable();

    while let Some(ch) = chars.next() {
        // Handle multi-line comments
        if !in_string && ch == '/' && chars.peek() == Some(&'*') {
            chars.next(); // consume '*'
            in_multiline_comment = true;
            continue;
        }

        if in_multiline_comment {
            if ch == '*' && chars.peek() == Some(&'/') {
                chars.next(); // consume '/'
                in_multiline_comment = false;
            }
            continue;
        }

        // Handle single-line comments
        if !in_string && ch == '-' && chars.peek() == Some(&'-') {
            // Skip until end of line
            for c in chars.by_ref() {
                if c == '\n' {
                    current_statement.push(c); // preserve newline for formatting
                    break;
                }
            }
            continue;
        }

        // Handle string literals
        if ch == '\'' {
            current_statement.push(ch);
            // Check for escaped quote ('' in SQL)
            if in_string && chars.peek() == Some(&'\'') {
                chars.next(); // consume the second quote
                current_statement.push('\'');
                continue;
            }
            in_string = !in_string;
            continue;
        }

        // Handle statement delimiter (semicolon)
        if !in_string && ch == ';' {
            let trimmed = current_statement.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current_statement.clear();
            continue;
        }

        // Handle newlines as delimiters for dot-commands (SQLite compatibility)
        // Dot-commands don't require semicolons - a newline ends them
        if !in_string && ch == '\n' {
            let trimmed = current_statement.trim();
            if !trimmed.is_empty() && (trimmed.starts_with('.') || trimmed.starts_with('\\')) {
                statements.push(trimmed.to_string());
                current_statement.clear();
                continue;
            }
        }

        // Regular character
        current_statement.push(ch);
    }

    // Add final statement if not empty
    let trimmed = current_statement.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_statement() {
        let script = "SELECT * FROM users;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT * FROM users");
    }

    #[test]
    fn test_parse_multiple_statements() {
        let script = "CREATE TABLE users (id INT); INSERT INTO users VALUES (1);";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "CREATE TABLE users (id INT)");
        assert_eq!(stmts[1], "INSERT INTO users VALUES (1)");
    }

    #[test]
    fn test_parse_with_whitespace() {
        let script = "  SELECT 1;  \n  SELECT 2;  ";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_parse_with_comments() {
        let script = "-- This is a comment\nSELECT 1;";
        let stmts = parse_statements(script);
        // Comment lines starting with -- are filtered out, leaving only SELECT 1
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1");
    }

    #[test]
    fn test_parse_empty_script() {
        let script = "";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 0);
    }

    #[test]
    fn test_parse_semicolon_in_string() {
        // Issue #1804: Semicolons inside string literals should not be treated as statement
        // delimiters
        let script = "INSERT INTO test VALUES (1, 'Error at position 10; expected value');";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "INSERT INTO test VALUES (1, 'Error at position 10; expected value')");
    }

    #[test]
    fn test_parse_escaped_quotes_in_string() {
        // SQL uses doubled single quotes for escaping
        let script = "INSERT INTO test VALUES ('It''s a test');";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "INSERT INTO test VALUES ('It''s a test')");
    }

    #[test]
    fn test_parse_multiline_comment() {
        let script = "/* This is a\nmulti-line comment */\nSELECT 1;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1");
    }

    #[test]
    fn test_parse_comment_with_semicolon() {
        let script = "-- This comment has a semicolon; but it should be ignored\nSELECT 1;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1");
    }

    #[test]
    fn test_parse_complex_error_message() {
        // Real-world test case from SQLLogicTest results
        let script = r#"INSERT INTO test_results (error_message) VALUES ('query result mismatch: [SQL] SELECT TIMESTAMP ''2025-11-15 00:00:00'' [Diff] expected; actual');"#;
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("TIMESTAMP"));
        assert!(stmts[0].contains("expected; actual"));
    }

    #[test]
    fn test_parse_multiple_with_strings_and_comments() {
        let script = r#"
-- First statement
INSERT INTO logs VALUES (1, 'Error: parse failed; retry');
/* Second statement
   with comment */
INSERT INTO logs VALUES (2, 'Success');
-- Done
"#;
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("parse failed; retry"));
        assert!(stmts[1].contains("Success"));
    }

    #[test]
    fn test_parse_dot_commands_without_semicolons() {
        // SQLite dot-commands don't require semicolons - newline ends them
        let script = ".tables\n.schema users\nSELECT * FROM users;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], ".tables");
        assert_eq!(stmts[1], ".schema users");
        assert_eq!(stmts[2], "SELECT * FROM users");
    }

    #[test]
    fn test_parse_backslash_commands_without_semicolons() {
        // Backslash commands also work without semicolons
        let script = "\\dt\n\\d users\nSELECT 1;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "\\dt");
        assert_eq!(stmts[1], "\\d users");
        assert_eq!(stmts[2], "SELECT 1");
    }

    #[test]
    fn test_parse_mixed_commands_and_sql() {
        let script = ".mode json\nSELECT * FROM users;\n.tables";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], ".mode json");
        assert_eq!(stmts[1], "SELECT * FROM users");
        assert_eq!(stmts[2], ".tables");
    }
}
