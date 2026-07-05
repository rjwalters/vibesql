use std::{
    fs,
    io::{self, Read},
};

use vibesql_l10n::vibe_msg;

use crate::{
    commands::MetaCommand,
    executor::{DbOpenOptions, SqlExecutor},
    formatter::{OutputFormat, ResultFormatter},
    util::is_memory_database,
};

/// Script executor - runs multiple SQL statements from files or stdin
pub struct ScriptExecutor {
    executor: SqlExecutor,
    formatter: ResultFormatter,
    verbose: bool,
    database_path: Option<String>,
}

impl ScriptExecutor {
    /// Construct a script executor.
    ///
    /// `options.wal` activates the WAL persistence path for file-backed
    /// databases (see `SqlExecutor::new_with_options`); `false` preserves the
    /// snapshot-on-exit behavior. `options.recover_fallback` opts into
    /// older-checkpoint recovery when the newest checkpoint is unreadable.
    pub fn new(
        database: Option<String>,
        verbose: bool,
        format: Option<OutputFormat>,
        options: DbOpenOptions,
    ) -> anyhow::Result<Self> {
        // Treat :memory: as an in-memory database (no file path for saving)
        let database_path = database.as_ref().filter(|p| !is_memory_database(p)).cloned();
        let executor = SqlExecutor::new_with_options(database, options)?;
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
        // Fail-closed persistence tracking (issue #5832): a checkpoint/save
        // failure must never end in exit 0. A later *successful* save clears
        // the flag because every checkpoint/snapshot captures the full
        // database state, superseding the earlier failure.
        let mut persist_failed = false;

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
                            // .quit/.exit in script mode - stop processing.
                            // A pending persistence failure must still surface
                            // as a non-zero exit (issue #5832).
                            if persist_failed {
                                return Err(anyhow::anyhow!(
                                    "failed to persist database changes; \
                                     see the ERROR output above"
                                ));
                            }
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

                    // Auto-save after modification statements if database path is provided.
                    //
                    // CRITICAL: Skip auto-save while a transaction is open. Persisting
                    // uncommitted changes turns ROLLBACK into a no-op across CLI
                    // invocations — the .vbsql dump captures the mid-transaction state
                    // and the next process loads it as committed. This silently broke
                    // deferred-FK semantics in the batched TCL shim path (every fkey6
                    // test that ran ROLLBACK after an INSERT/UPDATE/DELETE was
                    // observed to have lost data despite the rollback succeeding
                    // in memory). The next save naturally happens at COMMIT or
                    // ROLLBACK statement boundary, both of which match
                    // `is_modification_statement` only if we add them — instead, we
                    // also force a save when the transaction state transitions back
                    // to "no active transaction" after this statement.
                    if let Some(ref path) = self.database_path {
                        let in_txn = self.executor.in_transaction();
                        let should_save = is_modification_statement(stmt) && !in_txn;
                        // Also save on the COMMIT/ROLLBACK boundary so the
                        // post-commit (or post-rollback) state is durable.
                        let upper = stmt.trim().to_uppercase();
                        let is_txn_end = !in_txn
                            && (upper.starts_with("COMMIT")
                                || upper.starts_with("ROLLBACK")
                                || upper.starts_with("END"));
                        if should_save || is_txn_end {
                            match self.executor.save_database(path) {
                                Ok(()) => persist_failed = false,
                                Err(e) => {
                                    // Loud + fail-closed (issue #5832): the WAL
                                    // is never truncated on a failed checkpoint,
                                    // and the process must exit non-zero.
                                    persist_failed = true;
                                    crate::util::report_save_failure(
                                        path,
                                        self.executor.wal_active(),
                                        &e,
                                    );
                                }
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
                    // SQLite compatibility:
                    //   - Outside a transaction: stop on first error (issue #4731).
                    //     SQLite's TCL interface and CLI both stop execution on
                    //     statement error so that scripts like
                    //     `catchsql { CREATE TABLE t1; INSERT... }` don't continue
                    //     executing remaining statements after a failure.
                    //   - Inside an active transaction: keep running so that
                    //     subsequent statements (typically ROLLBACK or COMMIT)
                    //     can still execute. SQLite leaves the transaction in
                    //     a failed state after a constraint violation; the
                    //     caller is expected to ROLLBACK explicitly. If we
                    //     stopped on the failed statement we'd terminate the
                    //     CLI process and lose the in-memory transaction state,
                    //     making the explicit ROLLBACK arrive at a fresh
                    //     process with no active transaction (issue #5087).
                    if !self.executor.in_transaction() {
                        break;
                    }
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
        } else if persist_failed {
            // Issue #5832: a persistence failure must never exit 0, even when
            // every statement itself succeeded. The detailed ERROR lines were
            // already printed at failure time by `report_save_failure`.
            Err(anyhow::anyhow!("failed to persist database changes; see the ERROR output above"))
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
                        // Prevent saving to :memory: which would create a literal file
                        if is_memory_database(p) {
                            eprintln!(
                                "{}",
                                vibe_msg!("cannot-save-memory-database", path = p.as_str())
                            );
                        } else {
                            self.executor.save_database(p)?;
                            println!("{}", vibe_msg!("database-saved", path = p.as_str()));
                        }
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
    // Direct DDL/DML statements
    if upper.starts_with("CREATE ")
        || upper.starts_with("DROP ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("INSERT ")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("DELETE ")
        // REPLACE INTO is DML (delete conflicting rows + insert). Missing it
        // meant a REPLACE-only session never checkpointed at exit (#5835).
        || upper.starts_with("REPLACE ")
    {
        return true;
    }
    // CTEs (WITH clause) containing DML statements
    // e.g., WITH RECURSIVE ... INSERT INTO ... SELECT ...
    if upper.starts_with("WITH ") {
        // Check if the CTE contains INSERT, UPDATE, or DELETE after the WITH clause
        // Look for these keywords that aren't inside the CTE definition
        if upper.contains(" INSERT ") || upper.contains(" UPDATE ") || upper.contains(" DELETE ") {
            return true;
        }
    }
    false
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
    let mut begin_depth = 0; // Track BEGIN...END nesting for trigger bodies
    // Track CASE...END nesting *within* a trigger body. A `CASE ... END`
    // expression in a trigger action (e.g. `SELECT CASE WHEN ... THEN
    // RAISE(IGNORE) END`) introduces an inner `END` that must NOT be counted
    // as the trigger's terminating `END`. Without this, the splitter closes
    // the `CREATE TRIGGER` at the CASE's `END`, slicing one statement into
    // several malformed fragments (issue #5468). We only track CASE depth when
    // already inside a trigger body (`begin_depth > 0`); top-level CASE
    // expressions split correctly on their trailing `;` and need no tracking.
    let mut case_depth = 0;
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

        // Track BEGIN/END keywords for trigger body nesting (case-insensitive)
        if !in_string && ch.is_ascii_alphabetic() {
            current_statement.push(ch);
            // Peek ahead to check for BEGIN or END keyword
            let rest: String = chars.clone().take_while(|c| c.is_ascii_alphabetic()).collect();
            let word = format!("{}{}", ch, rest).to_uppercase();
            if word == "BEGIN" {
                // A BEGIN can only open a multi-statement body inside a
                // CREATE TRIGGER / PROCEDURE / FUNCTION statement. Anywhere
                // else the word is a transaction BEGIN or a plain identifier —
                // BEGIN is a SQLite *fallback* keyword, so `CREATE TABLE
                // begin(begin begin)` and `INSERT INTO begin VALUES(99)` are
                // legal (keyword1.test) — and must not suppress statement
                // splitting. Without this gate, an identifier `begin` set
                // begin_depth > 0 and glued every following statement into one.
                let prefix_upper = current_statement[..current_statement.len() - 1].to_uppercase();
                let in_body_context = prefix_upper
                    .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                    .any(|w| matches!(w, "TRIGGER" | "PROCEDURE" | "FUNCTION"));
                if !in_body_context {
                    // Treat as a normal word (transaction BEGIN or identifier).
                    continue;
                }
                // Consume the rest of the word
                for _ in 0..(rest.len()) {
                    if let Some(c) = chars.next() {
                        current_statement.push(c);
                    }
                }
                // Check if this is a transaction BEGIN (followed by ; or TRANSACTION)
                // or a trigger body BEGIN (followed by SQL statements)
                let mut peek_chars = chars.clone();
                // Skip whitespace
                while peek_chars.clone().next().is_some_and(|c| c.is_whitespace()) {
                    peek_chars.next();
                }
                // Check what follows
                let next_word: String =
                    peek_chars.clone().take_while(|c| c.is_ascii_alphabetic()).collect();
                let is_transaction = peek_chars.clone().next() == Some(';')
                    || next_word.eq_ignore_ascii_case("TRANSACTION")
                    || next_word.eq_ignore_ascii_case("DEFERRED")
                    || next_word.eq_ignore_ascii_case("IMMEDIATE")
                    || next_word.eq_ignore_ascii_case("EXCLUSIVE");
                if !is_transaction {
                    // Only increment depth for trigger body BEGIN, not transaction BEGIN
                    begin_depth += 1;
                }
                continue;
            } else if word == "CASE" && begin_depth > 0 {
                // A CASE expression inside a trigger body opens a block that is
                // closed by its own END. Track it so that END does not
                // prematurely terminate the trigger body (issue #5468).
                for _ in 0..(rest.len()) {
                    if let Some(c) = chars.next() {
                        current_statement.push(c);
                    }
                }
                case_depth += 1;
                continue;
            } else if word == "END" && begin_depth > 0 {
                // Consume the rest of the word
                for _ in 0..(rest.len()) {
                    if let Some(c) = chars.next() {
                        current_statement.push(c);
                    }
                }
                if case_depth > 0 {
                    // This END closes an inner CASE expression, not the
                    // trigger body.
                    case_depth -= 1;
                } else {
                    begin_depth -= 1;
                }
                continue;
            }
            // Not BEGIN/END, let normal processing continue
            continue;
        }

        // Handle statement delimiter (semicolon)
        // Include the semicolon in the statement so the parser can see it.
        // But don't split if we're inside a BEGIN...END block (trigger body)
        if !in_string && ch == ';' {
            current_statement.push(ch); // Include the semicolon
            if begin_depth == 0 {
                // Only split if we're not inside a BEGIN...END block.
                // SQLite treats consecutive semicolons (";;") and bare ";" as
                // empty statements (no-ops); reject anything that is purely
                // whitespace + semicolons so we don't pass it to the parser.
                let trimmed = current_statement.trim();
                if !trimmed.is_empty() && trimmed.chars().any(|c| c != ';' && !c.is_whitespace()) {
                    statements.push(trimmed.to_string());
                }
                current_statement.clear();
            }
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
        assert_eq!(stmts[0], "SELECT * FROM users;");
    }

    #[test]
    fn test_parse_multiple_statements() {
        let script = "CREATE TABLE users (id INT); INSERT INTO users VALUES (1);";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "CREATE TABLE users (id INT);");
        assert_eq!(stmts[1], "INSERT INTO users VALUES (1);");
    }

    #[test]
    fn test_parse_with_whitespace() {
        let script = "  SELECT 1;  \n  SELECT 2;  ";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1;");
        assert_eq!(stmts[1], "SELECT 2;");
    }

    #[test]
    fn test_parse_with_comments() {
        let script = "-- This is a comment\nSELECT 1;";
        let stmts = parse_statements(script);
        // Comment lines starting with -- are filtered out, leaving only SELECT 1;
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1;");
    }

    #[test]
    fn test_parse_empty_script() {
        let script = "";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 0);
    }

    #[test]
    fn test_parse_consecutive_semicolons() {
        // SQLite treats `;;` and bare `;` as no-ops (empty statements).
        // We should not pass them to the parser.
        let script = "CREATE TABLE t1 (a INT);; SELECT 1;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "CREATE TABLE t1 (a INT);");
        assert_eq!(stmts[1], "SELECT 1;");
    }

    #[test]
    fn test_parse_only_semicolons() {
        let script = ";;;";
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
        assert_eq!(
            stmts[0],
            "INSERT INTO test VALUES (1, 'Error at position 10; expected value');"
        );
    }

    #[test]
    fn test_parse_escaped_quotes_in_string() {
        // SQL uses doubled single quotes for escaping
        let script = "INSERT INTO test VALUES ('It''s a test');";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "INSERT INTO test VALUES ('It''s a test');");
    }

    #[test]
    fn test_parse_multiline_comment() {
        let script = "/* This is a\nmulti-line comment */\nSELECT 1;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1;");
    }

    #[test]
    fn test_parse_comment_with_semicolon() {
        let script = "-- This comment has a semicolon; but it should be ignored\nSELECT 1;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1;");
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
        assert_eq!(stmts[2], "SELECT * FROM users;");
    }

    #[test]
    fn test_parse_backslash_commands_without_semicolons() {
        // Backslash commands also work without semicolons
        let script = "\\dt\n\\d users\nSELECT 1;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "\\dt");
        assert_eq!(stmts[1], "\\d users");
        assert_eq!(stmts[2], "SELECT 1;");
    }

    #[test]
    fn test_parse_mixed_commands_and_sql() {
        let script = ".mode json\nSELECT * FROM users;\n.tables";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], ".mode json");
        assert_eq!(stmts[1], "SELECT * FROM users;");
        assert_eq!(stmts[2], ".tables");
    }

    #[test]
    fn test_parse_begin_as_identifier_does_not_glue_statements() {
        // Issue #5816 (keyword1.test): BEGIN is a SQLite fallback keyword, so
        // `begin` is a legal table/column name. An identifier `begin` must not
        // be mistaken for a trigger-body opener — that suppressed semicolon
        // splitting and glued every following statement into one.
        let script = "CREATE TABLE begin(begin begin);             INSERT INTO begin VALUES(99);             INSERT INTO begin SELECT a FROM t1;             SELECT * FROM begin ORDER BY begin ASC;";
        let stmts = parse_statements(script);
        assert_eq!(
            stmts.len(),
            4,
            "identifier `begin` must not suppress statement splitting, got: {:?}",
            stmts
        );
        assert_eq!(stmts[0], "CREATE TABLE begin(begin begin);");
        assert_eq!(stmts[1], "INSERT INTO begin VALUES(99);");
    }

    #[test]
    fn test_parse_transaction_begin_still_splits() {
        // A transaction BEGIN (statement-position, not inside CREATE
        // TRIGGER/PROCEDURE/FUNCTION) must not open a block either.
        let script = "BEGIN; INSERT INTO t VALUES(1); COMMIT;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 3, "transaction BEGIN must split normally, got: {:?}", stmts);

        // Bare BEGIN followed by a statement keyword (no semicolon glue).
        let script = "BEGIN
TRANSACTION;
INSERT INTO t VALUES(1);";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2, "BEGIN TRANSACTION must split normally, got: {:?}", stmts);
    }

    #[test]
    fn test_parse_trigger_body_simple() {
        // A trigger body with an inner `;` must stay a single statement.
        let script = "CREATE TRIGGER t AFTER INSERT ON tbl BEGIN \
            INSERT INTO log VALUES (1); UPDATE log SET x = 2; END;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1, "trigger body should be one statement");
        assert!(stmts[0].starts_with("CREATE TRIGGER t"));
        assert!(stmts[0].trim_end().ends_with("END;"));
    }

    #[test]
    fn test_parse_trigger_body_with_case_end() {
        // Issue #5468: the END of a CASE...END expression inside a trigger body
        // must not be treated as the trigger's terminating END.
        let script = "CREATE TRIGGER before_tbl_insert BEFORE INSERT ON tbl BEGIN \
            SELECT CASE WHEN (new.a = 4) THEN RAISE(IGNORE) END; END;";
        let stmts = parse_statements(script);
        assert_eq!(
            stmts.len(),
            1,
            "CASE...END inside a trigger body must not split the statement, got: {:?}",
            stmts
        );
        assert!(stmts[0].contains("CASE WHEN"));
        assert!(stmts[0].contains("RAISE(IGNORE)"));
        // The whole CREATE TRIGGER, including its terminating END, is captured.
        assert!(stmts[0].trim_end().ends_with("END;"));
    }

    #[test]
    fn test_parse_case_trigger_followed_by_statements() {
        // A CASE-trigger must split cleanly from statements that follow it.
        let script = "CREATE TABLE tbl(a, b, c); \
            CREATE TRIGGER before_tbl_insert BEFORE INSERT ON tbl BEGIN \
            SELECT CASE WHEN (new.a = 4) THEN RAISE(IGNORE) END; END; \
            INSERT INTO tbl VALUES (1, 2, 3); SELECT * FROM tbl;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 4, "expected 4 statements, got: {:?}", stmts);
        assert!(stmts[0].starts_with("CREATE TABLE tbl"));
        assert!(stmts[1].starts_with("CREATE TRIGGER"));
        assert!(stmts[1].contains("CASE WHEN"));
        assert!(stmts[1].trim_end().ends_with("END;"));
        assert!(stmts[2].starts_with("INSERT INTO tbl"));
        assert!(stmts[3].starts_with("SELECT * FROM tbl"));
    }

    #[test]
    fn test_parse_trigger_body_with_nested_case() {
        // Multiple / nested CASE...END expressions in one trigger body.
        let script = "CREATE TRIGGER t BEFORE UPDATE ON tbl BEGIN \
            SELECT CASE WHEN a = 1 THEN CASE WHEN b = 2 THEN RAISE(ABORT, 'x') END END; \
            SELECT CASE WHEN c = 3 THEN RAISE(IGNORE) END; END;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1, "nested CASE...END must stay one statement: {:?}", stmts);
        assert!(stmts[0].trim_end().ends_with("END;"));
    }

    #[test]
    fn test_parse_top_level_case_still_splits() {
        // A CASE...END expression OUTSIDE any trigger body must split normally
        // on its trailing semicolon (no false CASE tracking).
        let script = "SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM t; SELECT 2;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CASE WHEN"));
        assert_eq!(stmts[1], "SELECT 2;");
    }
}
