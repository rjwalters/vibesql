use std::time::SystemTime;

use rustyline::{error::ReadlineError, DefaultEditor};
use vibesql_l10n::vibe_msg;

use crate::{
    commands::MetaCommand,
    executor::SqlExecutor,
    formatter::{OutputFormat, ResultFormatter},
    util::is_memory_database,
};

#[derive(Debug, Clone)]
struct ErrorEntry {
    timestamp: SystemTime,
    message: String,
}

pub struct Repl {
    executor: SqlExecutor,
    editor: DefaultEditor,
    formatter: ResultFormatter,
    database_path: Option<String>,
    error_history: Vec<ErrorEntry>,
    has_modifications: bool,
}

impl Repl {
    /// Construct a REPL.
    ///
    /// `wal` activates the opt-in WAL persistence path for file-backed
    /// databases (see `SqlExecutor::new_with_wal`); `false` preserves the
    /// default snapshot-on-exit behavior.
    pub fn new(
        database: Option<String>,
        format: Option<OutputFormat>,
        wal: bool,
    ) -> anyhow::Result<Self> {
        // Treat :memory: as an in-memory database (no file path for saving)
        let database_path = database.as_ref().filter(|p| !is_memory_database(p)).cloned();
        let executor = SqlExecutor::new_with_wal(database, wal)?;
        let editor = DefaultEditor::new()?;
        let mut formatter = ResultFormatter::new();

        if let Some(fmt) = format {
            formatter.set_format(fmt);
        }

        Ok(Repl {
            executor,
            editor,
            formatter,
            database_path,
            error_history: Vec::new(),
            has_modifications: false,
        })
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        self.print_banner();

        loop {
            let prompt = "vibesql> ";
            match self.editor.readline(prompt) {
                Ok(line) => {
                    if line.trim().is_empty() {
                        continue;
                    }

                    // Add to history
                    let _ = self.editor.add_history_entry(line.as_str());

                    // Check if it's a meta-command
                    if let Some(meta_cmd) = MetaCommand::parse(&line) {
                        match self.handle_meta_command(meta_cmd) {
                            Ok(should_exit) => {
                                if should_exit {
                                    break;
                                }
                            }
                            Err(e) => {
                                let error_msg = format!("{}", e);
                                eprintln!("Error: {}", error_msg);
                                self.track_error(error_msg);
                            }
                        }
                    } else {
                        // Execute as SQL
                        match self.executor.execute(&line) {
                            Ok(result) => {
                                self.formatter.print_result(&result);

                                // Auto-save if database path is provided and this was a
                                // modification.
                                //
                                // CRITICAL: Skip auto-save while a transaction is open.
                                // Persisting uncommitted changes turns ROLLBACK into a
                                // no-op across sessions — the .vbsql dump captures the
                                // mid-transaction state and the next process loads it
                                // as committed. Also save on the COMMIT/ROLLBACK
                                // boundary so the final state is durable.
                                if let Some(ref path) = self.database_path {
                                    let in_txn = self.executor.in_transaction();
                                    let should_save = is_modification_statement(&line) && !in_txn;
                                    let upper = line.trim().to_uppercase();
                                    let is_txn_end = !in_txn
                                        && (upper.starts_with("COMMIT")
                                            || upper.starts_with("ROLLBACK")
                                            || upper.starts_with("END"));
                                    if should_save || is_txn_end {
                                        self.has_modifications = true;
                                        if let Err(e) = self.executor.save_database(path) {
                                            eprintln!(
                                                "{}",
                                                vibe_msg!(
                                                    "warning-auto-save-failed",
                                                    error = e.to_string()
                                                )
                                            );
                                        }
                                    } else if is_modification_statement(&line) {
                                        // Mark dirty so the exit-time save runs.
                                        self.has_modifications = true;
                                    }
                                }
                            }
                            Err(e) => {
                                let error_msg = format!("{}", e);
                                eprintln!("Error: {}", error_msg);
                                self.track_error(error_msg);
                            }
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("\\quit");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }

        // Save database on exit if modifications occurred
        if self.has_modifications {
            if let Some(ref path) = self.database_path {
                if let Err(e) = self.executor.save_database(path) {
                    eprintln!(
                        "{}",
                        vibe_msg!("warning-save-on-exit-failed", error = e.to_string())
                    );
                }
            }
        }

        self.print_goodbye();
        Ok(())
    }

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
                let format_name = match format {
                    crate::formatter::OutputFormat::Table => "table",
                    crate::formatter::OutputFormat::Json => "json",
                    crate::formatter::OutputFormat::Csv => "csv",
                    crate::formatter::OutputFormat::Markdown => "markdown",
                    crate::formatter::OutputFormat::Html => "html",
                    crate::formatter::OutputFormat::Raw => "raw",
                };
                println!("{}", vibe_msg!("format-changed", format = format_name));
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
                self.print_error_history();
            }
        }
        Ok(false)
    }

    fn print_banner(&self) {
        println!("{}", vibe_msg!("cli-banner", version = "0.1.0"));
        println!("{}\n", vibe_msg!("cli-help-hint"));
    }

    fn print_goodbye(&self) {
        println!("{}", vibe_msg!("cli-goodbye"));
    }

    fn track_error(&mut self, error_msg: String) {
        const MAX_ERROR_HISTORY: usize = 50;

        self.error_history.push(ErrorEntry { timestamp: SystemTime::now(), message: error_msg });

        // Keep only the last MAX_ERROR_HISTORY errors
        if self.error_history.len() > MAX_ERROR_HISTORY {
            self.error_history.remove(0);
        }
    }

    fn print_error_history(&self) {
        if self.error_history.is_empty() {
            println!("{}", vibe_msg!("no-errors"));
            return;
        }

        println!("{}", vibe_msg!("recent-errors"));
        for (idx, entry) in self.error_history.iter().enumerate() {
            let duration =
                entry.timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
            let secs = duration.as_secs();
            let time_str =
                format!("{:02}:{:02}:{:02}", (secs / 3600) % 24, (secs / 60) % 60, secs % 60);
            println!("{}. [{}] {}", idx + 1, time_str, entry.message);
        }
    }

    fn print_help(&self) {
        println!(
            "
Meta-commands (PostgreSQL-style):
  \\d [table]      - Describe table or list all tables
  \\dt             - List tables
  \\ds             - List schemas
  \\di             - List indexes
  \\du             - List roles/users
  \\f <format>     - Set output format (table, json, csv, markdown, html)
  \\timing         - Toggle query timing
  \\copy <table> TO <file>   - Export table to CSV/JSON file
  \\copy <table> FROM <file> - Import CSV file into table
  \\save [file]    - Save database to SQL dump file
  \\errors         - Show recent error history
  \\h, \\help      - Show this help
  \\q, \\quit      - Exit

Dot-commands (SQLite-style):
  .tables         - List tables
  .schema [table] - Show table schema or list tables
  .indexes        - List indexes
  .databases      - List schemas
  .mode <format>  - Set output format (table, json, csv, markdown, html)
  .timer          - Toggle query timing
  .import FILE TABLE - Import data from file
  .save [file]    - Save database
  .quit, .exit    - Exit

SQL Introspection:
  SHOW TABLES                  - List all tables
  SHOW DATABASES               - List all schemas/databases
  SHOW COLUMNS FROM <table>    - Show table columns
  SHOW INDEX FROM <table>      - Show table indexes
  SHOW CREATE TABLE <table>    - Show CREATE TABLE statement
  DESCRIBE <table>             - Alias for SHOW COLUMNS

Examples:
  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
  INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
  SELECT * FROM users;
  .tables
  .schema users
  .mode json
  \\f markdown
  \\copy users TO '/tmp/users.csv'
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
