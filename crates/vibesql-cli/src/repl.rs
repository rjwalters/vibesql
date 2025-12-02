use rustyline::{error::ReadlineError, DefaultEditor};
use std::time::SystemTime;

use crate::{
    commands::MetaCommand,
    executor::SqlExecutor,
    formatter::{OutputFormat, ResultFormatter},
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
    pub fn new(database: Option<String>, format: Option<OutputFormat>) -> anyhow::Result<Self> {
        let database_path = database.clone();
        let executor = SqlExecutor::new(database)?;
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
                                // modification
                                if let Some(ref path) = self.database_path {
                                    if is_modification_statement(&line) {
                                        self.has_modifications = true;
                                        if let Err(e) = self.executor.save_database(path) {
                                            eprintln!(
                                                "Warning: Failed to auto-save database: {}",
                                                e
                                            );
                                        }
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
                    eprintln!("Warning: Failed to save database on exit: {}", e);
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
                };
                println!("Output format set to: {}", format_name);
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
                        println!("Database saved to: {}", p);
                    }
                    None => {
                        eprintln!("Error: No database file specified. Use \\save <filename> or start with --database flag");
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
        println!("VibeSQL v0.1.0 - SQL:1999 FULL Compliance Database");
        println!("Type \\help for help, \\quit to exit\n");
    }

    fn print_goodbye(&self) {
        println!("Goodbye!");
    }

    fn track_error(&mut self, error_msg: String) {
        const MAX_ERROR_HISTORY: usize = 50;

        self.error_history.push(ErrorEntry {
            timestamp: SystemTime::now(),
            message: error_msg,
        });

        // Keep only the last MAX_ERROR_HISTORY errors
        if self.error_history.len() > MAX_ERROR_HISTORY {
            self.error_history.remove(0);
        }
    }

    fn print_error_history(&self) {
        if self.error_history.is_empty() {
            println!("No errors in this session.");
            return;
        }

        println!("Recent errors:");
        for (idx, entry) in self.error_history.iter().enumerate() {
            let duration = entry.timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = duration.as_secs();
            let time_str = format!(
                "{:02}:{:02}:{:02}",
                (secs / 3600) % 24,
                (secs / 60) % 60,
                secs % 60
            );
            println!("{}. [{}] {}", idx + 1, time_str, entry.message);
        }
    }

    fn print_help(&self) {
        println!(
            "
Meta-commands:
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
  SHOW TABLES;
  SHOW COLUMNS FROM users;
  DESCRIBE users;
  \\f json
  \\f markdown
  \\copy users TO '/tmp/users.csv'
  \\copy users FROM '/tmp/users.csv'
  \\copy users TO '/tmp/users.json'
  \\errors
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
