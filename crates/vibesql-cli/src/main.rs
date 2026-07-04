use clap::{Parser, Subcommand};

mod codegen;
mod commands;
mod config;
mod data_io;
mod executor;
mod formatter;
mod repl;
mod script;
mod sqlite_io;
mod util;

use config::Config;
use formatter::OutputFormat;
use repl::Repl;
use script::ScriptExecutor;
use vibesql_l10n::vibe_msg;

#[derive(Parser, Debug)]
#[command(name = "vibesql")]
#[command(version = "0.1.0")]
#[command(about = "VibeSQL - SQL:1999 FULL Compliance Database")]
#[command(long_about = "VibeSQL command-line interface

USAGE MODES:
  Interactive REPL:    vibesql [DATABASE] [OPTIONS]
  Execute Command:     vibesql [DATABASE] -c \"SELECT * FROM users\"
  Execute File:        vibesql [DATABASE] -f script.sql
  Execute from stdin:  cat data.sql | vibesql [DATABASE]
  Generate Types:      vibesql codegen --schema schema.sql --output types.ts

INTERACTIVE REPL:
  When started without -c, -f, or piped input, VibeSQL enters an interactive
  REPL with readline support, command history, and meta-commands like:
    \\d [table]  - Describe table or list all tables
    \\dt         - List tables
    \\f <format> - Set output format
    \\copy       - Import/export CSV/JSON
    \\help       - Show all REPL commands

SUBCOMMANDS:
  codegen           Generate TypeScript types from database schema

CONFIGURATION:
  Settings can be configured in ~/.vibesqlrc (TOML format):
    [display]
    format = \"table\"              # Default output format

    [database]
    default_path = \"~/data.db\"    # Default database file
    auto_save = true               # Auto-save on exit
    wal = true                     # Write-Ahead Log durability (default on; set false to opt out)

    [history]
    file = \"~/.vibesql_history\"   # Command history file
    max_entries = 10000            # Max history entries

    [query]
    timeout_seconds = 0            # Query timeout (0 = no limit)

WRITE-AHEAD LOG (WAL) DURABILITY (on by default):
  For a file-backed database the CLI keeps a Write-Ahead Log so committed
  changes survive an unclean shutdown (crash, SIGKILL, power loss). When
  active for 'mydata.vbsql', it maintains two sibling files next to the
  database:
    mydata.wal              # active write-ahead log
    mydata-checkpoints/     # checkpoint archive (checkpoint_*.vchk)
  On open the CLI recovers from the latest checkpoint and replays the WAL;
  on \\save / clean exit it writes a checkpoint and truncates the WAL.

  Both table schemas (DDL) and committed row data (DML inserts, updates,
  and deletes) are restored on recovery; uncommitted transactions at crash
  time are discarded. Set [database] wal = false to opt out and use the
  snapshot-only path instead (no WAL sibling files).

EXAMPLES:
  # Start interactive REPL with in-memory database
  vibesql

  # Use persistent database file (positional argument)
  vibesql mydata.db

  # Use persistent database file (flag)
  vibesql --database mydata.db

  # Execute single command
  vibesql mydata.db -c \"SELECT * FROM users\"

  # Run SQL script file
  vibesql mydata.db -f schema.sql -v

  # Import data from CSV
  echo \"\\\\copy users FROM 'data.csv'\" | vibesql mydata.db

  # Export query results as JSON
  vibesql mydata.db -c \"SELECT * FROM users\" --format json

  # Generate TypeScript types from a schema file
  vibesql codegen --schema schema.sql --output src/types.ts

  # Generate TypeScript types from a running database
  vibesql codegen --database mydata.db --output src/types.ts")]
struct Args {
    /// Database file path (positional argument, optional)
    #[arg(value_name = "DATABASE")]
    database_positional: Option<String>,

    /// Database file path (if not specified, uses in-memory database)
    #[arg(short, long, value_name = "FILE", global = true)]
    database: Option<String>,

    /// Execute SQL commands from file
    #[arg(short, long, value_name = "FILE")]
    file: Option<String>,

    /// Execute SQL command directly and exit
    #[arg(short, long, value_name = "SQL")]
    command: Option<String>,

    /// Read SQL commands from stdin (auto-detected when piped)
    #[arg(long)]
    stdin: bool,

    /// Show detailed output during file/stdin execution
    #[arg(short, long)]
    verbose: bool,

    /// Output format for query results
    #[arg(long, value_parser = ["table", "json", "csv", "markdown", "html", "raw"], value_name = "FORMAT")]
    format: Option<String>,

    /// Set the display language (e.g., en-US, es, ja)
    #[arg(long, value_name = "LOCALE", global = true)]
    lang: Option<String>,

    /// If the newest checkpoint is unreadable, recover from the newest
    /// readable older checkpoint instead of refusing to open. Skipped
    /// checkpoints are reported on stderr; changes committed after the loaded
    /// checkpoint may be missing. (Never applies to version mismatches: a
    /// database written by a newer VibeSQL always requires a newer binary.)
    #[arg(long)]
    recover_fallback: bool,

    #[command(subcommand)]
    subcommand: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Generate TypeScript type definitions from database schema
    #[command(
        about = "Generate TypeScript types from database schema",
        long_about = "Generate TypeScript type definitions from a VibeSQL database schema.

This command creates TypeScript interfaces for all tables in the database,
along with metadata objects for runtime type checking and IDE support.

INPUT SOURCES:
  --database <FILE>  Generate from an existing database file
  --schema <FILE>    Generate from a SQL schema file (CREATE TABLE statements)

OUTPUT:
  --output <FILE>    Write generated types to this file (default: types.ts)

OPTIONS:
  --camel-case       Convert column names to camelCase
  --no-metadata      Skip generating the tables metadata object

EXAMPLES:
  # From a database file
  vibesql codegen --database mydata.db --output src/db/types.ts

  # From a SQL schema file
  vibesql codegen --schema schema.sql --output src/db/types.ts

  # With camelCase property names
  vibesql codegen --schema schema.sql --output types.ts --camel-case"
    )]
    Codegen {
        /// SQL schema file containing CREATE TABLE statements
        #[arg(short, long, value_name = "FILE")]
        schema: Option<String>,

        /// Output file path for generated TypeScript
        #[arg(short, long, value_name = "FILE", default_value = "types.ts")]
        output: String,

        /// Convert column names to camelCase
        #[arg(long)]
        camel_case: bool,

        /// Skip generating table metadata object
        #[arg(long)]
        no_metadata: bool,
    },

    /// Import a SQLite database into VibeSQL format
    #[command(about = "Import a SQLite .db file into VibeSQL")]
    Import {
        /// Path to the SQLite .db file to import
        #[arg(value_name = "INPUT")]
        input: String,

        /// Output path for the VibeSQL database (default: replaces .db with .vbsql)
        #[arg(value_name = "OUTPUT")]
        output: Option<String>,
    },

    /// Export a VibeSQL database to SQLite format
    #[command(about = "Export a VibeSQL database to a SQLite .db file")]
    Export {
        /// Path to the VibeSQL database to export
        #[arg(value_name = "INPUT")]
        input: String,

        /// Output path for the SQLite .db file
        #[arg(value_name = "OUTPUT")]
        output: String,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize localization system
    if let Err(e) = vibesql_l10n::init(args.lang.as_deref()) {
        eprintln!("Warning: Failed to initialize localization: {}", e);
    }

    // Resolve database path: positional arg takes precedence over -d flag
    // Error if both are provided with different values
    let database_arg = resolve_database_arg(&args.database_positional, &args.database)?;

    // Handle subcommands first
    if let Some(cmd) = args.subcommand {
        return match cmd {
            Commands::Codegen { schema, output, camel_case, no_metadata } => {
                run_codegen(database_arg, schema, output, camel_case, no_metadata)
            }
            Commands::Import { input, output } => run_import(&input, output.as_deref()),
            Commands::Export { input, output } => run_export(&input, &output),
        };
    }

    // Load configuration from ~/.vibesqlrc
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!("{}", vibe_msg!("warning-config-load", error = e.to_string()));
        Config::default()
    });

    // Use command-line format if provided, otherwise use config default
    let format =
        args.format.as_deref().and_then(parse_format).or_else(|| config.get_output_format());

    // Use command-line database if provided, otherwise use config default
    let database = database_arg.or(config.database.default_path.clone());

    // WAL durability ([database] wal = true, the default). Committed DDL + DML
    // survive an unclean shutdown for file-backed databases. Set wal = false to
    // opt out and fall back to the snapshot-only path. --recover-fallback opts
    // into older-checkpoint recovery when the newest checkpoint is unreadable
    // (issue #5807: never a silent fallback).
    let open_options = executor::DbOpenOptions {
        wal: config.database.wal,
        recover_fallback: args.recover_fallback,
    };

    if let Some(cmd) = args.command {
        // Execute command mode
        execute_command(&cmd, database, format, open_options)?;
    } else if let Some(file_path) = args.file {
        // Execute file mode
        execute_file(&file_path, database, args.verbose, format, open_options)?;
    } else if args.stdin || is_stdin_piped() {
        // Execute from stdin
        execute_stdin(database, args.verbose, format, open_options)?;
    } else {
        // Interactive REPL mode
        let mut repl = Repl::new(database, format, open_options)?;
        repl.run()?;
    }

    Ok(())
}

fn run_import(input: &str, output: Option<&str>) -> anyhow::Result<()> {
    let result = sqlite_io::import_sqlite(input)?;

    for warning in &result.warnings {
        eprintln!("{}", warning);
    }

    // Determine output path
    let output_path = match output {
        Some(p) => p.to_string(),
        None => {
            let p = std::path::Path::new(input);
            p.with_extension("vbsql").to_string_lossy().to_string()
        }
    };

    // Save as VibeSQL binary format
    result
        .database
        .save(&output_path)
        .map_err(|e| anyhow::anyhow!("Failed to save database: {}", e))?;

    println!(
        "Imported {} tables ({} rows) from {}",
        result.tables_imported, result.rows_imported, input
    );
    if result.tables_skipped > 0 {
        println!("Skipped {} tables", result.tables_skipped);
    }
    println!("Saved to {}", output_path);
    Ok(())
}

fn run_export(input: &str, output: &str) -> anyhow::Result<()> {
    // Load the VibeSQL database. A forward-version error (file written by a
    // newer VibeSQL binary) is fatal and must not be masked by the SQL-dump
    // fallback (issue #5807).
    let db = match vibesql_storage::Database::load(input) {
        Ok(db) => db,
        Err(e @ vibesql_storage::StorageError::UnsupportedFormatVersion { .. }) => {
            return Err(anyhow::anyhow!("Failed to open database at {}: {}", input, e));
        }
        Err(_) => vibesql_executor::load_sql_dump(input)
            .map_err(|e| anyhow::anyhow!("Failed to load database: {}", e))?,
    };

    let result = sqlite_io::export_sqlite(&db, output)?;

    for warning in &result.warnings {
        eprintln!("{}", warning);
    }

    println!(
        "Exported {} tables ({} rows) to {}",
        result.tables_exported, result.rows_exported, output
    );
    Ok(())
}

fn run_codegen(
    database: Option<String>,
    schema: Option<String>,
    output: String,
    camel_case: bool,
    no_metadata: bool,
) -> anyhow::Result<()> {
    let config = codegen::CodegenConfig {
        output: output.clone(),
        include_metadata: !no_metadata,
        camel_case,
    };

    let typescript = if let Some(schema_path) = schema {
        // Generate from schema file
        println!("{}", vibe_msg!("codegen-from-schema", path = schema_path.as_str()));
        codegen::generate_from_schema_file(&schema_path, &config)?
    } else if let Some(db_path) = database {
        // Generate from database file
        println!("{}", vibe_msg!("codegen-from-database", path = db_path.as_str()));
        let db = vibesql_executor::load_sql_dump(&db_path).map_err(|e| {
            anyhow::anyhow!("{}", vibe_msg!("database-load-error", error = e.to_string()))
        })?;
        codegen::generate_from_database(&db, &config)?
    } else {
        return Err(anyhow::anyhow!("{}", vibe_msg!("codegen-error-no-source")));
    };

    // Write to output file
    codegen::write_to_file(&typescript, &output)?;
    println!("{}", vibe_msg!("codegen-written", path = output.as_str()));

    Ok(())
}

/// Resolve database path from positional argument and -d flag.
/// Returns error if both are provided with different values.
fn resolve_database_arg(
    positional: &Option<String>,
    flag: &Option<String>,
) -> anyhow::Result<Option<String>> {
    match (positional, flag) {
        (Some(pos), Some(flg)) if pos != flg => {
            Err(anyhow::anyhow!(
                "Conflicting database paths: positional argument '{}' and --database '{}'. Use one or the other.",
                pos,
                flg
            ))
        }
        (Some(pos), _) => Ok(Some(pos.clone())),
        (None, Some(flg)) => Ok(Some(flg.clone())),
        (None, None) => Ok(None),
    }
}

fn parse_format(format_str: &str) -> Option<OutputFormat> {
    match format_str {
        "table" => Some(OutputFormat::Table),
        "json" => Some(OutputFormat::Json),
        "csv" => Some(OutputFormat::Csv),
        "markdown" => Some(OutputFormat::Markdown),
        "html" => Some(OutputFormat::Html),
        "raw" => Some(OutputFormat::Raw),
        _ => None,
    }
}

fn execute_command(
    cmd: &str,
    database: Option<String>,
    format: Option<OutputFormat>,
    options: executor::DbOpenOptions,
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, false, format, options)?;
    executor.execute_script(cmd)?;
    Ok(())
}

fn execute_file(
    path: &str,
    database: Option<String>,
    verbose: bool,
    format: Option<OutputFormat>,
    options: executor::DbOpenOptions,
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, verbose, format, options)?;
    executor.execute_file(path)?;
    Ok(())
}

fn execute_stdin(
    database: Option<String>,
    verbose: bool,
    format: Option<OutputFormat>,
    options: executor::DbOpenOptions,
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, verbose, format, options)?;
    executor.execute_stdin()?;
    Ok(())
}

fn is_stdin_piped() -> bool {
    // Check if stdin is a pipe/file (not a terminal)
    !atty::is(atty::Stream::Stdin)
}
