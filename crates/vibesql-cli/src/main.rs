use clap::{Parser, Subcommand};

mod codegen;
mod commands;
mod config;
mod data_io;
mod executor;
mod formatter;
mod repl;
mod script;
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

    [history]
    file = \"~/.vibesql_history\"   # Command history file
    max_entries = 10000            # Max history entries

    [query]
    timeout_seconds = 0            # Query timeout (0 = no limit)

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

    if let Some(cmd) = args.command {
        // Execute command mode
        execute_command(&cmd, database, format)?;
    } else if let Some(file_path) = args.file {
        // Execute file mode
        execute_file(&file_path, database, args.verbose, format)?;
    } else if args.stdin || is_stdin_piped() {
        // Execute from stdin
        execute_stdin(database, args.verbose, format)?;
    } else {
        // Interactive REPL mode
        let mut repl = Repl::new(database, format)?;
        repl.run()?;
    }

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
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, false, format)?;
    executor.execute_script(cmd)?;
    Ok(())
}

fn execute_file(
    path: &str,
    database: Option<String>,
    verbose: bool,
    format: Option<OutputFormat>,
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, verbose, format)?;
    executor.execute_file(path)?;
    Ok(())
}

fn execute_stdin(
    database: Option<String>,
    verbose: bool,
    format: Option<OutputFormat>,
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, verbose, format)?;
    executor.execute_stdin()?;
    Ok(())
}

fn is_stdin_piped() -> bool {
    // Check if stdin is a pipe/file (not a terminal)
    !atty::is(atty::Stream::Stdin)
}
