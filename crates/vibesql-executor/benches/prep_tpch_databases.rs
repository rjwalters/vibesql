//! TPC-H Database Preparation Tool
//!
//! Creates pre-built TPC-H database files for CLI benchmarks.
//! Uses the fast Rust data generators to create database files that can be
//! used by the CLI benchmark script for fair apples-to-apples comparisons.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run
//! cargo bench --package vibesql-executor --bench prep_tpch_databases --features sqlite --no-run
//! ./target/release/deps/prep_tpch_databases-* [OPTIONS]
//!
//! # Examples
//! ./target/release/deps/prep_tpch_databases-* --output /tmp/tpch_bench
//! ./target/release/deps/prep_tpch_databases-* --scale 0.1 --output /tmp/tpch_bench
//! ```
//!
//! ## Output
//!
//! Creates database files:
//! - `{output}/tpch_sf{scale}.vbsql` - VibeSQL database
//! - `{output}/tpch_sf{scale}.sqlite` - SQLite database (if --sqlite flag used)

mod harness;
mod tpch;

use std::env;
use std::fs;
use std::path::PathBuf;

#[cfg(feature = "sqlite")]
use tpch::schema::load_sqlite_to_file;
use tpch::schema::load_vibesql;

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut scale_factor = 0.01;
    let mut output_dir = PathBuf::from("/tmp/tpch_bench");
    let mut create_sqlite = false;

    // Parse arguments
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--scale" | "-s" => {
                scale_factor = args[i + 1].parse().expect("Invalid scale factor");
                i += 2;
            }
            "--output" | "-o" => {
                output_dir = PathBuf::from(&args[i + 1]);
                i += 2;
            }
            "--sqlite" => {
                create_sqlite = true;
                i += 1;
            }
            "--help" | "-h" => {
                println!("TPC-H Database Preparation Tool");
                println!();
                println!("Creates pre-built database files for CLI benchmarks.");
                println!();
                println!("Usage: prep_tpch_databases [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -s, --scale FACTOR  Scale factor (default: 0.01)");
                println!("  -o, --output DIR    Output directory (default: /tmp/tpch_bench)");
                println!("  --sqlite            Also create SQLite database");
                println!("  -h, --help          Show this help");
                println!();
                println!("Examples:");
                println!("  ./prep_tpch_databases");
                println!("  ./prep_tpch_databases --scale 0.1 --output /tmp/bench");
                println!("  ./prep_tpch_databases --sqlite");
                return;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                std::process::exit(1);
            }
        }
    }

    // Create output directory
    fs::create_dir_all(&output_dir).expect("Failed to create output directory");

    println!("TPC-H Database Preparation Tool");
    println!("================================");
    println!("Scale Factor: {}", scale_factor);
    println!("Output Dir: {}", output_dir.display());
    println!();

    // Create VibeSQL database
    let vibesql_path = output_dir.join(format!("tpch_sf{}.vbsql", scale_factor));
    println!("Creating VibeSQL database: {}", vibesql_path.display());
    let db = load_vibesql(scale_factor);
    db.save_binary(&vibesql_path).expect("Failed to save VibeSQL database");
    println!("  Saved!");

    // Create SQLite database if requested
    #[cfg(feature = "sqlite")]
    if create_sqlite {
        let sqlite_path = output_dir.join(format!("tpch_sf{}.sqlite", scale_factor));
        println!("Creating SQLite database: {}", sqlite_path.display());

        // Load directly to file (includes ANALYZE)
        let _conn = load_sqlite_to_file(scale_factor, &sqlite_path);
        println!("  Saved!");
    }

    #[cfg(not(feature = "sqlite"))]
    if create_sqlite {
        eprintln!("Warning: SQLite support not compiled. Use --features sqlite");
    }

    println!();
    println!("Database files ready for CLI benchmarks!");
    println!();
    println!("Run benchmarks with:");
    println!("  ./scripts/bench-cli --db-dir {} --scale {}", output_dir.display(), scale_factor);
}
