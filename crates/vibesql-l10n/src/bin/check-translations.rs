//! Translation Completeness Checker
//!
//! This binary validates that all locales have complete translations compared to en-US.
//! It checks for:
//! - Missing message IDs in non-English locales
//! - Missing .ftl files in non-English locales
//! - Extra/obsolete messages not in en-US (warnings)
//!
//! Usage:
//!     cargo run -p vibesql-l10n --bin check-translations
//!     cargo run -p vibesql-l10n --bin check-translations -- --verbose
//!     cargo run -p vibesql-l10n --bin check-translations -- --locale es
//!     cargo run -p vibesql-l10n --bin check-translations -- --json
//!
//! Exit codes:
//!     0 - All translations complete
//!     1 - Missing translations found

use fluent_syntax::parser::parse;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// ANSI color codes for terminal output
mod colors {
    pub const RED: &str = "\x1b[31m";
    pub const GREEN: &str = "\x1b[32m";
    pub const YELLOW: &str = "\x1b[33m";
    pub const CYAN: &str = "\x1b[36m";
    pub const BOLD: &str = "\x1b[1m";
    pub const RESET: &str = "\x1b[0m";
}

/// Command-line arguments
struct Args {
    verbose: bool,
    json: bool,
    locale: Option<String>,
    strict: bool,
}

impl Args {
    fn parse() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut verbose = false;
        let mut json = false;
        let mut locale = None;
        let mut strict = false;

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--verbose" | "-v" => verbose = true,
                "--json" => json = true,
                "--strict" => strict = true,
                "--locale" | "-l" => {
                    i += 1;
                    if i < args.len() {
                        locale = Some(args[i].clone());
                    }
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => {}
            }
            i += 1;
        }

        Self {
            verbose,
            json,
            locale,
            strict,
        }
    }
}

fn print_help() {
    println!(
        r#"Translation Completeness Checker

USAGE:
    cargo run -p vibesql-l10n --bin check-translations [OPTIONS]

OPTIONS:
    -v, --verbose    Show all messages, including present ones
    -l, --locale     Check only a specific locale (e.g., --locale es)
        --json       Output results as JSON
        --strict     Treat warnings (extra messages) as errors
    -h, --help       Print this help message

EXAMPLES:
    # Check all locales
    cargo run -p vibesql-l10n --bin check-translations

    # Check Spanish translations only
    cargo run -p vibesql-l10n --bin check-translations -- --locale es

    # Verbose output showing all messages
    cargo run -p vibesql-l10n --bin check-translations -- --verbose

    # Output as JSON for CI integration
    cargo run -p vibesql-l10n --bin check-translations -- --json
"#
    );
}

/// Result for a single locale
#[derive(Default)]
struct LocaleResult {
    locale: String,
    missing_files: Vec<String>,
    missing_messages: BTreeMap<String, Vec<String>>, // file -> [message_ids]
    extra_messages: BTreeMap<String, Vec<String>>,   // file -> [message_ids]
    total_expected: usize,
    total_present: usize,
}

impl LocaleResult {
    fn coverage_percent(&self) -> f64 {
        if self.total_expected == 0 {
            100.0
        } else {
            (self.total_present as f64 / self.total_expected as f64) * 100.0
        }
    }

    fn has_issues(&self) -> bool {
        !self.missing_files.is_empty()
            || self.missing_messages.values().any(|v| !v.is_empty())
    }

    fn has_warnings(&self) -> bool {
        self.extra_messages.values().any(|v| !v.is_empty())
    }
}

/// Find the resources directory relative to the crate
fn find_resources_dir() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("crates/vibesql-l10n/resources"),
        PathBuf::from("resources"),
        PathBuf::from("../../../crates/vibesql-l10n/resources"),
    ];

    candidates.into_iter().find(|c| c.is_dir())
}

/// Get all locale directories in the resources directory
fn get_locales(resources_dir: &Path) -> Vec<String> {
    let mut locales = Vec::new();

    if let Ok(entries) = std::fs::read_dir(resources_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    locales.push(name.to_string_lossy().to_string());
                }
            }
        }
    }

    locales.sort();
    locales
}

/// Get all .ftl files in a directory (returns just filenames)
fn get_ftl_files(dir: &Path) -> Vec<String> {
    let mut files = Vec::new();

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "ftl") {
                if let Some(name) = path.file_name() {
                    files.push(name.to_string_lossy().to_string());
                }
            }
        }
    }

    files.sort();
    files
}

/// Extract all message IDs from a .ftl file
fn extract_message_ids(path: &Path) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();

    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return ids,
    };

    let resource = match parse(content.as_str()) {
        Ok(r) => r,
        Err((r, _errors)) => r,
    };

    for entry in &resource.body {
        match entry {
            fluent_syntax::ast::Entry::Message(msg) => {
                ids.insert(msg.id.name.to_string());
            }
            fluent_syntax::ast::Entry::Term(term) => {
                ids.insert(format!("-{}", term.id.name));
            }
            _ => {}
        }
    }

    ids
}

/// Check a single locale against en-US
fn check_locale(
    resources_dir: &Path,
    locale: &str,
    en_us_messages: &BTreeMap<String, BTreeSet<String>>,
) -> LocaleResult {
    let mut result = LocaleResult {
        locale: locale.to_string(),
        ..Default::default()
    };

    let locale_dir = resources_dir.join(locale);

    // Check for missing files
    for file in en_us_messages.keys() {
        let file_path = locale_dir.join(file);
        if !file_path.exists() {
            result.missing_files.push(file.clone());
            // All messages in this file are missing
            if let Some(msgs) = en_us_messages.get(file) {
                result
                    .missing_messages
                    .insert(file.clone(), msgs.iter().cloned().collect());
                result.total_expected += msgs.len();
            }
        } else {
            // File exists, check for missing messages
            let locale_ids = extract_message_ids(&file_path);
            if let Some(en_ids) = en_us_messages.get(file) {
                result.total_expected += en_ids.len();

                let missing: Vec<String> = en_ids.difference(&locale_ids).cloned().collect();
                let extra: Vec<String> = locale_ids.difference(en_ids).cloned().collect();

                result.total_present += en_ids.len() - missing.len();

                if !missing.is_empty() {
                    result.missing_messages.insert(file.clone(), missing);
                }
                if !extra.is_empty() {
                    result.extra_messages.insert(file.clone(), extra);
                }
            }
        }
    }

    result
}

/// Print results in human-readable format
fn print_results(results: &[LocaleResult], verbose: bool) {
    println!(
        "\n{}Translation Completeness Report{}",
        colors::BOLD,
        colors::RESET
    );
    println!("================================\n");

    for result in results {
        let coverage = result.coverage_percent();
        let color = if coverage >= 100.0 {
            colors::GREEN
        } else if coverage >= 80.0 {
            colors::YELLOW
        } else {
            colors::RED
        };

        println!(
            "{}{}{}  {:.1}% ({}/{} messages)",
            color,
            result.locale,
            colors::RESET,
            coverage,
            result.total_present,
            result.total_expected
        );

        // Missing files
        if !result.missing_files.is_empty() {
            println!(
                "  {}Missing files:{}",
                colors::RED,
                colors::RESET
            );
            for file in &result.missing_files {
                println!("    - {}", file);
            }
        }

        // Missing messages
        for (file, messages) in &result.missing_messages {
            if !messages.is_empty() && !result.missing_files.contains(file) {
                println!(
                    "  {}Missing in {}:{} ({} messages)",
                    colors::RED,
                    file,
                    colors::RESET,
                    messages.len()
                );
                if verbose {
                    for msg in messages {
                        println!("    - {}", msg);
                    }
                }
            }
        }

        // Extra messages (warnings)
        if result.has_warnings() {
            for (file, messages) in &result.extra_messages {
                if !messages.is_empty() {
                    println!(
                        "  {}Extra in {}:{} ({} messages, not in en-US)",
                        colors::YELLOW,
                        file,
                        colors::RESET,
                        messages.len()
                    );
                    if verbose {
                        for msg in messages {
                            println!("    - {}", msg);
                        }
                    }
                }
            }
        }

        println!();
    }
}

/// Print results in JSON format
fn print_json(results: &[LocaleResult]) {
    println!("{{");
    println!("  \"locales\": [");
    for (i, result) in results.iter().enumerate() {
        println!("    {{");
        println!("      \"locale\": \"{}\",", result.locale);
        println!("      \"coverage\": {:.2},", result.coverage_percent());
        println!("      \"total_expected\": {},", result.total_expected);
        println!("      \"total_present\": {},", result.total_present);
        println!("      \"missing_files\": {:?},", result.missing_files);

        // Missing messages
        println!("      \"missing_messages\": {{");
        let missing_entries: Vec<_> = result
            .missing_messages
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();
        for (j, (file, messages)) in missing_entries.iter().enumerate() {
            let comma = if j < missing_entries.len() - 1 { "," } else { "" };
            println!("        \"{}\": {:?}{}", file, messages, comma);
        }
        println!("      }},");

        // Extra messages
        println!("      \"extra_messages\": {{");
        let extra_entries: Vec<_> = result
            .extra_messages
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();
        for (j, (file, messages)) in extra_entries.iter().enumerate() {
            let comma = if j < extra_entries.len() - 1 { "," } else { "" };
            println!("        \"{}\": {:?}{}", file, messages, comma);
        }
        println!("      }}");

        let comma = if i < results.len() - 1 { "," } else { "" };
        println!("    }}{}", comma);
    }
    println!("  ]");
    println!("}}");
}

fn main() -> ExitCode {
    let args = Args::parse();

    // Find resources directory
    let resources_dir = match find_resources_dir() {
        Some(dir) => dir,
        None => {
            eprintln!(
                "{}Error: Could not find resources directory{}",
                colors::RED,
                colors::RESET
            );
            eprintln!("Run from workspace root or crate directory.");
            return ExitCode::FAILURE;
        }
    };

    if !args.json {
        println!(
            "{}Checking translation completeness...{}",
            colors::CYAN,
            colors::RESET
        );
        println!("Resources directory: {}\n", resources_dir.display());
    }

    // Get en-US as the source of truth
    let en_us_dir = resources_dir.join("en-US");
    if !en_us_dir.exists() {
        eprintln!(
            "{}Error: en-US locale directory not found{}",
            colors::RED,
            colors::RESET
        );
        return ExitCode::FAILURE;
    }

    // Build en-US message catalog
    let mut en_us_messages: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for file in get_ftl_files(&en_us_dir) {
        let ids = extract_message_ids(&en_us_dir.join(&file));
        en_us_messages.insert(file, ids);
    }

    let total_en_messages: usize = en_us_messages.values().map(|s| s.len()).sum();
    if !args.json {
        println!(
            "Reference locale (en-US): {} files, {} messages\n",
            en_us_messages.len(),
            total_en_messages
        );
    }

    // Get all locales to check
    let locales: Vec<String> = if let Some(ref locale) = args.locale {
        vec![locale.clone()]
    } else {
        get_locales(&resources_dir)
            .into_iter()
            .filter(|l| l != "en-US")
            .collect()
    };

    // Check each locale
    let mut results: Vec<LocaleResult> = Vec::new();
    for locale in &locales {
        let result = check_locale(&resources_dir, locale, &en_us_messages);
        results.push(result);
    }

    // Output results
    if args.json {
        print_json(&results);
    } else {
        print_results(&results, args.verbose);

        // Summary
        println!("{}Summary{}", colors::BOLD, colors::RESET);
        println!("=======");

        let complete_count = results.iter().filter(|r| !r.has_issues()).count();
        let total_missing: usize = results
            .iter()
            .flat_map(|r| r.missing_messages.values())
            .map(|v| v.len())
            .sum();

        println!("Locales checked: {}", results.len());
        println!(
            "Complete locales: {}{}/{}{}",
            if complete_count == results.len() {
                colors::GREEN
            } else {
                colors::YELLOW
            },
            complete_count,
            results.len(),
            colors::RESET
        );
        println!(
            "Total missing messages: {}{}{}",
            if total_missing > 0 {
                colors::RED
            } else {
                colors::GREEN
            },
            total_missing,
            colors::RESET
        );
    }

    // Determine exit code
    let has_missing = results.iter().any(|r| r.has_issues());
    let has_extra = results.iter().any(|r| r.has_warnings());

    if has_missing || (args.strict && has_extra) {
        if !args.json {
            println!(
                "\n{}INCOMPLETE: Missing translations found{}",
                colors::RED,
                colors::RESET
            );
        }
        ExitCode::FAILURE
    } else {
        if !args.json {
            println!(
                "\n{}COMPLETE: All translations present{}",
                colors::GREEN,
                colors::RESET
            );
        }
        ExitCode::SUCCESS
    }
}
