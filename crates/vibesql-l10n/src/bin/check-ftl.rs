//! Fluent Translation File Syntax Checker
//!
//! This binary validates all `.ftl` files in the vibesql-l10n resources directory.
//! It checks for:
//! - Valid Fluent syntax
//! - Parse errors
//! - Missing default variants in selectors
//!
//! Usage:
//!     cargo run -p vibesql-l10n --bin check-ftl
//!
//! Exit codes:
//!     0 - All files valid
//!     1 - One or more files have errors

use fluent_syntax::parser::parse;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// ANSI color codes for terminal output
mod colors {
    pub const RED: &str = "\x1b[31m";
    pub const GREEN: &str = "\x1b[32m";
    pub const YELLOW: &str = "\x1b[33m";
    pub const RESET: &str = "\x1b[0m";
}

/// Result of checking a single file
struct FileCheckResult {
    path: PathBuf,
    errors: Vec<String>,
    warnings: Vec<String>,
    message_count: usize,
}


/// Find the resources directory relative to the crate
fn find_resources_dir() -> Option<PathBuf> {
    // When running from workspace root
    let candidates = [
        PathBuf::from("crates/vibesql-l10n/resources"),
        // When running from crate directory
        PathBuf::from("resources"),
        // When running from target directory
        PathBuf::from("../../../crates/vibesql-l10n/resources"),
    ];

    candidates.into_iter().find(|c| c.is_dir())
}

/// Get all locale directories in the resources directory
fn get_locales(resources_dir: &Path) -> Vec<PathBuf> {
    let mut locales = Vec::new();

    if let Ok(entries) = std::fs::read_dir(resources_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                locales.push(path);
            }
        }
    }

    locales.sort();
    locales
}

/// Get all .ftl files in a directory
fn get_ftl_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "ftl") {
                files.push(path);
            }
        }
    }

    files.sort();
    files
}

/// Check a single .ftl file for syntax errors
fn check_ftl_file(path: &Path) -> FileCheckResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut message_count = 0;

    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            errors.push(format!("Failed to read file: {}", e));
            return FileCheckResult {
                path: path.to_path_buf(),
                errors,
                warnings,
                message_count,
            };
        }
    };

    // Parse the Fluent file
    let (resource, parse_errors) = match parse(content.as_str()) {
        Ok(r) => (r, vec![]),
        Err((r, errs)) => (r, errs),
    };

    // Check for parse errors
    for error in &parse_errors {
        errors.push(format!("Parse error: {:?}", error));
    }

    // Count messages and check for issues
    for entry in &resource.body {
        match entry {
            fluent_syntax::ast::Entry::Message(msg) => {
                message_count += 1;

                // Check for empty message value
                if msg.value.is_none() && msg.attributes.is_empty() {
                    warnings.push(format!(
                        "Message '{}' has no value or attributes",
                        msg.id.name
                    ));
                }

                // Check for select expressions without default
                if let Some(pattern) = &msg.value {
                    check_pattern_for_select(msg.id.name, pattern, &mut warnings);
                }

                for attr in &msg.attributes {
                    check_pattern_for_select(msg.id.name, &attr.value, &mut warnings);
                }
            }
            fluent_syntax::ast::Entry::Term(term) => {
                message_count += 1;
                let term_name = format!("-{}", term.id.name);
                check_pattern_for_select(&term_name, &term.value, &mut warnings);
            }
            _ => {}
        }
    }

    FileCheckResult {
        path: path.to_path_buf(),
        errors,
        warnings,
        message_count,
    }
}

/// Check a pattern for select expressions without default variants
fn check_pattern_for_select<S: AsRef<str>>(
    msg_id: &str,
    pattern: &fluent_syntax::ast::Pattern<S>,
    warnings: &mut Vec<String>,
) {
    for element in &pattern.elements {
        if let fluent_syntax::ast::PatternElement::Placeable { expression } = element {
            check_expression_for_select(msg_id, expression, warnings);
        }
    }
}

/// Check an expression for select expressions without default variants
fn check_expression_for_select<S: AsRef<str>>(
    msg_id: &str,
    expr: &fluent_syntax::ast::Expression<S>,
    warnings: &mut Vec<String>,
) {
    match expr {
        fluent_syntax::ast::Expression::Select { variants, .. } => {
            let has_default = variants.iter().any(|v| v.default);
            if !has_default {
                warnings.push(format!(
                    "Message '{}' has select expression without default variant",
                    msg_id
                ));
            }
        }
        fluent_syntax::ast::Expression::Inline(inline) => {
            if let fluent_syntax::ast::InlineExpression::Placeable { expression } = inline {
                check_expression_for_select(msg_id, expression, warnings);
            }
        }
    }
}

fn main() -> ExitCode {
    println!("Fluent Translation File Checker");
    println!("================================\n");

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

    println!("Resources directory: {}\n", resources_dir.display());

    // Get all locales
    let locales = get_locales(&resources_dir);
    if locales.is_empty() {
        eprintln!(
            "{}Error: No locale directories found{}",
            colors::RED,
            colors::RESET
        );
        return ExitCode::FAILURE;
    }

    let mut total_files = 0;
    let mut total_errors = 0;
    let mut total_warnings = 0;
    let mut total_messages = 0;
    let mut results: Vec<FileCheckResult> = Vec::new();

    // Check each locale
    for locale_dir in &locales {
        let locale_name = locale_dir.file_name().unwrap().to_string_lossy();
        let ftl_files = get_ftl_files(locale_dir);

        println!("Checking locale: {}", locale_name);

        for ftl_path in ftl_files {
            let result = check_ftl_file(&ftl_path);
            total_files += 1;
            total_errors += result.errors.len();
            total_warnings += result.warnings.len();
            total_messages += result.message_count;

            let file_name = ftl_path.file_name().unwrap().to_string_lossy();

            if result.errors.is_empty() && result.warnings.is_empty() {
                println!(
                    "  {}[OK]{} {} ({} messages)",
                    colors::GREEN,
                    colors::RESET,
                    file_name,
                    result.message_count
                );
            } else if !result.errors.is_empty() {
                println!(
                    "  {}[ERROR]{} {} ({} messages)",
                    colors::RED,
                    colors::RESET,
                    file_name,
                    result.message_count
                );
            } else {
                println!(
                    "  {}[WARN]{} {} ({} messages)",
                    colors::YELLOW,
                    colors::RESET,
                    file_name,
                    result.message_count
                );
            }

            if !result.errors.is_empty() || !result.warnings.is_empty() {
                results.push(result);
            }
        }

        println!();
    }

    // Print detailed errors and warnings
    if !results.is_empty() {
        println!("Detailed Results");
        println!("================\n");

        for result in &results {
            let file_name = result.path.display();

            if !result.errors.is_empty() {
                println!("{}Errors in {}:{}", colors::RED, file_name, colors::RESET);
                for error in &result.errors {
                    println!("  - {}", error);
                }
                println!();
            }

            if !result.warnings.is_empty() {
                println!(
                    "{}Warnings in {}:{}",
                    colors::YELLOW,
                    file_name,
                    colors::RESET
                );
                for warning in &result.warnings {
                    println!("  - {}", warning);
                }
                println!();
            }
        }
    }

    // Print summary
    println!("Summary");
    println!("=======");
    println!("Locales checked: {}", locales.len());
    println!("Files checked: {}", total_files);
    println!("Total messages: {}", total_messages);
    println!(
        "Errors: {}{}{}",
        if total_errors > 0 {
            colors::RED
        } else {
            colors::GREEN
        },
        total_errors,
        colors::RESET
    );
    println!(
        "Warnings: {}{}{}",
        if total_warnings > 0 {
            colors::YELLOW
        } else {
            colors::GREEN
        },
        total_warnings,
        colors::RESET
    );

    if total_errors > 0 {
        println!(
            "\n{}FAILED: {} error(s) found{}",
            colors::RED,
            total_errors,
            colors::RESET
        );
        ExitCode::FAILURE
    } else {
        println!(
            "\n{}PASSED: All files valid{}",
            colors::GREEN,
            colors::RESET
        );
        ExitCode::SUCCESS
    }
}
