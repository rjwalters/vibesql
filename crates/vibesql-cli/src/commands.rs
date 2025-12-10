use crate::formatter::OutputFormat;

#[derive(Debug, Clone)]
pub enum MetaCommand {
    Quit,
    Help,
    DescribeTable(String),
    ListTables,
    ListSchemas,
    ListIndexes,
    ListRoles,
    SetFormat(OutputFormat),
    Timing,
    Copy { table: String, file_path: String, direction: CopyDirection, format: CopyFormat },
    Save(Option<String>),
    Errors,
}

#[derive(Debug, Clone)]
pub enum CopyDirection {
    Import, // FROM file
    Export, // TO file
}

#[derive(Debug, Clone)]
pub enum CopyFormat {
    Csv,
    Json,
}

impl MetaCommand {
    pub fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim();

        // Support both PostgreSQL-style backslash commands and SQLite-style dot commands
        if trimmed.starts_with('\\') {
            Self::parse_backslash_command(trimmed)
        } else if trimmed.starts_with('.') {
            Self::parse_dot_command(trimmed)
        } else {
            None
        }
    }

    /// Parse PostgreSQL-style backslash commands (\d, \dt, etc.)
    fn parse_backslash_command(trimmed: &str) -> Option<Self> {
        let parts: Vec<&str> = trimmed.split_whitespace().collect();

        match parts.first() {
            Some(&"\\q") | Some(&"\\quit") => Some(MetaCommand::Quit),
            Some(&"\\h") | Some(&"\\help") => Some(MetaCommand::Help),
            Some(&"\\d") => {
                if let Some(table_name) = parts.get(1) {
                    Some(MetaCommand::DescribeTable(table_name.to_string()))
                } else {
                    Some(MetaCommand::ListTables)
                }
            }
            Some(&"\\dt") => Some(MetaCommand::ListTables),
            Some(&"\\ds") => Some(MetaCommand::ListSchemas),
            Some(&"\\di") => Some(MetaCommand::ListIndexes),
            Some(&"\\du") => Some(MetaCommand::ListRoles),
            Some(&"\\f") => {
                if let Some(format_str) = parts.get(1) {
                    match *format_str {
                        "table" => Some(MetaCommand::SetFormat(OutputFormat::Table)),
                        "json" => Some(MetaCommand::SetFormat(OutputFormat::Json)),
                        "csv" => Some(MetaCommand::SetFormat(OutputFormat::Csv)),
                        "markdown" | "md" => Some(MetaCommand::SetFormat(OutputFormat::Markdown)),
                        "html" => Some(MetaCommand::SetFormat(OutputFormat::Html)),
                        "raw" => Some(MetaCommand::SetFormat(OutputFormat::Raw)),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            Some(&"\\timing") => Some(MetaCommand::Timing),
            Some(&"\\copy") => {
                // Parse: \copy table_name TO/FROM 'file_path'
                // Format: \copy users TO '/tmp/users.csv'
                if parts.len() < 4 {
                    return None;
                }

                let table = parts[1].to_string();
                let direction_str = parts[2];
                let file_path =
                    parts[3..].join(" ").trim_matches('\'').trim_matches('"').to_string();

                let direction = match direction_str.to_uppercase().as_str() {
                    "TO" => CopyDirection::Export,
                    "FROM" => CopyDirection::Import,
                    _ => return None,
                };

                // Infer format from file extension
                let format = if file_path.ends_with(".json") {
                    CopyFormat::Json
                } else {
                    CopyFormat::Csv // Default to CSV
                };

                Some(MetaCommand::Copy { table, file_path, direction, format })
            }
            Some(&"\\save") => {
                // Optional filename argument
                let filename = parts.get(1).map(|s| s.to_string());
                Some(MetaCommand::Save(filename))
            }
            Some(&"\\errors") => Some(MetaCommand::Errors),
            _ => None,
        }
    }

    /// Parse SQLite-style dot commands (.tables, .schema, etc.)
    fn parse_dot_command(trimmed: &str) -> Option<Self> {
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        let cmd = parts.first()?;

        match *cmd {
            // Exit commands
            ".quit" | ".exit" | ".q" => Some(MetaCommand::Quit),

            // Help
            ".help" | ".h" => Some(MetaCommand::Help),

            // List tables
            ".tables" => Some(MetaCommand::ListTables),

            // Schema - show CREATE statement for a table, or list all tables if no argument
            ".schema" => {
                if let Some(table_name) = parts.get(1) {
                    Some(MetaCommand::DescribeTable(table_name.to_string()))
                } else {
                    Some(MetaCommand::ListTables)
                }
            }

            // List indexes
            ".indexes" | ".indices" => {
                // .indexes [table] - if table provided, could filter (not implemented yet)
                Some(MetaCommand::ListIndexes)
            }

            // List databases/schemas
            ".databases" => Some(MetaCommand::ListSchemas),

            // Output mode
            ".mode" => {
                if let Some(mode) = parts.get(1) {
                    match *mode {
                        "table" | "box" | "column" => {
                            Some(MetaCommand::SetFormat(OutputFormat::Table))
                        }
                        "json" => Some(MetaCommand::SetFormat(OutputFormat::Json)),
                        "csv" => Some(MetaCommand::SetFormat(OutputFormat::Csv)),
                        "markdown" => Some(MetaCommand::SetFormat(OutputFormat::Markdown)),
                        "html" => Some(MetaCommand::SetFormat(OutputFormat::Html)),
                        "raw" => Some(MetaCommand::SetFormat(OutputFormat::Raw)),
                        _ => None,
                    }
                } else {
                    None
                }
            }

            // Save database
            ".save" | ".backup" => {
                let filename = parts.get(1).map(|s| s.to_string());
                Some(MetaCommand::Save(filename))
            }

            // Timer/timing
            ".timer" => Some(MetaCommand::Timing),

            // Import (similar to \copy FROM)
            ".import" => {
                // .import FILE TABLE
                if parts.len() < 3 {
                    return None;
                }
                let file_path = parts[1].trim_matches('\'').trim_matches('"').to_string();
                let table = parts[2].to_string();

                let format = if file_path.ends_with(".json") {
                    CopyFormat::Json
                } else {
                    CopyFormat::Csv
                };

                Some(MetaCommand::Copy {
                    table,
                    file_path,
                    direction: CopyDirection::Import,
                    format,
                })
            }

            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quit() {
        assert!(matches!(MetaCommand::parse("\\q"), Some(MetaCommand::Quit)));
        assert!(matches!(MetaCommand::parse("\\quit"), Some(MetaCommand::Quit)));
    }

    #[test]
    fn test_parse_help() {
        assert!(matches!(MetaCommand::parse("\\h"), Some(MetaCommand::Help)));
        assert!(matches!(MetaCommand::parse("\\help"), Some(MetaCommand::Help)));
    }

    #[test]
    fn test_parse_list_tables() {
        assert!(matches!(MetaCommand::parse("\\d"), Some(MetaCommand::ListTables)));
        assert!(matches!(MetaCommand::parse("\\dt"), Some(MetaCommand::ListTables)));
    }

    #[test]
    fn test_parse_describe_table() {
        if let Some(MetaCommand::DescribeTable(name)) = MetaCommand::parse("\\d users") {
            assert_eq!(name, "users");
        } else {
            panic!("Failed to parse describe table command");
        }
    }

    #[test]
    fn test_parse_timing() {
        assert!(matches!(MetaCommand::parse("\\timing"), Some(MetaCommand::Timing)));
    }

    #[test]
    fn test_parse_list_schemas() {
        assert!(matches!(MetaCommand::parse("\\ds"), Some(MetaCommand::ListSchemas)));
    }

    #[test]
    fn test_parse_list_indexes() {
        assert!(matches!(MetaCommand::parse("\\di"), Some(MetaCommand::ListIndexes)));
    }

    #[test]
    fn test_parse_list_roles() {
        assert!(matches!(MetaCommand::parse("\\du"), Some(MetaCommand::ListRoles)));
    }

    #[test]
    fn test_parse_set_format() {
        assert!(matches!(
            MetaCommand::parse("\\f table"),
            Some(MetaCommand::SetFormat(OutputFormat::Table))
        ));
        assert!(matches!(
            MetaCommand::parse("\\f json"),
            Some(MetaCommand::SetFormat(OutputFormat::Json))
        ));
        assert!(matches!(
            MetaCommand::parse("\\f csv"),
            Some(MetaCommand::SetFormat(OutputFormat::Csv))
        ));
        assert!(matches!(
            MetaCommand::parse("\\f markdown"),
            Some(MetaCommand::SetFormat(OutputFormat::Markdown))
        ));
        assert!(matches!(
            MetaCommand::parse("\\f md"),
            Some(MetaCommand::SetFormat(OutputFormat::Markdown))
        ));
        assert!(matches!(
            MetaCommand::parse("\\f html"),
            Some(MetaCommand::SetFormat(OutputFormat::Html))
        ));
        assert!(matches!(
            MetaCommand::parse("\\f raw"),
            Some(MetaCommand::SetFormat(OutputFormat::Raw))
        ));
    }

    #[test]
    fn test_non_meta_command() {
        assert!(MetaCommand::parse("SELECT * FROM users").is_none());
    }

    #[test]
    fn test_parse_copy_export_csv() {
        if let Some(MetaCommand::Copy { table, file_path, direction, format }) =
            MetaCommand::parse("\\copy users TO '/tmp/users.csv'")
        {
            assert_eq!(table, "users");
            assert_eq!(file_path, "/tmp/users.csv");
            assert!(matches!(direction, CopyDirection::Export));
            assert!(matches!(format, CopyFormat::Csv));
        } else {
            panic!("Failed to parse copy export CSV command");
        }
    }

    #[test]
    fn test_parse_copy_import_csv() {
        if let Some(MetaCommand::Copy { table, file_path, direction, format }) =
            MetaCommand::parse("\\copy users FROM '/tmp/users.csv'")
        {
            assert_eq!(table, "users");
            assert_eq!(file_path, "/tmp/users.csv");
            assert!(matches!(direction, CopyDirection::Import));
            assert!(matches!(format, CopyFormat::Csv));
        } else {
            panic!("Failed to parse copy import CSV command");
        }
    }

    #[test]
    fn test_parse_copy_export_json() {
        if let Some(MetaCommand::Copy { table, file_path, direction, format }) =
            MetaCommand::parse("\\copy users TO /tmp/users.json")
        {
            assert_eq!(table, "users");
            assert_eq!(file_path, "/tmp/users.json");
            assert!(matches!(direction, CopyDirection::Export));
            assert!(matches!(format, CopyFormat::Json));
        } else {
            panic!("Failed to parse copy export JSON command");
        }
    }

    #[test]
    fn test_parse_errors() {
        assert!(matches!(MetaCommand::parse("\\errors"), Some(MetaCommand::Errors)));
    }

    // SQLite dot-command tests
    #[test]
    fn test_dot_quit() {
        assert!(matches!(MetaCommand::parse(".quit"), Some(MetaCommand::Quit)));
        assert!(matches!(MetaCommand::parse(".exit"), Some(MetaCommand::Quit)));
        assert!(matches!(MetaCommand::parse(".q"), Some(MetaCommand::Quit)));
    }

    #[test]
    fn test_dot_help() {
        assert!(matches!(MetaCommand::parse(".help"), Some(MetaCommand::Help)));
        assert!(matches!(MetaCommand::parse(".h"), Some(MetaCommand::Help)));
    }

    #[test]
    fn test_dot_tables() {
        assert!(matches!(MetaCommand::parse(".tables"), Some(MetaCommand::ListTables)));
    }

    #[test]
    fn test_dot_schema() {
        // Without argument - lists tables
        assert!(matches!(MetaCommand::parse(".schema"), Some(MetaCommand::ListTables)));
        // With argument - describes table
        if let Some(MetaCommand::DescribeTable(name)) = MetaCommand::parse(".schema users") {
            assert_eq!(name, "users");
        } else {
            panic!("Failed to parse .schema users command");
        }
    }

    #[test]
    fn test_dot_indexes() {
        assert!(matches!(MetaCommand::parse(".indexes"), Some(MetaCommand::ListIndexes)));
        assert!(matches!(MetaCommand::parse(".indices"), Some(MetaCommand::ListIndexes)));
    }

    #[test]
    fn test_dot_databases() {
        assert!(matches!(MetaCommand::parse(".databases"), Some(MetaCommand::ListSchemas)));
    }

    #[test]
    fn test_dot_mode() {
        assert!(matches!(
            MetaCommand::parse(".mode table"),
            Some(MetaCommand::SetFormat(OutputFormat::Table))
        ));
        assert!(matches!(
            MetaCommand::parse(".mode json"),
            Some(MetaCommand::SetFormat(OutputFormat::Json))
        ));
        assert!(matches!(
            MetaCommand::parse(".mode csv"),
            Some(MetaCommand::SetFormat(OutputFormat::Csv))
        ));
        assert!(matches!(
            MetaCommand::parse(".mode markdown"),
            Some(MetaCommand::SetFormat(OutputFormat::Markdown))
        ));
        assert!(matches!(
            MetaCommand::parse(".mode html"),
            Some(MetaCommand::SetFormat(OutputFormat::Html))
        ));
        assert!(matches!(
            MetaCommand::parse(".mode raw"),
            Some(MetaCommand::SetFormat(OutputFormat::Raw))
        ));
        // SQLite aliases
        assert!(matches!(
            MetaCommand::parse(".mode box"),
            Some(MetaCommand::SetFormat(OutputFormat::Table))
        ));
        assert!(matches!(
            MetaCommand::parse(".mode column"),
            Some(MetaCommand::SetFormat(OutputFormat::Table))
        ));
    }

    #[test]
    fn test_dot_save() {
        assert!(matches!(MetaCommand::parse(".save"), Some(MetaCommand::Save(None))));
        if let Some(MetaCommand::Save(Some(path))) = MetaCommand::parse(".save mydb.sql") {
            assert_eq!(path, "mydb.sql");
        } else {
            panic!("Failed to parse .save with path");
        }
        // .backup is an alias
        assert!(matches!(MetaCommand::parse(".backup"), Some(MetaCommand::Save(None))));
    }

    #[test]
    fn test_dot_timer() {
        assert!(matches!(MetaCommand::parse(".timer"), Some(MetaCommand::Timing)));
    }

    #[test]
    fn test_dot_import() {
        if let Some(MetaCommand::Copy { table, file_path, direction, format }) =
            MetaCommand::parse(".import /tmp/data.csv users")
        {
            assert_eq!(table, "users");
            assert_eq!(file_path, "/tmp/data.csv");
            assert!(matches!(direction, CopyDirection::Import));
            assert!(matches!(format, CopyFormat::Csv));
        } else {
            panic!("Failed to parse .import command");
        }

        // JSON import
        if let Some(MetaCommand::Copy { format, .. }) =
            MetaCommand::parse(".import /tmp/data.json users")
        {
            assert!(matches!(format, CopyFormat::Json));
        } else {
            panic!("Failed to parse .import JSON command");
        }
    }

    #[test]
    fn test_dot_command_with_whitespace() {
        assert!(matches!(MetaCommand::parse("  .tables  "), Some(MetaCommand::ListTables)));
        assert!(matches!(MetaCommand::parse("\t.quit\t"), Some(MetaCommand::Quit)));
    }
}
