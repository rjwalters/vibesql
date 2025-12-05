//! SQL mode configuration for dialect-specific behavior.

#![allow(clippy::derivable_impls)]

mod config;
mod operators;
mod strings;
pub mod types;

pub use config::MySqlModeFlags;

// Re-export string types and traits
pub use strings::{Collation, StringBehavior};

// Re-export operator types and traits
pub use operators::{ConcatOperator, DivisionBehavior, OperatorBehavior};

/// SQL compatibility mode
///
/// VibeSQL supports different SQL dialect modes to match the behavior
/// of different database systems. This is necessary because SQL standards
/// allow implementation-defined behavior in certain areas.
///
/// ## Differences by Mode
///
/// See SQL_COMPATIBILITY_MODE.md in the repository root for a comprehensive list
/// of behavioral differences between modes.
///
/// ### Division Operator (`/`)
/// - **MySQL**: `INTEGER / INTEGER → DECIMAL` (floating-point division)
///   - Example: `83 / 6 = 13.8333`
/// - **SQLite**: `INTEGER / INTEGER → INTEGER` (truncated division)
///   - Example: `83 / 6 = 13`
///
/// ## Default Mode
///
/// MySQL mode is the default to maximize compatibility with the
/// dolthub/sqllogictest test suite, which was generated from MySQL 8.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SqlMode {
    /// MySQL 8.0+ compatibility mode (default)
    ///
    /// - Division returns DECIMAL (floating-point)
    /// - Other MySQL-specific behaviors controlled by flags
    MySQL { flags: MySqlModeFlags },

    /// SQLite 3 compatibility mode
    ///
    /// - Division returns INTEGER (truncated)
    /// - Other SQLite-specific behaviors
    ///
    /// Note: Currently not fully implemented. Many features will error
    /// with "TODO: SQLite mode not yet supported" messages.
    SQLite,
}

impl Default for SqlMode {
    fn default() -> Self {
        // Default to MySQL mode for SQLLogicTest compatibility
        // The dolthub/sqllogictest corpus was regenerated against MySQL 8.x
        // and expects MySQL semantics including decimal division
        // (INTEGER / INTEGER → DECIMAL)
        SqlMode::MySQL { flags: MySqlModeFlags::default() }
    }
}

impl std::fmt::Display for SqlMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlMode::MySQL { .. } => write!(f, "mysql"),
            SqlMode::SQLite => write!(f, "sqlite"),
        }
    }
}

impl std::str::FromStr for SqlMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // First try simple mode names
        match s.to_lowercase().as_str() {
            "mysql" => return Ok(SqlMode::MySQL { flags: MySqlModeFlags::default() }),
            // MySQL mode with SQLite division semantics
            // This is used by SQLLogicTest where MySQL syntax is needed (e.g., CAST...AS SIGNED)
            // but division should behave like SQLite (INTEGER / INTEGER → INTEGER)
            // because the expected results were generated using SQLite.
            "mysql_slt" => {
                return Ok(SqlMode::MySQL {
                    flags: MySqlModeFlags::with_sqlite_division_semantics(),
                })
            }
            "sqlite" => return Ok(SqlMode::SQLite),
            _ => {}
        }

        // Try parsing as MySQL comma-separated mode flags
        // e.g., "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,..."
        // Also handles leading/trailing commas and empty segments
        parse_mysql_mode_flags(s)
    }
}

/// Parse MySQL comma-separated mode flags
///
/// This handles strings like:
/// - "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
/// - ",STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,..." (leading comma from REPLACE() removing first mode)
/// - "STRICT_TRANS_TABLES," (trailing comma)
/// - "MODE1,,MODE2" (empty segments)
///
/// MySQL mode flags we recognize and map:
/// - STRICT_TRANS_TABLES → strict_mode
/// - PIPES_AS_CONCAT → pipes_as_concat
/// - ANSI_QUOTES → ansi_quotes
/// - ANSI → pipes_as_concat + ansi_quotes
///
/// Other MySQL modes (NO_ZERO_IN_DATE, NO_ZERO_DATE, ERROR_FOR_DIVISION_BY_ZERO,
/// NO_ENGINE_SUBSTITUTION, ONLY_FULL_GROUP_BY, etc.) are accepted but ignored
/// as they don't affect our behavior.
fn parse_mysql_mode_flags(s: &str) -> Result<SqlMode, String> {
    let mut flags = MySqlModeFlags::default();

    // Split by comma and process each mode
    for mode in s.split(',') {
        // Skip empty segments (handles leading/trailing commas and consecutive commas)
        let mode = mode.trim();
        if mode.is_empty() {
            continue;
        }

        // Match MySQL mode flags (case-insensitive)
        match mode.to_uppercase().as_str() {
            // Modes we actually implement
            "STRICT_TRANS_TABLES" | "STRICT_ALL_TABLES" => {
                flags.strict_mode = true;
            }
            "PIPES_AS_CONCAT" => {
                flags.pipes_as_concat = true;
            }
            "ANSI_QUOTES" => {
                flags.ansi_quotes = true;
            }
            "ANSI" => {
                // ANSI mode is a combination
                flags.pipes_as_concat = true;
                flags.ansi_quotes = true;
            }

            // Common MySQL modes we accept but don't implement
            // These are standard MySQL 8.0 defaults and commonly used modes
            "NO_ZERO_IN_DATE"
            | "NO_ZERO_DATE"
            | "ERROR_FOR_DIVISION_BY_ZERO"
            | "NO_ENGINE_SUBSTITUTION"
            | "ONLY_FULL_GROUP_BY"
            | "NO_AUTO_CREATE_USER"
            | "NO_AUTO_VALUE_ON_ZERO"
            | "NO_BACKSLASH_ESCAPES"
            | "NO_DIR_IN_CREATE"
            | "NO_UNSIGNED_SUBTRACTION"
            | "PAD_CHAR_TO_FULL_LENGTH"
            | "REAL_AS_FLOAT"
            | "TIME_TRUNCATE_FRACTIONAL"
            | "IGNORE_SPACE"
            | "TRADITIONAL"
            | "ALLOW_INVALID_DATES"
            | "HIGH_NOT_PRECEDENCE" => {
                // Accepted but not implemented - no action needed
            }

            // Unknown mode - but we're lenient to allow future MySQL versions
            // We only error on truly unrecognized patterns
            other => {
                // Check if it looks like a MySQL mode (alphanumeric with underscores)
                if other.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    // Looks like a MySQL mode we don't know - accept it silently
                    // This allows forward compatibility with new MySQL modes
                } else {
                    return Err(format!(
                        "Unknown SQL mode: '{}'. Valid modes: mysql, mysql_slt, sqlite, or MySQL mode flags",
                        s
                    ));
                }
            }
        }
    }

    Ok(SqlMode::MySQL { flags })
}

impl SqlMode {
    /// Get the MySQL mode flags, if in MySQL mode
    pub fn mysql_flags(&self) -> Option<&MySqlModeFlags> {
        match self {
            SqlMode::MySQL { flags } => Some(flags),
            SqlMode::SQLite => None,
        }
    }
}

// Supported collations for each SQL mode
#[allow(dead_code)]
const MYSQL_SUPPORTED_COLLATIONS: &[Collation] =
    &[Collation::Binary, Collation::Utf8Binary, Collation::Utf8GeneralCi];

#[allow(dead_code)]
const SQLITE_SUPPORTED_COLLATIONS: &[Collation] =
    &[Collation::Binary, Collation::NoCase, Collation::Rtrim];

impl StringBehavior for SqlMode {
    fn default_string_comparison_case_sensitive(&self) -> bool {
        match self {
            SqlMode::MySQL { .. } => false, // MySQL defaults to case-insensitive
            SqlMode::SQLite => true,        // SQLite defaults to case-sensitive
        }
    }

    fn default_collation(&self) -> Collation {
        match self {
            SqlMode::MySQL { .. } => Collation::Utf8GeneralCi, // MySQL's default collation
            SqlMode::SQLite => Collation::Binary,              // SQLite's default collation
        }
    }

    fn supported_collations(&self) -> &[Collation] {
        match self {
            SqlMode::MySQL { .. } => MYSQL_SUPPORTED_COLLATIONS,
            SqlMode::SQLite => SQLITE_SUPPORTED_COLLATIONS,
        }
    }
}

impl OperatorBehavior for SqlMode {
    fn integer_division_behavior(&self) -> DivisionBehavior {
        match self {
            SqlMode::MySQL { .. } => DivisionBehavior::Decimal,
            SqlMode::SQLite => DivisionBehavior::Integer,
        }
    }

    fn supports_xor(&self) -> bool {
        match self {
            SqlMode::MySQL { .. } => true,
            SqlMode::SQLite => false,
        }
    }

    fn supports_integer_div_operator(&self) -> bool {
        match self {
            SqlMode::MySQL { .. } => true,
            SqlMode::SQLite => false,
        }
    }

    fn string_concat_operator(&self) -> ConcatOperator {
        match self {
            SqlMode::MySQL { .. } => ConcatOperator::Function,
            SqlMode::SQLite => ConcatOperator::PipePipe,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_mysql() {
        // Default is MySQL for SQLLogicTest compatibility (MySQL 8.x test suite)
        let mode = SqlMode::default();
        assert!(matches!(mode, SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_mysql_mode_has_default_flags() {
        // Test that MySQL mode can be constructed with default flags
        let mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        if let SqlMode::MySQL { flags } = mode {
            assert_eq!(flags, MySqlModeFlags::default());
        } else {
            panic!("Expected MySQL mode");
        }
    }

    #[test]
    fn test_division_behavior() {
        use crate::sql_mode::types::{TypeBehavior, ValueType};
        use crate::SqlValue;

        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        // MySQL returns Numeric for division
        assert_eq!(
            mysql_mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Integer(2)),
            ValueType::Numeric
        );

        let sqlite_mode = SqlMode::SQLite;
        // SQLite returns Integer for int/int division
        assert_eq!(
            sqlite_mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Integer(2)),
            ValueType::Integer
        );
    }

    #[test]
    fn test_mysql_flags_accessor() {
        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::with_pipes_as_concat() };
        assert!(mysql_mode.mysql_flags().is_some());
        assert!(mysql_mode.mysql_flags().unwrap().pipes_as_concat);

        let sqlite_mode = SqlMode::SQLite;
        assert!(sqlite_mode.mysql_flags().is_none());
    }

    #[test]
    fn test_sqlmode_with_flags() {
        let mode = SqlMode::MySQL {
            flags: MySqlModeFlags { pipes_as_concat: true, ..Default::default() },
        };

        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.pipes_as_concat);
                assert!(!flags.ansi_quotes);
                assert!(!flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_sqlmode_with_custom_flags() {
        let mode = SqlMode::MySQL { flags: MySqlModeFlags::ansi() };

        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.pipes_as_concat);
                assert!(flags.ansi_quotes);
                assert!(!flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_mysql_string_comparison() {
        let mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        assert!(!mode.default_string_comparison_case_sensitive());
        assert_eq!(mode.default_collation(), Collation::Utf8GeneralCi);
    }

    #[test]
    fn test_sqlite_string_comparison() {
        let mode = SqlMode::SQLite;
        assert!(mode.default_string_comparison_case_sensitive());
        assert_eq!(mode.default_collation(), Collation::Binary);
    }

    #[test]
    fn test_mysql_supported_collations() {
        let mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        let collations = mode.supported_collations();
        assert_eq!(collations.len(), 3);
        assert!(collations.contains(&Collation::Binary));
        assert!(collations.contains(&Collation::Utf8Binary));
        assert!(collations.contains(&Collation::Utf8GeneralCi));
    }

    #[test]
    fn test_sqlite_supported_collations() {
        let mode = SqlMode::SQLite;
        let collations = mode.supported_collations();
        assert_eq!(collations.len(), 3);
        assert!(collations.contains(&Collation::Binary));
        assert!(collations.contains(&Collation::NoCase));
        assert!(collations.contains(&Collation::Rtrim));
    }

    #[test]
    fn test_collation_case_sensitivity_consistency() {
        // MySQL default collation should be case-insensitive
        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        assert!(!mysql_mode.default_string_comparison_case_sensitive());
        assert_eq!(mysql_mode.default_collation(), Collation::Utf8GeneralCi);

        // SQLite default collation should be case-sensitive
        let sqlite_mode = SqlMode::SQLite;
        assert!(sqlite_mode.default_string_comparison_case_sensitive());
        assert_eq!(sqlite_mode.default_collation(), Collation::Binary);
    }

    // Tests for OperatorBehavior trait implementation

    #[test]
    fn test_integer_division_behavior() {
        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        assert_eq!(mysql_mode.integer_division_behavior(), DivisionBehavior::Decimal);

        let sqlite_mode = SqlMode::SQLite;
        assert_eq!(sqlite_mode.integer_division_behavior(), DivisionBehavior::Integer);
    }

    #[test]
    fn test_xor_support() {
        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        assert!(mysql_mode.supports_xor());

        let sqlite_mode = SqlMode::SQLite;
        assert!(!sqlite_mode.supports_xor());
    }

    #[test]
    fn test_integer_div_operator_support() {
        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        assert!(mysql_mode.supports_integer_div_operator());

        let sqlite_mode = SqlMode::SQLite;
        assert!(!sqlite_mode.supports_integer_div_operator());
    }

    #[test]
    fn test_string_concat_operator() {
        let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        assert_eq!(mysql_mode.string_concat_operator(), ConcatOperator::Function);

        let sqlite_mode = SqlMode::SQLite;
        assert_eq!(sqlite_mode.string_concat_operator(), ConcatOperator::PipePipe);
    }

    // Tests for FromStr and Display implementations

    #[test]
    fn test_display_mysql() {
        let mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        assert_eq!(mode.to_string(), "mysql");
    }

    #[test]
    fn test_display_sqlite() {
        let mode = SqlMode::SQLite;
        assert_eq!(mode.to_string(), "sqlite");
    }

    #[test]
    fn test_from_str_mysql() {
        let mode: SqlMode = "mysql".parse().unwrap();
        assert!(matches!(mode, SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_from_str_sqlite() {
        let mode: SqlMode = "sqlite".parse().unwrap();
        assert!(matches!(mode, SqlMode::SQLite));
    }

    #[test]
    fn test_from_str_case_insensitive() {
        let mode1: SqlMode = "MYSQL".parse().unwrap();
        assert!(matches!(mode1, SqlMode::MySQL { .. }));

        let mode2: SqlMode = "SQLite".parse().unwrap();
        assert!(matches!(mode2, SqlMode::SQLite));

        let mode3: SqlMode = "MySql".parse().unwrap();
        assert!(matches!(mode3, SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_from_str_mysql_slt() {
        // mysql_slt mode: MySQL syntax with SQLite division semantics
        let mode: SqlMode = "mysql_slt".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.sqlite_division_semantics);
                assert!(!flags.pipes_as_concat);
                assert!(!flags.ansi_quotes);
                assert!(!flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_invalid() {
        // Invalid patterns with special characters should still error
        let result: Result<SqlMode, _> = "invalid!@#".parse();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Unknown SQL mode"));
    }

    // Tests for MySQL mode flags parsing (issue #3074)

    #[test]
    fn test_from_str_mysql_mode_flags() {
        // Standard MySQL 8.0 default mode string
        let mode: SqlMode =
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
                .parse()
                .unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
                assert!(!flags.pipes_as_concat);
                assert!(!flags.ansi_quotes);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_leading_comma() {
        // Leading comma (from REPLACE() removing first mode like ONLY_FULL_GROUP_BY)
        let mode: SqlMode =
            ",STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
                .parse()
                .unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_trailing_comma() {
        // Trailing comma
        let mode: SqlMode = "STRICT_TRANS_TABLES,".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_empty_segments() {
        // Empty segments between commas
        let mode: SqlMode = "STRICT_TRANS_TABLES,,PIPES_AS_CONCAT".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
                assert!(flags.pipes_as_concat);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_empty_string() {
        // Empty string should result in default flags
        let mode: SqlMode = "".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(!flags.strict_mode);
                assert!(!flags.pipes_as_concat);
                assert!(!flags.ansi_quotes);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_only_commas() {
        // Only commas should result in default flags
        let mode: SqlMode = ",,,".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(!flags.strict_mode);
                assert!(!flags.pipes_as_concat);
                assert!(!flags.ansi_quotes);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_ansi_mode() {
        // ANSI mode should set both pipes_as_concat and ansi_quotes
        let mode: SqlMode = "ANSI".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.pipes_as_concat);
                assert!(flags.ansi_quotes);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_pipes_as_concat() {
        let mode: SqlMode = "PIPES_AS_CONCAT".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.pipes_as_concat);
                assert!(!flags.ansi_quotes);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_ansi_quotes() {
        let mode: SqlMode = "ANSI_QUOTES".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(!flags.pipes_as_concat);
                assert!(flags.ansi_quotes);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_mode_flags_case_insensitive() {
        // Mode flags should be case-insensitive
        let mode: SqlMode = "strict_trans_tables,pipes_as_concat".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
                assert!(flags.pipes_as_concat);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_mode_flags_with_whitespace() {
        // Whitespace around modes should be trimmed
        let mode: SqlMode = " STRICT_TRANS_TABLES , PIPES_AS_CONCAT ".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
                assert!(flags.pipes_as_concat);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_from_str_unknown_mode_accepted() {
        // Unknown MySQL-like modes should be accepted silently (forward compatibility)
        let mode: SqlMode = "SOME_FUTURE_MODE,STRICT_TRANS_TABLES".parse().unwrap();
        match mode {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_roundtrip() {
        // MySQL roundtrip
        let mysql = SqlMode::MySQL { flags: MySqlModeFlags::default() };
        let mysql_str = mysql.to_string();
        let mysql_parsed: SqlMode = mysql_str.parse().unwrap();
        assert!(matches!(mysql_parsed, SqlMode::MySQL { .. }));

        // SQLite roundtrip
        let sqlite = SqlMode::SQLite;
        let sqlite_str = sqlite.to_string();
        let sqlite_parsed: SqlMode = sqlite_str.parse().unwrap();
        assert!(matches!(sqlite_parsed, SqlMode::SQLite));
    }
}
