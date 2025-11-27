mod config;
pub mod types;
mod strings;
mod operators;

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
        // Default to SQLite mode for SQLLogicTest compatibility
        // The SQLLogicTest suite is derived from SQLite and expects SQLite semantics
        // including integer division (INTEGER / INTEGER → INTEGER, truncated)
        SqlMode::SQLite
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
        match s.to_lowercase().as_str() {
            "mysql" => Ok(SqlMode::MySQL {
                flags: MySqlModeFlags::default(),
            }),
            "sqlite" => Ok(SqlMode::SQLite),
            _ => Err(format!(
                "Unknown SQL mode: '{}'. Valid modes: mysql, sqlite",
                s
            )),
        }
    }
}

impl SqlMode {
    /// Check if division should return floating-point (true) or integer (false)
    #[deprecated(
        since = "0.1.0",
        note = "Use TypeBehavior::division_result_type() instead for more precise type information"
    )]
    pub fn division_returns_float(&self) -> bool {
        match self {
            SqlMode::MySQL { .. } => true,
            SqlMode::SQLite => false,
        }
    }

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
const MYSQL_SUPPORTED_COLLATIONS: &[Collation] = &[
    Collation::Binary,
    Collation::Utf8Binary,
    Collation::Utf8GeneralCi,
];

#[allow(dead_code)]
const SQLITE_SUPPORTED_COLLATIONS: &[Collation] = &[
    Collation::Binary,
    Collation::NoCase,
    Collation::Rtrim,
];

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
    fn test_default_is_sqlite() {
        // Default is SQLite for SQLLogicTest compatibility (SQLite-derived test suite)
        let mode = SqlMode::default();
        assert!(matches!(mode, SqlMode::SQLite));
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

        let mysql_mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
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
        let mysql_mode = SqlMode::MySQL {
            flags: MySqlModeFlags::with_pipes_as_concat(),
        };
        assert!(mysql_mode.mysql_flags().is_some());
        assert!(mysql_mode.mysql_flags().unwrap().pipes_as_concat);

        let sqlite_mode = SqlMode::SQLite;
        assert!(sqlite_mode.mysql_flags().is_none());
    }

    #[test]
    fn test_sqlmode_with_flags() {
        let mode = SqlMode::MySQL {
            flags: MySqlModeFlags {
                pipes_as_concat: true,
                ..Default::default()
            },
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
        let mode = SqlMode::MySQL {
            flags: MySqlModeFlags::ansi(),
        };

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
        let mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
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
        let mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
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
        let mysql_mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
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
        let mysql_mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
        assert_eq!(
            mysql_mode.integer_division_behavior(),
            DivisionBehavior::Decimal
        );

        let sqlite_mode = SqlMode::SQLite;
        assert_eq!(
            sqlite_mode.integer_division_behavior(),
            DivisionBehavior::Integer
        );
    }

    #[test]
    fn test_xor_support() {
        let mysql_mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
        assert!(mysql_mode.supports_xor());

        let sqlite_mode = SqlMode::SQLite;
        assert!(!sqlite_mode.supports_xor());
    }

    #[test]
    fn test_integer_div_operator_support() {
        let mysql_mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
        assert!(mysql_mode.supports_integer_div_operator());

        let sqlite_mode = SqlMode::SQLite;
        assert!(!sqlite_mode.supports_integer_div_operator());
    }

    #[test]
    fn test_string_concat_operator() {
        let mysql_mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
        assert_eq!(
            mysql_mode.string_concat_operator(),
            ConcatOperator::Function
        );

        let sqlite_mode = SqlMode::SQLite;
        assert_eq!(
            sqlite_mode.string_concat_operator(),
            ConcatOperator::PipePipe
        );
    }

    // Tests for FromStr and Display implementations

    #[test]
    fn test_display_mysql() {
        let mode = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
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
    fn test_from_str_invalid() {
        let result: Result<SqlMode, _> = "invalid".parse();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Unknown SQL mode"));
        assert!(err.contains("invalid"));
    }

    #[test]
    fn test_roundtrip() {
        // MySQL roundtrip
        let mysql = SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        };
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
