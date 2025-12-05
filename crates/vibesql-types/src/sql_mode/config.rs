/// MySQL-specific mode flags (similar to MySQL's sql_mode variable)
///
/// These flags control various MySQL-specific behaviors that differ from
/// standard SQL or other database implementations.
///
/// ## References
/// - [MySQL 8.0 sql_mode Documentation](https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct MySqlModeFlags {
    /// Whether || acts as string concat (vs OR operator)
    ///
    /// Corresponds to MySQL PIPES_AS_CONCAT mode.
    ///
    /// - `true`: `'a' || 'b'` returns `'ab'` (string concatenation)
    /// - `false` (default): `'a' || 'b'` returns `1` or `0` (logical OR)
    pub pipes_as_concat: bool,

    /// Whether " acts as identifier quote (vs string literal)
    ///
    /// Corresponds to MySQL ANSI_QUOTES mode.
    ///
    /// - `true`: `"col"` is an identifier (like backticks)
    /// - `false` (default): `"col"` is a string literal (like single quotes)
    pub ansi_quotes: bool,

    /// Strict mode for type coercion and errors
    ///
    /// Corresponds to MySQL STRICT_TRANS_TABLES.
    ///
    /// - `true`: Strict type checking, errors on invalid data
    /// - `false` (default): Permissive mode with warnings
    pub strict_mode: bool,

    /// Use SQLite division semantics (INTEGER / INTEGER → INTEGER)
    ///
    /// This flag is used when MySQL syntax is needed (e.g., `CAST...AS SIGNED`)
    /// but SQLite division semantics are required for compatibility with test
    /// suites that were generated using SQLite.
    ///
    /// - `true`: `5 / 2 = 2` (truncated integer division, SQLite behavior)
    /// - `false` (default): `5 / 2 = 2.5` (exact decimal division, MySQL behavior)
    ///
    /// This is particularly useful for SQLLogicTest compatibility where tests
    /// are tagged `onlyif mysql` for syntax reasons but expect SQLite division
    /// results.
    pub sqlite_division_semantics: bool,
}

impl MySqlModeFlags {
    /// Create MySqlModeFlags with all default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Create MySqlModeFlags with PIPES_AS_CONCAT enabled
    pub fn with_pipes_as_concat() -> Self {
        Self { pipes_as_concat: true, ..Default::default() }
    }

    /// Create MySqlModeFlags with ANSI_QUOTES enabled
    pub fn with_ansi_quotes() -> Self {
        Self { ansi_quotes: true, ..Default::default() }
    }

    /// Create MySqlModeFlags with STRICT_MODE enabled
    pub fn with_strict_mode() -> Self {
        Self { strict_mode: true, ..Default::default() }
    }

    /// Create MySqlModeFlags with ANSI mode (combination of ANSI_QUOTES and PIPES_AS_CONCAT)
    pub fn ansi() -> Self {
        Self { pipes_as_concat: true, ansi_quotes: true, ..Default::default() }
    }

    /// Create MySqlModeFlags with SQLite division semantics enabled
    ///
    /// This is useful for SQLLogicTest compatibility where MySQL syntax is needed
    /// but division should behave like SQLite (INTEGER / INTEGER → INTEGER).
    pub fn with_sqlite_division_semantics() -> Self {
        Self { sqlite_division_semantics: true, ..Default::default() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_mysql_flags() {
        let flags = MySqlModeFlags::default();
        assert!(!flags.pipes_as_concat); // || is OR by default
        assert!(!flags.ansi_quotes); // " is string by default
        assert!(!flags.strict_mode); // Permissive by default
    }

    #[test]
    fn test_new_equals_default() {
        assert_eq!(MySqlModeFlags::new(), MySqlModeFlags::default());
    }

    #[test]
    fn test_with_pipes_as_concat() {
        let flags = MySqlModeFlags::with_pipes_as_concat();
        assert!(flags.pipes_as_concat);
        assert!(!flags.ansi_quotes);
        assert!(!flags.strict_mode);
    }

    #[test]
    fn test_with_ansi_quotes() {
        let flags = MySqlModeFlags::with_ansi_quotes();
        assert!(!flags.pipes_as_concat);
        assert!(flags.ansi_quotes);
        assert!(!flags.strict_mode);
    }

    #[test]
    fn test_with_strict_mode() {
        let flags = MySqlModeFlags::with_strict_mode();
        assert!(!flags.pipes_as_concat);
        assert!(!flags.ansi_quotes);
        assert!(flags.strict_mode);
    }

    #[test]
    fn test_ansi_mode() {
        let flags = MySqlModeFlags::ansi();
        assert!(flags.pipes_as_concat);
        assert!(flags.ansi_quotes);
        assert!(!flags.strict_mode);
    }

    #[test]
    fn test_flag_combinations() {
        let flags = MySqlModeFlags {
            pipes_as_concat: true,
            ansi_quotes: true,
            strict_mode: true,
            sqlite_division_semantics: false,
        };
        assert!(flags.pipes_as_concat);
        assert!(flags.ansi_quotes);
        assert!(flags.strict_mode);
        assert!(!flags.sqlite_division_semantics);
    }

    #[test]
    fn test_with_sqlite_division_semantics() {
        let flags = MySqlModeFlags::with_sqlite_division_semantics();
        assert!(!flags.pipes_as_concat);
        assert!(!flags.ansi_quotes);
        assert!(!flags.strict_mode);
        assert!(flags.sqlite_division_semantics);
    }
}
