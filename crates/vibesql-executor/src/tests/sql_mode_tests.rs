//! Tests for SET SQL_MODE statement and runtime dialect switching
//!
//! Issue #2658: Implement SET SQL_MODE statement for runtime dialect switching

#[cfg(test)]
mod tests {
    use crate::{SchemaExecutor, SelectExecutor};
    use vibesql_parser::Parser;
    use vibesql_storage::Database;
    use vibesql_types::SqlMode;

    /// Helper to execute a SET variable statement
    fn execute_set_variable(
        db: &mut Database,
        sql: &str,
    ) -> Result<String, crate::errors::ExecutorError> {
        let stmt = Parser::parse_sql(sql).expect("Failed to parse SET statement");
        if let vibesql_ast::Statement::SetVariable(set_stmt) = stmt {
            SchemaExecutor::execute_set_variable(&set_stmt, db)
        } else {
            panic!("Expected SetVariable statement, got {:?}", stmt);
        }
    }

    /// Helper to execute a SELECT and return the first column of the first row
    fn select_single_value(db: &Database, sql: &str) -> vibesql_types::SqlValue {
        let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(db);
            let rows = executor.execute(&select_stmt).expect("Failed to execute SELECT");
            rows[0].values[0].clone()
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_set_sql_mode_mysql() {
        let mut db = Database::new();

        // Default is MySQL mode (for SQLLogicTest compatibility - dolthub corpus was regenerated against MySQL 8.x)
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

        // Set to sqlite
        execute_set_variable(&mut db, "SET sql_mode = 'sqlite'").unwrap();
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));

        // Set back to mysql
        let result = execute_set_variable(&mut db, "SET sql_mode = 'mysql'").unwrap();
        assert!(result.contains("mysql"));
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_set_sql_mode_sqlite() {
        let mut db = Database::new();

        let result = execute_set_variable(&mut db, "SET sql_mode = 'sqlite'").unwrap();
        assert!(result.contains("sqlite"));
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));
    }

    #[test]
    fn test_set_sql_mode_case_insensitive() {
        let mut db = Database::new();

        // Test uppercase
        execute_set_variable(&mut db, "SET sql_mode = 'MYSQL'").unwrap();
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

        execute_set_variable(&mut db, "SET sql_mode = 'SQLITE'").unwrap();
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));

        // Test mixed case
        execute_set_variable(&mut db, "SET sql_mode = 'MySql'").unwrap();
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_set_sql_mode_invalid() {
        let mut db = Database::new();

        // Note: MySQL-like mode names (alphanumeric with underscores) are now accepted
        // for forward compatibility with future MySQL versions.
        // Only strings with special characters are rejected.
        let result = execute_set_variable(&mut db, "SET sql_mode = 'invalid!@#'");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("Unknown SQL mode"));
    }

    #[test]
    fn test_set_sql_mode_mysql_flags() {
        let mut db = Database::new();

        // Test MySQL comma-separated mode flags (issue #3074)
        let result = execute_set_variable(
            &mut db,
            "SET sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'",
        );
        assert!(result.is_ok());
        match db.sql_mode() {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_set_sql_mode_leading_comma() {
        let mut db = Database::new();

        // Test leading comma (from REPLACE() removing first mode like ONLY_FULL_GROUP_BY)
        // This is the actual bug from issue #3074
        let result = execute_set_variable(
            &mut db,
            "SET sql_mode = ',STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'",
        );
        assert!(result.is_ok());
        match db.sql_mode() {
            SqlMode::MySQL { flags } => {
                assert!(flags.strict_mode);
            }
            _ => panic!("Expected MySQL mode"),
        }
    }

    #[test]
    fn test_select_session_variable_sql_mode() {
        let mut db = Database::new();

        // Set to SQLite mode
        execute_set_variable(&mut db, "SET sql_mode = 'sqlite'").unwrap();

        // Query the session variable
        let value = select_single_value(&db, "SELECT @@sql_mode");

        // The session variable should reflect the mode change
        match value {
            vibesql_types::SqlValue::Varchar(s) => {
                // SQLite mode should be reflected in the session variable
                assert!(s.to_lowercase().contains("sqlite"));
            }
            _ => panic!("Expected Varchar value, got {:?}", value),
        }
    }

    #[test]
    fn test_division_behavior_changes_with_mode() {
        let mut db = Database::new();

        // MySQL mode: 5 / 2 = 2.5 (decimal division)
        execute_set_variable(&mut db, "SET sql_mode = 'mysql'").unwrap();
        let mysql_result = select_single_value(&db, "SELECT 5 / 2");

        // Should be a decimal value close to 2.5
        match mysql_result {
            vibesql_types::SqlValue::Numeric(n) => {
                let f: f64 = n.to_string().parse().unwrap();
                assert!((f - 2.5).abs() < 0.0001, "MySQL division 5/2 should be 2.5, got {}", f);
            }
            vibesql_types::SqlValue::Real(f) => {
                assert!((f - 2.5).abs() < 0.0001, "MySQL division 5/2 should be 2.5, got {}", f);
            }
            other => {
                // Accept any numeric-ish result
                let s = other.to_string();
                let f: f64 = s.parse().unwrap_or(0.0);
                assert!((f - 2.5).abs() < 0.0001, "MySQL division 5/2 should be 2.5, got {}", s);
            }
        }

        // SQLite mode: 5 / 2 = 2 (integer division)
        execute_set_variable(&mut db, "SET sql_mode = 'sqlite'").unwrap();
        let sqlite_result = select_single_value(&db, "SELECT 5 / 2");

        // Should be an integer value of 2
        match sqlite_result {
            vibesql_types::SqlValue::Integer(i) => {
                assert_eq!(i, 2, "SQLite division 5/2 should be 2, got {}", i);
            }
            other => {
                let s = other.to_string();
                let i: i64 = s.parse().unwrap_or(0);
                assert_eq!(i, 2, "SQLite division 5/2 should be 2, got {}", s);
            }
        }
    }

    #[test]
    fn test_set_sql_mode_variable_name_case_insensitive() {
        let mut db = Database::new();

        // Variable name should be case insensitive
        execute_set_variable(&mut db, "SET SQL_MODE = 'sqlite'").unwrap();
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));

        execute_set_variable(&mut db, "SET Sql_Mode = 'mysql'").unwrap();
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_cast_float_to_integer_mysql_rounds() {
        let mut db = Database::new();

        // MySQL mode: CAST rounds to nearest integer
        execute_set_variable(&mut db, "SET sql_mode = 'mysql'").unwrap();

        // 5.7 rounds to 6
        let result = select_single_value(&db, "SELECT CAST(5.7 AS SIGNED)");
        match result {
            vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, 6, "MySQL: CAST(5.7 AS SIGNED) should be 6, got {}", n)
            }
            vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, 6, "MySQL: CAST(5.7 AS SIGNED) should be 6, got {}", n)
            }
            other => panic!("Expected integer type, got {:?}", other),
        }

        // 5.4 rounds to 5
        let result = select_single_value(&db, "SELECT CAST(5.4 AS SIGNED)");
        match result {
            vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, 5, "MySQL: CAST(5.4 AS SIGNED) should be 5, got {}", n)
            }
            vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, 5, "MySQL: CAST(5.4 AS SIGNED) should be 5, got {}", n)
            }
            other => panic!("Expected integer type, got {:?}", other),
        }

        // 5.5 rounds to 6 (banker's rounding rounds to nearest even, but MySQL uses round half up)
        let result = select_single_value(&db, "SELECT CAST(5.5 AS SIGNED)");
        match result {
            vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, 6, "MySQL: CAST(5.5 AS SIGNED) should be 6, got {}", n)
            }
            vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, 6, "MySQL: CAST(5.5 AS SIGNED) should be 6, got {}", n)
            }
            other => panic!("Expected integer type, got {:?}", other),
        }

        // Negative: -5.7 rounds to -6
        let result = select_single_value(&db, "SELECT CAST(-5.7 AS SIGNED)");
        match result {
            vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, -6, "MySQL: CAST(-5.7 AS SIGNED) should be -6, got {}", n)
            }
            vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, -6, "MySQL: CAST(-5.7 AS SIGNED) should be -6, got {}", n)
            }
            other => panic!("Expected integer type, got {:?}", other),
        }
    }

    #[test]
    fn test_cast_float_to_integer_sqlite_truncates() {
        let mut db = Database::new();

        // SQLite mode: CAST truncates toward zero
        execute_set_variable(&mut db, "SET sql_mode = 'sqlite'").unwrap();

        // 5.7 truncates to 5
        let result = select_single_value(&db, "SELECT CAST(5.7 AS INTEGER)");
        match result {
            vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, 5, "SQLite: CAST(5.7 AS INTEGER) should be 5, got {}", n)
            }
            vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, 5, "SQLite: CAST(5.7 AS INTEGER) should be 5, got {}", n)
            }
            other => panic!("Expected integer type, got {:?}", other),
        }

        // 5.4 truncates to 5
        let result = select_single_value(&db, "SELECT CAST(5.4 AS INTEGER)");
        match result {
            vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, 5, "SQLite: CAST(5.4 AS INTEGER) should be 5, got {}", n)
            }
            vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, 5, "SQLite: CAST(5.4 AS INTEGER) should be 5, got {}", n)
            }
            other => panic!("Expected integer type, got {:?}", other),
        }

        // 5.9 truncates to 5
        let result = select_single_value(&db, "SELECT CAST(5.9 AS INTEGER)");
        match result {
            vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, 5, "SQLite: CAST(5.9 AS INTEGER) should be 5, got {}", n)
            }
            vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, 5, "SQLite: CAST(5.9 AS INTEGER) should be 5, got {}", n)
            }
            other => panic!("Expected integer type, got {:?}", other),
        }

        // Negative: -5.7 truncates to -5 (toward zero)
        let result = select_single_value(&db, "SELECT CAST(-5.7 AS INTEGER)");
        match result {
            vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, -5, "SQLite: CAST(-5.7 AS INTEGER) should be -5, got {}", n)
            }
            vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, -5, "SQLite: CAST(-5.7 AS INTEGER) should be -5, got {}", n)
            }
            other => panic!("Expected integer type, got {:?}", other),
        }
    }

    #[test]
    fn test_cast_mode_switching() {
        let mut db = Database::new();

        // Start in MySQL mode
        execute_set_variable(&mut db, "SET sql_mode = 'mysql'").unwrap();
        let result = select_single_value(&db, "SELECT CAST(5.7 AS SIGNED)");
        match result {
            vibesql_types::SqlValue::Bigint(n) | vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, 6, "MySQL mode should round 5.7 to 6")
            }
            other => panic!("Expected integer type, got {:?}", other),
        }

        // Switch to SQLite mode
        execute_set_variable(&mut db, "SET sql_mode = 'sqlite'").unwrap();
        let result = select_single_value(&db, "SELECT CAST(5.7 AS INTEGER)");
        match result {
            vibesql_types::SqlValue::Integer(n) | vibesql_types::SqlValue::Bigint(n) => {
                assert_eq!(n, 5, "SQLite mode should truncate 5.7 to 5")
            }
            other => panic!("Expected integer type, got {:?}", other),
        }

        // Switch back to MySQL mode
        execute_set_variable(&mut db, "SET sql_mode = 'mysql'").unwrap();
        let result = select_single_value(&db, "SELECT CAST(5.7 AS SIGNED)");
        match result {
            vibesql_types::SqlValue::Bigint(n) | vibesql_types::SqlValue::Integer(n) => {
                assert_eq!(n, 6, "MySQL mode should round 5.7 to 6")
            }
            other => panic!("Expected integer type, got {:?}", other),
        }
    }
}
