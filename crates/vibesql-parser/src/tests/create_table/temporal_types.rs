use super::super::*;

// ========================================================================
// Temporal Data Types - TIME, TIMESTAMP, and INTERVAL Types
// ========================================================================

#[test]
fn test_parse_create_table_time_without_timezone() {
    let result = Parser::parse_sql("CREATE TABLE events (start_time TIME);");
    assert!(result.is_ok(), "Should parse TIME");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                vibesql_types::DataType::Time { with_timezone: false } => {} // Success
                _ => {
                    panic!("Expected TIME without timezone, got {:?}", create.columns[0].data_type)
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_time_with_timezone() {
    let result = Parser::parse_sql("CREATE TABLE events (start_time TIME WITH TIME ZONE);");
    assert!(result.is_ok(), "Should parse TIME WITH TIME ZONE");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                vibesql_types::DataType::Time { with_timezone: true } => {} // Success
                _ => panic!("Expected TIME WITH TIME ZONE, got {:?}", create.columns[0].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_time_without_timezone_explicit() {
    let result = Parser::parse_sql("CREATE TABLE events (start_time TIME WITHOUT TIME ZONE);");
    if let Err(ref e) = result {
        eprintln!("Parse error: {:?}", e);
    }
    assert!(result.is_ok(), "Should parse TIME WITHOUT TIME ZONE");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                vibesql_types::DataType::Time { with_timezone: false } => {} // Success
                _ => {
                    panic!("Expected TIME WITHOUT TIME ZONE, got {:?}", create.columns[0].data_type)
                }
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_timestamp_types() {
    let result = Parser::parse_sql(
        "CREATE TABLE logs (
            created TIMESTAMP,
            modified TIMESTAMP WITH TIME ZONE,
            deleted TIMESTAMP WITHOUT TIME ZONE
        );",
    );
    assert!(result.is_ok(), "Should parse all TIMESTAMP variants");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            match create.columns[0].data_type {
                vibesql_types::DataType::Timestamp { with_timezone: false } => {} // Default
                _ => panic!(
                    "Expected TIMESTAMP without timezone, got {:?}",
                    create.columns[0].data_type
                ),
            }

            match create.columns[1].data_type {
                vibesql_types::DataType::Timestamp { with_timezone: true } => {} // Success
                _ => panic!(
                    "Expected TIMESTAMP WITH TIME ZONE, got {:?}",
                    create.columns[1].data_type
                ),
            }

            match create.columns[2].data_type {
                vibesql_types::DataType::Timestamp { with_timezone: false } => {} // Success
                _ => panic!(
                    "Expected TIMESTAMP WITHOUT TIME ZONE, got {:?}",
                    create.columns[2].data_type
                ),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_interval_single_field() {
    let result = Parser::parse_sql(
        "CREATE TABLE intervals (
            years INTERVAL YEAR,
            months INTERVAL MONTH,
            days INTERVAL DAY,
            hours INTERVAL HOUR,
            minutes INTERVAL MINUTE,
            seconds INTERVAL SECOND
        );",
    );
    assert!(result.is_ok(), "Should parse single-field intervals");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 6);

            match &create.columns[0].data_type {
                vibesql_types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, vibesql_types::IntervalField::Year));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL YEAR, got {:?}", create.columns[0].data_type),
            }

            match &create.columns[1].data_type {
                vibesql_types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, vibesql_types::IntervalField::Month));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL MONTH, got {:?}", create.columns[1].data_type),
            }

            match &create.columns[2].data_type {
                vibesql_types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, vibesql_types::IntervalField::Day));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL DAY, got {:?}", create.columns[2].data_type),
            }

            match &create.columns[3].data_type {
                vibesql_types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, vibesql_types::IntervalField::Hour));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL HOUR, got {:?}", create.columns[3].data_type),
            }

            match &create.columns[4].data_type {
                vibesql_types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, vibesql_types::IntervalField::Minute));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL MINUTE, got {:?}", create.columns[4].data_type),
            }

            match &create.columns[5].data_type {
                vibesql_types::DataType::Interval { start_field, end_field } => {
                    assert!(matches!(start_field, vibesql_types::IntervalField::Second));
                    assert!(end_field.is_none());
                }
                _ => panic!("Expected INTERVAL SECOND, got {:?}", create.columns[5].data_type),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_interval_year_to_month() {
    let result = Parser::parse_sql("CREATE TABLE test (duration INTERVAL YEAR TO MONTH);");
    assert!(result.is_ok(), "Should parse INTERVAL YEAR TO MONTH");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.columns[0].data_type {
            vibesql_types::DataType::Interval { start_field, end_field } => {
                assert!(matches!(start_field, vibesql_types::IntervalField::Year));
                assert!(matches!(end_field, Some(vibesql_types::IntervalField::Month)));
            }
            _ => panic!("Expected INTERVAL YEAR TO MONTH, got {:?}", create.columns[0].data_type),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_interval_day_to_second() {
    let result = Parser::parse_sql("CREATE TABLE test (duration INTERVAL DAY TO SECOND);");
    assert!(result.is_ok(), "Should parse INTERVAL DAY TO SECOND");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.columns[0].data_type {
            vibesql_types::DataType::Interval { start_field, end_field } => {
                assert!(matches!(start_field, vibesql_types::IntervalField::Day));
                assert!(matches!(end_field, Some(vibesql_types::IntervalField::Second)));
            }
            _ => panic!("Expected INTERVAL DAY TO SECOND, got {:?}", create.columns[0].data_type),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// Temporal Keywords as Column Names (Unreserved Keywords)
// ========================================================================

#[test]
fn test_temporal_keywords_as_column_names() {
    // Test that TIMESTAMP, DATE, TIME, and INTERVAL can be used as unquoted column names
    let result = Parser::parse_sql(
        "CREATE TABLE test (
            timestamp TEXT,
            date TEXT,
            time TEXT,
            interval INTEGER
        );",
    );
    assert!(result.is_ok(), "Should parse temporal keywords as column names: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 4);
            assert_eq!(create.columns[0].name, "TIMESTAMP");
            assert_eq!(create.columns[1].name, "DATE");
            assert_eq!(create.columns[2].name, "TIME");
            assert_eq!(create.columns[3].name, "INTERVAL");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_timestamp_column_with_constraints() {
    // Test from issue #1214 - TIMESTAMP with constraints
    let result = Parser::parse_sql(
        "CREATE TABLE test_runs (
            run_id INTEGER PRIMARY KEY,
            timestamp TEXT NOT NULL,
            workers INTEGER CHECK (workers > 0)
        );",
    );
    assert!(
        result.is_ok(),
        "Should parse TIMESTAMP as column name with constraints: {:?}",
        result.err()
    );
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);
            assert_eq!(create.columns[0].name, "RUN_ID");
            assert_eq!(create.columns[1].name, "TIMESTAMP");
            assert_eq!(create.columns[2].name, "WORKERS");

            // Verify timestamp column has NOT NULL
            assert!(!create.columns[1].nullable, "timestamp column should be NOT NULL");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_date_as_column_name() {
    let result = Parser::parse_sql("CREATE TABLE events (date TEXT);");
    assert!(result.is_ok(), "Should parse DATE as column name");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "DATE");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_time_as_column_name() {
    let result = Parser::parse_sql("CREATE TABLE logs (time TEXT);");
    assert!(result.is_ok(), "Should parse TIME as column name");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "TIME");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_interval_as_column_name() {
    let result = Parser::parse_sql("CREATE TABLE metrics (interval INTEGER);");
    assert!(result.is_ok(), "Should parse INTERVAL as column name");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].name, "INTERVAL");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// DATETIME Type Tests (MySQL/SQLite compatibility)
// ========================================================================

#[test]
fn test_parse_create_table_datetime() {
    let result = Parser::parse_sql("CREATE TABLE events (created_at DATETIME);");
    assert!(result.is_ok(), "Should parse DATETIME");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match create.columns[0].data_type {
                vibesql_types::DataType::Timestamp { with_timezone: false } => {} // Success - DATETIME maps to TIMESTAMP
                _ => panic!(
                    "Expected DATETIME to map to TIMESTAMP, got {:?}",
                    create.columns[0].data_type
                ),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_datetime_with_constraints() {
    // Test from issue #1619 - the exact statement that was failing
    let result = Parser::parse_sql("CREATE TABLE `t1f69e` (`c1` DATETIME KEY, c2 DATETIME NULL);");
    assert!(result.is_ok(), "Should parse DATETIME with constraints");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t1f69e");
            assert_eq!(create.columns.len(), 2);

            // Verify first column: c1 DATETIME KEY
            assert_eq!(create.columns[0].name, "c1");
            match create.columns[0].data_type {
                vibesql_types::DataType::Timestamp { with_timezone: false } => {} // Success
                _ => panic!(
                    "Expected c1 to be DATETIME (TIMESTAMP), got {:?}",
                    create.columns[0].data_type
                ),
            }

            // Verify second column: c2 DATETIME NULL (uppercased because not in backticks)
            assert_eq!(create.columns[1].name, "C2");
            match create.columns[1].data_type {
                vibesql_types::DataType::Timestamp { with_timezone: false } => {} // Success
                _ => panic!(
                    "Expected c2 to be DATETIME (TIMESTAMP), got {:?}",
                    create.columns[1].data_type
                ),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// YEAR Type Tests (MySQL compatibility)
// ========================================================================

#[test]
fn test_parse_create_table_year() {
    let result = Parser::parse_sql("CREATE TABLE events (year_column YEAR);");
    assert!(result.is_ok(), "Should parse YEAR");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } if type_name == "YEAR" => {} // Success
                _ => panic!(
                    "Expected YEAR to map to UserDefined type, got {:?}",
                    create.columns[0].data_type
                ),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_year_with_constraints() {
    // Test from issue #1627 - example statement from createtable1.test
    let result = Parser::parse_sql("CREATE TABLE `t20b4e` (`c1` YEAR KEY, c2 YEAR NULL);");
    assert!(result.is_ok(), "Should parse YEAR with constraints");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t20b4e");
            assert_eq!(create.columns.len(), 2);

            // Verify first column: c1 YEAR KEY
            assert_eq!(create.columns[0].name, "c1");
            match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } if type_name == "YEAR" => {} // Success
                _ => panic!(
                    "Expected c1 to be YEAR (UserDefined), got {:?}",
                    create.columns[0].data_type
                ),
            }

            // Verify second column: c2 YEAR NULL (uppercased because not in backticks)
            assert_eq!(create.columns[1].name, "C2");
            match &create.columns[1].data_type {
                vibesql_types::DataType::UserDefined { type_name } if type_name == "YEAR" => {} // Success
                _ => panic!(
                    "Expected c2 to be YEAR (UserDefined), got {:?}",
                    create.columns[1].data_type
                ),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
