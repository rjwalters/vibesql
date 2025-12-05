//! Tests for CREATE/DROP ASSERTION (SQL:1999 Feature F671/F672)

use vibesql_executor::advanced_objects;
use vibesql_storage::Database;

#[test]
fn test_create_assertion_success() {
    let mut db = Database::new();
    let stmt = vibesql_ast::CreateAssertionStmt {
        assertion_name: "valid_balance".to_string(),
        check_condition: Box::new(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::GreaterThanOrEqual,
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("accounts".to_string()),
                column: "balance".to_string(),
            }),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0))),
        }),
    };

    let result = advanced_objects::execute_create_assertion(&stmt, &mut db);
    assert!(result.is_ok());
    assert!(db.catalog.assertion_exists("valid_balance"));
}

#[test]
fn test_create_assertion_duplicate() {
    let mut db = Database::new();
    let stmt = vibesql_ast::CreateAssertionStmt {
        assertion_name: "valid_balance".to_string(),
        check_condition: Box::new(vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Boolean(true),
        )),
    };

    // Create first time - should succeed
    advanced_objects::execute_create_assertion(&stmt, &mut db).unwrap();

    // Create second time - should fail
    let result = advanced_objects::execute_create_assertion(&stmt, &mut db);
    assert!(result.is_err());
}

#[test]
fn test_drop_assertion_success() {
    let mut db = Database::new();

    // Create an assertion first
    let create_stmt = vibesql_ast::CreateAssertionStmt {
        assertion_name: "valid_balance".to_string(),
        check_condition: Box::new(vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Boolean(true),
        )),
    };
    advanced_objects::execute_create_assertion(&create_stmt, &mut db).unwrap();

    // Drop it
    let drop_stmt = vibesql_ast::DropAssertionStmt {
        assertion_name: "valid_balance".to_string(),
        cascade: false,
    };
    let result = advanced_objects::execute_drop_assertion(&drop_stmt, &mut db);

    assert!(result.is_ok());
    assert!(!db.catalog.assertion_exists("valid_balance"));
}

#[test]
fn test_drop_assertion_with_cascade() {
    let mut db = Database::new();

    // Create an assertion first
    let create_stmt = vibesql_ast::CreateAssertionStmt {
        assertion_name: "valid_data".to_string(),
        check_condition: Box::new(vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Boolean(true),
        )),
    };
    advanced_objects::execute_create_assertion(&create_stmt, &mut db).unwrap();

    // Drop with CASCADE
    let drop_stmt =
        vibesql_ast::DropAssertionStmt { assertion_name: "valid_data".to_string(), cascade: true };
    let result = advanced_objects::execute_drop_assertion(&drop_stmt, &mut db);

    assert!(result.is_ok());
    assert!(!db.catalog.assertion_exists("valid_data"));
}

#[test]
fn test_drop_assertion_not_found() {
    let mut db = Database::new();
    let drop_stmt = vibesql_ast::DropAssertionStmt {
        assertion_name: "nonexistent".to_string(),
        cascade: false,
    };

    let result = advanced_objects::execute_drop_assertion(&drop_stmt, &mut db);
    assert!(result.is_err());
}

#[test]
fn test_create_drop_round_trip() {
    let mut db = Database::new();

    // Create assertion
    let create_stmt = vibesql_ast::CreateAssertionStmt {
        assertion_name: "positive_values".to_string(),
        check_condition: Box::new(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::GreaterThan,
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("data".to_string()),
                column: "value".to_string(),
            }),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0))),
        }),
    };
    advanced_objects::execute_create_assertion(&create_stmt, &mut db).unwrap();

    // Verify it exists
    assert!(db.catalog.assertion_exists("positive_values"));
    let assertion = db.catalog.get_assertion("positive_values");
    assert!(assertion.is_some());
    assert_eq!(assertion.unwrap().name, "positive_values");

    // Drop it
    let drop_stmt = vibesql_ast::DropAssertionStmt {
        assertion_name: "positive_values".to_string(),
        cascade: false,
    };
    advanced_objects::execute_drop_assertion(&drop_stmt, &mut db).unwrap();

    // Verify it's gone
    assert!(!db.catalog.assertion_exists("positive_values"));
    assert!(db.catalog.get_assertion("positive_values").is_none());
}

#[test]
fn test_multiple_assertions() {
    let mut db = Database::new();

    // Create multiple assertions
    let assertions = vec!["check_balance", "check_age", "check_status"];
    for name in &assertions {
        let stmt = vibesql_ast::CreateAssertionStmt {
            assertion_name: name.to_string(),
            check_condition: Box::new(vibesql_ast::Expression::Literal(
                vibesql_types::SqlValue::Boolean(true),
            )),
        };
        advanced_objects::execute_create_assertion(&stmt, &mut db).unwrap();
    }

    // Verify all exist
    for name in &assertions {
        assert!(db.catalog.assertion_exists(name));
    }

    // Drop one
    let drop_stmt =
        vibesql_ast::DropAssertionStmt { assertion_name: "check_age".to_string(), cascade: false };
    advanced_objects::execute_drop_assertion(&drop_stmt, &mut db).unwrap();

    // Verify check_age is gone, others still exist
    assert!(db.catalog.assertion_exists("check_balance"));
    assert!(!db.catalog.assertion_exists("check_age"));
    assert!(db.catalog.assertion_exists("check_status"));
}

#[test]
fn test_assertion_with_complex_condition() {
    let mut db = Database::new();

    // Create assertion with complex condition
    let stmt = vibesql_ast::CreateAssertionStmt {
        assertion_name: "complex_check".to_string(),
        check_condition: Box::new(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::And,
            left: Box::new(vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::GreaterThan,
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "age".to_string(),
                }),
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(18),
                )),
            }),
            right: Box::new(vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::LessThan,
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "age".to_string(),
                }),
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(100),
                )),
            }),
        }),
    };

    let result = advanced_objects::execute_create_assertion(&stmt, &mut db);
    assert!(result.is_ok());
    assert!(db.catalog.assertion_exists("complex_check"));

    // Verify the assertion was stored correctly
    let assertion = db.catalog.get_assertion("complex_check").unwrap();
    assert_eq!(assertion.name, "complex_check");
}

#[test]
fn test_in_operator_empty_list() {
    use vibesql_executor::SelectExecutor;
    use vibesql_parser::Parser;

    let db = Database::new();

    // Test empty IN lists (allowed per SQL:1999)
    // Empty IN list should evaluate to FALSE
    // Empty NOT IN list should evaluate to TRUE
    let test_cases = vec![
        ("SELECT 1 IN ()", "false"),
        ("SELECT 1 NOT IN ()", "true"),
        ("SELECT 1 IN (2)", "false"),
        ("SELECT 1 NOT IN (2)", "true"),
        ("SELECT 1 IN (1)", "true"),
        ("SELECT 1 NOT IN (1)", "false"),
    ];

    for (sql, expected_desc) in test_cases {
        let expected = match expected_desc {
            "true" => true,
            "false" => false,
            _ => panic!("Invalid expected value"),
        };

        let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&db);
                let rows = executor.execute(&select_stmt).expect("Failed to execute query");

                assert_eq!(rows.len(), 1, "Should return one row");
                assert_eq!(rows[0].values.len(), 1, "Should return one column");

                match &rows[0].values[0] {
                    vibesql_types::SqlValue::Boolean(actual) if *actual == expected => (),
                    other => {
                        panic!("Query '{}' expected Boolean({}), got {:?}", sql, expected, other)
                    }
                }
            }
            _ => panic!("Not a SELECT statement"),
        }
    }
}
