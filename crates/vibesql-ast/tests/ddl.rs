use vibesql_ast::*;

// ============================================================================
// DDL Tests - CREATE TABLE
// ============================================================================

#[test]
fn test_create_table_statement() {
    let stmt = Statement::CreateTable(CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(255) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    });

    match stmt {
        Statement::CreateTable(_) => {} // Success
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_column_def() {
    let col = ColumnDef {
        name: "email".to_string(),
        data_type: vibesql_types::DataType::Varchar { max_length: Some(100) },
        nullable: false,
        constraints: vec![],
        default_value: None,
        comment: None,
    };
    assert_eq!(col.name, "email");
    assert!(!col.nullable);
}

// ============================================================================
// DDL Tests - ASSERTION
// ============================================================================

#[test]
fn test_create_assertion_statement() {
    let stmt = Statement::CreateAssertion(CreateAssertionStmt {
        assertion_name: "valid_balance".to_string(),
        check_condition: Box::new(Expression::BinaryOp {
            op: BinaryOperator::GreaterThanOrEqual,
            left: Box::new(Expression::ColumnRef { table: None, column: "balance".to_string() }),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(0))),
        }),
    });

    match stmt {
        Statement::CreateAssertion(assertion) => {
            assert_eq!(assertion.assertion_name, "valid_balance");
        }
        _ => panic!("Expected CREATE ASSERTION statement"),
    }
}

#[test]
fn test_drop_assertion_restrict() {
    let stmt = Statement::DropAssertion(DropAssertionStmt {
        assertion_name: "old_constraint".to_string(),
        cascade: false, // RESTRICT
    });

    match stmt {
        Statement::DropAssertion(drop) => {
            assert_eq!(drop.assertion_name, "old_constraint");
            assert!(!drop.cascade);
        }
        _ => panic!("Expected DROP ASSERTION statement"),
    }
}

#[test]
fn test_drop_assertion_cascade() {
    let stmt = Statement::DropAssertion(DropAssertionStmt {
        assertion_name: "cascade_constraint".to_string(),
        cascade: true, // CASCADE
    });

    match stmt {
        Statement::DropAssertion(drop) => {
            assert_eq!(drop.assertion_name, "cascade_constraint");
            assert!(drop.cascade);
        }
        _ => panic!("Expected DROP ASSERTION statement"),
    }
}
