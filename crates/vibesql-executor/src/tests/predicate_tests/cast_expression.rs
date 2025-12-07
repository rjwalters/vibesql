//! CAST expression tests
//!
//! Tests for SQL CAST(expr AS type) expressions:
//! - Integer to VARCHAR conversion
//! - VARCHAR to Integer conversion
//! - NULL casting (preserves NULL)
//! - Type coercion behavior

use super::super::super::*;

#[test]
fn test_cast_integer_to_varchar() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(123 AS VARCHAR(10))
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    123,
                ))),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(10) },
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("123")));
}

#[test]
fn test_cast_varchar_to_integer() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST('456' AS INTEGER)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("456")))),
                data_type: vibesql_types::DataType::Integer,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(456));
}

#[test]
fn test_cast_null() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(NULL AS INTEGER)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
                data_type: vibesql_types::DataType::Integer,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Null);
}

#[test]
fn test_cast_integer_to_unsigned() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(42 AS UNSIGNED)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    42,
                ))),
                data_type: vibesql_types::DataType::Unsigned,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Unsigned(42));
}

#[test]
fn test_cast_negative_integer_to_unsigned() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(-1 AS UNSIGNED) - should wrap around (MySQL behavior)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    -1,
                ))),
                data_type: vibesql_types::DataType::Unsigned,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // -1 as u64 wraps to u64::MAX
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Unsigned(u64::MAX));
}

#[test]
fn test_cast_varchar_to_unsigned() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST('123' AS UNSIGNED)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("123")))),
                data_type: vibesql_types::DataType::Unsigned,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Unsigned(123));
}

#[test]
fn test_cast_float_to_unsigned() {
    let mut db = vibesql_storage::Database::new();
    // Use MySQL mode for rounding behavior
    db.set_sql_mode(vibesql_types::SqlMode::MySQL {
        flags: vibesql_types::MySqlModeFlags::default(),
    });
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(5.7 AS UNSIGNED) - should round to nearest (MySQL behavior)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Float(
                    5.7,
                ))),
                data_type: vibesql_types::DataType::Unsigned,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // MySQL rounds to nearest integer when casting to UNSIGNED
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Unsigned(6));
}

#[test]
fn test_cast_as_signed_positive() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(42 AS SIGNED) - should produce Bigint
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    42,
                ))),
                data_type: vibesql_types::DataType::Bigint,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(42));

    // Verify display format (no decimal point)
    assert_eq!(format!("{}", result[0].values[0]), "42");
}

#[test]
fn test_cast_as_signed_negative() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(-4 AS SIGNED) - should produce Bigint with integer format
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    -4,
                ))),
                data_type: vibesql_types::DataType::Bigint,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(-4));

    // Verify display format (no decimal point)
    assert_eq!(format!("{}", result[0].values[0]), "-4");
}

#[test]
fn test_cast_as_signed_from_float() {
    let mut db = vibesql_storage::Database::new();
    // Use MySQL mode for rounding behavior
    db.set_sql_mode(vibesql_types::SqlMode::MySQL {
        flags: vibesql_types::MySqlModeFlags::default(),
    });
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(5.7 AS SIGNED) - should round to Bigint (MySQL behavior)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Float(
                    5.7,
                ))),
                data_type: vibesql_types::DataType::Bigint,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // MySQL rounds to nearest integer when casting to SIGNED
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(6));

    // Verify display format (no decimal point)
    assert_eq!(format!("{}", result[0].values[0]), "6");
}

#[test]
fn test_cast_negative_float_to_signed_rounds() {
    let mut db = vibesql_storage::Database::new();
    // Use MySQL mode for rounding behavior
    db.set_sql_mode(vibesql_types::SqlMode::MySQL {
        flags: vibesql_types::MySqlModeFlags::default(),
    });
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(-0.7 AS SIGNED) - should round to -1 (MySQL behavior)
    // This is the exact case from issue #2639
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Numeric(
                    -0.7,
                ))),
                data_type: vibesql_types::DataType::Bigint,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // MySQL rounds -0.7 to -1 (away from zero)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(-1));
}

#[test]
fn test_cast_rounding_edge_cases() {
    let mut db = vibesql_storage::Database::new();
    // Use MySQL mode for rounding behavior
    db.set_sql_mode(vibesql_types::SqlMode::MySQL {
        flags: vibesql_types::MySqlModeFlags::default(),
    });
    let executor = SelectExecutor::new(&db);

    // Helper to run CAST and extract result
    let cast_to_signed = |value: f64| -> i64 {
        let stmt = vibesql_ast::SelectStmt {
            with_clause: None,
            set_operation: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Cast {
                    expr: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Numeric(value),
                    )),
                    data_type: vibesql_types::DataType::Bigint,
                },
                alias: Some("result".to_string()),
            }],
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            into_table: None,
            into_variables: None,
        };
        let result = executor.execute(&stmt).unwrap();
        match &result[0].values[0] {
            vibesql_types::SqlValue::Bigint(n) => *n,
            _ => panic!("Expected Bigint"),
        }
    };

    // Test positive rounding
    assert_eq!(cast_to_signed(0.4), 0); // rounds down
    assert_eq!(cast_to_signed(0.5), 1); // rounds up at 0.5
    assert_eq!(cast_to_signed(0.7), 1); // rounds up

    // Test negative rounding (Rust's round() rounds away from zero)
    assert_eq!(cast_to_signed(-0.4), 0); // rounds toward zero
    assert_eq!(cast_to_signed(-0.5), -1); // rounds away from zero at -0.5
    assert_eq!(cast_to_signed(-0.7), -1); // rounds away from zero
}
