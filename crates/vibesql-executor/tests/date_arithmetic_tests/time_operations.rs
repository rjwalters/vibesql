//! Tests for time arithmetic operations (HOUR, MINUTE, SECOND calculations)

use crate::common::create_test_evaluator;

// ==================== TIME COMPONENT TESTS ====================

#[test]
fn test_date_add_hours_across_midnight() {
    let (evaluator, row) = create_test_evaluator();

    // 11 PM + 2 hours → 1 AM next day
    let expr = vibesql_ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(
                "2024-01-15 23:00:00".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("HOUR"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Timestamp("2024-01-16 01:00:00".parse().unwrap()));
}

#[test]
fn test_date_add_minutes_overflow() {
    let (evaluator, row) = create_test_evaluator();

    // 90 minutes from 11:30 → 1:00 PM
    let expr = vibesql_ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(
                "2024-01-15 11:30:00".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(90)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("MINUTE"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Timestamp("2024-01-15 13:00:00".parse().unwrap()));
}

#[test]
fn test_date_add_seconds_overflow() {
    let (evaluator, row) = create_test_evaluator();

    // 3661 seconds (1 hour, 1 minute, 1 second)
    let expr = vibesql_ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(
                "2024-01-15 10:30:30".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3661)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("SECOND"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Timestamp("2024-01-15 11:31:31".parse().unwrap()));
}

#[test]
fn test_date_sub_hours_across_midnight() {
    let (evaluator, row) = create_test_evaluator();

    // 1 AM - 2 hours → 11 PM previous day
    let expr = vibesql_ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(
                "2024-01-16 01:00:00".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("HOUR"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Timestamp("2024-01-15 23:00:00".parse().unwrap()));
}
