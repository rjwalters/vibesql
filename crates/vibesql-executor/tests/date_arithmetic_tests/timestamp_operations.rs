//! Tests for timestamp operations with time components

use crate::common::create_test_evaluator;

#[test]
fn test_datediff_with_timestamps() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(
                "2024-01-10 15:30:00".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(
                "2024-01-05 08:00:00".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(5)); // Time component ignored
}

#[test]
fn test_date_add_timestamp_with_time() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(
                "2024-01-15 14:30:00".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("HOUR"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Timestamp("2024-01-15 16:30:00".parse().unwrap()));
}

#[test]
fn test_extract_hour_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("HOUR"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(
                "2024-03-15 14:30:45".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(14));
}
