//! Tests for conditional functions (GREATEST, LEAST, IF)

use crate::common::create_test_evaluator;

#[test]
fn test_greatest_integers() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "GREATEST".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(7)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(10));
}

#[test]
fn test_greatest_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "GREATEST".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(10)); // NULL is ignored
}

#[test]
fn test_least_integers() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "LEAST".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(7)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_least_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "LEAST".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3)); // NULL is ignored
}

#[test]
fn test_if_true_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("yes"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("no"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("yes")));
}

#[test]
fn test_if_false_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("yes"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("no"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("no")));
}

#[test]
fn test_if_null_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("yes"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("no"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("no"))); // NULL treated as false
}
