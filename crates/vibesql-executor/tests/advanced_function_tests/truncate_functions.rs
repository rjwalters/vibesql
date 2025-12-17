//! Tests for TRUNCATE function with various precisions and edge cases

use crate::common::create_test_evaluator;

#[test]
fn test_truncate_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("TRUNCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(3.14159)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // TRUNCATE(3.14159, 2) should be 3.14
    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 3.14).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_truncate_no_precision() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("TRUNCATE"),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(3.999))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // TRUNCATE(3.999) should be 3.0
    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 3.0).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_truncate_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("TRUNCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(-1.999)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // TRUNCATE(-1.999, 1) should be -1.9
    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - (-1.9)).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_truncate_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("TRUNCATE"),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_truncate_negative_precision() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("TRUNCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(123.456)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(-1)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // TRUNCATE(123.456, -1) should be 120.0
    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 120.0).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}
