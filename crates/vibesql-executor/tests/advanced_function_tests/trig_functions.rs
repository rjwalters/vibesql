//! Tests for trigonometric functions (SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, RADIANS, DEGREES)

use crate::common::create_test_evaluator;

#[test]
fn test_sin_function() {
    let (evaluator, row) = create_test_evaluator();

    // sin(0) = 0
    let expr = vibesql_ast::Expression::Function {
        name: "SIN".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(0.0));
}

#[test]
fn test_cos_function() {
    let (evaluator, row) = create_test_evaluator();

    // cos(0) = 1
    let expr = vibesql_ast::Expression::Function {
        name: "COS".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(1.0));
}

#[test]
fn test_tan_function() {
    let (evaluator, row) = create_test_evaluator();

    // tan(0) = 0
    let expr = vibesql_ast::Expression::Function {
        name: "TAN".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(0.0));
}

#[test]
fn test_asin_function() {
    let (evaluator, row) = create_test_evaluator();

    // asin(0.5) ≈ 0.5236 (30 degrees in radians)
    let expr = vibesql_ast::Expression::Function {
        name: "ASIN".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(0.5))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 0.5236).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_acos_function() {
    let (evaluator, row) = create_test_evaluator();

    // acos(0.5) ≈ 1.0472 (60 degrees in radians)
    let expr = vibesql_ast::Expression::Function {
        name: "ACOS".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(0.5))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 1.0472).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_atan_function() {
    let (evaluator, row) = create_test_evaluator();

    // atan(1) ≈ 0.7854 (45 degrees in radians)
    let expr = vibesql_ast::Expression::Function {
        name: "ATAN".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 0.7854).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_atan2_function() {
    let (evaluator, row) = create_test_evaluator();

    // atan2(1, 1) ≈ 0.7854 (45 degrees)
    let expr = vibesql_ast::Expression::Function {
        name: "ATAN2".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 0.7854).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_radians_function() {
    let (evaluator, row) = create_test_evaluator();

    // 180 degrees = π radians
    let expr = vibesql_ast::Expression::Function {
        name: "RADIANS".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(180))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - std::f64::consts::PI).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_degrees_function() {
    let (evaluator, row) = create_test_evaluator();

    // π radians = 180 degrees
    let expr = vibesql_ast::Expression::Function {
        name: "DEGREES".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(
            std::f64::consts::PI,
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 180.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}
