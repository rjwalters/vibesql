//! Tests for complex nested expressions combining multiple functions

use crate::common::create_test_evaluator;

#[test]
fn test_trig_with_pi() {
    let (evaluator, row) = create_test_evaluator();

    // SIN(PI()) should be approximately 0
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("SIN"),
        args: vec![vibesql_ast::Expression::Function {
            name: vibesql_ast::FunctionIdentifier::new("PI"),
            args: vec![],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!(val.abs() < 0.0001); // sin(π) ≈ 0
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_radians_degrees_roundtrip() {
    let (evaluator, row) = create_test_evaluator();

    // DEGREES(RADIANS(90)) should equal 90
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("DEGREES"),
        args: vec![vibesql_ast::Expression::Function {
            name: vibesql_ast::FunctionIdentifier::new("RADIANS"),
            args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(90))],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 90.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_exp_ln_roundtrip() {
    let (evaluator, row) = create_test_evaluator();

    // LN(EXP(2)) should equal 2
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LN"),
        args: vec![vibesql_ast::Expression::Function {
            name: vibesql_ast::FunctionIdentifier::new("EXP"),
            args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2))],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 2.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_greatest_with_abs() {
    let (evaluator, row) = create_test_evaluator();

    // GREATEST(ABS(-5), ABS(3)) should be 5
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("GREATEST"),
        args: vec![
            vibesql_ast::Expression::Function {
                name: vibesql_ast::FunctionIdentifier::new("ABS"),
                args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(-5))],
                character_unit: None,
            },
            vibesql_ast::Expression::Function {
                name: vibesql_ast::FunctionIdentifier::new("ABS"),
                args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3))],
                character_unit: None,
            },
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(5));
}
