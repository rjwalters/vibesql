//! Tests for advanced math functions (EXP, LN, LOG, SIGN, PI)

use crate::common::create_test_evaluator;

#[test]
fn test_exp_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "EXP".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // e^1 ≈ 2.718281828
    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 2.718281828).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_ln_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "LN".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(2.718281828))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // ln(e) ≈ 1.0
    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 1.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_log_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "LOG".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(2.718281828))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    // LOG is alias for LN
    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 1.0).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

#[test]
fn test_log10_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "LOG10".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(100))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(2.0)); // log10(100) = 2
}

#[test]
fn test_sign_positive() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "SIGN".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(1));
}

#[test]
fn test_sign_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "SIGN".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(-42))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(-1));
}

#[test]
fn test_sign_zero() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "SIGN".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_pi_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "PI".to_string(),
        args: vec![],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();

    if let vibesql_types::SqlValue::Double(val) = result {
        assert!((val - 3.14159265).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}
