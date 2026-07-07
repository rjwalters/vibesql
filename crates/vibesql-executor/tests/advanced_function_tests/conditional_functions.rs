//! Tests for conditional functions (GREATEST, LEAST, IF)

use crate::common::create_test_evaluator;

#[test]
fn test_greatest_integers() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("GREATEST"),
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
        name: vibesql_ast::FunctionIdentifier::new("GREATEST"),
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
        name: vibesql_ast::FunctionIdentifier::new("LEAST"),
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
        name: vibesql_ast::FunctionIdentifier::new("LEAST"),
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
        name: vibesql_ast::FunctionIdentifier::new("IF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("yes"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("no"),
            )),
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
        name: vibesql_ast::FunctionIdentifier::new("IF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("yes"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("no"),
            )),
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
        name: vibesql_ast::FunctionIdentifier::new("IF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("yes"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("no"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("no")));
    // NULL treated as false
}

#[test]
fn test_if_integer_condition_truthy() {
    let (evaluator, row) = create_test_evaluator();

    // if(1, 'yes', 'no') -> 'yes' (integer condition, SQLite truthiness)
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("IF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("yes"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("no"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("yes")));
}

#[test]
fn test_if_integer_condition_falsy() {
    let (evaluator, row) = create_test_evaluator();

    // if(0, 'yes', 'no') -> 'no'
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("IF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("yes"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("no"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("no")));
}

#[test]
fn test_if_variadic_case_chain() {
    let (evaluator, row) = create_test_evaluator();

    // if(false, 'a', true, 'b', 'else') -> 'b' (second branch matches)
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("IF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("a"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("b"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("else"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("b")));
}

#[test]
fn test_if_variadic_falls_through_to_else() {
    let (evaluator, row) = create_test_evaluator();

    // if(false, 'a', false, 'b', 'else') -> 'else' (no branch matches)
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("IF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("a"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("b"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("else"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("else")));
}

#[test]
fn test_iif_ternary() {
    let (evaluator, row) = create_test_evaluator();

    // iif(1, 'x', 'y') -> 'x'
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("IIF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("x"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("y"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("x")));
}

#[test]
fn test_iif_variadic_case_chain() {
    let (evaluator, row) = create_test_evaluator();

    // iif(0, 'a', 5, 'b', 'else') -> 'b' (second condition non-zero -> truthy)
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("IIF"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("a"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("b"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("else"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("b")));
}
