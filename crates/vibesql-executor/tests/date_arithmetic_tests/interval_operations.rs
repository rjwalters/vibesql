//! Tests for interval operations (EXTRACT, AGE functions)

use crate::common::create_test_evaluator;

// ==================== EXTRACT TESTS ====================

#[test]
fn test_extract_year() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("YEAR"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2024-03-15".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(2024));
}

#[test]
fn test_extract_month() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("MONTH"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2024-03-15".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_extract_day() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("DAY"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2024-03-15".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(15));
}

// ==================== AGE TESTS ====================

#[test]
fn test_age_two_dates_years_only() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2024-01-15".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2020-01-15".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Varchar(age_str) = result {
        assert!(age_str.contains("4 years"));
    } else {
        panic!("Expected VARCHAR result from AGE");
    }
}

#[test]
fn test_age_two_dates_complex() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2024-05-20".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2022-02-15".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Varchar(age_str) = result {
        // Should be approximately 2 years 3 months 5 days
        assert!(age_str.contains("2 years"));
        assert!(age_str.contains("3 months"));
    } else {
        panic!("Expected VARCHAR result from AGE");
    }
}

#[test]
fn test_age_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2020-01-15".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2024-01-15".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Varchar(age_str) = result {
        // Should be negative
        assert!(age_str.starts_with('-'));
    } else {
        panic!("Expected VARCHAR result from AGE");
    }
}

#[test]
fn test_age_same_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "AGE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2024-01-15".parse().unwrap(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(
                "2024-01-15".parse().unwrap(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Varchar(age_str) = result {
        assert_eq!(age_str.as_str(), "0 days");
    } else {
        panic!("Expected VARCHAR result from AGE");
    }
}
