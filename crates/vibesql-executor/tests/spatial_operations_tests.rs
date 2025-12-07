//! Spatial operation function tests

use vibesql_ast::Expression;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::ExpressionEvaluator;
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

/// Helper to create a simple schema for testing
fn create_test_schema() -> TableSchema {
    TableSchema::new(
        "test_table".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    )
}

/// Helper to evaluate an expression
fn eval_expr(expr: Expression) -> Result<SqlValue, String> {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = Row::new(vec![SqlValue::Integer(1)]);

    evaluator.eval(&expr, &row).map_err(|e| format!("{:?}", e))
}

#[test]
fn test_st_simplify_linestring() {
    // ST_Simplify with a line that has some minor detours
    // The original line goes: (0,0) -> (1,0.1) -> (2,0) -> (3,0)
    // With a high tolerance, it should simplify to: (0,0) -> (3,0)
    let expr = Expression::Function {
        name: "ST_ASTEXT".to_string(),
        args: vec![Expression::Function {
            name: "ST_SIMPLIFY".to_string(),
            args: vec![
                Expression::Function {
                    name: "ST_GEOMFROMTEXT".to_string(),
                    args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("LINESTRING(0 0, 1 0.1, 2 0, 3 0)")))],
                    character_unit: None,
                },
                Expression::Literal(SqlValue::Double(0.5)), // High tolerance
            ],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Varchar(s) | SqlValue::Character(s) = result {
        assert!(s.contains("LINESTRING"), "Expected LINESTRING, got {}", s);
    } else {
        panic!("Expected Varchar, got {:?}", result);
    }
}

#[test]
fn test_st_simplify_polygon() {
    // ST_Simplify on a polygon with small deviations
    let expr = Expression::Function {
        name: "ST_ASTEXT".to_string(),
        args: vec![Expression::Function {
            name: "ST_SIMPLIFY".to_string(),
            args: vec![
                Expression::Function {
                    name: "ST_GEOMFROMTEXT".to_string(),
                    args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 10 0, 10.1 5, 10 10, 0 10, 0 0))")))],
                    character_unit: None,
                },
                Expression::Literal(SqlValue::Double(1.0)), // Tolerance
            ],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Varchar(s) | SqlValue::Character(s) = result {
        assert!(s.contains("POLYGON"), "Expected POLYGON, got {}", s);
    } else {
        panic!("Expected Varchar, got {:?}", result);
    }
}

#[test]
fn test_st_simplify_null_handling() {
    let expr = Expression::Function {
        name: "ST_SIMPLIFY".to_string(),
        args: vec![Expression::Literal(SqlValue::Null), Expression::Literal(SqlValue::Double(0.1))],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();
    assert!(matches!(result, SqlValue::Null), "Expected NULL, got {:?}", result);
}

#[test]
fn test_st_simplify_zero_tolerance() {
    // Zero tolerance should still work (no simplification)
    let expr = Expression::Function {
        name: "ST_ASTEXT".to_string(),
        args: vec![Expression::Function {
            name: "ST_SIMPLIFY".to_string(),
            args: vec![
                Expression::Function {
                    name: "ST_GEOMFROMTEXT".to_string(),
                    args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("LINESTRING(0 0, 1 1, 2 2)")))],
                    character_unit: None,
                },
                Expression::Literal(SqlValue::Double(0.0)), // Zero tolerance
            ],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Varchar(s) | SqlValue::Character(s) = result {
        assert!(s.contains("LINESTRING"), "Expected LINESTRING, got {}", s);
    } else {
        panic!("Expected Varchar, got {:?}", result);
    }
}

#[test]
fn test_st_simplify_wrong_arity() {
    let expr = Expression::Function {
        name: "ST_SIMPLIFY".to_string(),
        args: vec![Expression::Function {
            name: "ST_GEOMFROMTEXT".to_string(),
            args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("LINESTRING(0 0, 1 1)")))],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr);
    assert!(result.is_err(), "Expected error for wrong arity");
}

#[test]
fn test_st_simplify_negative_tolerance() {
    let expr = Expression::Function {
        name: "ST_SIMPLIFY".to_string(),
        args: vec![
            Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("LINESTRING(0 0, 1 1)")))],
                character_unit: None,
            },
            Expression::Literal(SqlValue::Double(-1.0)), // Negative tolerance
        ],
        character_unit: None,
    };

    let result = eval_expr(expr);
    assert!(result.is_err(), "Expected error for negative tolerance");
}

#[test]
fn test_st_simplify_with_integer_tolerance() {
    // ST_Simplify should handle integer tolerance values too
    let expr = Expression::Function {
        name: "ST_ASTEXT".to_string(),
        args: vec![Expression::Function {
            name: "ST_SIMPLIFY".to_string(),
            args: vec![
                Expression::Function {
                    name: "ST_GEOMFROMTEXT".to_string(),
                    args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("LINESTRING(0 0, 1 1, 2 2)")))],
                    character_unit: None,
                },
                Expression::Literal(SqlValue::Integer(1)), // Integer tolerance
            ],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Varchar(s) | SqlValue::Character(s) = result {
        assert!(s.contains("LINESTRING"), "Expected LINESTRING, got {}", s);
    } else {
        panic!("Expected Varchar, got {:?}", result);
    }
}
