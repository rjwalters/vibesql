//! Spatial set operation function tests

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

/// Helper to create a function call expression
fn function_call(name: &str, args: Vec<Expression>) -> Expression {
    Expression::Function { name: name.to_string(), args, character_unit: None }
}

/// Helper to create a literal expression
fn literal(value: SqlValue) -> Expression {
    Expression::Literal(value)
}

#[test]
fn test_st_union_polygons() {
    // Two adjacent squares should union into a result geometry
    let expr = function_call(
        "ST_UNION",
        vec![
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")))],
            ),
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))")))],
            ),
        ],
    );

    let result = eval_expr(expr).unwrap();

    // Result should be a geometry (WKT format)
    match result {
        SqlValue::Varchar(s) => {
            assert!(s.starts_with("__GEOMETRY__"), "Expected geometry marker, got: {}", s);
        }
        _ => panic!("Expected Varchar result, got: {:?}", result),
    }
}

#[test]
fn test_st_union_null_handling() {
    // Union with NULL should return NULL
    let expr = function_call(
        "ST_UNION",
        vec![
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")))],
            ),
            literal(SqlValue::Null),
        ],
    );

    let result = eval_expr(expr).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_st_intersection_overlapping_polygons() {
    // Intersection of two overlapping squares
    let expr = function_call(
        "ST_INTERSECTION",
        vec![
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")))],
            ),
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))")))],
            ),
        ],
    );

    let result = eval_expr(expr).unwrap();
    match result {
        SqlValue::Varchar(s) => {
            assert!(s.starts_with("__GEOMETRY__"), "Expected geometry, got: {}", s);
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_st_intersection_null_handling() {
    let expr = function_call(
        "ST_INTERSECTION",
        vec![
            literal(SqlValue::Null),
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")))],
            ),
        ],
    );

    let result = eval_expr(expr).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_st_difference_polygons() {
    // Difference of two overlapping squares
    let expr = function_call(
        "ST_ASTEXT",
        vec![function_call(
            "ST_DIFFERENCE",
            vec![
                function_call(
                    "ST_GEOMFROMTEXT",
                    vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")))],
                ),
                function_call(
                    "ST_GEOMFROMTEXT",
                    vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))")))],
                ),
            ],
        )],
    );

    let result = eval_expr(expr).unwrap();
    match result {
        SqlValue::Varchar(s) => {
            assert!(
                s.contains("POLYGON") || s.contains("MULTIPOLYGON"),
                "Expected POLYGON or MULTIPOLYGON, got: {}",
                s
            );
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_st_difference_null_handling() {
    let expr = function_call(
        "ST_DIFFERENCE",
        vec![
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")))],
            ),
            literal(SqlValue::Null),
        ],
    );

    let result = eval_expr(expr).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_st_symdifference_polygons() {
    // Symmetric difference of two overlapping squares
    let expr = function_call(
        "ST_ASTEXT",
        vec![function_call(
            "ST_SYMDIFFERENCE",
            vec![
                function_call(
                    "ST_GEOMFROMTEXT",
                    vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")))],
                ),
                function_call(
                    "ST_GEOMFROMTEXT",
                    vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))")))],
                ),
            ],
        )],
    );

    let result = eval_expr(expr).unwrap();
    match result {
        SqlValue::Varchar(s) => {
            assert!(!s.is_empty(), "Expected result");
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_st_symdifference_null_handling() {
    let expr = function_call(
        "ST_SYMDIFFERENCE",
        vec![
            literal(SqlValue::Null),
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")))],
            ),
        ],
    );

    let result = eval_expr(expr).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_st_boundary_polygon() {
    // Boundary of a polygon is its rings
    let expr = function_call(
        "ST_ASTEXT",
        vec![function_call(
            "ST_BOUNDARY",
            vec![function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")))],
            )],
        )],
    );

    let result = eval_expr(expr).unwrap();
    match result {
        SqlValue::Varchar(s) => {
            assert!(
                s.contains("LINESTRING") || s.contains("MULTILINESTRING"),
                "Expected LINESTRING or MULTILINESTRING, got: {}",
                s
            );
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_st_boundary_linestring() {
    // Boundary of a line is its endpoints
    let expr = function_call(
        "ST_ASTEXT",
        vec![function_call(
            "ST_BOUNDARY",
            vec![function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("LINESTRING(0 0, 1 1, 2 0)")))],
            )],
        )],
    );

    let result = eval_expr(expr).unwrap();
    match result {
        SqlValue::Varchar(s) => {
            assert!(
                s.contains("POINT") || s.contains("MULTIPOINT"),
                "Expected POINT or MULTIPOINT, got: {}",
                s
            );
        }
        _ => panic!("Expected Varchar result"),
    }
}

#[test]
fn test_st_boundary_null_handling() {
    let expr = function_call("ST_BOUNDARY", vec![literal(SqlValue::Null)]);

    let result = eval_expr(expr).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_st_hausdorff_distance_points() {
    // Hausdorff distance between two points should be the distance between them
    let expr = function_call(
        "ST_HAUSDORFF_DISTANCE",
        vec![
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(0 0)")))],
            ),
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(3 4)")))],
            ),
        ],
    );

    let result = eval_expr(expr).unwrap();
    match result {
        SqlValue::Double(d) => {
            // Distance should be 5 (3-4-5 triangle)
            assert!((d - 5.0).abs() < 0.001, "Expected distance ~5, got {}", d);
        }
        _ => panic!("Expected Double result"),
    }
}

#[test]
fn test_st_hausdorff_distance_null_handling() {
    let expr = function_call(
        "ST_HAUSDORFF_DISTANCE",
        vec![
            literal(SqlValue::Null),
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(0 0)")))],
            ),
        ],
    );

    let result = eval_expr(expr).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_st_hausdorff_distance_same_geometry() {
    // Hausdorff distance of a geometry to itself should be 0
    let expr = function_call(
        "ST_HAUSDORFF_DISTANCE",
        vec![
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")))],
            ),
            function_call(
                "ST_GEOMFROMTEXT",
                vec![literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")))],
            ),
        ],
    );

    let result = eval_expr(expr).unwrap();
    match result {
        SqlValue::Double(d) => {
            assert!(d.abs() < 0.001, "Expected distance ~0, got {}", d);
        }
        _ => panic!("Expected Double result"),
    }
}
