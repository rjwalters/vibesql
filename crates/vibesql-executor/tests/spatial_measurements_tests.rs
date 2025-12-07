//! Spatial measurement function tests

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
fn test_st_distance_point_to_point() {
    // ST_Distance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'))
    let expr = Expression::Function {
        name: "ST_DISTANCE".to_string(),
        args: vec![
            Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(0 0)")))],
                character_unit: None,
            },
            Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(3 4)")))],
                character_unit: None,
            },
        ],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Double(d) = result {
        assert!((d - 5.0).abs() < 0.0001, "Expected 5.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_st_distance_same_point() {
    let expr = Expression::Function {
        name: "ST_DISTANCE".to_string(),
        args: vec![
            Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(1 1)")))],
                character_unit: None,
            },
            Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(1 1)")))],
                character_unit: None,
            },
        ],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Double(d) = result {
        assert!((d - 0.0).abs() < 0.0001, "Expected 0.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_st_length_linestring() {
    let expr = Expression::Function {
        name: "ST_LENGTH".to_string(),
        args: vec![Expression::Function {
            name: "ST_GEOMFROMTEXT".to_string(),
            args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("LINESTRING(0 0, 3 0, 3 4)")))],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Double(d) = result {
        assert!((d - 7.0).abs() < 0.0001, "Expected 7.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_st_area_polygon() {
    let expr = Expression::Function {
        name: "ST_AREA".to_string(),
        args: vec![Expression::Function {
            name: "ST_GEOMFROMTEXT".to_string(),
            args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")))],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Double(d) = result {
        assert!((d - 100.0).abs() < 0.0001, "Expected 100.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_st_centroid_polygon() {
    let expr = Expression::Function {
        name: "ST_ASTEXT".to_string(),
        args: vec![Expression::Function {
            name: "ST_CENTROID".to_string(),
            args: vec![Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")))],
                character_unit: None,
            }],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Varchar(s) | SqlValue::Character(s) = result {
        assert!(s.contains("POINT"), "Expected POINT, got {}", s);
    } else {
        panic!("Expected Varchar, got {:?}", result);
    }
}

#[test]
fn test_st_envelope() {
    let expr = Expression::Function {
        name: "ST_ASTEXT".to_string(),
        args: vec![Expression::Function {
            name: "ST_ENVELOPE".to_string(),
            args: vec![Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(5 5)")))],
                character_unit: None,
            }],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Varchar(s) | SqlValue::Character(s) = result {
        assert!(s.contains("POLYGON") || s.contains("POINT"), "Expected geometry, got {}", s);
    } else {
        panic!("Expected Varchar, got {:?}", result);
    }
}

#[test]
fn test_st_null_handling_distance() {
    let expr = Expression::Function {
        name: "ST_DISTANCE".to_string(),
        args: vec![
            Expression::Literal(SqlValue::Null),
            Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(0 0)")))],
                character_unit: None,
            },
        ],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();
    assert!(matches!(result, SqlValue::Null), "Expected NULL, got {:?}", result);
}

#[test]
fn test_st_null_handling_area() {
    let expr = Expression::Function {
        name: "ST_AREA".to_string(),
        args: vec![Expression::Literal(SqlValue::Null)],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();
    assert!(matches!(result, SqlValue::Null), "Expected NULL, got {:?}", result);
}

#[test]
fn test_st_perimeter_polygon() {
    let expr = Expression::Function {
        name: "ST_PERIMETER".to_string(),
        args: vec![Expression::Function {
            name: "ST_GEOMFROMTEXT".to_string(),
            args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")))],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Double(d) = result {
        assert!((d - 40.0).abs() < 0.0001, "Expected 40.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_st_convex_hull() {
    let expr = Expression::Function {
        name: "ST_ASTEXT".to_string(),
        args: vec![Expression::Function {
            name: "ST_CONVEXHULL".to_string(),
            args: vec![Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("LINESTRING(0 0, 1 1, 0 1)")))],
                character_unit: None,
            }],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Varchar(s) | SqlValue::Character(s) = result {
        assert!(s.contains("POLYGON") || s.contains("LINESTRING"), "Expected geometry, got {}", s);
    } else {
        panic!("Expected Varchar, got {:?}", result);
    }
}

#[test]
fn test_st_point_on_surface_point() {
    let expr = Expression::Function {
        name: "ST_ASTEXT".to_string(),
        args: vec![Expression::Function {
            name: "ST_POINTONSURFACE".to_string(),
            args: vec![Expression::Function {
                name: "ST_GEOMFROMTEXT".to_string(),
                args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(5 5)")))],
                character_unit: None,
            }],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr).unwrap();

    if let SqlValue::Varchar(s) | SqlValue::Character(s) = result {
        assert!(s.contains("POINT"), "Expected POINT, got {}", s);
    } else {
        panic!("Expected Varchar, got {:?}", result);
    }
}

#[test]
fn test_st_wrong_arity_distance() {
    let expr = Expression::Function {
        name: "ST_DISTANCE".to_string(),
        args: vec![Expression::Function {
            name: "ST_GEOMFROMTEXT".to_string(),
            args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(0 0)")))],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr);
    assert!(result.is_err(), "Expected error for wrong arity");
}

#[test]
fn test_st_wrong_type_area() {
    // ST_Area on a Point should error
    let expr = Expression::Function {
        name: "ST_AREA".to_string(),
        args: vec![Expression::Function {
            name: "ST_GEOMFROMTEXT".to_string(),
            args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(5 5)")))],
            character_unit: None,
        }],
        character_unit: None,
    };

    let result = eval_expr(expr);
    assert!(result.is_err(), "Expected error for wrong geometry type");
}
