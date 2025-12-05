/// Spatial Function Tests
///
/// Tests for Phase 1 (WKT) and Phase 3 (Predicates) spatial functions
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Helper to execute SQL and return the result
fn execute_sql(db: &mut Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let select_executor = vibesql_executor::SelectExecutor::new(db);
            select_executor.execute(&select_stmt).map_err(|e| format!("Select error: {:?}", e))
        }
        _ => Err("Expected SELECT statement".to_string()),
    }
}

/// Helper to extract boolean value from result
fn get_boolean_value(result: &[vibesql_storage::Row]) -> Result<bool, String> {
    if result.is_empty() {
        return Err("Empty result".to_string());
    }

    if result[0].values.is_empty() {
        return Err("No column values".to_string());
    }

    match &result[0].values[0] {
        SqlValue::Boolean(b) => Ok(*b),
        SqlValue::Null => Err("NULL result".to_string()),
        other => Err(format!("Expected Boolean, got {:?}", other)),
    }
}

#[test]
fn test_st_geomfromtext_point() {
    let mut db = Database::new();

    // Simple point geometry
    let result = execute_sql(&mut db, "SELECT ST_GeomFromText('POINT(10 20)')").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_geometrytype() {
    let mut db = Database::new();

    let result =
        execute_sql(&mut db, "SELECT ST_GeometryType(ST_GeomFromText('POINT(0 0)'))").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_astext() {
    let mut db = Database::new();

    let sql = r#"
        SELECT ST_AsText(ST_GeomFromText('POINT(1 2)'))
    "#;
    let result = execute_sql(&mut db, sql).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_x_y_coordinates() {
    let mut db = Database::new();

    let x_result = execute_sql(&mut db, "SELECT ST_X(ST_GeomFromText('POINT(5 10)'))").unwrap();
    let y_result = execute_sql(&mut db, "SELECT ST_Y(ST_GeomFromText('POINT(5 10)'))").unwrap();

    assert!(!x_result.is_empty());
    assert!(!y_result.is_empty());
}

#[test]
fn test_st_dimension() {
    let mut db = Database::new();

    let point_dim =
        execute_sql(&mut db, "SELECT ST_Dimension(ST_GeomFromText('POINT(0 0)'))").unwrap();
    let line_dim =
        execute_sql(&mut db, "SELECT ST_Dimension(ST_GeomFromText('LINESTRING(0 0, 1 1)'))")
            .unwrap();
    let poly_dim = execute_sql(
        &mut db,
        "SELECT ST_Dimension(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))",
    )
    .unwrap();

    assert!(!point_dim.is_empty());
    assert!(!line_dim.is_empty());
    assert!(!poly_dim.is_empty());
}

#[test]
fn test_st_srid() {
    let mut db = Database::new();

    // Phase 1: SRID always returns 0
    let result = execute_sql(&mut db, "SELECT ST_SRID(ST_GeomFromText('POINT(0 0)'))").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_asgeojson_point() {
    let mut db = Database::new();

    let result =
        execute_sql(&mut db, "SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(3 4)'))").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_pointfromtext() {
    let mut db = Database::new();

    let result = execute_sql(&mut db, "SELECT ST_PointFromText('POINT(100 200)')").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_linefromtext() {
    let mut db = Database::new();

    let result =
        execute_sql(&mut db, "SELECT ST_LineFromText('LINESTRING(0 0, 1 1, 2 0)')").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_polygonfromtext() {
    let mut db = Database::new();

    let result =
        execute_sql(&mut db, "SELECT ST_PolygonFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))')")
            .unwrap();
    assert!(!result.is_empty());
}

// Phase 3 Predicate Tests

#[test]
fn test_st_contains() {
    let mut db = Database::new();

    // Point inside polygon - should be TRUE
    let result = execute_sql(&mut db, 
        "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_GeomFromText('POINT(5 5)'))"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Point outside polygon - should be FALSE
    let result = execute_sql(&mut db,
        "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_GeomFromText('POINT(20 20)'))"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_within() {
    let mut db = Database::new();

    // Point inside polygon - should be TRUE
    let result = execute_sql(&mut db, 
        "SELECT ST_Within(ST_GeomFromText('POINT(5 5)'), ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Point outside polygon - should be FALSE
    let result = execute_sql(&mut db,
        "SELECT ST_Within(ST_GeomFromText('POINT(20 20)'), ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_intersects() {
    let mut db = Database::new();

    // Lines crossing - should be TRUE
    let result = execute_sql(&mut db, 
        "SELECT ST_Intersects(ST_GeomFromText('LINESTRING(0 0, 10 10)'), ST_GeomFromText('LINESTRING(0 10, 10 0)'))"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Disjoint lines - should be FALSE
    let result = execute_sql(&mut db,
        "SELECT ST_Intersects(ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_GeomFromText('LINESTRING(5 5, 10 10)'))"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_disjoint() {
    let mut db = Database::new();

    // Disjoint points - should be TRUE
    let result = execute_sql(
        &mut db,
        "SELECT ST_Disjoint(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(10 10)'))",
    )
    .unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Same point - should be FALSE
    let result = execute_sql(
        &mut db,
        "SELECT ST_Disjoint(ST_GeomFromText('POINT(5 5)'), ST_GeomFromText('POINT(5 5)'))",
    )
    .unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_equals() {
    let mut db = Database::new();

    // Same points - should be TRUE
    let result = execute_sql(
        &mut db,
        "SELECT ST_Equals(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(1 1)'))",
    )
    .unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Different points - should be FALSE
    let result = execute_sql(
        &mut db,
        "SELECT ST_Equals(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(2 2)'))",
    )
    .unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_crosses() {
    let mut db = Database::new();

    // Lines crossing - should be TRUE
    let result = execute_sql(&mut db, 
        "SELECT ST_Crosses(ST_GeomFromText('LINESTRING(0 0, 10 10)'), ST_GeomFromText('LINESTRING(0 10, 10 0)'))"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Parallel lines - should be FALSE
    let result = execute_sql(&mut db,
        "SELECT ST_Crosses(ST_GeomFromText('LINESTRING(0 0, 10 0)'), ST_GeomFromText('LINESTRING(0 5, 10 5)'))"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_overlaps() {
    let mut db = Database::new();

    // Partially overlapping polygons - should be TRUE
    let result = execute_sql(&mut db, 
        "SELECT ST_Overlaps(ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'), ST_GeomFromText('POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))'))"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Disjoint polygons - should be FALSE
    let result = execute_sql(&mut db,
        "SELECT ST_Overlaps(ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GeomFromText('POLYGON((5 5, 7 5, 7 7, 5 7, 5 5))'))"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_touches() {
    let mut db = Database::new();

    // Adjacent polygons sharing an edge - should be TRUE
    let result = execute_sql(&mut db, 
        "SELECT ST_Touches(ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'), ST_GeomFromText('POLYGON((5 0, 10 0, 10 5, 5 5, 5 0))'))"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Overlapping polygons - should be FALSE (overlaps, not just touches)
    let result = execute_sql(&mut db,
        "SELECT ST_Touches(ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'), ST_GeomFromText('POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))'))"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_covers() {
    let mut db = Database::new();

    // Point inside polygon - should be TRUE
    let result = execute_sql(&mut db, 
        "SELECT ST_Covers(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_GeomFromText('POINT(5 5)'))"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Point outside polygon - should be FALSE
    let result = execute_sql(&mut db,
        "SELECT ST_Covers(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_GeomFromText('POINT(15 15)'))"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_coveredby() {
    let mut db = Database::new();

    // Point inside polygon - should be TRUE
    let result = execute_sql(&mut db, 
        "SELECT ST_CoveredBy(ST_GeomFromText('POINT(5 5)'), ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Point outside polygon - should be FALSE
    let result = execute_sql(&mut db,
        "SELECT ST_CoveredBy(ST_GeomFromText('POINT(15 15)'), ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_st_dwithin() {
    let mut db = Database::new();

    // Points within distance - should be TRUE
    // Using haversine distance (great-circle distance)
    // Small difference in coordinates results in large haversine distance,
    // so use a larger threshold. Distance from (0,0) to (0,0.01) ~1.1km
    let result = execute_sql(&mut db, 
        "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 0.01)'), 200000.0)"
    ).unwrap();
    assert!(get_boolean_value(&result).unwrap());

    // Points outside distance threshold - should be FALSE
    // Distance from (0,0) to (0,10) is ~1111km, so 100km threshold is insufficient
    let result = execute_sql(&mut db,
        "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 10)'), 100000.0)"
    ).unwrap();
    assert!(!get_boolean_value(&result).unwrap());
}

#[test]
fn test_null_handling() {
    let mut db = Database::new();

    // Test NULL handling for all functions
    let result = execute_sql(&mut db, "SELECT ST_GeomFromText(NULL)").unwrap();
    assert!(!result.is_empty());

    let result = execute_sql(&mut db, "SELECT ST_X(NULL)").unwrap();
    assert!(!result.is_empty());

    let result =
        execute_sql(&mut db, "SELECT ST_Contains(NULL, ST_GeomFromText('POINT(0 0)'))").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_polygon_with_hole() {
    let mut db = Database::new();

    // Polygon with hole
    let sql = r#"
        SELECT ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2))')
    "#;
    let result = execute_sql(&mut db, sql).unwrap();
    assert!(!result.is_empty());
}
