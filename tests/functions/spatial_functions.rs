use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn execute_select(db: &Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

#[test]
fn test_st_geom_from_text_point() {
    let db = Database::new();
    let sql = "SELECT ST_GeomFromText('POINT(10 20)')";

    let result = execute_select(&db, sql).expect("Failed to execute");

    // Check that we got one row with the geometry
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.values.len(), 1);

    // The value should be a varchar containing the geometry
    match &row.values[0] {
        SqlValue::Varchar(s) => {
            assert!(s.contains("POINT"));
            assert!(s.contains("10"));
            assert!(s.contains("20"));
        }
        _ => panic!("Expected Varchar geometry"),
    }
}

#[test]
fn test_st_x_st_y() {
    let db = Database::new();
    let sql = "SELECT ST_X(ST_GeomFromText('POINT(15 25)')), ST_Y(ST_GeomFromText('POINT(15 25)'))";

    let result = execute_select(&db, sql).expect("Failed to execute");

    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.values.len(), 2);

    match (&row.values[0], &row.values[1]) {
        (SqlValue::Double(x), SqlValue::Double(y)) => {
            assert_eq!(*x, 15.0);
            assert_eq!(*y, 25.0);
        }
        _ => panic!("Expected Double values for coordinates"),
    }
}

#[test]
fn test_st_geometry_type() {
    let db = Database::new();
    let sql = "SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'))";

    let result = execute_select(&db, sql).expect("Failed to execute");

    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.values.len(), 1);

    match &row.values[0] {
        SqlValue::Varchar(s) => {
            assert_eq!(s, "LINESTRING");
        }
        _ => panic!("Expected Varchar geometry type"),
    }
}

#[test]
fn test_st_as_text() {
    let db = Database::new();
    let sql = "SELECT ST_AsText(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))";

    let result = execute_select(&db, sql).expect("Failed to execute");

    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.values.len(), 1);

    match &row.values[0] {
        SqlValue::Varchar(s) => {
            assert!(s.contains("POLYGON"));
        }
        _ => panic!("Expected Varchar WKT"),
    }
}

#[test]
fn test_st_as_geojson() {
    let db = Database::new();
    let sql = "SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(30 40)'))";

    let result = execute_select(&db, sql).expect("Failed to execute");

    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.values.len(), 1);

    match &row.values[0] {
        SqlValue::Varchar(s) => {
            assert!(s.contains("Point"));
            assert!(s.contains("30"));
            assert!(s.contains("40"));
        }
        _ => panic!("Expected Varchar GeoJSON"),
    }
}
