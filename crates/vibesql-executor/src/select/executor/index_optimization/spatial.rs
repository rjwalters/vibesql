//! Spatial index optimization for WHERE clause filtering
//!
//! Implements automatic spatial index usage for spatial predicates in WHERE clauses.
//! Uses two-phase filtering: R-tree MBR query (fast, approximate) followed by
//! full spatial predicate evaluation (slower, exact).

use rstar::AABB;
use vibesql_ast::Expression;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::evaluator::functions::spatial::{constructors::parse_wkt, Geometry};
use crate::schema::CombinedSchema;

/// Spatial index usage information
#[derive(Debug)]
pub struct SpatialIndexUsage {
    pub index_name: String,
    pub column_name: String,
    pub predicate: SpatialPredicate,
    pub query_mbr: AABB<[f64; 2]>,
    pub query_geometry: Geometry,
}

/// Supported spatial predicates for index optimization
#[derive(Debug, Clone)]
pub enum SpatialPredicate {
    Contains,                  // ST_Contains(indexed_col, literal_geom)
    Intersects,                // ST_Intersects(indexed_col, literal_geom)
    Within,                    // ST_Within(indexed_col, literal_geom)
    DWithin { distance: f64 }, // ST_DWithin(indexed_col, literal_geom, distance)
}

/// Try to use spatial index for WHERE clause optimization
/// Returns Some(rows) if optimization applied, None otherwise
pub(in crate::select::executor) fn try_spatial_index_optimization(
    database: &Database,
    where_expr: Option<&Expression>,
    all_rows: &[Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<Row>>, ExecutorError> {
    let where_expr = match where_expr {
        Some(expr) => expr,
        None => return Ok(None), // No WHERE clause
    };

    // Try to detect spatial predicate and find matching index
    let spatial_usage = match detect_spatial_index_usage(where_expr, database, schema)? {
        Some(usage) => usage,
        None => return Ok(None), // No spatial index can be used
    };

    // Apply two-phase filtering
    apply_two_phase_filtering(database, &spatial_usage, all_rows, schema)
}

/// Detect if WHERE expression can use a spatial index
fn detect_spatial_index_usage(
    expr: &Expression,
    database: &Database,
    schema: &CombinedSchema,
) -> Result<Option<SpatialIndexUsage>, ExecutorError> {
    // Match function calls like ST_Contains(column, geometry_literal)
    if let Expression::Function { name, args, .. } = expr {
        // ST_Contains(indexed_col, literal_geom)
        if name.eq_ignore_ascii_case("ST_Contains") && args.len() == 2 {
            return try_detect_spatial_predicate(
                &args[0],
                &args[1],
                SpatialPredicate::Contains,
                database,
                schema,
            );
        }

        // ST_Intersects(indexed_col, literal_geom)
        if name.eq_ignore_ascii_case("ST_Intersects") && args.len() == 2 {
            return try_detect_spatial_predicate(
                &args[0],
                &args[1],
                SpatialPredicate::Intersects,
                database,
                schema,
            );
        }

        // ST_Within(indexed_col, literal_geom)
        if name.eq_ignore_ascii_case("ST_Within") && args.len() == 2 {
            return try_detect_spatial_predicate(
                &args[0],
                &args[1],
                SpatialPredicate::Within,
                database,
                schema,
            );
        }

        // ST_DWithin(indexed_col, literal_geom, distance)
        if name.eq_ignore_ascii_case("ST_DWithin") && args.len() == 3 {
            // Extract distance from third argument
            let distance = match &args[2] {
                Expression::Literal(SqlValue::Integer(d)) => *d as f64,
                Expression::Literal(SqlValue::Float(d)) => *d as f64,
                _ => return Ok(None), // Distance must be a literal
            };

            return try_detect_spatial_predicate(
                &args[0],
                &args[1],
                SpatialPredicate::DWithin { distance },
                database,
                schema,
            );
        }
    }

    Ok(None)
}

/// Try to detect spatial predicate pattern and find matching index
fn try_detect_spatial_predicate(
    column_expr: &Expression,
    geometry_expr: &Expression,
    predicate: SpatialPredicate,
    database: &Database,
    schema: &CombinedSchema,
) -> Result<Option<SpatialIndexUsage>, ExecutorError> {
    // First argument should be a column reference
    let column_name = match column_expr {
        Expression::ColumnRef { table: None, column } => column.clone(),
        Expression::ColumnRef { table: Some(_table), column } => column.clone(),
        _ => return Ok(None), // Not a column reference
    };

    // Find which table this column belongs to
    let table_name = find_table_for_column(&column_name, schema)?;
    if table_name.is_none() {
        return Ok(None);
    }
    let table_name = table_name.unwrap();

    // Check if there's a spatial index on this table and column
    let spatial_indexes = database.get_spatial_indexes_for_table(&table_name);
    let matching_index =
        spatial_indexes.iter().find(|(metadata, _)| metadata.column_name == column_name);

    if matching_index.is_none() {
        return Ok(None); // No spatial index on this column
    }

    let (metadata, _) = matching_index.unwrap();

    // Extract geometry from second argument
    let query_geometry = match extract_geometry_from_expr(geometry_expr)? {
        Some(geom) => geom,
        None => return Ok(None), // Could not extract geometry
    };

    // Compute MBR from query geometry
    let query_mbr = compute_mbr(&query_geometry, &predicate)?;

    Ok(Some(SpatialIndexUsage {
        index_name: metadata.index_name.clone(),
        column_name: column_name.clone(),
        predicate,
        query_mbr,
        query_geometry,
    }))
}

/// Find which table a column belongs to
fn find_table_for_column(
    column_name: &str,
    schema: &CombinedSchema,
) -> Result<Option<String>, ExecutorError> {
    for (table, (_start_idx, table_schema)) in &schema.table_schemas {
        if table_schema.get_column_index(column_name).is_some() {
            return Ok(Some(table.to_string()));
        }
    }
    Ok(None)
}

/// Extract geometry from expression (handles ST_GeomFromText and direct WKT)
fn extract_geometry_from_expr(expr: &Expression) -> Result<Option<Geometry>, ExecutorError> {
    match expr {
        // Case 1: ST_GeomFromText('POINT(10 20)')
        Expression::Function { name, args, .. }
            if name.eq_ignore_ascii_case("ST_GeomFromText") && !args.is_empty() =>
        {
            if let Expression::Literal(SqlValue::Varchar(wkt) | SqlValue::Character(wkt)) = &args[0]
            {
                return Ok(Some(parse_wkt(wkt)?));
            }
        }
        // Case 2: Direct string literal 'POINT(10 20)'
        Expression::Literal(SqlValue::Varchar(wkt) | SqlValue::Character(wkt)) => {
            // Try to parse as WKT
            if let Ok(geom) = parse_wkt(wkt) {
                return Ok(Some(geom));
            }
        }
        _ => {}
    }
    Ok(None)
}

/// Compute MBR (Minimum Bounding Rectangle) from geometry
fn compute_mbr(
    geometry: &Geometry,
    predicate: &SpatialPredicate,
) -> Result<AABB<[f64; 2]>, ExecutorError> {
    let base_mbr = match geometry {
        Geometry::Point { x, y } => {
            // Point MBR is just the point itself
            AABB::from_point([*x, *y])
        }
        Geometry::LineString { points } => {
            if points.is_empty() {
                return Err(ExecutorError::Other("Empty LineString".to_string()));
            }

            let xs: Vec<f64> = points.iter().map(|(x, _)| *x).collect();
            let ys: Vec<f64> = points.iter().map(|(_, y)| *y).collect();

            let min_x = xs.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_x = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let min_y = ys.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_y = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

            AABB::from_corners([min_x, min_y], [max_x, max_y])
        }
        Geometry::Polygon { rings } => {
            if rings.is_empty() {
                return Err(ExecutorError::Other("Empty Polygon".to_string()));
            }

            // Compute MBR from exterior ring (rings[0])
            let exterior = &rings[0];
            if exterior.is_empty() {
                return Err(ExecutorError::Other("Empty Polygon exterior ring".to_string()));
            }

            let xs: Vec<f64> = exterior.iter().map(|(x, _)| *x).collect();
            let ys: Vec<f64> = exterior.iter().map(|(_, y)| *y).collect();

            let min_x = xs.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_x = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let min_y = ys.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_y = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

            AABB::from_corners([min_x, min_y], [max_x, max_y])
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "MBR computation for {} not yet supported",
                geometry.geometry_type()
            )))
        }
    };

    // For ST_DWithin, expand MBR by distance
    if let SpatialPredicate::DWithin { distance } = predicate {
        let lower = base_mbr.lower();
        let upper = base_mbr.upper();
        return Ok(AABB::from_corners(
            [lower[0] - distance, lower[1] - distance],
            [upper[0] + distance, upper[1] + distance],
        ));
    }

    Ok(base_mbr)
}

/// Apply two-phase filtering: R-tree query + full predicate evaluation
fn apply_two_phase_filtering(
    database: &Database,
    spatial_usage: &SpatialIndexUsage,
    all_rows: &[Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<Row>>, ExecutorError> {
    // Phase 1: R-tree MBR query (fast, approximate)
    let spatial_index = database.get_spatial_index(&spatial_usage.index_name);
    if spatial_index.is_none() {
        return Ok(None); // Index disappeared?
    }
    let spatial_index = spatial_index.unwrap();

    let candidate_row_ids = spatial_index.locate_in_envelope(&spatial_usage.query_mbr);

    // Convert row IDs to actual rows
    let candidate_rows: Vec<Row> = candidate_row_ids
        .iter()
        .filter_map(
            |&row_id| {
                if row_id < all_rows.len() {
                    Some(all_rows[row_id].clone())
                } else {
                    None
                }
            },
        )
        .collect();

    // Phase 2: Full spatial predicate evaluation (exact)
    let filtered_rows = apply_full_spatial_predicate(candidate_rows, spatial_usage, schema)?;

    Ok(Some(filtered_rows))
}

/// Apply full spatial predicate to filter false positives from R-tree results
fn apply_full_spatial_predicate(
    candidate_rows: Vec<Row>,
    spatial_usage: &SpatialIndexUsage,
    schema: &CombinedSchema,
) -> Result<Vec<Row>, ExecutorError> {
    // Find column index for the spatial column
    let column_index = find_column_index(&spatial_usage.column_name, schema)?;
    if column_index.is_none() {
        return Err(ExecutorError::Other(format!(
            "Column '{}' not found in schema",
            spatial_usage.column_name
        )));
    }
    let column_index = column_index.unwrap();

    // Import spatial predicate functions
    use crate::evaluator::functions::spatial::predicates::{
        st_contains, st_dwithin, st_intersects, st_within,
    };

    // Filter candidates by applying full predicate
    let mut filtered_rows = Vec::new();

    for row in candidate_rows {
        // Get geometry value from row
        let row_geometry_value = &row.values[column_index];

        // Skip NULL geometries
        if matches!(row_geometry_value, SqlValue::Null) {
            continue;
        }

        // Convert query geometry to SqlValue for predicate functions
        let query_geom_wkt = SqlValue::Varchar(arcstr::ArcStr::from(spatial_usage.query_geometry.to_wkt()));

        // Apply the appropriate spatial predicate
        let predicate_result = match &spatial_usage.predicate {
            SpatialPredicate::Contains => {
                st_contains(&[row_geometry_value.clone(), query_geom_wkt])?
            }
            SpatialPredicate::Intersects => {
                st_intersects(&[row_geometry_value.clone(), query_geom_wkt])?
            }
            SpatialPredicate::Within => st_within(&[row_geometry_value.clone(), query_geom_wkt])?,
            SpatialPredicate::DWithin { distance } => st_dwithin(&[
                row_geometry_value.clone(),
                query_geom_wkt,
                SqlValue::Float(*distance as f32),
            ])?,
        };

        // Include row if predicate is true
        if let SqlValue::Boolean(true) = predicate_result {
            filtered_rows.push(row);
        }
    }

    Ok(filtered_rows)
}

/// Find column index in combined schema
fn find_column_index(
    column_name: &str,
    schema: &CombinedSchema,
) -> Result<Option<usize>, ExecutorError> {
    for (start_idx, table_schema) in schema.table_schemas.values() {
        if let Some(col_idx) = table_schema.get_column_index(column_name) {
            return Ok(Some(start_idx + col_idx));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_mbr_point() {
        let geom = Geometry::Point { x: 10.0, y: 20.0 };
        let mbr = compute_mbr(&geom, &SpatialPredicate::Contains).unwrap();
        assert_eq!(mbr.lower(), [10.0, 20.0]);
        assert_eq!(mbr.upper(), [10.0, 20.0]);
    }

    #[test]
    fn test_compute_mbr_polygon() {
        let geom = Geometry::Polygon {
            rings: vec![vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]],
        };
        let mbr = compute_mbr(&geom, &SpatialPredicate::Contains).unwrap();
        assert_eq!(mbr.lower(), [0.0, 0.0]);
        assert_eq!(mbr.upper(), [10.0, 10.0]);
    }

    #[test]
    fn test_compute_mbr_dwithin_expansion() {
        let geom = Geometry::Point { x: 50.0, y: 50.0 };
        let mbr = compute_mbr(&geom, &SpatialPredicate::DWithin { distance: 10.0 }).unwrap();
        assert_eq!(mbr.lower(), [40.0, 40.0]);
        assert_eq!(mbr.upper(), [60.0, 60.0]);
    }

    #[test]
    fn test_extract_geometry_from_wkt_literal() {
        let expr = Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("POINT(5 10)")));
        let geom = extract_geometry_from_expr(&expr).unwrap();
        assert!(geom.is_some());
        if let Some(Geometry::Point { x, y }) = geom {
            assert_eq!(x, 5.0);
            assert_eq!(y, 10.0);
        } else {
            panic!("Expected Point geometry");
        }
    }
}
