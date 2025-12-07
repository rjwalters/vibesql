//! Spatial accessor functions
//!
//! Extract coordinates and metadata from geometries.

use super::{sql_value_to_geometry, Geometry};
use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

/// ST_X(point) - Get X coordinate from POINT
pub fn st_x(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_X requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom_with_srid = sql_value_to_geometry(&args[0])?;
            match geom_with_srid.geometry {
                Geometry::Point { x, .. } => Ok(SqlValue::Double(x)),
                _ => Err(ExecutorError::UnsupportedFeature(
                    "ST_X requires a POINT geometry".to_string(),
                )),
            }
        }
    }
}

/// ST_Y(point) - Get Y coordinate from POINT
pub fn st_y(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_Y requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom_with_srid = sql_value_to_geometry(&args[0])?;
            match geom_with_srid.geometry {
                Geometry::Point { y, .. } => Ok(SqlValue::Double(y)),
                _ => Err(ExecutorError::UnsupportedFeature(
                    "ST_Y requires a POINT geometry".to_string(),
                )),
            }
        }
    }
}

/// ST_GeometryType(geom) - Get geometry type name
pub fn st_geometry_type(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_GeometryType requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom_with_srid = sql_value_to_geometry(&args[0])?;
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(geom_with_srid.geometry_type().to_string())))
        }
    }
}

/// ST_Dimension(geom) - Get dimension (0=point, 1=line, 2=polygon)
pub fn st_dimension(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_Dimension requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom_with_srid = sql_value_to_geometry(&args[0])?;
            Ok(SqlValue::Integer(geom_with_srid.dimension() as i64))
        }
    }
}

/// ST_AsText(geom) - Convert geometry to WKT (Well-Known Text)
/// Returns EWKT format (SRID=xxx;POINT(...)) if SRID is set
pub fn st_as_text(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_AsText requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom_with_srid = sql_value_to_geometry(&args[0])?;
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(geom_with_srid.to_ewkt())))
        }
    }
}

/// ST_AsBinary(geom) - Convert geometry to WKB (Well-Known Binary) - Phase 2
/// Returns EWKB (Extended WKB with SRID) if SRID is set
pub fn st_as_binary(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_AsBinary requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom_with_srid = sql_value_to_geometry(&args[0])?;
            // Use EWKB format to include SRID
            let wkb_data = geom_with_srid.to_ewkb();
            // Return as hex-encoded string for display (0x...)
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(format!("0x{}", hex::encode(wkb_data)))))
        }
    }
}

/// ST_AsGeoJSON(geom) - Convert geometry to GeoJSON
pub fn st_as_geojson(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_AsGeoJSON requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom_with_srid = sql_value_to_geometry(&args[0])?;
            let geojson = geometry_to_geojson(&geom_with_srid.geometry);
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(geojson)))
        }
    }
}

/// Convert geometry to simple GeoJSON representation
fn geometry_to_geojson(geom: &Geometry) -> String {
    match geom {
        Geometry::Point { x, y } => {
            format!(r#"{{"type":"Point","coordinates":[{},{}]}}"#, x, y)
        }
        Geometry::LineString { points } => {
            let coords =
                points.iter().map(|(x, y)| format!("[{},{}]", x, y)).collect::<Vec<_>>().join(",");
            format!(r#"{{"type":"LineString","coordinates":[{}]}}"#, coords)
        }
        Geometry::Polygon { rings } => {
            let rings_str = rings
                .iter()
                .map(|ring| {
                    let coords = ring
                        .iter()
                        .map(|(x, y)| format!("[{},{}]", x, y))
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("[{}]", coords)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"Polygon","coordinates":[{}]}}"#, rings_str)
        }
        Geometry::MultiPoint { points } => {
            let coords =
                points.iter().map(|(x, y)| format!("[{},{}]", x, y)).collect::<Vec<_>>().join(",");
            format!(r#"{{"type":"MultiPoint","coordinates":[{}]}}"#, coords)
        }
        Geometry::MultiLineString { lines } => {
            let lines_str = lines
                .iter()
                .map(|line| {
                    let coords = line
                        .iter()
                        .map(|(x, y)| format!("[{},{}]", x, y))
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("[{}]", coords)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"MultiLineString","coordinates":[{}]}}"#, lines_str)
        }
        Geometry::MultiPolygon { polygons } => {
            let polys_str = polygons
                .iter()
                .map(|poly| {
                    let rings_str = poly
                        .iter()
                        .map(|ring| {
                            let coords = ring
                                .iter()
                                .map(|(x, y)| format!("[{},{}]", x, y))
                                .collect::<Vec<_>>()
                                .join(",");
                            format!("[{}]", coords)
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("[{}]", rings_str)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"MultiPolygon","coordinates":[{}]}}"#, polys_str)
        }
        Geometry::Collection { geometries } => {
            let geoms = geometries
                .iter()
                .map(|g| {
                    // Extract just the geometry object without the full GeoJSON wrapper

                    geometry_to_geojson(g)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"GeometryCollection","geometries":[{}]}}"#, geoms)
        }
    }
}
