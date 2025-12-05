//! SRID (Spatial Reference System Identifier) tracking functions
//!
//! Provides functions for getting and setting SRID on geometries.

use super::{geometry_to_sql_value, sql_value_to_geometry};
use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

/// ST_SRID(geom) - Get the SRID of a geometry
pub fn st_srid(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_SRID requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    // Handle NULL
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let geom = sql_value_to_geometry(&args[0])?;
    Ok(SqlValue::Integer(geom.srid as i64))
}

/// ST_SetSRID(geom, srid) - Set the SRID of a geometry
pub fn st_set_srid(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_SetSRID requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    // Handle NULL geometry
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    // Parse SRID
    let srid = match &args[1] {
        SqlValue::Integer(i) => *i as i32,
        SqlValue::Bigint(i) => *i as i32,
        SqlValue::Smallint(i) => *i as i32,
        SqlValue::Null => {
            return Err(ExecutorError::UnsupportedFeature("SRID cannot be NULL".to_string()))
        }
        _ => return Err(ExecutorError::UnsupportedFeature("SRID must be an integer".to_string())),
    };

    // Validate SRID (0 is allowed = undefined, others should be valid EPSG codes)
    if srid < 0 {
        return Err(ExecutorError::UnsupportedFeature(format!("SRID must be >= 0, got {}", srid)));
    }

    let mut geom = sql_value_to_geometry(&args[0])?;
    geom.set_srid(srid);

    Ok(geometry_to_sql_value(geom.geometry, geom.srid))
}

/// ST_Transform(geom, target_srid) - Transform geometry to different SRID (stub for Phase 4)
#[allow(dead_code)]
pub fn st_transform(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_Transform requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    // For Phase 2, we just return a stub - full transformation requires projection library
    Err(ExecutorError::UnsupportedFeature(
        "ST_Transform is not yet implemented (Phase 4 feature)".to_string(),
    ))
}

// Common EPSG codes for reference (validation could be added in Phase 3+)
const EPSG_WGS84: i32 = 4326; // GPS coordinates (lat/lon)
const EPSG_WEB_MERCATOR: i32 = 3857; // Web Mercator (web maps)
const EPSG_US_NATIONAL_ATLAS: i32 = 2163; // US National Atlas Equal Area

/// Check if SRID is a valid/known EPSG code
#[allow(dead_code)]
fn is_valid_srid(srid: i32) -> bool {
    match srid {
        0 => true, // Undefined
        EPSG_WGS84 => true,
        EPSG_WEB_MERCATOR => true,
        EPSG_US_NATIONAL_ATLAS => true,
        _ => false, // In Phase 3+, we could check against full EPSG database
    }
}
