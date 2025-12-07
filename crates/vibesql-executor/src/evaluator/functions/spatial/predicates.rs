//! Spatial Predicate Functions
//!
//! Implements spatial relationship tests between geometries using the DE-9IM (Dimensionally Extended 9-Intersection Model).
//! Phase 3: Basic spatial predicates with proper DE-9IM semantics.
//! Phase 4+: Full DE-9IM support for complex patterns and optimizations.

#![cfg(feature = "spatial")]

use super::{sql_value_to_geometry, Geometry};
use crate::errors::ExecutorError;
use geo::algorithm::relate::Relate;
use geo::algorithm::Intersects;
use geo::Contains;
use geo::{Distance, Euclidean, Haversine};
use vibesql_types::SqlValue;

/// Helper function to convert WKT string to geo::Geometry
fn wkt_to_geo(wkt_str: &str) -> Result<geo::Geometry<f64>, ExecutorError> {
    // Parse WKT string into internal Geometry enum
    let sql_value = SqlValue::Varchar(arcstr::ArcStr::from(wkt_str));
    let geom_with_srid = sql_value_to_geometry(&sql_value)?;

    // Convert internal Geometry to geo::Geometry
    to_geo_geometry(&geom_with_srid.geometry)
}

/// Convert internal Geometry to geo::Geometry for spatial operations
fn to_geo_geometry(geom: &Geometry) -> Result<geo::Geometry<f64>, ExecutorError> {
    match geom {
        Geometry::Point { x, y } => Ok(geo::Geometry::Point(geo::Point::new(*x, *y))),
        Geometry::LineString { points } => {
            let coords: Vec<geo::Coord<f64>> =
                points.iter().map(|(x, y)| geo::Coord { x: *x, y: *y }).collect();
            Ok(geo::Geometry::LineString(geo::LineString(coords)))
        }
        Geometry::Polygon { rings } => {
            if rings.is_empty() {
                return Err(ExecutorError::SpatialGeometryError {
                    function_name: "to_geo_geometry".to_string(),
                    message: "Empty polygon".to_string(),
                });
            }

            let exterior: Vec<geo::Coord<f64>> =
                rings[0].iter().map(|(x, y)| geo::Coord { x: *x, y: *y }).collect();
            let exterior_ring = geo::LineString(exterior);

            let interiors: Vec<geo::LineString<f64>> = rings[1..]
                .iter()
                .map(|ring| {
                    let coords: Vec<geo::Coord<f64>> =
                        ring.iter().map(|(x, y)| geo::Coord { x: *x, y: *y }).collect();
                    geo::LineString(coords)
                })
                .collect();

            Ok(geo::Geometry::Polygon(geo::Polygon::new(exterior_ring, interiors)))
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Geometry type {} not yet fully supported for spatial predicates",
            geom.geometry_type()
        ))),
    }
}

/// ST_Contains(geom1, geom2) - Does geom1 completely contain geom2?
pub fn st_contains(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Contains".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let result = geom1.contains(&geom2);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Contains".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Within(geom1, geom2) - Is geom1 completely within geom2?
pub fn st_within(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Within".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let result = geom2.contains(&geom1);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Within".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Intersects(geom1, geom2) - Do geom1 and geom2 share any space?
pub fn st_intersects(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Intersects".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let result = geom1.intersects(&geom2);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Intersects".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Disjoint(geom1, geom2) - Do geom1 and geom2 share no space?
pub fn st_disjoint(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Disjoint".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            // Disjoint = NOT Intersects
            let result = !geom1.intersects(&geom2);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Disjoint".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Equals(geom1, geom2) - Are geom1 and geom2 spatially equal?
pub fn st_equals(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Equals".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let result = geom1 == geom2;
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Equals".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Touches(geom1, geom2) - DE-9IM: Boundaries touch but interiors don't intersect
/// Pattern: FT******* or F**T***** or F***T****
///
/// True when: Boundaries intersect, and at least one interior is disjoint from the other
pub fn st_touches(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Touches".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            // Use DE-9IM Relate for proper Touches predicate
            let relate_matrix = geom1.relate(&geom2);
            let result = relate_matrix.is_touches();

            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Touches".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Crosses(geom1, geom2) - DE-9IM: Geometries cross
///
/// True when:
/// - For point/line: geometries intersect, and their dimensions don't match (dimension mismatch)
/// - For line/polygon: geometries share some interior points but not all
/// - For other combos: topological crossing exists (dimension-dependent)
pub fn st_crosses(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Crosses".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            // Use DE-9IM Relate for proper Crosses predicate
            let relate_matrix = geom1.relate(&geom2);
            let result = relate_matrix.is_crosses();

            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Crosses".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Overlaps(geom1, geom2) - DE-9IM: Same-dimension geometries with overlapping interiors
/// Pattern: T*T***T** (for same-dimension geometries)
///
/// True when:
/// - Geometries have the same dimension
/// - Their interiors intersect (have points in common)
/// - Neither geometry is completely contained in the other
pub fn st_overlaps(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Overlaps".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            // Use DE-9IM Relate for proper Overlaps predicate
            let relate_matrix = geom1.relate(&geom2);
            let result = relate_matrix.is_overlaps();

            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Overlaps".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Covers(geom1, geom2) - Does geom1 cover geom2? (includes boundary)
pub fn st_covers(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Covers".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            // Covers is similar to Contains but includes boundaries
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            // For now, use Contains as approximation
            let result = geom1.contains(&geom2);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Covers".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_CoveredBy(geom1, geom2) - Is geom1 covered by geom2?
pub fn st_coveredby(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_CoveredBy".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let result = geom2.contains(&geom1);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_CoveredBy".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_DWithin(geom1, geom2, distance) - Are geometries within distance of each other?
///
/// Calculates Euclidean distance for all geometry type combinations.
/// Returns TRUE if distance(geom1, geom2) <= distance parameter.
///
/// Supported combinations:
/// - Point to Point: haversine distance (great-circle distance on sphere)
/// - Point to LineString: minimum distance to any point on the line
/// - Point to Polygon: 0 if inside, else distance to nearest boundary
/// - LineString to LineString: minimum distance between any points
/// - LineString to Polygon: 0 if intersecting, else distance to boundary
/// - Polygon to Polygon: 0 if intersecting/touching, else distance to nearest point
/// - All combinations using EuclideanDistance trait
pub fn st_dwithin(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_DWithin".to_string(),
            expected: "exactly 3 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    // Extract distance value as f64
    let distance = match &args[2] {
        SqlValue::Double(d) => *d,
        SqlValue::Numeric(d) => *d,
        SqlValue::Integer(i) => *i as f64,
        SqlValue::Float(f) => *f as f64,
        SqlValue::Null => return Ok(SqlValue::Null),
        _ => {
            return Err(ExecutorError::SpatialArgumentError {
                function_name: "ST_DWithin".to_string(),
                expected: "numeric distance".to_string(),
                actual: format!("{:?}", args[2].type_name()),
            })
        }
    };

    // Reject negative distances
    if distance < 0.0 {
        return Ok(SqlValue::Boolean(false));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            // Use Distance trait for all geometry combinations
            let dist = match (&geom1, &geom2) {
                (geo::Geometry::Point(p1), geo::Geometry::Point(p2)) => {
                    // For points, use haversine distance (great-circle distance on sphere)
                    Haversine.distance(*p1, *p2)
                }
                _ => {
                    // For all other geometry combinations, use Euclidean distance
                    Euclidean.distance(&geom1, &geom2)
                }
            };

            let result = dist <= distance;
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_DWithin".to_string(),
            expected: "(VARCHAR, VARCHAR, NUMERIC) arguments".to_string(),
            actual: format!(
                "{:?}, {:?}, {:?}",
                args[0].type_name(),
                args[1].type_name(),
                args[2].type_name()
            ),
        }),
    }
}

/// ST_Relate(geom1, geom2) - Return DE-9IM relationship (simplified version)
/// ST_Relate(geom1, geom2, pattern) - Test DE-9IM relationship against a pattern
///
/// Note: Simplified implementation - full DE-9IM computation is deferred to Phase 4
pub fn st_relate(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Relate".to_string(),
            expected: "2 or 3 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (
            SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
            SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2),
        ) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            if args.len() == 2 {
                // ST_Relate(geom1, geom2) - return DE-9IM string
                // Simplified: return a basic relationship indicator
                // Full implementation would compute the 9-intersection matrix
                if !geom1.intersects(&geom2) {
                    // Disjoint
                    Ok(SqlValue::Varchar(arcstr::ArcStr::from("FF*FF****")))
                } else if geom1.contains(&geom2) && !geom2.contains(&geom1) {
                    // Contains (but not equal)
                    Ok(SqlValue::Varchar(arcstr::ArcStr::from("T*F**F***")))
                } else if geom2.contains(&geom1) && !geom1.contains(&geom2) {
                    // Within (but not equal)
                    Ok(SqlValue::Varchar(arcstr::ArcStr::from("F*T**F***")))
                } else if geom1 == geom2 {
                    // Equals
                    Ok(SqlValue::Varchar(arcstr::ArcStr::from("T*F**FFF*")))
                } else {
                    // Intersects (but not fully one way or the other)
                    Ok(SqlValue::Varchar(arcstr::ArcStr::from("T*T***T**")))
                }
            } else {
                // ST_Relate(geom1, geom2, pattern) - test against pattern
                // This requires full DE-9IM computation which is deferred to Phase 4
                Err(ExecutorError::UnsupportedFeature(
                    "ST_Relate with pattern matching requires full DE-9IM implementation (Phase 4)"
                        .to_string(),
                ))
            }
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Relate".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}
