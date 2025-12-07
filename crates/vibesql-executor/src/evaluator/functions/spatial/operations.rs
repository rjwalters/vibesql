//! Spatial Operation Functions
//!
//! Implements set operations like union, intersection, difference, simplification, and buffering.
//! Uses geo-types and geo crate algorithms.

#![cfg(feature = "spatial")]

use super::{geometry_to_sql_value, sql_value_to_geometry, Geometry};
use crate::errors::ExecutorError;
use geo::algorithm::{BooleanOps, Simplify};
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
                    message: "empty polygon has no rings".to_string(),
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
        Geometry::MultiPoint { points } => {
            let points: Vec<geo::Point<f64>> =
                points.iter().map(|(x, y)| geo::Point::new(*x, *y)).collect();
            Ok(geo::Geometry::MultiPoint(geo::MultiPoint(points)))
        }
        Geometry::MultiLineString { lines } => {
            let lines: Vec<geo::LineString<f64>> = lines
                .iter()
                .map(|line| {
                    let coords: Vec<geo::Coord<f64>> =
                        line.iter().map(|(x, y)| geo::Coord { x: *x, y: *y }).collect();
                    geo::LineString(coords)
                })
                .collect();
            Ok(geo::Geometry::MultiLineString(geo::MultiLineString(lines)))
        }
        Geometry::MultiPolygon { polygons } => {
            let polys: Vec<geo::Polygon<f64>> = polygons
                .iter()
                .map(|poly_rings| {
                    if poly_rings.is_empty() {
                        Err(ExecutorError::SpatialGeometryError {
                            function_name: "to_geo_geometry".to_string(),
                            message: "empty polygon in multipolygon".to_string(),
                        })
                    } else {
                        let exterior: Vec<geo::Coord<f64>> = poly_rings[0]
                            .iter()
                            .map(|(x, y)| geo::Coord { x: *x, y: *y })
                            .collect();
                        let exterior_ring = geo::LineString(exterior);

                        let interiors: Vec<geo::LineString<f64>> = poly_rings[1..]
                            .iter()
                            .map(|ring| {
                                let coords: Vec<geo::Coord<f64>> =
                                    ring.iter().map(|(x, y)| geo::Coord { x: *x, y: *y }).collect();
                                geo::LineString(coords)
                            })
                            .collect();

                        Ok(geo::Polygon::new(exterior_ring, interiors))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(geo::Geometry::MultiPolygon(geo::MultiPolygon(polys)))
        }
        Geometry::Collection { geometries } => {
            let geoms: Vec<geo::Geometry<f64>> =
                geometries.iter().map(to_geo_geometry).collect::<Result<Vec<_>, _>>()?;
            Ok(geo::Geometry::GeometryCollection(geo::GeometryCollection(geoms)))
        }
    }
}

/// Convert geo::Geometry back to internal Geometry representation
#[allow(dead_code)]
fn from_geo_geometry(geom: &geo::Geometry<f64>) -> Result<Geometry, ExecutorError> {
    match geom {
        geo::Geometry::Point(p) => Ok(Geometry::Point { x: p.x(), y: p.y() }),
        geo::Geometry::LineString(ls) => {
            let points = ls.coords().map(|c| (c.x, c.y)).collect();
            Ok(Geometry::LineString { points })
        }
        geo::Geometry::Polygon(poly) => {
            let mut rings = vec![];
            let exterior: Vec<(f64, f64)> = poly.exterior().coords().map(|c| (c.x, c.y)).collect();
            rings.push(exterior);
            for interior in poly.interiors() {
                let ring: Vec<(f64, f64)> = interior.coords().map(|c| (c.x, c.y)).collect();
                rings.push(ring);
            }
            Ok(Geometry::Polygon { rings })
        }
        geo::Geometry::MultiPoint(mp) => {
            let points = mp.0.iter().map(|p| (p.x(), p.y())).collect();
            Ok(Geometry::MultiPoint { points })
        }
        geo::Geometry::MultiLineString(mls) => {
            let lines = mls.0.iter().map(|ls| ls.coords().map(|c| (c.x, c.y)).collect()).collect();
            Ok(Geometry::MultiLineString { lines })
        }
        geo::Geometry::MultiPolygon(mp) => {
            let polygons = mp
                .0
                .iter()
                .map(|poly| {
                    let mut rings = vec![];
                    let exterior: Vec<(f64, f64)> =
                        poly.exterior().coords().map(|c| (c.x, c.y)).collect();
                    rings.push(exterior);
                    for interior in poly.interiors() {
                        let ring: Vec<(f64, f64)> = interior.coords().map(|c| (c.x, c.y)).collect();
                        rings.push(ring);
                    }
                    rings
                })
                .collect();
            Ok(Geometry::MultiPolygon { polygons })
        }
        geo::Geometry::GeometryCollection(gc) => {
            let geometries = gc.0.iter().map(from_geo_geometry).collect::<Result<Vec<_>, _>>()?;
            Ok(Geometry::Collection { geometries })
        }
        _ => Err(ExecutorError::UnsupportedFeature(
            "Geometry type not yet supported for operations".to_string(),
        )),
    }
}

/// ST_Simplify(geom, tolerance) - Simplify geometry using Douglas-Peucker algorithm
pub fn st_simplify(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Simplify".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt), tol) | (SqlValue::Character(wkt), tol) => {
            let tolerance = match tol {
                SqlValue::Double(d) => *d,
                SqlValue::Integer(i) => *i as f64,
                _ => {
                    return Err(ExecutorError::SpatialArgumentError {
                        function_name: "ST_Simplify".to_string(),
                        expected: "DOUBLE tolerance".to_string(),
                        actual: format!("{:?}", tol),
                    })
                }
            };

            if tolerance < 0.0 {
                return Err(ExecutorError::SpatialGeometryError {
                    function_name: "ST_Simplify".to_string(),
                    message: "tolerance must be non-negative".to_string(),
                });
            }

            let geom = wkt_to_geo(wkt)?;

            let simplified = match geom {
                geo::Geometry::LineString(ls) => {
                    let simplified_ls = ls.simplify(tolerance);
                    geo::Geometry::LineString(simplified_ls)
                }
                geo::Geometry::MultiLineString(mls) => {
                    let simplified_mls = mls.0.iter().map(|ls| ls.simplify(tolerance)).collect();
                    geo::Geometry::MultiLineString(geo::MultiLineString(simplified_mls))
                }
                geo::Geometry::Polygon(poly) => {
                    let simplified_poly = poly.simplify(tolerance);
                    geo::Geometry::Polygon(simplified_poly)
                }
                geo::Geometry::MultiPolygon(mp) => {
                    let simplified_mp = mp.0.iter().map(|p| p.simplify(tolerance)).collect();
                    geo::Geometry::MultiPolygon(geo::MultiPolygon(simplified_mp))
                }
                _ => {
                    // For Point geometries, simplification has no effect
                    geom
                }
            };

            let result_geom = from_geo_geometry(&simplified)?;
            let result_value = geometry_to_sql_value(result_geom, 0);
            Ok(result_value)
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Simplify".to_string(),
            expected: "VARCHAR geometry and DOUBLE tolerance".to_string(),
            actual: format!("{:?}, {:?}", args[0], args[1]),
        }),
    }
}

/// ST_Union(geom1, geom2) - Combine two geometries
pub fn st_union(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Union".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1), SqlValue::Varchar(wkt2))
        | (SqlValue::Character(wkt1), SqlValue::Character(wkt2))
        | (SqlValue::Varchar(wkt1), SqlValue::Character(wkt2))
        | (SqlValue::Character(wkt1), SqlValue::Varchar(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let unioned = match (&geom1, &geom2) {
                (geo::Geometry::Polygon(p1), geo::Geometry::Polygon(p2)) => {
                    let union_result = p1.union(p2);
                    union_result.into()
                }
                _ => {
                    // For non-polygon geometries, return a GeometryCollection
                    geo::Geometry::GeometryCollection(geo::GeometryCollection(vec![geom1, geom2]))
                }
            };

            let result_geom = from_geo_geometry(&unioned)?;
            let result_value = geometry_to_sql_value(result_geom, 0);
            Ok(result_value)
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Union".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0], args[1]),
        }),
    }
}

/// ST_Intersection(geom1, geom2) - Find intersection of two geometries
pub fn st_intersection(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Intersection".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1), SqlValue::Varchar(wkt2))
        | (SqlValue::Character(wkt1), SqlValue::Character(wkt2))
        | (SqlValue::Varchar(wkt1), SqlValue::Character(wkt2))
        | (SqlValue::Character(wkt1), SqlValue::Varchar(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let intersection = match (&geom1, &geom2) {
                (geo::Geometry::Polygon(p1), geo::Geometry::Polygon(p2)) => {
                    let int_result = p1.intersection(p2);
                    int_result.into()
                }
                _ => {
                    return Err(ExecutorError::SpatialGeometryError {
                        function_name: "ST_Intersection".to_string(),
                        message: "currently only supports polygon-polygon intersection".to_string(),
                    });
                }
            };

            let result_geom = from_geo_geometry(&intersection)?;
            let result_value = geometry_to_sql_value(result_geom, 0);
            Ok(result_value)
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Intersection".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0], args[1]),
        }),
    }
}

/// ST_Difference(geom1, geom2) - Subtract geom2 from geom1
pub fn st_difference(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Difference".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1), SqlValue::Varchar(wkt2))
        | (SqlValue::Character(wkt1), SqlValue::Character(wkt2))
        | (SqlValue::Varchar(wkt1), SqlValue::Character(wkt2))
        | (SqlValue::Character(wkt1), SqlValue::Varchar(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let difference = match (&geom1, &geom2) {
                (geo::Geometry::Polygon(p1), geo::Geometry::Polygon(p2)) => {
                    let diff_result = p1.difference(p2);
                    diff_result.into()
                }
                _ => {
                    return Err(ExecutorError::SpatialGeometryError {
                        function_name: "ST_Difference".to_string(),
                        message: "currently only supports polygon-polygon difference".to_string(),
                    });
                }
            };

            let result_geom = from_geo_geometry(&difference)?;
            let result_value = geometry_to_sql_value(result_geom, 0);
            Ok(result_value)
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Difference".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0], args[1]),
        }),
    }
}

/// ST_SymDifference(geom1, geom2) - Parts in either but not both (symmetric difference / XOR)
pub fn st_sym_difference(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_SymDifference".to_string(),
            expected: "exactly 2 arguments".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1), SqlValue::Varchar(wkt2))
        | (SqlValue::Character(wkt1), SqlValue::Character(wkt2))
        | (SqlValue::Varchar(wkt1), SqlValue::Character(wkt2))
        | (SqlValue::Character(wkt1), SqlValue::Varchar(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;

            let sym_diff = match (&geom1, &geom2) {
                (geo::Geometry::Polygon(p1), geo::Geometry::Polygon(p2)) => {
                    // Symmetric difference = (A - B) UNION (B - A)
                    let diff_1_2 = p1.difference(p2); // Returns MultiPolygon
                    let diff_2_1 = p2.difference(p1); // Returns MultiPolygon

                    // Union the two result multipolygons
                    let union_result = diff_1_2.union(&diff_2_1);
                    geo::Geometry::MultiPolygon(union_result)
                }
                _ => {
                    return Err(ExecutorError::SpatialGeometryError {
                        function_name: "ST_SymDifference".to_string(),
                        message: "currently only supports polygon-polygon symmetric difference"
                            .to_string(),
                    });
                }
            };

            let result_geom = from_geo_geometry(&sym_diff)?;
            let result_value = geometry_to_sql_value(result_geom, 0);
            Ok(result_value)
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_SymDifference".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0], args[1]),
        }),
    }
}
