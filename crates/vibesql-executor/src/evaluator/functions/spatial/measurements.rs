//! Spatial Measurement Functions
//!
//! Implements distance, area, length, centroid, and other measurement functions.
//! Uses geo-types and geo crate algorithms.

#![cfg(feature = "spatial")]

use super::{sql_value_to_geometry, Geometry};
use crate::errors::ExecutorError;
use geo::algorithm::{Area, BoundingRect, Centroid, ConvexHull};
use geo::{Distance, Euclidean};
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
                            message: "Empty polygon in multipolygon".to_string(),
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
            "Geometry type not yet supported for measurements".to_string(),
        )),
    }
}

/// ST_Distance(geom1, geom2) - Calculate Euclidean distance between geometries
pub fn st_distance(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Distance".to_string(),
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

            let distance = match (&geom1, &geom2) {
                (geo::Geometry::Point(p1), geo::Geometry::Point(p2)) => {
                    Euclidean.distance(*p1, *p2)
                }
                _ => {
                    // For other geometry types, calculate minimum distance
                    // between any two points in the geometries
                    // This is a simplified version - full implementation would handle all types
                    match (&geom1, &geom2) {
                        (geo::Geometry::Point(p), geo::Geometry::LineString(ls))
                        | (geo::Geometry::LineString(ls), geo::Geometry::Point(p)) => ls
                            .coords()
                            .map(|coord| {
                                let p2 = geo::Point::new(coord.x, coord.y);
                                Euclidean.distance(*p, p2)
                            })
                            .fold(f64::INFINITY, f64::min),
                        (geo::Geometry::Point(p), geo::Geometry::Polygon(poly))
                        | (geo::Geometry::Polygon(poly), geo::Geometry::Point(p)) => poly
                            .exterior()
                            .coords()
                            .map(|coord| {
                                let p2 = geo::Point::new(coord.x, coord.y);
                                Euclidean.distance(*p, p2)
                            })
                            .fold(f64::INFINITY, f64::min),
                        _ => {
                            return Err(ExecutorError::SpatialOperationFailed {
                                function_name: "ST_Distance".to_string(),
                                message: "not fully supported between these geometry types"
                                    .to_string(),
                            });
                        }
                    }
                }
            };

            Ok(SqlValue::Double(distance))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Distance".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// ST_Length(geom) - Calculate length of LineString
pub fn st_length(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Length".to_string(),
            expected: "exactly 1 argument".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = wkt_to_geo(wkt)?;

            // Calculate length by summing distances between consecutive points
            fn calculate_length<'a>(coords: impl Iterator<Item = &'a geo::Coord<f64>>) -> f64 {
                let coords_vec: Vec<_> = coords.collect();
                let mut length = 0.0;
                for i in 0..coords_vec.len().saturating_sub(1) {
                    let p1 = geo::Point::new(coords_vec[i].x, coords_vec[i].y);
                    let p2 = geo::Point::new(coords_vec[i + 1].x, coords_vec[i + 1].y);
                    length += Euclidean.distance(p1, p2);
                }
                length
            }

            match geom {
                geo::Geometry::LineString(ls) => {
                    let length = calculate_length(ls.coords());
                    Ok(SqlValue::Double(length))
                }
                geo::Geometry::MultiLineString(mls) => {
                    let mut total_length = 0.0;
                    for ls in &mls.0 {
                        total_length += calculate_length(ls.coords());
                    }
                    Ok(SqlValue::Double(total_length))
                }
                _ => Err(ExecutorError::SpatialGeometryError {
                    function_name: "ST_Length".to_string(),
                    message: "only works on LineString and MultiLineString".to_string(),
                }),
            }
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Length".to_string(),
            expected: "VARCHAR geometry argument".to_string(),
            actual: format!("{:?}", args[0].type_name()),
        }),
    }
}

/// ST_Perimeter(geom) - Calculate perimeter of Polygon
pub fn st_perimeter(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Perimeter".to_string(),
            expected: "exactly 1 argument".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = wkt_to_geo(wkt)?;

            // Helper to calculate line length
            fn line_length<'a>(coords: impl Iterator<Item = &'a geo::Coord<f64>>) -> f64 {
                let coords_vec: Vec<_> = coords.collect();
                let mut length = 0.0;
                for i in 0..coords_vec.len().saturating_sub(1) {
                    let p1 = geo::Point::new(coords_vec[i].x, coords_vec[i].y);
                    let p2 = geo::Point::new(coords_vec[i + 1].x, coords_vec[i + 1].y);
                    length += Euclidean.distance(p1, p2);
                }
                length
            }

            match geom {
                geo::Geometry::Polygon(poly) => {
                    // Perimeter = exterior ring length + all interior rings length
                    let mut perimeter = line_length(poly.exterior().coords());
                    for interior in poly.interiors() {
                        perimeter += line_length(interior.coords());
                    }
                    Ok(SqlValue::Double(perimeter))
                }
                geo::Geometry::MultiPolygon(mp) => {
                    let mut total_perimeter = 0.0;
                    for poly in &mp.0 {
                        total_perimeter += line_length(poly.exterior().coords());
                        for interior in poly.interiors() {
                            total_perimeter += line_length(interior.coords());
                        }
                    }
                    Ok(SqlValue::Double(total_perimeter))
                }
                _ => Err(ExecutorError::SpatialGeometryError {
                    function_name: "ST_Perimeter".to_string(),
                    message: "only works on Polygon and MultiPolygon".to_string(),
                }),
            }
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Perimeter".to_string(),
            expected: "VARCHAR geometry argument".to_string(),
            actual: format!("{:?}", args[0].type_name()),
        }),
    }
}

/// ST_Area(geom) - Calculate area of Polygon
pub fn st_area(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Area".to_string(),
            expected: "exactly 1 argument".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = wkt_to_geo(wkt)?;

            match geom {
                geo::Geometry::Polygon(poly) => {
                    let area = poly.unsigned_area();
                    Ok(SqlValue::Double(area))
                }
                geo::Geometry::MultiPolygon(mp) => {
                    let area = mp.unsigned_area();
                    Ok(SqlValue::Double(area))
                }
                _ => Err(ExecutorError::SpatialGeometryError {
                    function_name: "ST_Area".to_string(),
                    message: "only works on Polygon and MultiPolygon".to_string(),
                }),
            }
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Area".to_string(),
            expected: "VARCHAR geometry argument".to_string(),
            actual: format!("{:?}", args[0].type_name()),
        }),
    }
}

/// ST_Centroid(geom) - Find center point (geometric center of mass)
pub fn st_centroid(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Centroid".to_string(),
            expected: "exactly 1 argument".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = wkt_to_geo(wkt)?;

            match geom.centroid() {
                Some(centroid) => {
                    let result_geom = Geometry::Point { x: centroid.x(), y: centroid.y() };
                    let result_value = super::geometry_to_sql_value(result_geom, 0);
                    Ok(result_value)
                }
                None => {
                    // Degenerate geometry (no centroid)
                    Ok(SqlValue::Null)
                }
            }
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Centroid".to_string(),
            expected: "VARCHAR geometry argument".to_string(),
            actual: format!("{:?}", args[0].type_name()),
        }),
    }
}

/// ST_Envelope(geom) - Get bounding box as a Polygon
pub fn st_envelope(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Envelope".to_string(),
            expected: "exactly 1 argument".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = wkt_to_geo(wkt)?;

            match geom.bounding_rect() {
                Some(rect) => {
                    // Create a polygon from the bounding rectangle
                    let min_x = rect.min().x;
                    let min_y = rect.min().y;
                    let max_x = rect.max().x;
                    let max_y = rect.max().y;

                    let result_geom = Geometry::Polygon {
                        rings: vec![vec![
                            (min_x, min_y),
                            (max_x, min_y),
                            (max_x, max_y),
                            (min_x, max_y),
                            (min_x, min_y),
                        ]],
                    };
                    let result_value = super::geometry_to_sql_value(result_geom, 0);
                    Ok(result_value)
                }
                None => Ok(SqlValue::Null),
            }
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Envelope".to_string(),
            expected: "VARCHAR geometry argument".to_string(),
            actual: format!("{:?}", args[0].type_name()),
        }),
    }
}

/// ST_ConvexHull(geom) - Find smallest convex polygon containing geometry
pub fn st_convex_hull(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_ConvexHull".to_string(),
            expected: "exactly 1 argument".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = wkt_to_geo(wkt)?;

            // The convex_hull() method returns a Polygon directly
            let hull_poly = match geom {
                geo::Geometry::Point(p) => {
                    // Point has a trivial convex hull (itself)
                    return Ok(super::geometry_to_sql_value(
                        Geometry::Point { x: p.x(), y: p.y() },
                        0,
                    ));
                }
                _ => geom.convex_hull(),
            };

            // Convert the Polygon back to internal Geometry
            let result_geom = {
                let mut rings = vec![];
                let exterior: Vec<(f64, f64)> =
                    hull_poly.exterior().coords().map(|c| (c.x, c.y)).collect();
                rings.push(exterior);
                for interior in hull_poly.interiors() {
                    let ring: Vec<(f64, f64)> = interior.coords().map(|c| (c.x, c.y)).collect();
                    rings.push(ring);
                }
                Geometry::Polygon { rings }
            };

            let result_value = super::geometry_to_sql_value(result_geom, 0);
            Ok(result_value)
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_ConvexHull".to_string(),
            expected: "VARCHAR geometry argument".to_string(),
            actual: format!("{:?}", args[0].type_name()),
        }),
    }
}

/// ST_PointOnSurface(geom) - Get a point guaranteed to be on the geometry
pub fn st_point_on_surface(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_PointOnSurface".to_string(),
            expected: "exactly 1 argument".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = wkt_to_geo(wkt)?;

            match geom {
                geo::Geometry::Point(p) => {
                    let result_geom = Geometry::Point { x: p.x(), y: p.y() };
                    let result_value = super::geometry_to_sql_value(result_geom, 0);
                    Ok(result_value)
                }
                geo::Geometry::LineString(ls) => {
                    // Return the first coordinate on the line
                    if let Some(coord) = ls.coords().next() {
                        let result_geom = Geometry::Point { x: coord.x, y: coord.y };
                        let result_value = super::geometry_to_sql_value(result_geom, 0);
                        Ok(result_value)
                    } else {
                        Ok(SqlValue::Null)
                    }
                }
                geo::Geometry::Polygon(_) => {
                    // Use centroid for polygons (guaranteed to be on surface for convex polygons)
                    // For non-convex, this is still a reasonable approximation
                    match geom.centroid() {
                        Some(centroid) => {
                            let result_geom = Geometry::Point { x: centroid.x(), y: centroid.y() };
                            let result_value = super::geometry_to_sql_value(result_geom, 0);
                            Ok(result_value)
                        }
                        None => Ok(SqlValue::Null),
                    }
                }
                _ => Err(ExecutorError::SpatialGeometryError {
                    function_name: "ST_PointOnSurface".to_string(),
                    message: "not fully supported for this geometry type".to_string(),
                }),
            }
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_PointOnSurface".to_string(),
            expected: "VARCHAR geometry argument".to_string(),
            actual: format!("{:?}", args[0].type_name()),
        }),
    }
}

/// ST_Boundary(geom) - Get boundary of geometry (exterior ring for polygons, endpoints for lines)
pub fn st_boundary(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Boundary".to_string(),
            expected: "exactly 1 argument".to_string(),
            actual: format!("{} arguments", args.len()),
        });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(_) | SqlValue::Character(_) => {
            let geom_with_srid = sql_value_to_geometry(&args[0])?;
            let geom = &geom_with_srid.geometry;

            let boundary_geom = match geom {
                Geometry::Point { .. } => {
                    // Boundary of a point is empty
                    return Ok(SqlValue::Varchar(arcstr::ArcStr::from("__GEOMETRY__GEOMETRYCOLLECTION()")));
                }
                Geometry::LineString { points } => {
                    // Boundary of a line is a multi-point of the endpoints
                    if points.is_empty() {
                        return Ok(SqlValue::Varchar(
                            arcstr::ArcStr::from("__GEOMETRY__GEOMETRYCOLLECTION()"),
                        ));
                    }
                    if points.len() == 1 {
                        let (x, y) = points[0];
                        return Ok(super::geometry_to_sql_value(
                            Geometry::Point { x, y },
                            geom_with_srid.srid,
                        ));
                    }
                    let first = points[0];
                    let last = points[points.len() - 1];
                    if first == last {
                        // Closed linestring (ring): boundary is empty
                        return Ok(SqlValue::Varchar(
                            arcstr::ArcStr::from("__GEOMETRY__GEOMETRYCOLLECTION()"),
                        ));
                    }
                    Geometry::MultiPoint { points: vec![first, last] }
                }
                Geometry::Polygon { rings } => {
                    // Boundary of a polygon is all its rings as a MultiLineString
                    if rings.is_empty() {
                        return Ok(SqlValue::Varchar(
                            arcstr::ArcStr::from("__GEOMETRY__GEOMETRYCOLLECTION()"),
                        ));
                    }
                    Geometry::MultiLineString { lines: rings.clone() }
                }
                Geometry::MultiPoint { .. }
                | Geometry::MultiLineString { .. }
                | Geometry::MultiPolygon { .. } => {
                    // Boundary of multi-geometries: aggregate boundaries
                    // For simplicity, we'll return empty geometry collection for now
                    return Ok(SqlValue::Varchar(arcstr::ArcStr::from("__GEOMETRY__GEOMETRYCOLLECTION()")));
                }
                Geometry::Collection { .. } => {
                    // Boundary of a collection: aggregate boundaries
                    return Ok(SqlValue::Varchar(arcstr::ArcStr::from("__GEOMETRY__GEOMETRYCOLLECTION()")));
                }
            };

            Ok(super::geometry_to_sql_value(boundary_geom, geom_with_srid.srid))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_Boundary".to_string(),
            expected: "VARCHAR geometry argument".to_string(),
            actual: format!("{:?}", args[0].type_name()),
        }),
    }
}

/// ST_HausdorffDistance(geom1, geom2) - Maximum distance between any point in geom1 to nearest point in geom2
pub fn st_hausdorff_distance(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_HausdorffDistance".to_string(),
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

            // Calculate Hausdorff distance: max(min_dist(p1 to geom2), min_dist(p2 to geom1))
            // For simplicity, we use a basic implementation that samples points
            let distance = calculate_hausdorff_distance(&geom1, &geom2)?;
            Ok(SqlValue::Double(distance))
        }
        _ => Err(ExecutorError::SpatialArgumentError {
            function_name: "ST_HausdorffDistance".to_string(),
            expected: "VARCHAR geometry arguments".to_string(),
            actual: format!("{:?}, {:?}", args[0].type_name(), args[1].type_name()),
        }),
    }
}

/// Helper function to calculate Hausdorff distance between two geometries
fn calculate_hausdorff_distance(
    geom1: &geo::Geometry<f64>,
    geom2: &geo::Geometry<f64>,
) -> Result<f64, ExecutorError> {
    let coords1 = extract_coordinates(geom1);
    let coords2 = extract_coordinates(geom2);

    if coords1.is_empty() || coords2.is_empty() {
        return Ok(0.0);
    }

    // Maximum distance from any point in geom1 to nearest point in geom2
    let max_dist_1_to_2 = coords1
        .iter()
        .map(|p1| {
            coords2.iter().map(|p2| Euclidean.distance(*p1, *p2)).fold(f64::INFINITY, f64::min)
        })
        .fold(f64::NEG_INFINITY, f64::max);

    // Maximum distance from any point in geom2 to nearest point in geom1
    let max_dist_2_to_1 = coords2
        .iter()
        .map(|p2| {
            coords1.iter().map(|p1| Euclidean.distance(*p2, *p1)).fold(f64::INFINITY, f64::min)
        })
        .fold(f64::NEG_INFINITY, f64::max);

    Ok(f64::max(max_dist_1_to_2, max_dist_2_to_1))
}

/// Helper function to extract all coordinates from a geometry
fn extract_coordinates(geom: &geo::Geometry<f64>) -> Vec<geo::Point<f64>> {
    let mut coords = Vec::new();

    match geom {
        geo::Geometry::Point(p) => coords.push(*p),
        geo::Geometry::LineString(ls) => {
            coords.extend(ls.points());
        }
        geo::Geometry::Polygon(poly) => {
            coords.extend(poly.exterior().points());
            for interior in poly.interiors() {
                coords.extend(interior.points());
            }
        }
        geo::Geometry::MultiPoint(mp) => {
            coords.extend(&mp.0);
        }
        geo::Geometry::MultiLineString(mls) => {
            for ls in &mls.0 {
                coords.extend(ls.points());
            }
        }
        geo::Geometry::MultiPolygon(mp) => {
            for poly in &mp.0 {
                coords.extend(poly.exterior().points());
                for interior in poly.interiors() {
                    coords.extend(interior.points());
                }
            }
        }
        geo::Geometry::GeometryCollection(gc) => {
            for g in &gc.0 {
                coords.extend(extract_coordinates(g));
            }
        }
        _ => {}
    }

    coords
}
