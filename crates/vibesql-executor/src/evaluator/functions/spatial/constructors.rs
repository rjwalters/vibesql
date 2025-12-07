//! Spatial constructor functions
//!
//! Creates geometry from text (WKT) and binary (WKB) formats.

use super::Geometry;
use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

/// Parse WKT (Well-Known Text) format
/// Examples: POINT(1 2), LINESTRING(0 0, 1 1), POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))
pub fn parse_wkt(wkt: &str) -> Result<Geometry, ExecutorError> {
    let wkt = wkt.trim();

    if wkt.starts_with("POINT") {
        parse_point(wkt)
    } else if wkt.starts_with("LINESTRING") {
        parse_linestring(wkt)
    } else if wkt.starts_with("POLYGON") {
        parse_polygon(wkt)
    } else if wkt.starts_with("MULTIPOINT") {
        parse_multipoint(wkt)
    } else if wkt.starts_with("MULTILINESTRING") {
        parse_multilinestring(wkt)
    } else if wkt.starts_with("MULTIPOLYGON") {
        parse_multipolygon(wkt)
    } else if wkt.starts_with("GEOMETRYCOLLECTION") {
        parse_geometrycollection(wkt)
    } else {
        Err(ExecutorError::UnsupportedFeature(format!("Unknown WKT geometry type: {}", wkt)))
    }
}

fn parse_point(wkt: &str) -> Result<Geometry, ExecutorError> {
    // POINT(x y) or POINT EMPTY
    if wkt.contains("EMPTY") {
        return Err(ExecutorError::UnsupportedFeature(
            "Empty geometries not yet supported".to_string(),
        ));
    }

    let coords = extract_coordinates(wkt, 1)?;
    if coords.is_empty() || coords[0].is_empty() {
        return Err(ExecutorError::UnsupportedFeature("Invalid POINT format".to_string()));
    }

    let (x, y) = coords[0][0];
    Ok(Geometry::Point { x, y })
}

fn parse_linestring(wkt: &str) -> Result<Geometry, ExecutorError> {
    // LINESTRING(x1 y1, x2 y2, ...)
    if wkt.contains("EMPTY") {
        return Err(ExecutorError::UnsupportedFeature(
            "Empty geometries not yet supported".to_string(),
        ));
    }

    let coords = extract_coordinates(wkt, 1)?;
    if coords.is_empty() || coords[0].len() < 2 {
        return Err(ExecutorError::UnsupportedFeature(
            "LineString must have at least 2 points".to_string(),
        ));
    }

    Ok(Geometry::LineString { points: coords[0].clone() })
}

fn parse_polygon(wkt: &str) -> Result<Geometry, ExecutorError> {
    // POLYGON((x1 y1, x2 y2, ...), (x1 y1, ...))
    if wkt.contains("EMPTY") {
        return Err(ExecutorError::UnsupportedFeature(
            "Empty geometries not yet supported".to_string(),
        ));
    }

    let coords = extract_coordinates(wkt, 2)?;
    if coords.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "Polygon must have at least one ring".to_string(),
        ));
    }

    Ok(Geometry::Polygon { rings: coords })
}

fn parse_multipoint(wkt: &str) -> Result<Geometry, ExecutorError> {
    // MULTIPOINT(x1 y1, x2 y2, ...) or MULTIPOINT((x1 y1), (x2 y2), ...)
    if wkt.contains("EMPTY") {
        return Err(ExecutorError::UnsupportedFeature(
            "Empty geometries not yet supported".to_string(),
        ));
    }

    let content = extract_parentheses_content(wkt)?;

    // Check if this is the parenthesized format: (x1 y1), (x2 y2), ...
    // by looking for opening paren after potential whitespace
    let has_inner_parens = content.trim_start().starts_with('(');

    let coords = if has_inner_parens {
        // Format: (x1 y1), (x2 y2), ...
        // Parse each parenthesized group
        let mut points = Vec::new();
        let mut current_group = String::new();
        let mut paren_count = 0;

        for ch in content.chars() {
            match ch {
                '(' => {
                    paren_count += 1;
                    if paren_count > 1 {
                        current_group.push(ch);
                    }
                }
                ')' => {
                    paren_count -= 1;
                    if paren_count > 0 {
                        current_group.push(ch);
                    } else if !current_group.is_empty() {
                        let coords = parse_coordinate_pair(current_group.trim())?;
                        points.push(coords);
                        current_group.clear();
                    }
                }
                ',' if paren_count == 0 => {
                    // Skip commas between parenthesized groups
                }
                _ => {
                    if paren_count > 0 {
                        current_group.push(ch);
                    }
                }
            }
        }

        if points.is_empty() {
            return Err(ExecutorError::UnsupportedFeature(
                "MultiPoint must have at least one point".to_string(),
            ));
        }

        vec![points]
    } else {
        // Format: x1 y1, x2 y2, ...
        extract_coordinates(wkt, 1)?
    };

    if coords.is_empty() || coords[0].is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "MultiPoint must have at least one point".to_string(),
        ));
    }

    Ok(Geometry::MultiPoint { points: coords[0].clone() })
}

fn parse_multilinestring(wkt: &str) -> Result<Geometry, ExecutorError> {
    // MULTILINESTRING((x1 y1, x2 y2, ...), (x1 y1, ...))
    if wkt.contains("EMPTY") {
        return Err(ExecutorError::UnsupportedFeature(
            "Empty geometries not yet supported".to_string(),
        ));
    }

    let coords = extract_coordinates(wkt, 2)?;
    if coords.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "MultiLineString must have at least one linestring".to_string(),
        ));
    }

    Ok(Geometry::MultiLineString { lines: coords })
}

fn parse_multipolygon(wkt: &str) -> Result<Geometry, ExecutorError> {
    // MULTIPOLYGON(((x1 y1, x2 y2, ...), (x1 y1, ...)), ...)
    if wkt.contains("EMPTY") {
        return Err(ExecutorError::UnsupportedFeature(
            "Empty geometries not yet supported".to_string(),
        ));
    }

    let content = extract_parentheses_content(wkt)?;

    // Split by top-level commas to get individual polygons
    let mut polygons = Vec::new();
    let mut current_polygon = String::new();
    let mut paren_count = 0;

    for ch in content.chars() {
        match ch {
            '(' => {
                paren_count += 1;
                current_polygon.push(ch);
            }
            ')' => {
                paren_count -= 1;
                current_polygon.push(ch);
            }
            ',' if paren_count == 0 => {
                // Top-level comma separates polygons
                if !current_polygon.is_empty() {
                    let poly_wkt = format!("POLYGON{}", current_polygon.trim());
                    let poly = parse_polygon(&poly_wkt)?;
                    if let Geometry::Polygon { rings } = poly {
                        polygons.push(rings);
                    }
                    current_polygon.clear();
                }
            }
            _ => {
                current_polygon.push(ch);
            }
        }
    }

    // Don't forget the last polygon
    if !current_polygon.is_empty() {
        let poly_wkt = format!("POLYGON{}", current_polygon.trim());
        let poly = parse_polygon(&poly_wkt)?;
        if let Geometry::Polygon { rings } = poly {
            polygons.push(rings);
        }
    }

    if polygons.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "MultiPolygon must have at least one polygon".to_string(),
        ));
    }

    Ok(Geometry::MultiPolygon { polygons })
}

fn parse_geometrycollection(wkt: &str) -> Result<Geometry, ExecutorError> {
    // GEOMETRYCOLLECTION(POINT(...), LINESTRING(...), ...)
    if wkt.contains("EMPTY") {
        return Err(ExecutorError::UnsupportedFeature(
            "Empty geometries not yet supported".to_string(),
        ));
    }

    // Extract the content between parentheses
    let content = extract_parentheses_content(wkt)?;

    // Split by top-level commas and parse each geometry
    let geometries = split_geometries(&content)?
        .iter()
        .map(|g| parse_wkt(g.trim()))
        .collect::<Result<Vec<_>, _>>()?;

    if geometries.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "GeometryCollection must have at least one geometry".to_string(),
        ));
    }

    Ok(Geometry::Collection { geometries })
}

/// Extract coordinates from WKT format
/// paren_depth: 1 for POINT/LINESTRING, 2 for POLYGON/MULTIPOINT/MULTILINESTRING, 3 for MULTIPOLYGON
fn extract_coordinates(
    wkt: &str,
    paren_depth: usize,
) -> Result<Vec<Vec<(f64, f64)>>, ExecutorError> {
    let content = extract_parentheses_content(wkt)?;

    let mut result = Vec::new();
    let mut current_coords = Vec::new();

    let mut paren_count = 0;
    let mut current_group = String::new();

    for ch in content.chars() {
        match ch {
            '(' => {
                paren_count += 1;
                if paren_count > 1 || paren_depth == 1 {
                    current_group.push(ch);
                }
            }
            ')' => {
                paren_count -= 1;
                if paren_count > 0 || paren_depth == 1 {
                    current_group.push(ch);
                } else if !current_group.is_empty() {
                    // Process the group
                    let coords = parse_coordinate_list(&current_group)?;
                    current_coords.extend(coords);
                    current_group.clear();
                }
            }
            ',' if paren_count == 0 => {
                if paren_depth == 1 {
                    // Parse as individual points
                    let coords = parse_coordinate_pair(current_group.trim())?;
                    current_coords.push(coords);
                    current_group.clear();
                } else if !current_coords.is_empty() {
                    result.push(current_coords.clone());
                    current_coords.clear();
                }
            }
            _ => current_group.push(ch),
        }
    }

    // Handle remaining data
    if !current_group.is_empty() && paren_depth == 1 {
        let coords = parse_coordinate_pair(current_group.trim())?;
        current_coords.push(coords);
    }

    if !current_coords.is_empty() {
        result.push(current_coords);
    }

    Ok(result)
}

fn extract_parentheses_content(wkt: &str) -> Result<String, ExecutorError> {
    let start = wkt.find('(').ok_or_else(|| {
        ExecutorError::UnsupportedFeature(
            "Invalid WKT format: missing opening parenthesis".to_string(),
        )
    })?;
    let end = wkt.rfind(')').ok_or_else(|| {
        ExecutorError::UnsupportedFeature(
            "Invalid WKT format: missing closing parenthesis".to_string(),
        )
    })?;

    if start >= end {
        return Err(ExecutorError::UnsupportedFeature("Invalid WKT format".to_string()));
    }

    Ok(wkt[start + 1..end].to_string())
}

fn parse_coordinate_list(coords_str: &str) -> Result<Vec<(f64, f64)>, ExecutorError> {
    coords_str.split(',').map(|pair| parse_coordinate_pair(pair.trim())).collect()
}

fn parse_coordinate_pair(pair: &str) -> Result<(f64, f64), ExecutorError> {
    let parts: Vec<&str> = pair.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "Invalid coordinate pair: {}",
            pair
        )));
    }

    let x = parts[0].parse::<f64>().map_err(|_| {
        ExecutorError::UnsupportedFeature(format!("Invalid X coordinate: {}", parts[0]))
    })?;

    let y = parts[1].parse::<f64>().map_err(|_| {
        ExecutorError::UnsupportedFeature(format!("Invalid Y coordinate: {}", parts[1]))
    })?;

    Ok((x, y))
}

fn split_geometries(content: &str) -> Result<Vec<String>, ExecutorError> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_count = 0;

    for ch in content.chars() {
        match ch {
            '(' => {
                paren_count += 1;
                current.push(ch);
            }
            ')' => {
                paren_count -= 1;
                current.push(ch);
            }
            ',' if paren_count == 0 => {
                if !current.is_empty() {
                    result.push(current.trim().to_string());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        result.push(current.trim().to_string());
    }

    Ok(result)
}

/// ST_GeomFromText(wkt_string) - Create geometry from Well-Known Text
pub fn st_geom_from_text(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_GeomFromText requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let wkt = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => &**s,
        SqlValue::Null => return Ok(SqlValue::Null),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ST_GeomFromText requires a string argument".to_string(),
            ))
        }
    };

    let geom = parse_wkt(wkt)?;
    Ok(super::geometry_to_sql_value(geom, 0))
}

/// ST_PointFromText(wkt_string) - Create POINT from WKT
pub fn st_point_from_text(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_PointFromText requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let wkt = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => &**s,
        SqlValue::Null => return Ok(SqlValue::Null),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ST_PointFromText requires a string argument".to_string(),
            ))
        }
    };

    let geom = parse_wkt(wkt)?;
    if !matches!(geom, Geometry::Point { .. }) {
        return Err(ExecutorError::UnsupportedFeature(
            "ST_PointFromText requires a POINT geometry".to_string(),
        ));
    }

    Ok(super::geometry_to_sql_value(geom, 0))
}

/// ST_LineFromText(wkt_string) - Create LINESTRING from WKT
pub fn st_line_from_text(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_LineFromText requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let wkt = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => &**s,
        SqlValue::Null => return Ok(SqlValue::Null),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ST_LineFromText requires a string argument".to_string(),
            ))
        }
    };

    let geom = parse_wkt(wkt)?;
    if !matches!(geom, Geometry::LineString { .. }) {
        return Err(ExecutorError::UnsupportedFeature(
            "ST_LineFromText requires a LINESTRING geometry".to_string(),
        ));
    }

    Ok(super::geometry_to_sql_value(geom, 0))
}

/// ST_PolygonFromText(wkt_string) - Create POLYGON from WKT
pub fn st_polygon_from_text(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_PolygonFromText requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let wkt = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => &**s,
        SqlValue::Null => return Ok(SqlValue::Null),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ST_PolygonFromText requires a string argument".to_string(),
            ))
        }
    };

    let geom = parse_wkt(wkt)?;
    if !matches!(geom, Geometry::Polygon { .. }) {
        return Err(ExecutorError::UnsupportedFeature(
            "ST_PolygonFromText requires a POLYGON geometry".to_string(),
        ));
    }

    Ok(super::geometry_to_sql_value(geom, 0))
}

/// ST_GeomFromWKB(wkb_binary) - Create geometry from WKB (Phase 2)
pub fn st_geom_from_wkb(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_GeomFromWKB requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    // Handle NULL
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let wkb_data = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => super::parse_binary_data(s)?,
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ST_GeomFromWKB requires binary data".to_string(),
            ))
        }
    };

    let geom = super::wkb::wkb_to_geometry(&wkb_data)?;

    // Optional SRID parameter
    let srid = if args.len() > 1 {
        match &args[1] {
            SqlValue::Integer(i) => *i as i32,
            SqlValue::Bigint(i) => *i as i32,
            SqlValue::Smallint(i) => *i as i32,
            SqlValue::Null => 0,
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "SRID must be an integer".to_string(),
                ))
            }
        }
    } else {
        0
    };

    Ok(super::geometry_to_sql_value(geom, srid))
}

/// ST_PointFromWKB(wkb_binary) - Create POINT from WKB
pub fn st_point_from_wkb(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_PointFromWKB requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    // Handle NULL
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let wkb_data = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => super::parse_binary_data(s)?,
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ST_PointFromWKB requires binary data".to_string(),
            ))
        }
    };

    let geom = super::wkb::wkb_to_geometry(&wkb_data)?;
    if !matches!(geom, Geometry::Point { .. }) {
        return Err(ExecutorError::UnsupportedFeature(
            "ST_PointFromWKB requires a POINT geometry".to_string(),
        ));
    }

    let srid = if args.len() > 1 {
        match &args[1] {
            SqlValue::Integer(i) => *i as i32,
            SqlValue::Bigint(i) => *i as i32,
            SqlValue::Smallint(i) => *i as i32,
            SqlValue::Null => 0,
            _ => 0,
        }
    } else {
        0
    };

    Ok(super::geometry_to_sql_value(geom, srid))
}

/// ST_LineFromWKB(wkb_binary) - Create LINESTRING from WKB
pub fn st_line_from_wkb(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_LineFromWKB requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    // Handle NULL
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let wkb_data = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => super::parse_binary_data(s)?,
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ST_LineFromWKB requires binary data".to_string(),
            ))
        }
    };

    let geom = super::wkb::wkb_to_geometry(&wkb_data)?;
    if !matches!(geom, Geometry::LineString { .. }) {
        return Err(ExecutorError::UnsupportedFeature(
            "ST_LineFromWKB requires a LINESTRING geometry".to_string(),
        ));
    }

    let srid = if args.len() > 1 {
        match &args[1] {
            SqlValue::Integer(i) => *i as i32,
            SqlValue::Bigint(i) => *i as i32,
            SqlValue::Smallint(i) => *i as i32,
            SqlValue::Null => 0,
            _ => 0,
        }
    } else {
        0
    };

    Ok(super::geometry_to_sql_value(geom, srid))
}

/// ST_PolygonFromWKB(wkb_binary) - Create POLYGON from WKB
pub fn st_polygon_from_wkb(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_PolygonFromWKB requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    // Handle NULL
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let wkb_data = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => super::parse_binary_data(s)?,
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "ST_PolygonFromWKB requires binary data".to_string(),
            ))
        }
    };

    let geom = super::wkb::wkb_to_geometry(&wkb_data)?;
    if !matches!(geom, Geometry::Polygon { .. }) {
        return Err(ExecutorError::UnsupportedFeature(
            "ST_PolygonFromWKB requires a POLYGON geometry".to_string(),
        ));
    }

    let srid = if args.len() > 1 {
        match &args[1] {
            SqlValue::Integer(i) => *i as i32,
            SqlValue::Bigint(i) => *i as i32,
            SqlValue::Smallint(i) => *i as i32,
            SqlValue::Null => 0,
            _ => 0,
        }
    } else {
        0
    };

    Ok(super::geometry_to_sql_value(geom, srid))
}
