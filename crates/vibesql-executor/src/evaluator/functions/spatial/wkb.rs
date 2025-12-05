//! WKB (Well-Known Binary) format parser and serializer
//!
//! Implements parsing and serialization of geometries in WKB format.
//! References:
//! - ISO/IEC 13249-3:2016 (SQL/MM Part 3)
//! - OGC Simple Features for SQL v1.1.1

use super::Geometry;
use crate::errors::ExecutorError;
use std::io::{Cursor, Read};

// WKB Geometry Type Codes
const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_MULTIPOINT: u32 = 4;
const WKB_MULTILINESTRING: u32 = 5;
const WKB_MULTIPOLYGON: u32 = 6;
const WKB_GEOMETRYCOLLECTION: u32 = 7;

// EWKB SRID flag (PostGIS extension)
const EWKB_SRID_FLAG: u32 = 0x20000000;

/// Byte order marker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ByteOrder {
    LittleEndian = 1,
    BigEndian = 0,
}

impl ByteOrder {
    fn from_u8(val: u8) -> Result<Self, ExecutorError> {
        match val {
            0 => Ok(ByteOrder::BigEndian),
            1 => Ok(ByteOrder::LittleEndian),
            _ => Err(ExecutorError::UnsupportedFeature(format!("Invalid WKB byte order: {}", val))),
        }
    }
}

/// Parse WKB (Well-Known Binary) data
pub fn wkb_to_geometry(data: &[u8]) -> Result<Geometry, ExecutorError> {
    let mut cursor = Cursor::new(data);
    parse_geometry(&mut cursor)
}

fn parse_geometry(cursor: &mut Cursor<&[u8]>) -> Result<Geometry, ExecutorError> {
    let byte_order = read_byte_order(cursor)?;
    let geom_type = read_u32(cursor, byte_order)?;

    // Remove SRID flag if present (EWKB extension)
    let has_srid = (geom_type & EWKB_SRID_FLAG) != 0;
    let pure_type = geom_type & !EWKB_SRID_FLAG;

    // Skip SRID if present
    if has_srid {
        let _ = read_i32(cursor, byte_order)?;
    }

    match pure_type {
        WKB_POINT => parse_point(cursor, byte_order),
        WKB_LINESTRING => parse_linestring(cursor, byte_order),
        WKB_POLYGON => parse_polygon(cursor, byte_order),
        WKB_MULTIPOINT => parse_multipoint(cursor, byte_order),
        WKB_MULTILINESTRING => parse_multilinestring(cursor, byte_order),
        WKB_MULTIPOLYGON => parse_multipolygon(cursor, byte_order),
        WKB_GEOMETRYCOLLECTION => parse_geometrycollection(cursor, byte_order),
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Unknown WKB geometry type: {}",
            pure_type
        ))),
    }
}

fn parse_point(
    cursor: &mut Cursor<&[u8]>,
    byte_order: ByteOrder,
) -> Result<Geometry, ExecutorError> {
    let x = read_f64(cursor, byte_order)?;
    let y = read_f64(cursor, byte_order)?;
    Ok(Geometry::Point { x, y })
}

fn parse_linestring(
    cursor: &mut Cursor<&[u8]>,
    byte_order: ByteOrder,
) -> Result<Geometry, ExecutorError> {
    let num_points = read_u32(cursor, byte_order)? as usize;
    let mut points = Vec::with_capacity(num_points);

    for _ in 0..num_points {
        let x = read_f64(cursor, byte_order)?;
        let y = read_f64(cursor, byte_order)?;
        points.push((x, y));
    }

    if points.len() < 2 {
        return Err(ExecutorError::UnsupportedFeature(
            "LineString must have at least 2 points".to_string(),
        ));
    }

    Ok(Geometry::LineString { points })
}

fn parse_polygon(
    cursor: &mut Cursor<&[u8]>,
    byte_order: ByteOrder,
) -> Result<Geometry, ExecutorError> {
    let num_rings = read_u32(cursor, byte_order)? as usize;
    let mut rings = Vec::with_capacity(num_rings);

    for _ in 0..num_rings {
        let num_points = read_u32(cursor, byte_order)? as usize;
        let mut ring = Vec::with_capacity(num_points);

        for _ in 0..num_points {
            let x = read_f64(cursor, byte_order)?;
            let y = read_f64(cursor, byte_order)?;
            ring.push((x, y));
        }

        rings.push(ring);
    }

    if rings.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "Polygon must have at least one ring".to_string(),
        ));
    }

    Ok(Geometry::Polygon { rings })
}

fn parse_multipoint(
    cursor: &mut Cursor<&[u8]>,
    byte_order: ByteOrder,
) -> Result<Geometry, ExecutorError> {
    let num_points = read_u32(cursor, byte_order)? as usize;
    let mut points = Vec::with_capacity(num_points);

    for _ in 0..num_points {
        // Each point in multipoint has its own header
        let _ = read_byte_order(cursor)?;
        let geom_type = read_u32(cursor, byte_order)?;

        if (geom_type & !EWKB_SRID_FLAG) != WKB_POINT {
            return Err(ExecutorError::UnsupportedFeature(
                "Invalid MULTIPOINT geometry".to_string(),
            ));
        }

        let x = read_f64(cursor, byte_order)?;
        let y = read_f64(cursor, byte_order)?;
        points.push((x, y));
    }

    if points.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "MultiPoint must have at least one point".to_string(),
        ));
    }

    Ok(Geometry::MultiPoint { points })
}

fn parse_multilinestring(
    cursor: &mut Cursor<&[u8]>,
    byte_order: ByteOrder,
) -> Result<Geometry, ExecutorError> {
    let num_lines = read_u32(cursor, byte_order)? as usize;
    let mut lines = Vec::with_capacity(num_lines);

    for _ in 0..num_lines {
        // Each linestring in multilinestring has its own header
        let _ = read_byte_order(cursor)?;
        let geom_type = read_u32(cursor, byte_order)?;

        if (geom_type & !EWKB_SRID_FLAG) != WKB_LINESTRING {
            return Err(ExecutorError::UnsupportedFeature(
                "Invalid MULTILINESTRING geometry".to_string(),
            ));
        }

        let num_points = read_u32(cursor, byte_order)? as usize;
        let mut line = Vec::with_capacity(num_points);

        for _ in 0..num_points {
            let x = read_f64(cursor, byte_order)?;
            let y = read_f64(cursor, byte_order)?;
            line.push((x, y));
        }

        lines.push(line);
    }

    if lines.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "MultiLineString must have at least one linestring".to_string(),
        ));
    }

    Ok(Geometry::MultiLineString { lines })
}

fn parse_multipolygon(
    cursor: &mut Cursor<&[u8]>,
    byte_order: ByteOrder,
) -> Result<Geometry, ExecutorError> {
    let num_polygons = read_u32(cursor, byte_order)? as usize;
    let mut polygons = Vec::with_capacity(num_polygons);

    for _ in 0..num_polygons {
        // Each polygon in multipolygon has its own header
        let _ = read_byte_order(cursor)?;
        let geom_type = read_u32(cursor, byte_order)?;

        if (geom_type & !EWKB_SRID_FLAG) != WKB_POLYGON {
            return Err(ExecutorError::UnsupportedFeature(
                "Invalid MULTIPOLYGON geometry".to_string(),
            ));
        }

        let num_rings = read_u32(cursor, byte_order)? as usize;
        let mut rings = Vec::with_capacity(num_rings);

        for _ in 0..num_rings {
            let num_points = read_u32(cursor, byte_order)? as usize;
            let mut ring = Vec::with_capacity(num_points);

            for _ in 0..num_points {
                let x = read_f64(cursor, byte_order)?;
                let y = read_f64(cursor, byte_order)?;
                ring.push((x, y));
            }

            rings.push(ring);
        }

        polygons.push(rings);
    }

    if polygons.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "MultiPolygon must have at least one polygon".to_string(),
        ));
    }

    Ok(Geometry::MultiPolygon { polygons })
}

fn parse_geometrycollection(
    cursor: &mut Cursor<&[u8]>,
    byte_order: ByteOrder,
) -> Result<Geometry, ExecutorError> {
    let num_geoms = read_u32(cursor, byte_order)? as usize;
    let mut geometries = Vec::with_capacity(num_geoms);

    for _ in 0..num_geoms {
        // Each geometry in the collection has its own full WKB
        geometries.push(parse_geometry(cursor)?);
    }

    if geometries.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "GeometryCollection must have at least one geometry".to_string(),
        ));
    }

    Ok(Geometry::Collection { geometries })
}

// Helper functions for reading WKB data

fn read_byte_order(cursor: &mut Cursor<&[u8]>) -> Result<ByteOrder, ExecutorError> {
    let mut buf = [0u8; 1];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| ExecutorError::UnsupportedFeature("Unexpected end of WKB data".to_string()))?;
    ByteOrder::from_u8(buf[0])
}

fn read_u32(cursor: &mut Cursor<&[u8]>, byte_order: ByteOrder) -> Result<u32, ExecutorError> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| ExecutorError::UnsupportedFeature("Unexpected end of WKB data".to_string()))?;

    Ok(match byte_order {
        ByteOrder::LittleEndian => u32::from_le_bytes(buf),
        ByteOrder::BigEndian => u32::from_be_bytes(buf),
    })
}

fn read_i32(cursor: &mut Cursor<&[u8]>, byte_order: ByteOrder) -> Result<i32, ExecutorError> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| ExecutorError::UnsupportedFeature("Unexpected end of WKB data".to_string()))?;

    Ok(match byte_order {
        ByteOrder::LittleEndian => i32::from_le_bytes(buf),
        ByteOrder::BigEndian => i32::from_be_bytes(buf),
    })
}

fn read_f64(cursor: &mut Cursor<&[u8]>, byte_order: ByteOrder) -> Result<f64, ExecutorError> {
    let mut buf = [0u8; 8];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| ExecutorError::UnsupportedFeature("Unexpected end of WKB data".to_string()))?;

    Ok(match byte_order {
        ByteOrder::LittleEndian => f64::from_le_bytes(buf),
        ByteOrder::BigEndian => f64::from_be_bytes(buf),
    })
}

// WKB Serialization

/// Convert a geometry to WKB (Well-Known Binary) format
#[allow(dead_code)]
pub fn geometry_to_wkb(geom: &Geometry) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(ByteOrder::LittleEndian as u8); // Use little-endian
    serialize_geometry(geom, &mut buf, None);
    buf
}

/// Convert a geometry to EWKB (Extended WKB) format with SRID
pub fn geometry_to_ewkb(geom: &Geometry, srid: i32) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(ByteOrder::LittleEndian as u8); // Use little-endian

    // If SRID is specified, include it
    if srid > 0 {
        serialize_geometry(geom, &mut buf, Some(srid));
    } else {
        serialize_geometry(geom, &mut buf, None);
    }

    buf
}

fn serialize_geometry(geom: &Geometry, buf: &mut Vec<u8>, srid: Option<i32>) {
    match geom {
        Geometry::Point { x, y } => {
            if let Some(s) = srid {
                write_u32(buf, WKB_POINT | EWKB_SRID_FLAG);
                write_i32(buf, s);
            } else {
                write_u32(buf, WKB_POINT);
            }
            write_f64(buf, *x);
            write_f64(buf, *y);
        }
        Geometry::LineString { points } => {
            if let Some(s) = srid {
                write_u32(buf, WKB_LINESTRING | EWKB_SRID_FLAG);
                write_i32(buf, s);
            } else {
                write_u32(buf, WKB_LINESTRING);
            }
            write_u32(buf, points.len() as u32);
            for (x, y) in points {
                write_f64(buf, *x);
                write_f64(buf, *y);
            }
        }
        Geometry::Polygon { rings } => {
            if let Some(s) = srid {
                write_u32(buf, WKB_POLYGON | EWKB_SRID_FLAG);
                write_i32(buf, s);
            } else {
                write_u32(buf, WKB_POLYGON);
            }
            write_u32(buf, rings.len() as u32);
            for ring in rings {
                write_u32(buf, ring.len() as u32);
                for (x, y) in ring {
                    write_f64(buf, *x);
                    write_f64(buf, *y);
                }
            }
        }
        Geometry::MultiPoint { points } => {
            if let Some(s) = srid {
                write_u32(buf, WKB_MULTIPOINT | EWKB_SRID_FLAG);
                write_i32(buf, s);
            } else {
                write_u32(buf, WKB_MULTIPOINT);
            }
            write_u32(buf, points.len() as u32);
            for (x, y) in points {
                buf.push(ByteOrder::LittleEndian as u8);
                write_u32(buf, WKB_POINT);
                write_f64(buf, *x);
                write_f64(buf, *y);
            }
        }
        Geometry::MultiLineString { lines } => {
            if let Some(s) = srid {
                write_u32(buf, WKB_MULTILINESTRING | EWKB_SRID_FLAG);
                write_i32(buf, s);
            } else {
                write_u32(buf, WKB_MULTILINESTRING);
            }
            write_u32(buf, lines.len() as u32);
            for line in lines {
                buf.push(ByteOrder::LittleEndian as u8);
                write_u32(buf, WKB_LINESTRING);
                write_u32(buf, line.len() as u32);
                for (x, y) in line {
                    write_f64(buf, *x);
                    write_f64(buf, *y);
                }
            }
        }
        Geometry::MultiPolygon { polygons } => {
            if let Some(s) = srid {
                write_u32(buf, WKB_MULTIPOLYGON | EWKB_SRID_FLAG);
                write_i32(buf, s);
            } else {
                write_u32(buf, WKB_MULTIPOLYGON);
            }
            write_u32(buf, polygons.len() as u32);
            for rings_list in polygons {
                buf.push(ByteOrder::LittleEndian as u8);
                write_u32(buf, WKB_POLYGON);
                write_u32(buf, rings_list.len() as u32);
                for ring in rings_list {
                    write_u32(buf, ring.len() as u32);
                    for (x, y) in ring {
                        write_f64(buf, *x);
                        write_f64(buf, *y);
                    }
                }
            }
        }
        Geometry::Collection { geometries } => {
            if let Some(s) = srid {
                write_u32(buf, WKB_GEOMETRYCOLLECTION | EWKB_SRID_FLAG);
                write_i32(buf, s);
            } else {
                write_u32(buf, WKB_GEOMETRYCOLLECTION);
            }
            write_u32(buf, geometries.len() as u32);
            for geom in geometries {
                buf.push(ByteOrder::LittleEndian as u8);
                serialize_geometry(geom, buf, None);
            }
        }
    }
}

fn write_u32(buf: &mut Vec<u8>, val: u32) {
    buf.extend_from_slice(&val.to_le_bytes());
}

fn write_i32(buf: &mut Vec<u8>, val: i32) {
    buf.extend_from_slice(&val.to_le_bytes());
}

fn write_f64(buf: &mut Vec<u8>, val: f64) {
    buf.extend_from_slice(&val.to_le_bytes());
}
