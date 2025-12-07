//! Spatial/Geometric Query Functions
//!
//! Implements ST_* operations for working with spatial data types.
//! Supports WKT (Well-Known Text) parsing, WKB (Well-Known Binary) format, SRID tracking, and spatial predicates.
//!
//! Functions are organized as:
//! - Constructor functions: Create geometry from text/binary
//! - Accessor functions: Extract coordinates and metadata
//! - SRID functions: Get/set spatial reference system ID
//! - Predicate functions: Spatial relationship tests
//! - Output functions: Convert to text/binary formats
//! - Measurement functions: Distance and area calculations

pub mod accessors;
pub mod constructors;
pub mod measurements;
pub mod operations;
pub mod predicates;
pub mod srid;
pub(crate) mod wkb;

use crate::errors::ExecutorError;
use hex;
use vibesql_types::SqlValue;

/// A geometry representation with SRID support (Phase 2+)
/// Stores geometries internally for efficient processing
#[derive(Debug, Clone, PartialEq)]
pub struct GeometryWithSRID {
    pub geometry: Geometry,
    pub srid: i32, // 0 = undefined, otherwise EPSG code
}

/// Core geometry enum for Phase 1+
/// Stores geometries as structured data internally
#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    Point { x: f64, y: f64 },
    LineString { points: Vec<(f64, f64)> },
    Polygon { rings: Vec<Vec<(f64, f64)>> },
    MultiPoint { points: Vec<(f64, f64)> },
    MultiLineString { lines: Vec<Vec<(f64, f64)>> },
    MultiPolygon { polygons: Vec<Vec<Vec<(f64, f64)>>> },
    Collection { geometries: Vec<Geometry> },
}

#[allow(dead_code)]
impl Geometry {
    /// Get the type name
    pub fn geometry_type(&self) -> &'static str {
        match self {
            Geometry::Point { .. } => "POINT",
            Geometry::LineString { .. } => "LINESTRING",
            Geometry::Polygon { .. } => "POLYGON",
            Geometry::MultiPoint { .. } => "MULTIPOINT",
            Geometry::MultiLineString { .. } => "MULTILINESTRING",
            Geometry::MultiPolygon { .. } => "MULTIPOLYGON",
            Geometry::Collection { .. } => "GEOMETRYCOLLECTION",
        }
    }

    /// Get the dimension (0 for point, 1 for line, 2 for polygon)
    pub fn dimension(&self) -> i32 {
        match self {
            Geometry::Point { .. } | Geometry::MultiPoint { .. } => 0,
            Geometry::LineString { .. } | Geometry::MultiLineString { .. } => 1,
            Geometry::Polygon { .. } | Geometry::MultiPolygon { .. } => 2,
            Geometry::Collection { geometries } => {
                geometries.iter().map(|g| g.dimension()).max().unwrap_or(-1)
            }
        }
    }

    /// Convert to WKT string
    pub fn to_wkt(&self) -> String {
        match self {
            Geometry::Point { x, y } => {
                format!("POINT({} {})", x, y)
            }
            Geometry::LineString { points } => {
                let coords = points
                    .iter()
                    .map(|(x, y)| format!("{} {}", x, y))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("LINESTRING({})", coords)
            }
            Geometry::Polygon { rings } => {
                let ring_strs = rings
                    .iter()
                    .map(|ring| {
                        let coords = ring
                            .iter()
                            .map(|(x, y)| format!("{} {}", x, y))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({})", coords)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("POLYGON({})", ring_strs)
            }
            Geometry::MultiPoint { points } => {
                let coords = points
                    .iter()
                    .map(|(x, y)| format!("({} {})", x, y))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("MULTIPOINT({})", coords)
            }
            Geometry::MultiLineString { lines } => {
                let line_strs = lines
                    .iter()
                    .map(|line| {
                        let coords = line
                            .iter()
                            .map(|(x, y)| format!("{} {}", x, y))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({})", coords)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("MULTILINESTRING({})", line_strs)
            }
            Geometry::MultiPolygon { polygons } => {
                let poly_strs = polygons
                    .iter()
                    .map(|poly| {
                        let ring_strs = poly
                            .iter()
                            .map(|ring| {
                                let coords = ring
                                    .iter()
                                    .map(|(x, y)| format!("{} {}", x, y))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("({})", coords)
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({})", ring_strs)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("MULTIPOLYGON({})", poly_strs)
            }
            Geometry::Collection { geometries } => {
                let geo_strs = geometries.iter().map(|g| g.to_wkt()).collect::<Vec<_>>().join(", ");
                format!("GEOMETRYCOLLECTION({})", geo_strs)
            }
        }
    }

    /// Convert to EWKT (Extended WKT) format with SRID
    pub fn to_ewkt(&self, srid: i32) -> String {
        if srid > 0 {
            format!("SRID={};{}", srid, self.to_wkt())
        } else {
            self.to_wkt()
        }
    }

    /// Convert to WKB (Well-Known Binary) format
    pub fn to_wkb(&self) -> Vec<u8> {
        wkb::geometry_to_wkb(self)
    }

    /// Convert to WKB with SRID (EWKB format)
    pub fn to_ewkb(&self, srid: i32) -> Vec<u8> {
        wkb::geometry_to_ewkb(self, srid)
    }
}

#[allow(dead_code)]
impl GeometryWithSRID {
    /// Create new geometry with SRID
    pub fn new(geometry: Geometry, srid: i32) -> Self {
        GeometryWithSRID { geometry, srid }
    }

    /// Get the type name
    pub fn geometry_type(&self) -> &'static str {
        self.geometry.geometry_type()
    }

    /// Get the dimension
    pub fn dimension(&self) -> i32 {
        self.geometry.dimension()
    }

    /// Get SRID
    pub fn srid(&self) -> i32 {
        self.srid
    }

    /// Set SRID
    pub fn set_srid(&mut self, srid: i32) {
        self.srid = srid;
    }

    /// Convert to EWKT format
    pub fn to_ewkt(&self) -> String {
        self.geometry.to_ewkt(self.srid)
    }

    /// Convert to WKT format
    pub fn to_wkt(&self) -> String {
        self.geometry.to_wkt()
    }

    /// Convert to WKB format
    pub fn to_wkb(&self) -> Vec<u8> {
        self.geometry.to_wkb()
    }

    /// Convert to EWKB format (with SRID)
    pub fn to_ewkb(&self) -> Vec<u8> {
        wkb::geometry_to_ewkb(&self.geometry, self.srid)
    }
}

/// Store geometry as a serialized string in SqlValue
/// Format: "__GEOMETRY__|SRID|wkt_content" or "__GEOMETRY__|0|wkt_content"
pub const GEOMETRY_TYPE_MARKER: &str = "__GEOMETRY__";
const GEOMETRY_SEPARATOR: &str = "|";

pub fn geometry_to_sql_value(geom: Geometry, srid: i32) -> SqlValue {
    let wkt = geom.to_ewkt(srid);
    SqlValue::Varchar(arcstr::ArcStr::from(format!(
        "{}{}{}{}",
        GEOMETRY_TYPE_MARKER,
        GEOMETRY_SEPARATOR,
        srid,
        GEOMETRY_SEPARATOR.to_string() + &wkt
    )))
}

pub fn sql_value_to_geometry(value: &SqlValue) -> Result<GeometryWithSRID, ExecutorError> {
    match value {
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            if let Some(content) = s.strip_prefix(GEOMETRY_TYPE_MARKER) {
                let parts: Vec<&str> = content.splitn(3, GEOMETRY_SEPARATOR).collect();
                if parts.len() >= 3 {
                    let srid: i32 = parts[0].parse().unwrap_or(0);
                    let wkt = parts[2];
                    let geometry = constructors::parse_wkt(wkt)?;
                    Ok(GeometryWithSRID::new(geometry, srid))
                } else {
                    // Old format without SRID
                    let geometry = constructors::parse_wkt(content)?;
                    Ok(GeometryWithSRID::new(geometry, 0))
                }
            } else if s.starts_with("SRID=") {
                // Parse EWKT format: SRID=4326;POINT(1 2)
                if let Some(semicolon_pos) = s.find(';') {
                    let srid_str = &s[5..semicolon_pos]; // Skip "SRID="
                    let wkt = &s[semicolon_pos + 1..];
                    let srid: i32 = srid_str.parse().unwrap_or(0);
                    let geometry = constructors::parse_wkt(wkt)?;
                    Ok(GeometryWithSRID::new(geometry, srid))
                } else {
                    Err(ExecutorError::UnsupportedFeature("Invalid EWKT format".to_string()))
                }
            } else {
                // Try to parse as WKT directly if not marked
                let geometry = constructors::parse_wkt(s)?;
                Ok(GeometryWithSRID::new(geometry, 0))
            }
        }
        SqlValue::Null => {
            Err(ExecutorError::UnsupportedFeature("Cannot convert NULL to geometry".to_string()))
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Cannot convert {} to geometry",
            value.type_name()
        ))),
    }
}

/// Binary data value for WKB representation
#[allow(dead_code)]
pub fn binary_to_sql_value(data: Vec<u8>) -> SqlValue {
    SqlValue::Varchar(arcstr::ArcStr::from(format!("0x{}", hex::encode(data))))
}

/// Parse binary data from hex string
pub fn parse_binary_data(hex_str: &str) -> Result<Vec<u8>, ExecutorError> {
    let hex_str = if hex_str.starts_with("0x") || hex_str.starts_with("0X") {
        &hex_str[2..]
    } else {
        hex_str
    };

    hex::decode(hex_str)
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid hex data: {}", e)))
}
