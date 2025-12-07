//! Spatial Index - R-tree implementation for spatial queries
//!
//! This module provides R-tree spatial indexing for efficient spatial queries.
//! Uses the `rstar` crate for R*-tree (enhanced R-tree variant) implementation.

use rstar::{RTree, RTreeObject, AABB};
use vibesql_types::SqlValue;

/// A spatial index entry that can be stored in an R-tree
#[derive(Debug, Clone)]
pub struct SpatialIndexEntry {
    /// Row ID in the table
    pub row_id: usize,
    /// Minimum Bounding Rectangle (envelope) for the geometry
    pub mbr: AABB<[f64; 2]>,
}

impl RTreeObject for SpatialIndexEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.mbr
    }
}

impl PartialEq for SpatialIndexEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row_id == other.row_id
    }
}

/// Spatial index using R-tree for efficient spatial queries
#[derive(Debug, Clone)]
pub struct SpatialIndex {
    /// R-tree data structure
    rtree: RTree<SpatialIndexEntry>,
    /// Column name this index is on
    pub column_name: String,
}

impl SpatialIndex {
    /// Create a new empty spatial index
    pub fn new(column_name: String) -> Self {
        SpatialIndex { rtree: RTree::new(), column_name }
    }

    /// Create a spatial index from a bulk load of entries
    /// This is more efficient than inserting entries one by one
    pub fn bulk_load(column_name: String, entries: Vec<SpatialIndexEntry>) -> Self {
        SpatialIndex { rtree: RTree::bulk_load(entries), column_name }
    }

    /// Insert a geometry into the spatial index
    pub fn insert(&mut self, row_id: usize, mbr: AABB<[f64; 2]>) {
        let entry = SpatialIndexEntry { row_id, mbr };
        self.rtree.insert(entry);
    }

    /// Remove a geometry from the spatial index
    pub fn remove(&mut self, row_id: usize, mbr: &AABB<[f64; 2]>) -> bool {
        let entry = SpatialIndexEntry { row_id, mbr: *mbr };
        self.rtree.remove(&entry).is_some()
    }

    /// Query the spatial index for geometries intersecting the given MBR
    /// Returns a vector of row IDs that potentially match
    pub fn locate_in_envelope(&self, mbr: &AABB<[f64; 2]>) -> Vec<usize> {
        self.rtree.locate_in_envelope(mbr).map(|entry| entry.row_id).collect()
    }

    /// Query the spatial index for geometries containing the given point
    /// This is done by querying for a point MBR (zero-size envelope)
    pub fn locate_at_point(&self, point: &[f64; 2]) -> Vec<usize> {
        let point_mbr = AABB::from_point(*point);
        self.locate_in_envelope(&point_mbr)
    }

    /// Get all row IDs in the index (for testing/debugging)
    pub fn all_row_ids(&self) -> Vec<usize> {
        self.rtree.iter().map(|entry| entry.row_id).collect()
    }

    /// Get the number of entries in the index
    pub fn len(&self) -> usize {
        self.rtree.size()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.rtree.size() == 0
    }

    /// Clear all entries from the index
    pub fn clear(&mut self) {
        self.rtree = RTree::new();
    }
}

/// Extract minimum bounding rectangle from a geometry SqlValue
/// Returns None if the value is not a valid geometry
pub fn extract_mbr_from_sql_value(value: &SqlValue) -> Option<AABB<[f64; 2]>> {
    // Parse the geometry from SqlValue
    // Format: "__GEOMETRY__|SRID|wkt_content"
    const GEOMETRY_TYPE_MARKER: &str = "__GEOMETRY__";

    match value {
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            if !s.starts_with(GEOMETRY_TYPE_MARKER) {
                // Try parsing as WKT directly
                return parse_wkt_mbr(s);
            }

            // Extract WKT from marker format
            let parts: Vec<&str> = s.splitn(3, '|').collect();
            if parts.len() >= 3 {
                let wkt = parts[2];
                parse_wkt_mbr(wkt)
            } else {
                None
            }
        }
        SqlValue::Null => None,
        _ => None,
    }
}

/// Parse WKT and extract minimum bounding rectangle
fn parse_wkt_mbr(wkt: &str) -> Option<AABB<[f64; 2]>> {
    // Strip SRID prefix if present (SRID=4326;POINT(1 2))
    let wkt = if wkt.starts_with("SRID=") {
        wkt.split_once(';').map(|(_, w)| w).unwrap_or(wkt)
    } else {
        wkt
    };

    let wkt = wkt.trim();

    // Extract coordinates based on geometry type
    if wkt.starts_with("POINT") {
        parse_point_mbr(wkt)
    } else if wkt.starts_with("LINESTRING") {
        parse_linestring_mbr(wkt)
    } else if wkt.starts_with("POLYGON") {
        parse_polygon_mbr(wkt)
    } else if wkt.starts_with("MULTIPOINT") {
        parse_multipoint_mbr(wkt)
    } else if wkt.starts_with("MULTILINESTRING") {
        parse_multilinestring_mbr(wkt)
    } else if wkt.starts_with("MULTIPOLYGON") {
        parse_multipolygon_mbr(wkt)
    } else {
        None
    }
}

/// Parse POINT WKT and return MBR
fn parse_point_mbr(wkt: &str) -> Option<AABB<[f64; 2]>> {
    // POINT(x y)
    let coords_str = wkt.strip_prefix("POINT")?.trim();
    let coords_str = coords_str.strip_prefix('(')?.strip_suffix(')')?;
    let parts: Vec<&str> = coords_str.split_whitespace().collect();

    if parts.len() >= 2 {
        let x: f64 = parts[0].parse().ok()?;
        let y: f64 = parts[1].parse().ok()?;
        Some(AABB::from_point([x, y]))
    } else {
        None
    }
}

/// Parse LINESTRING WKT and return MBR
fn parse_linestring_mbr(wkt: &str) -> Option<AABB<[f64; 2]>> {
    // LINESTRING(x1 y1, x2 y2, ...)
    let coords_str = wkt.strip_prefix("LINESTRING")?.trim();
    let coords_str = coords_str.strip_prefix('(')?.strip_suffix(')')?;

    let points = parse_coordinate_list(coords_str)?;
    calculate_mbr_from_points(&points)
}

/// Parse POLYGON WKT and return MBR
fn parse_polygon_mbr(wkt: &str) -> Option<AABB<[f64; 2]>> {
    // POLYGON((x1 y1, x2 y2, ...), (...))
    let coords_str = wkt.strip_prefix("POLYGON")?.trim();
    let coords_str = coords_str.strip_prefix('(')?.strip_suffix(')')?;

    // Extract exterior ring (first ring)
    // For single ring: "(0 0, 10 0, 10 10, 0 10, 0 0)"
    // For multiple rings: "(0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)"
    let exterior = if coords_str.contains("),") {
        // Multiple rings - split and take first
        let rings: Vec<&str> = coords_str.split("),").collect();
        rings[0].trim().strip_prefix('(').unwrap_or(rings[0])
    } else {
        // Single ring
        coords_str.strip_prefix('(')?.strip_suffix(')')?
    };

    let points = parse_coordinate_list(exterior)?;
    calculate_mbr_from_points(&points)
}

/// Parse MULTIPOINT WKT and return MBR
fn parse_multipoint_mbr(wkt: &str) -> Option<AABB<[f64; 2]>> {
    // MULTIPOINT((x1 y1), (x2 y2), ...)
    let coords_str = wkt.strip_prefix("MULTIPOINT")?.trim();
    let coords_str = coords_str.strip_prefix('(')?.strip_suffix(')')?;

    // Remove parentheses around individual points
    let coords_str = coords_str.replace("(", "").replace(")", "");
    let points = parse_coordinate_list(&coords_str)?;
    calculate_mbr_from_points(&points)
}

/// Parse MULTILINESTRING WKT and return MBR
fn parse_multilinestring_mbr(wkt: &str) -> Option<AABB<[f64; 2]>> {
    // MULTILINESTRING((x1 y1, x2 y2), (x3 y3, x4 y4))
    let coords_str = wkt.strip_prefix("MULTILINESTRING")?.trim();
    let coords_str = coords_str.strip_prefix('(')?.strip_suffix(')')?;

    let mut all_points = Vec::new();
    let lines: Vec<&str> = coords_str.split("),").collect();

    for line in lines {
        let line = line.trim().strip_prefix('(').unwrap_or(line);
        let line = line.trim().strip_suffix(')').unwrap_or(line);
        if let Some(mut points) = parse_coordinate_list(line) {
            all_points.append(&mut points);
        }
    }

    calculate_mbr_from_points(&all_points)
}

/// Parse MULTIPOLYGON WKT and return MBR
fn parse_multipolygon_mbr(wkt: &str) -> Option<AABB<[f64; 2]>> {
    // MULTIPOLYGON(((x1 y1, ...)), ((x2 y2, ...)))
    let coords_str = wkt.strip_prefix("MULTIPOLYGON")?.trim();
    let coords_str = coords_str.strip_prefix('(')?.strip_suffix(')')?;

    let mut all_points = Vec::new();

    // Split into polygons
    let polygons: Vec<&str> = coords_str.split(")),").collect();

    for polygon in polygons {
        let polygon = polygon.trim().strip_prefix('(').unwrap_or(polygon);
        let polygon = polygon.trim().strip_suffix(')').unwrap_or(polygon);

        // Extract exterior ring (first ring)
        let rings: Vec<&str> = polygon.split("),").collect();
        if !rings.is_empty() {
            let exterior = rings[0].trim().strip_prefix('(').unwrap_or(rings[0]);
            if let Some(mut points) = parse_coordinate_list(exterior) {
                all_points.append(&mut points);
            }
        }
    }

    calculate_mbr_from_points(&all_points)
}

/// Parse coordinate list "x1 y1, x2 y2, ..."
fn parse_coordinate_list(coords_str: &str) -> Option<Vec<[f64; 2]>> {
    let mut points = Vec::new();

    for coord_pair in coords_str.split(',') {
        let parts: Vec<&str> = coord_pair.split_whitespace().collect();
        if parts.len() >= 2 {
            let x: f64 = parts[0].parse().ok()?;
            let y: f64 = parts[1].parse().ok()?;
            points.push([x, y]);
        }
    }

    if points.is_empty() {
        None
    } else {
        Some(points)
    }
}

/// Calculate MBR from a list of points
fn calculate_mbr_from_points(points: &[[f64; 2]]) -> Option<AABB<[f64; 2]>> {
    if points.is_empty() {
        return None;
    }

    let mut min_x = f64::INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    for &[x, y] in points {
        min_x = min_x.min(x);
        min_y = min_y.min(y);
        max_x = max_x.max(x);
        max_y = max_y.max(y);
    }

    Some(AABB::from_corners([min_x, min_y], [max_x, max_y]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spatial_index_insert_and_query() {
        let mut index = SpatialIndex::new("location".to_string());

        // Insert some points
        index.insert(0, AABB::from_point([0.0, 0.0]));
        index.insert(1, AABB::from_point([10.0, 10.0]));
        index.insert(2, AABB::from_point([20.0, 20.0]));

        assert_eq!(index.len(), 3);

        // Query for points in a region
        let query_mbr = AABB::from_corners([5.0, 5.0], [15.0, 15.0]);
        let results = index.locate_in_envelope(&query_mbr);

        // Should find row 1 (10, 10) which is within the query MBR
        assert!(results.contains(&1));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_spatial_index_remove() {
        let mut index = SpatialIndex::new("location".to_string());

        let mbr = AABB::from_point([10.0, 10.0]);
        index.insert(0, mbr);
        assert_eq!(index.len(), 1);

        let removed = index.remove(0, &mbr);
        assert!(removed);
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_extract_mbr_from_point() {
        let sql_value = SqlValue::Varchar(arcstr::ArcStr::from("__GEOMETRY__|0|POINT(10 20)"));
        let mbr = extract_mbr_from_sql_value(&sql_value);

        assert!(mbr.is_some());
        let mbr = mbr.unwrap();
        assert_eq!(mbr.lower(), [10.0, 20.0]);
        assert_eq!(mbr.upper(), [10.0, 20.0]);
    }

    #[test]
    fn test_extract_mbr_from_polygon() {
        let sql_value = SqlValue::Varchar(arcstr::ArcStr::from(
            "__GEOMETRY__|0|POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))",
        ));
        let mbr = extract_mbr_from_sql_value(&sql_value);

        assert!(mbr.is_some());
        let mbr = mbr.unwrap();
        assert_eq!(mbr.lower(), [0.0, 0.0]);
        assert_eq!(mbr.upper(), [100.0, 100.0]);
    }

    #[test]
    fn test_parse_point_mbr() {
        let mbr = parse_point_mbr("POINT(5 10)");
        assert!(mbr.is_some());
        let mbr = mbr.unwrap();
        assert_eq!(mbr.lower(), [5.0, 10.0]);
        assert_eq!(mbr.upper(), [5.0, 10.0]);
    }

    #[test]
    fn test_parse_linestring_mbr() {
        let mbr = parse_linestring_mbr("LINESTRING(0 0, 10 10, 20 5)");
        assert!(mbr.is_some());
        let mbr = mbr.unwrap();
        assert_eq!(mbr.lower(), [0.0, 0.0]);
        assert_eq!(mbr.upper(), [20.0, 10.0]);
    }

    #[test]
    fn test_spatial_index_bulk_load() {
        let entries = vec![
            SpatialIndexEntry { row_id: 0, mbr: AABB::from_point([0.0, 0.0]) },
            SpatialIndexEntry { row_id: 1, mbr: AABB::from_point([10.0, 10.0]) },
            SpatialIndexEntry { row_id: 2, mbr: AABB::from_point([20.0, 20.0]) },
        ];

        let index = SpatialIndex::bulk_load("location".to_string(), entries);
        assert_eq!(index.len(), 3);

        // Verify query works
        let query_mbr = AABB::from_corners([5.0, 5.0], [15.0, 15.0]);
        let results = index.locate_in_envelope(&query_mbr);
        assert!(results.contains(&1));
    }

    #[test]
    fn test_extract_mbr_null_value() {
        let sql_value = SqlValue::Null;
        let mbr = extract_mbr_from_sql_value(&sql_value);
        assert!(mbr.is_none());
    }

    #[test]
    fn test_extract_mbr_with_srid() {
        let sql_value = SqlValue::Varchar(arcstr::ArcStr::from("__GEOMETRY__|4326|SRID=4326;POINT(10 20)"));
        let mbr = extract_mbr_from_sql_value(&sql_value);

        assert!(mbr.is_some());
        let mbr = mbr.unwrap();
        assert_eq!(mbr.lower(), [10.0, 20.0]);
    }
}
