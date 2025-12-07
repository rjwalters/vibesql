//! Benchmarks for spatial index performance
//!
//! These benchmarks measure the performance benefits of spatial indexes (R-tree)
//! for spatial queries (ST_Contains, ST_Intersects, ST_Within, ST_DWithin).
//!
//! Compares indexed spatial queries vs full table scans to demonstrate speedup.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;
use vibesql_ast::Statement;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{CreateIndexExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

/// Parse a SELECT statement from SQL
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Create spatial index via SQL (uses real executor path)
fn create_spatial_index(db: &mut Database, index_name: &str, table_name: &str, column_name: &str) {
    let sql = format!("CREATE SPATIAL INDEX {} ON {}({})", index_name, table_name, column_name);
    match Parser::parse_sql(&sql) {
        Ok(Statement::CreateIndex(stmt)) => {
            CreateIndexExecutor::execute(&stmt, db).unwrap();
        }
        _ => panic!("Failed to parse CREATE SPATIAL INDEX"),
    }
}

/// Create table with 10k polygons in 100x100 grid
fn create_test_db_with_10k_polygons() -> Database {
    let mut db = Database::new();

    // Create table (uppercase since SQL parser normalizes identifiers)
    let schema = TableSchema::new(
        "PARCELS".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "BOUNDARY".to_string(),
                data_type: DataType::Varchar { max_length: Some(2000) },
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    // Insert 100x100 grid of 10x10 polygons
    // Grid spans (0,0) to (1000,1000)
    for y in 0..100 {
        for x in 0..100 {
            let x0 = x * 10;
            let y0 = y * 10;
            let x1 = x0 + 10;
            let y1 = y0 + 10;

            let id = y * 100 + x;
            let wkt = format!(
                "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
                x0, y0, x1, y0, x1, y1, x0, y1, x0, y0
            );

            let row = vibesql_storage::Row::new(vec![
                SqlValue::Integer(id as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(wkt)),
            ]);
            db.insert_row("PARCELS", row).unwrap();
        }
    }

    db
}

/// Create table with 100k polygons in 316x316 grid
fn create_test_db_with_100k_polygons() -> Database {
    let mut db = Database::new();

    // Create table
    let schema = TableSchema::new(
        "LARGE_PARCELS".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "BOUNDARY".to_string(),
                data_type: DataType::Varchar { max_length: Some(2000) },
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    // Insert 316x316 grid of 10x10 polygons (99,856 ≈ 100k)
    // Grid spans (0,0) to (3160,3160)
    for y in 0..316 {
        for x in 0..316 {
            let x0 = x * 10;
            let y0 = y * 10;
            let x1 = x0 + 10;
            let y1 = y0 + 10;

            let id = y * 316 + x;
            let wkt = format!(
                "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
                x0, y0, x1, y0, x1, y1, x0, y1, x0, y0
            );

            let row = vibesql_storage::Row::new(vec![
                SqlValue::Integer(id as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(wkt)),
            ]);
            db.insert_row("LARGE_PARCELS", row).unwrap();
        }
    }

    db
}

/// Create table with 10k random points for distance queries
fn create_test_db_with_10k_points() -> Database {
    let mut db = Database::new();

    // Create table
    let schema = TableSchema::new(
        "POINTS_OF_INTEREST".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "LOCATION".to_string(),
                data_type: DataType::Varchar { max_length: Some(500) },
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    // Insert 10k random points scattered across (0,0) to (1000,1000)
    // Use deterministic "random" pattern for reproducibility
    for i in 0..10_000 {
        let x = ((i * 7919) % 1000) as f64; // Prime number for pseudo-random distribution
        let y = ((i * 7907) % 1000) as f64;

        let wkt = format!("POINT({} {})", x, y);

        let row =
            vibesql_storage::Row::new(vec![SqlValue::Integer(i as i64), SqlValue::Varchar(arcstr::ArcStr::from(wkt))]);
        db.insert_row("POINTS_OF_INTEREST", row).unwrap();
    }

    db
}

/// Benchmark: Point-in-polygon queries on 10k polygons
fn bench_point_in_polygon_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_in_polygon_10k");

    // Test point at (505, 505) - should be in polygon at grid position (50, 50)
    let query =
        "SELECT id FROM parcels WHERE ST_Contains(boundary, ST_GeomFromText('POINT(505 505)'))";

    // Benchmark WITHOUT index
    {
        let db = create_test_db_with_10k_polygons();
        group.throughput(Throughput::Elements(1)); // Expect 1 result

        group.bench_function("no_index", |b| {
            b.iter(|| {
                let stmt = parse_select(query);
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    // Benchmark WITH spatial index
    {
        let mut db = create_test_db_with_10k_polygons();
        create_spatial_index(&mut db, "idx_boundary", "parcels", "boundary");
        group.throughput(Throughput::Elements(1));

        group.bench_function("with_index", |b| {
            b.iter(|| {
                let stmt = parse_select(query);
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark: Polygon intersection queries on 10k polygons
fn bench_polygon_intersection_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("polygon_intersection_10k");

    // Query polygon from (500, 500) to (600, 600) - should intersect multiple grid cells
    let query = "SELECT id FROM parcels WHERE ST_Intersects(boundary, \
                 ST_GeomFromText('POLYGON((500 500, 600 500, 600 600, 500 600, 500 500))'))";

    // Benchmark WITHOUT index
    {
        let db = create_test_db_with_10k_polygons();
        group.throughput(Throughput::Elements(100)); // Expect ~100 results (10x10 grid cells)

        group.bench_function("no_index", |b| {
            b.iter(|| {
                let stmt = parse_select(query);
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    // Benchmark WITH spatial index
    {
        let mut db = create_test_db_with_10k_polygons();
        create_spatial_index(&mut db, "idx_boundary", "parcels", "boundary");
        group.throughput(Throughput::Elements(100));

        group.bench_function("with_index", |b| {
            b.iter(|| {
                let stmt = parse_select(query);
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark: Point-in-polygon queries on 100k polygons (large dataset)
fn bench_point_in_polygon_100k(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_in_polygon_100k");

    // Test point at (1580, 1580) - center of grid
    let query = "SELECT id FROM large_parcels WHERE ST_Contains(boundary, ST_GeomFromText('POINT(1580 1580)'))";

    // Benchmark WITHOUT index
    {
        let db = create_test_db_with_100k_polygons();
        group.throughput(Throughput::Elements(1)); // Expect 1 result

        group.bench_function("no_index", |b| {
            b.iter(|| {
                let stmt = parse_select(query);
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    // Benchmark WITH spatial index
    {
        let mut db = create_test_db_with_100k_polygons();
        create_spatial_index(&mut db, "idx_boundary", "large_parcels", "boundary");
        group.throughput(Throughput::Elements(1));

        group.bench_function("with_index", |b| {
            b.iter(|| {
                let stmt = parse_select(query);
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark: Distance queries on 10k points
fn bench_distance_query_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_query_10k");

    // Find points within 50 units of (500, 500)
    let query = "SELECT id FROM points_of_interest WHERE ST_DWithin(location, ST_GeomFromText('POINT(500 500)'), 50)";

    // Benchmark WITHOUT index
    {
        let db = create_test_db_with_10k_points();
        group.throughput(Throughput::Elements(100)); // Expect ~100 results (π * 50^2 / 1000^2 * 10000 ≈ 78)

        group.bench_function("no_index", |b| {
            b.iter(|| {
                let stmt = parse_select(query);
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    // Benchmark WITH spatial index
    {
        let mut db = create_test_db_with_10k_points();
        create_spatial_index(&mut db, "idx_location", "points_of_interest", "location");
        group.throughput(Throughput::Elements(100));

        group.bench_function("with_index", |b| {
            b.iter(|| {
                let stmt = parse_select(query);
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_point_in_polygon_10k,
    bench_polygon_intersection_10k,
    bench_point_in_polygon_100k,
    bench_distance_query_10k,
);
criterion_main!(benches);

#[cfg(test)]
mod correctness_tests {

    /// Verify that spatial index returns same results as full scan for point-in-polygon
    #[test]
    fn test_spatial_index_correctness_point_in_polygon() {
        let mut db = create_test_db_with_10k_polygons();

        // Query WITHOUT index
        let query = "SELECT id FROM parcels WHERE ST_Contains(boundary, ST_GeomFromText('POINT(505 505)')) ORDER BY id";
        let stmt = parse_select(query);
        let executor = SelectExecutor::new(&db);
        let results_no_index = executor.execute(&stmt).unwrap();

        // Create spatial index
        create_spatial_index(&mut db, "idx_boundary", "parcels", "boundary");

        // Query WITH index (optimizer should use it automatically)
        let stmt = parse_select(query);
        let executor = SelectExecutor::new(&db);
        let results_with_index = executor.execute(&stmt).unwrap();

        // Results MUST be identical
        assert_eq!(
            results_no_index, results_with_index,
            "Spatial index returned different results than full scan for point-in-polygon!"
        );

        // Verify we got exactly 1 result (the polygon containing the point)
        assert_eq!(results_with_index.len(), 1, "Expected exactly 1 result");
    }

    /// Verify that spatial index returns same results as full scan for polygon intersection
    #[test]
    fn test_spatial_index_correctness_polygon_intersection() {
        let mut db = create_test_db_with_10k_polygons();

        // Query WITHOUT index
        let query = "SELECT id FROM parcels WHERE ST_Intersects(boundary, \
                     ST_GeomFromText('POLYGON((500 500, 600 500, 600 600, 500 600, 500 500))')) ORDER BY id";
        let stmt = parse_select(query);
        let executor = SelectExecutor::new(&db);
        let results_no_index = executor.execute(&stmt).unwrap();

        // Create spatial index
        create_spatial_index(&mut db, "idx_boundary", "parcels", "boundary");

        // Query WITH index
        let stmt = parse_select(query);
        let executor = SelectExecutor::new(&db);
        let results_with_index = executor.execute(&stmt).unwrap();

        // Results MUST be identical
        assert_eq!(
            results_no_index, results_with_index,
            "Spatial index returned different results than full scan for polygon intersection!"
        );

        // Verify we got expected number of results (10x10 grid = 100 polygons)
        assert!(results_with_index.len() >= 100, "Expected at least 100 results");
    }

    /// Verify that spatial index returns same results as full scan for distance queries
    #[test]
    fn test_spatial_index_correctness_distance() {
        let mut db = create_test_db_with_10k_points();

        // Query WITHOUT index
        let query = "SELECT id FROM points_of_interest WHERE ST_DWithin(location, ST_GeomFromText('POINT(500 500)'), 50) ORDER BY id";
        let stmt = parse_select(query);
        let executor = SelectExecutor::new(&db);
        let results_no_index = executor.execute(&stmt).unwrap();

        // Create spatial index
        create_spatial_index(&mut db, "idx_location", "points_of_interest", "location");

        // Query WITH index
        let stmt = parse_select(query);
        let executor = SelectExecutor::new(&db);
        let results_with_index = executor.execute(&stmt).unwrap();

        // Results MUST be identical
        assert_eq!(
            results_no_index, results_with_index,
            "Spatial index returned different results than full scan for distance query!"
        );

        // Verify we got some results (should be ~78 based on area calculation)
        assert!(results_with_index.len() > 0, "Expected at least some results within distance");
    }
}
