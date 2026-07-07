//! Integration tests for spatial indexes

use vibesql_ast::{ColumnDef, CreateIndexStmt, CreateTableStmt, IndexColumn, OrderDirection};
use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DropIndexExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_create_spatial_index_basic() {
    let mut db = Database::new();

    // Create table with geometry column
    let create_table_stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "places".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
            ColumnDef {
                name: "location".to_string(),
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Create spatial index
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: false,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![IndexColumn::Column {
            column_name: "location".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
            collation: None,
        }],
        where_clause: None,
    };

    let result = CreateIndexExecutor::execute(&create_index_stmt, &mut db);
    assert!(result.is_ok(), "Failed to create spatial index: {:?}", result.err());
    assert!(db.spatial_index_exists("idx_location"));
}

#[test]
fn test_spatial_index_multiple_columns_error() {
    let mut db = Database::new();

    // Create table
    let create_table_stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "places".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
            ColumnDef {
                name: "location1".to_string(),
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
            ColumnDef {
                name: "location2".to_string(),
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Try to create spatial index on multiple columns (should fail)
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: false,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![
            IndexColumn::Column {
                column_name: "location1".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
            },
            IndexColumn::Column {
                column_name: "location2".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
            },
        ],
        where_clause: None,
    };

    let result = CreateIndexExecutor::execute(&create_index_stmt, &mut db);
    assert!(result.is_err(), "Expected error for multiple columns");
}

#[test]
fn test_drop_spatial_index() {
    let mut db = Database::new();

    // Create table with geometry column
    let create_table_stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "places".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
            ColumnDef {
                name: "location".to_string(),
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Create spatial index
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: false,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![IndexColumn::Column {
            column_name: "location".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
            collation: None,
        }],
        where_clause: None,
    };

    CreateIndexExecutor::execute(&create_index_stmt, &mut db).unwrap();
    assert!(db.spatial_index_exists("idx_location"));

    // Drop spatial index
    let drop_stmt =
        vibesql_ast::DropIndexStmt { index_name: "idx_location".to_string(), if_exists: false };

    let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
    assert!(result.is_ok(), "Failed to drop spatial index: {:?}", result.err());
    assert!(!db.spatial_index_exists("idx_location"));
}

#[test]
fn test_spatial_index_if_not_exists() {
    let mut db = Database::new();

    // Create table
    let create_table_stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "places".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
            ColumnDef {
                name: "location".to_string(),
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Create spatial index
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: false,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![IndexColumn::Column {
            column_name: "location".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
            collation: None,
        }],
        where_clause: None,
    };

    CreateIndexExecutor::execute(&create_index_stmt, &mut db).unwrap();

    // Try to create again with IF NOT EXISTS (should succeed)
    let create_index_stmt2 = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: true,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![IndexColumn::Column {
            column_name: "location".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
            collation: None,
        }],
        where_clause: None,
    };

    let result = CreateIndexExecutor::execute(&create_index_stmt2, &mut db);
    assert!(result.is_ok(), "IF NOT EXISTS should succeed");
}

// ============================================================================
// #5558: spatial indexes are schema-aware (same-name cross-schema coexistence)
// ============================================================================

fn run(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse `{sql}`: {e}"));
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).unwrap_or_else(|e| panic!("`{sql}`: {e}"));
        }
        vibesql_ast::Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db).unwrap_or_else(|e| panic!("`{sql}`: {e}"));
        }
        vibesql_ast::Statement::DropIndex(s) => {
            DropIndexExecutor::execute(&s, db).unwrap_or_else(|e| panic!("`{sql}`: {e}"));
        }
        vibesql_ast::Statement::Insert(s) => {
            vibesql_executor::InsertExecutor::execute(db, &s)
                .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
        }
        other => panic!("unexpected statement for `{sql}`: {other:?}"),
    }
}

/// Pull `name` (column 0) strings out of a SELECT result.
fn index_names(db: &Database, sql: &str) -> Vec<String> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let vibesql_ast::Statement::Select(s) = stmt else { panic!("expected SELECT") };
    let r = SelectExecutor::new(db).execute_with_columns(&s).expect("SELECT");
    r.rows
        .iter()
        .map(|row| match &row.values[0] {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
            other => panic!("expected text name, got {other:?}"),
        })
        .collect()
}

/// #5558 (mirrors #5540's `..._coexist` for B-tree): a SPATIAL index named `ix`
/// in `main` and a SPATIAL index named `ix` in a temp schema must coexist as
/// distinct storage entries, both usable, with unqualified `DROP INDEX`
/// resolving temp-shadows-main — matching the schema-aware B-tree behavior.
#[test]
fn same_named_spatial_index_across_schemas_coexist() {
    let mut db = Database::new();

    // Same-named table in main and temp, each with a geometry column.
    run(&mut db, "CREATE TABLE main_t (id INTEGER, g VARCHAR(200))");
    run(&mut db, "CREATE TEMP TABLE temp_t (id INTEGER, g VARCHAR(200))");

    // Distinct geometry data so each index is observably populated from its
    // own table. extract_mbr_from_sql_value accepts raw WKT.
    run(&mut db, "INSERT INTO main_t VALUES (1, 'POINT(1 1)')");
    run(&mut db, "INSERT INTO main_t VALUES (2, 'POINT(2 2)')");
    run(&mut db, "INSERT INTO temp.temp_t VALUES (10, 'POINT(50 50)')");

    // Both CREATE SPATIAL INDEX with the bare name `ix` must succeed: main.ix
    // and temp.ix are distinct objects (previously the second collided).
    run(&mut db, "CREATE SPATIAL INDEX ix ON main_t(g)");
    run(&mut db, "CREATE SPATIAL INDEX ix ON temp_t(g)");

    // Both indexes are registered in their respective schemas' master tables.
    assert!(
        index_names(&db, "SELECT name FROM sqlite_master WHERE type='index'")
            .contains(&"ix".to_string()),
        "main.ix must be in sqlite_master"
    );
    assert!(
        index_names(&db, "SELECT name FROM sqlite_temp_master WHERE type='index'")
            .contains(&"ix".to_string()),
        "temp.ix must be in sqlite_temp_master"
    );

    // Both spatial indexes coexist in storage and are reachable via their
    // schema-qualified names with their own contents (no cross-schema leakage).
    let main_ix = db.get_spatial_index("main.ix").expect("main.ix in storage");
    assert_eq!(main_ix.len(), 2, "main.ix indexes both main_t rows");
    // main.ix contains the main_t points, not temp_t's far-away point.
    assert_eq!(main_ix.locate_at_point(&[1.0, 1.0]).len(), 1, "main.ix has POINT(1 1)");
    assert!(main_ix.locate_at_point(&[50.0, 50.0]).is_empty(), "main.ix has no temp point");

    // Temp schema name is session-specific; resolve via the catalog.
    let temp_qualified = format!("{}.ix", db.catalog.temp_schema_name());
    let temp_ix = db.get_spatial_index(&temp_qualified).expect("temp.ix in storage");
    assert_eq!(temp_ix.len(), 1, "temp.ix indexes the single temp_t row");
    assert_eq!(temp_ix.locate_at_point(&[50.0, 50.0]).len(), 1, "temp.ix has POINT(50 50)");
    assert!(temp_ix.locate_at_point(&[1.0, 1.0]).is_empty(), "temp.ix has no main point");

    // Unqualified DROP INDEX resolves temp-shadows-main: drops temp.ix, leaves
    // main.ix intact.
    run(&mut db, "DROP INDEX ix");
    assert!(
        !index_names(&db, "SELECT name FROM sqlite_temp_master WHERE type='index'")
            .contains(&"ix".to_string()),
        "unqualified DROP INDEX must remove the temp spatial index first"
    );
    assert!(
        index_names(&db, "SELECT name FROM sqlite_master WHERE type='index'")
            .contains(&"ix".to_string()),
        "main.ix must survive an unqualified DROP INDEX that resolved to temp.ix"
    );
    assert!(db.get_spatial_index("main.ix").is_some(), "main.ix storage entry survives");

    // A second unqualified DROP INDEX now removes main.ix.
    run(&mut db, "DROP INDEX ix");
    assert!(
        !index_names(&db, "SELECT name FROM sqlite_master WHERE type='index'")
            .contains(&"ix".to_string()),
        "the second DROP INDEX removes the remaining main spatial index"
    );
    assert!(!db.spatial_index_exists("ix"), "no spatial index named ix remains");
}
