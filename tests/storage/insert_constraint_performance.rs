//! Test INSERT performance with merged constraint validation

use vibesql_ast::{Expression, InsertSource, InsertStmt};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::InsertExecutor;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_insert_with_merged_constraint_validation() {
    let mut db = Database::new();

    // Create test table with PRIMARY KEY, NOT NULL, and UNIQUE constraints
    let schema = TableSchema::with_all_constraints(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false, // NOT NULL
            ),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(255) },
                false, // NOT NULL
            ),
        ],
        Some(vec!["id".to_string()]),    // PRIMARY KEY
        vec![vec!["email".to_string()]], // UNIQUE constraints
    );

    db.create_table(schema).unwrap();

    eprintln!("Testing INSERT performance with merged constraint validation...");
    eprintln!("Each INSERT validates: NOT NULL, PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY");

    let num_rows = 1000;
    let start = std::time::Instant::now();

    // Batch insert multiple rows
    for i in 0..num_rows {
        let stmt = InsertStmt {
            with_clause: None,
            schema_name: None,
            schema_quoted: false,
            table_name: "test_table".to_string(),
            table_quoted: false,
            columns: vec![],
            source: InsertSource::Values(vec![vec![
                Expression::Literal(SqlValue::Integer(i)),
                Expression::Literal(SqlValue::Varchar(format!("User{}", i).into())),
                Expression::Literal(SqlValue::Varchar(format!("user{}@example.com", i).into())),
            ]]),
            conflict_clause: None,
            on_conflict: None,
            on_duplicate_key_update: None,
        };

        InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis();

    eprintln!("Inserted {} rows in {} ms", num_rows, elapsed_ms);
    eprintln!("Average per INSERT: {} µs", elapsed.as_micros() / (num_rows as u128));
    eprintln!("Throughput: {:.2} rows/sec", num_rows as f64 / elapsed.as_secs_f64());

    // Verify all rows inserted
    let table = db.get_table("test_table").unwrap();
    assert_eq!(table.row_count(), num_rows as usize);

    // Performance expectation: With merged validation, should be reasonably fast
    // This is a smoke test - actual performance will vary by system
    // We're just ensuring it completes without issues
}

#[test]
fn test_insert_multi_row_with_constraints() {
    let mut db = Database::new();

    // Create table with PK and UNIQUE constraints
    let schema = TableSchema::with_primary_key(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(255) },
                false,
            ),
        ],
        vec!["id".to_string()],
    );
    let mut schema = schema;
    schema.unique_constraints = vec![vec!["email".to_string()]];
    db.create_table(schema).unwrap();

    eprintln!("Testing multi-row INSERT with constraint validation...");

    // Multi-row insert - all 100 rows validated in one transaction
    let num_rows = 100;
    let mut rows = Vec::new();
    for i in 0..num_rows {
        rows.push(vec![
            Expression::Literal(SqlValue::Integer(i)),
            Expression::Literal(SqlValue::Varchar(format!("user{}@example.com", i).into())),
        ]);
    }

    let start = std::time::Instant::now();

    let stmt = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_name: "test_table".to_string(),
        table_quoted: false,
        columns: vec![],
        source: InsertSource::Values(rows),
        conflict_clause: None,
        on_conflict: None,
        on_duplicate_key_update: None,
    };

    InsertExecutor::execute(&mut db, &stmt).unwrap();

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis();

    eprintln!("Multi-row INSERT of {} rows: {} ms", num_rows, elapsed_ms);
    eprintln!("Average per row: {} µs", elapsed.as_micros() / (num_rows as u128));

    // Verify all rows inserted
    let table = db.get_table("test_table").unwrap();
    assert_eq!(table.row_count(), num_rows as usize);
}
