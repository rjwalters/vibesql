//! Tests for NATURAL JOIN functionality

use super::super::*;

#[test]
fn test_natural_join_single_common_column() {
    let mut db = vibesql_storage::Database::new();

    // Create t1 table
    let t1_schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(20) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "x".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(t1_schema).unwrap();

    // Create t2 table
    let t2_schema = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "value".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "y".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(t2_schema).unwrap();

    // Insert test data into t1
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("a")),
            vibesql_types::SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("b")),
            vibesql_types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("c")),
            vibesql_types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();

    // Insert test data into t2
    db.insert_row(
        "t2",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Integer(111),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t2",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(200),
            vibesql_types::SqlValue::Integer(222),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t2",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Integer(400),
            vibesql_types::SqlValue::Integer(444),
        ]),
    )
    .unwrap();

    // Execute NATURAL JOIN query
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                name: "t1".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None,
            natural: true, // NATURAL JOIN
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    let result = executor.execute(&stmt).unwrap();

    // Should return 2 rows (only id=1 and id=2 match)
    // Common column 'id' should appear only once
    assert_eq!(result.len(), 2);

    // First row: id=1
    assert_eq!(result[0].values.len(), 5); // id, name, x, value, y
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1)); // id
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("a"))); // name
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(10)); // x
    assert_eq!(result[0].values[3], vibesql_types::SqlValue::Integer(100)); // value
    assert_eq!(result[0].values[4], vibesql_types::SqlValue::Integer(111)); // y

    // Second row: id=2
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2)); // id
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("b")));
    // name
}

// Error case tests

#[test]
fn test_natural_cross_join_error() {
    // NATURAL CROSS JOIN is not valid SQL
    let result = vibesql_parser::Parser::parse_sql("SELECT * FROM t1 NATURAL CROSS JOIN t2");

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("NATURAL CROSS JOIN"));
}

#[test]
fn test_natural_join_with_on_clause_error() {
    // NATURAL JOIN...ON is not valid SQL (NATURAL implies the join condition)
    let result =
        vibesql_parser::Parser::parse_sql("SELECT * FROM t1 NATURAL JOIN t2 ON t1.id = t2.id");

    assert!(result.is_err());
    let err = result.unwrap_err();
    // Error should mention that ON clause is not allowed with NATURAL JOIN
    assert!(err.to_string().contains("NATURAL") || err.to_string().contains("ON"));
}

// TODO: Add more comprehensive tests for:
// - NATURAL JOIN with no common columns (should behave like CROSS JOIN)
// - NATURAL LEFT JOIN
// - NATURAL RIGHT JOIN
// - NATURAL JOIN with multiple common columns
// - NATURAL JOIN with case-insensitive column matching
