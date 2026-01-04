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
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "t1".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None,
            using_columns: None,
            natural: true, // NATURAL JOIN
            alias: None,
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

// SQLite compatibility tests

#[test]
fn test_natural_cross_join_accepted() {
    // NATURAL CROSS JOIN is valid SQL - SQLite applies the natural join condition
    // (matching on common column names), not a cartesian product
    let result = vibesql_parser::Parser::parse_sql("SELECT * FROM t1 NATURAL CROSS JOIN t2");

    assert!(result.is_ok(), "NATURAL CROSS JOIN should parse successfully");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, natural, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::Cross);
                // NATURAL flag should be preserved
                assert!(*natural);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

// Error case tests

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

// Issue #4708: NATURAL JOIN should respect column COLLATE declarations
#[test]
fn test_natural_join_respects_collation() {
    let mut db = vibesql_storage::Database::new();

    // Create people table with case-insensitive NAME column
    let mut name_col = vibesql_catalog::ColumnSchema::new(
        "NAME".to_string(),
        vibesql_types::DataType::Varchar { max_length: Some(50) },
        false,
    );
    name_col.collation = Some("NOCASE".to_string());
    let people_schema = vibesql_catalog::TableSchema::new("PEOPLE".to_string(), vec![name_col]);
    db.create_table(people_schema).unwrap();

    // Create greetings table with case-insensitive NAME column
    let mut greeting_name_col = vibesql_catalog::ColumnSchema::new(
        "NAME".to_string(),
        vibesql_types::DataType::Varchar { max_length: Some(50) },
        false,
    );
    greeting_name_col.collation = Some("NOCASE".to_string());
    let greeting_col = vibesql_catalog::ColumnSchema::new(
        "GREETING".to_string(),
        vibesql_types::DataType::Varchar { max_length: Some(100) },
        false,
    );
    let greetings_schema = vibesql_catalog::TableSchema::new(
        "GREETINGS".to_string(),
        vec![greeting_name_col, greeting_col],
    );
    db.create_table(greetings_schema).unwrap();

    // Insert data - 'Alice' in people, 'ALICE' in greetings
    db.insert_row(
        "people",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
            "Alice",
        ))]),
    )
    .unwrap();

    db.insert_row(
        "greetings",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("ALICE")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello, ALICE!")),
        ]),
    )
    .unwrap();

    // Execute NATURAL JOIN query - should match 'Alice' with 'ALICE' due to NOCASE collation
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "people".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "greetings".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None,
            using_columns: None,
            natural: true, // NATURAL JOIN
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    let result = executor.execute(&stmt).unwrap();

    // Should return 1 row - 'Alice' matches 'ALICE' with NOCASE collation
    assert_eq!(
        result.len(),
        1,
        "NATURAL JOIN should match 'Alice' with 'ALICE' using NOCASE collation"
    );

    // Verify the row contains the expected values
    assert_eq!(
        result[0].values[0],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))
    );
    assert_eq!(
        result[0].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello, ALICE!"))
    );
}

// Issue #4708: USING clause should also respect column COLLATE declarations
#[test]
fn test_using_join_respects_collation() {
    let mut db = vibesql_storage::Database::new();

    // Create people table with case-insensitive NAME column
    let mut name_col = vibesql_catalog::ColumnSchema::new(
        "NAME".to_string(),
        vibesql_types::DataType::Varchar { max_length: Some(50) },
        false,
    );
    name_col.collation = Some("NOCASE".to_string());
    let people_schema = vibesql_catalog::TableSchema::new("PEOPLE".to_string(), vec![name_col]);
    db.create_table(people_schema).unwrap();

    // Create greetings table with case-insensitive NAME column
    let mut greeting_name_col = vibesql_catalog::ColumnSchema::new(
        "NAME".to_string(),
        vibesql_types::DataType::Varchar { max_length: Some(50) },
        false,
    );
    greeting_name_col.collation = Some("NOCASE".to_string());
    let greeting_col = vibesql_catalog::ColumnSchema::new(
        "GREETING".to_string(),
        vibesql_types::DataType::Varchar { max_length: Some(100) },
        false,
    );
    let greetings_schema = vibesql_catalog::TableSchema::new(
        "GREETINGS".to_string(),
        vec![greeting_name_col, greeting_col],
    );
    db.create_table(greetings_schema).unwrap();

    // Insert data - 'bob' in people, 'BOB' in greetings
    db.insert_row(
        "people",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
            "bob",
        ))]),
    )
    .unwrap();

    db.insert_row(
        "greetings",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("BOB")),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello, BOB!")),
        ]),
    )
    .unwrap();

    // Execute JOIN USING query - should match 'bob' with 'BOB' due to NOCASE collation
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "people".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "greetings".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None,
            using_columns: Some(vec!["NAME".to_string()]), // USING (NAME)
            natural: false,
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    let result = executor.execute(&stmt).unwrap();

    // Should return 1 row - 'bob' matches 'BOB' with NOCASE collation
    assert_eq!(result.len(), 1, "JOIN USING should match 'bob' with 'BOB' using NOCASE collation");

    // Verify the row contains the expected values
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("bob")));
    assert_eq!(
        result[0].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello, BOB!"))
    );
}

// TODO: Add more comprehensive tests for:
// - NATURAL JOIN with no common columns (should behave like CROSS JOIN)
// - NATURAL LEFT JOIN
// - NATURAL RIGHT JOIN
// - NATURAL JOIN with multiple common columns
// - NATURAL JOIN with case-insensitive column matching

/// Test for issue #4843: INNER JOIN USING correctly handles rows with NULL USING columns
/// from nested LEFT JOIN.
///
/// When an INNER JOIN USING(a) is applied to a subquery that contains NULL values in column
/// `a` (from a nested LEFT JOIN), rows with NULL USING columns should NOT be incorrectly
/// filtered out.
///
/// The USING clause should match on the COALESCED value of the column, not require ALL
/// columns with that name to match.
#[test]
fn test_issue_4843_inner_join_using_with_nested_left_join_nulls() {
    let mut db = vibesql_storage::Database::new();

    // Create tables: t2, t3, t4, t5 all with column 'a'
    for table_name in &["t2", "t3", "t4", "t5"] {
        let schema = vibesql_catalog::TableSchema::new(
            table_name.to_string(),
            vec![vibesql_catalog::ColumnSchema::new(
                "a".to_string(),
                vibesql_types::DataType::Integer,
                true, // nullable
            )],
        );
        db.create_table(schema).unwrap();
    }

    // Insert data:
    // t2: 1, 2
    // t3: 1, 2
    // t4: 2 (only 2 - no match for 1)
    // t5: 2 (only 2 - no match for 1)
    for val in &[1, 2] {
        db.insert_row(
            "t2",
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(*val)]),
        )
        .unwrap();
        db.insert_row(
            "t3",
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(*val)]),
        )
        .unwrap();
    }
    db.insert_row("t4", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("t5", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query: t2 INNER JOIN (t3 LEFT JOIN (t4 INNER JOIN t5 USING(a)) USING(a)) USING(a)
    // Expected: 2 rows
    // - Row 1: a=1, t3.a=1, t4.a=NULL, t5.a=NULL (from LEFT JOIN with no matching t4/t5)
    // - Row 2: a=2, t3.a=2, t4.a=2, t5.a=2
    //
    // The inner join (t4 INNER JOIN t5 USING(a)) produces 1 row: a=2
    // The left join (t3 LEFT JOIN ... USING(a)) produces 2 rows:
    //   - t3.a=1 with t4.a=NULL, t5.a=NULL
    //   - t3.a=2 with t4.a=2, t5.a=2
    // The outer inner join (t2 INNER JOIN ... USING(a)) should produce 2 rows:
    //   - t2.a=1 matches the row where t3.a=1 (coalesced USING value is 1)
    //   - t2.a=2 matches the row where t3.a=2 (coalesced USING value is 2)

    // Build the nested join structure
    let t5_ref = vibesql_ast::FromClause::Table {
        quoted: false,
        name: "t5".to_string(),
        alias: None,
        column_aliases: None,
    };
    let t4_ref = vibesql_ast::FromClause::Table {
        quoted: false,
        name: "t4".to_string(),
        alias: None,
        column_aliases: None,
    };
    let t3_ref = vibesql_ast::FromClause::Table {
        quoted: false,
        name: "t3".to_string(),
        alias: None,
        column_aliases: None,
    };
    let t2_ref = vibesql_ast::FromClause::Table {
        quoted: false,
        name: "t2".to_string(),
        alias: None,
        column_aliases: None,
    };

    // t4 INNER JOIN t5 USING(a)
    let t4_t5_join = vibesql_ast::FromClause::Join {
        left: Box::new(t4_ref),
        right: Box::new(t5_ref),
        join_type: vibesql_ast::JoinType::Inner,
        condition: None,
        using_columns: Some(vec!["a".to_string()]),
        natural: false,
        alias: None,
    };

    // t3 LEFT JOIN (t4 INNER JOIN t5 USING(a)) USING(a)
    let t3_t4t5_join = vibesql_ast::FromClause::Join {
        left: Box::new(t3_ref),
        right: Box::new(t4_t5_join),
        join_type: vibesql_ast::JoinType::LeftOuter,
        condition: None,
        using_columns: Some(vec!["a".to_string()]),
        natural: false,
        alias: None,
    };

    // t2 INNER JOIN (...) USING(a)
    let full_join = vibesql_ast::FromClause::Join {
        left: Box::new(t2_ref),
        right: Box::new(t3_t4t5_join),
        join_type: vibesql_ast::JoinType::Inner,
        condition: None,
        using_columns: Some(vec!["a".to_string()]),
        natural: false,
        alias: None,
    };

    // SELECT t2.a, t3.a, t4.a, t5.a FROM ... ORDER BY t2.a
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                source_text: None,
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    "t2", false, "a", false,
                )),
                alias: Some("t2_a".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                source_text: None,
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    "t3", false, "a", false,
                )),
                alias: Some("t3_a".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                source_text: None,
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    "t4", false, "a", false,
                )),
                alias: Some("t4_a".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                source_text: None,
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    "t5", false, "a", false,
                )),
                alias: Some("t5_a".to_string()),
            },
        ],
        from: Some(full_join),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "t2", false, "a", false,
            )),
            direction: vibesql_ast::OrderDirection::Asc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 2 rows, not 1
    assert_eq!(
        result.len(),
        2,
        "Issue #4843: INNER JOIN USING should match on coalesced column value, not filter out rows with NULL in nested columns"
    );

    // Row 1: t2.a=1, t3.a=1, t4.a=NULL, t5.a=NULL
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Null);
    assert_eq!(result[0].values[3], vibesql_types::SqlValue::Null);

    // Row 2: t2.a=2, t3.a=2, t4.a=2, t5.a=2
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[1].values[2], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[1].values[3], vibesql_types::SqlValue::Integer(2));
}
