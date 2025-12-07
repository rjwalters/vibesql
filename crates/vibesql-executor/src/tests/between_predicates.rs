//! BETWEEN predicate tests

use super::super::*;

#[test]
fn test_between_integer() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Integer(25),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Integer(35),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("David")),
            vibesql_types::SqlValue::Integer(40),
        ]),
    )
    .unwrap();

    // Test: age BETWEEN 28 AND 36
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "age".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(28))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(36))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return Bob (30) and Charlie (35)
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(30));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(35));
}

#[test]
fn test_not_between() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "products".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "price".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Cheap")),
            vibesql_types::SqlValue::Integer(5),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Mid")),
            vibesql_types::SqlValue::Integer(15),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Expensive")),
            vibesql_types::SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    // Test: price NOT BETWEEN 10 AND 20
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "name".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "price".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: true, // NOT BETWEEN
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return Cheap (5) and Expensive (25)
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Cheap")));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Expensive")));
}

#[test]
fn test_between_boundary_inclusive() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "DATA".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "VALUE".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert boundary and middle values
    db.insert_row("DATA", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(9)]))
        .unwrap();
    db.insert_row("DATA", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]))
        .unwrap(); // Lower boundary
    db.insert_row("DATA", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)]))
        .unwrap(); // Middle
    db.insert_row("DATA", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)]))
        .unwrap(); // Upper boundary
    db.insert_row("DATA", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(21)]))
        .unwrap();

    // Test: BETWEEN is inclusive of boundaries
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "DATA".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "VALUE".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 10, 15, 20 (boundaries included)
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(10));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(15));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(20));
}

#[test]
fn test_between_with_column_references() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "ranges".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "VALUE".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "min_val".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "max_val".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data with different ranges
    db.insert_row(
        "ranges",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(5),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ranges",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(15),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ranges",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(8),
            vibesql_types::SqlValue::Integer(5),
            vibesql_types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    // Test: value BETWEEN min_val AND max_val
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "VALUE".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "ranges".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "VALUE".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "min_val".to_string(),
            }),
            high: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "max_val".to_string(),
            }),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return rows where value is within its range
    // Row 1: 5 BETWEEN 1 AND 10 = true
    // Row 2: 15 BETWEEN 1 AND 10 = false
    // Row 3: 8 BETWEEN 5 AND 20 = true
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(8));
}

#[test]
fn test_between_symmetric_swaps_bounds() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "NUMBERS".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "VALUE".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert test data: values from 1 to 10
    for i in 1..=10 {
        db.insert_row(
            "NUMBERS",
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i)]),
        )
        .unwrap();
    }

    // Test: WHERE value BETWEEN SYMMETRIC 10 AND 1
    // Should match values 1 through 10 (swaps bounds since 10 > 1)
    let sql = "SELECT value FROM numbers WHERE value BETWEEN SYMMETRIC 10 AND 1";
    let ast = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let executor = SelectExecutor::new(&db);

    if let vibesql_ast::Statement::Select(stmt) = ast {
        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 10, "SYMMETRIC should swap 10 AND 1 to 1 AND 10");
        for (i, row) in result.iter().enumerate() {
            assert_eq!(row.values[0], vibesql_types::SqlValue::Integer((i + 1) as i64));
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_asymmetric_does_not_swap() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "NUMBERS".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "VALUE".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    for i in 1..=10 {
        db.insert_row(
            "NUMBERS",
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i)]),
        )
        .unwrap();
    }

    // Test: WHERE value BETWEEN ASYMMETRIC 10 AND 1
    // Should match nothing (10 <= value <= 1 is impossible)
    let sql = "SELECT value FROM numbers WHERE value BETWEEN ASYMMETRIC 10 AND 1";
    let ast = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let executor = SelectExecutor::new(&db);

    if let vibesql_ast::Statement::Select(stmt) = ast {
        let result = executor.execute(&stmt).unwrap();
        assert_eq!(
            result.len(),
            0,
            "ASYMMETRIC should NOT swap bounds, so 10 <= value <= 1 matches nothing"
        );
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_default_is_asymmetric() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "NUMBERS".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "VALUE".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    for i in 1..=10 {
        db.insert_row(
            "NUMBERS",
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i)]),
        )
        .unwrap();
    }

    // Test: BETWEEN without modifier should behave like ASYMMETRIC
    let sql1 = "SELECT value FROM numbers WHERE value BETWEEN 1 AND 10";
    let sql2 = "SELECT value FROM numbers WHERE value BETWEEN ASYMMETRIC 1 AND 10";

    let ast1 = vibesql_parser::Parser::parse_sql(sql1).unwrap();
    let ast2 = vibesql_parser::Parser::parse_sql(sql2).unwrap();
    let executor = SelectExecutor::new(&db);

    let result1 = if let vibesql_ast::Statement::Select(stmt) = ast1 {
        executor.execute(&stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    };

    let result2 = if let vibesql_ast::Statement::Select(stmt) = ast2 {
        executor.execute(&stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    };

    assert_eq!(result1.len(), result2.len(), "BETWEEN default should be same as ASYMMETRIC");
    assert_eq!(result1, result2, "Results should be identical");
}

#[test]
fn test_symmetric_with_equal_bounds() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "NUMBERS".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "VALUE".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    for i in 1..=10 {
        db.insert_row(
            "NUMBERS",
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i)]),
        )
        .unwrap();
    }

    // Test: WHERE value BETWEEN SYMMETRIC 5 AND 5
    // Should match only value = 5
    let sql = "SELECT value FROM numbers WHERE value BETWEEN SYMMETRIC 5 AND 5";
    let ast = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let executor = SelectExecutor::new(&db);

    if let vibesql_ast::Statement::Select(stmt) = ast {
        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between_symmetric() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "NUMBERS".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "VALUE".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    for i in 1..=10 {
        db.insert_row(
            "NUMBERS",
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i)]),
        )
        .unwrap();
    }

    // Test: WHERE value NOT BETWEEN SYMMETRIC 8 AND 2
    // Should match values < 2 OR > 8 (after swapping to 2 AND 8)
    // So should match: 1, 9, 10
    let sql = "SELECT value FROM numbers WHERE value NOT BETWEEN SYMMETRIC 8 AND 2";
    let ast = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let executor = SelectExecutor::new(&db);

    if let vibesql_ast::Statement::Select(stmt) = ast {
        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
        assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(9));
        assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(10));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_symmetric_with_null() {
    let mut db = vibesql_storage::Database::new();

    // Create test table
    let schema = vibesql_catalog::TableSchema::new(
        "DATA".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "VALUE".to_string(),
            vibesql_types::DataType::Integer,
            true, // nullable
        )],
    );
    db.create_table(schema).unwrap();

    // Insert test data including NULL
    db.insert_row("DATA", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null])).unwrap();
    db.insert_row("DATA", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();
    db.insert_row("DATA", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)]))
        .unwrap();

    // Test: WHERE value BETWEEN SYMMETRIC 1 AND 10
    // NULL should not match
    let sql = "SELECT value FROM data WHERE value BETWEEN SYMMETRIC 1 AND 10";
    let ast = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let executor = SelectExecutor::new(&db);

    if let vibesql_ast::Statement::Select(stmt) = ast {
        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
    } else {
        panic!("Expected SELECT statement");
    }
}
