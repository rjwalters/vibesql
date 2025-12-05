//! Tests for DISTINCT in aggregate functions (E091-07)
//!
//! Tests COUNT, SUM, AVG, MIN, MAX with DISTINCT quantifier
//! as specified in SQL:1999 Section 6.16 (Set functions)

use super::super::*;

/// Helper to create a test database with duplicate values
fn create_test_db_with_duplicates() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "amount".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert data with duplicate amounts: 100, 100, 200, 100, 300, 200
    // Unique values: 100, 200, 300 (3 distinct values)
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(5),
            vibesql_types::SqlValue::Integer(300),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(6),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_count_distinct_basic() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(DISTINCT amount) FROM sales
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: true,
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Should count 3 distinct values: 100, 200, 300
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_count_distinct_vs_count_all() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(amount), COUNT(DISTINCT amount) FROM sales
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: true,
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(6)); // COUNT(amount) - all rows
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(3)); // COUNT(DISTINCT amount) - unique
                                                                          // values
}

#[test]
fn test_sum_distinct() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT SUM(DISTINCT amount) FROM sales
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "SUM".to_string(),
                distinct: true,
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Should sum unique values: 100 + 200 + 300 = 600
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(600));
}

#[test]
fn test_sum_distinct_vs_sum_all() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT SUM(amount), SUM(DISTINCT amount) FROM sales
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: true,
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM: 100+100+200+100+300+200 = 1000
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1000));
    // SUM(DISTINCT): 100+200+300 = 600
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(600));
}

#[test]
fn test_avg_distinct() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT AVG(DISTINCT amount) FROM sales
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "AVG".to_string(),
                distinct: true,
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Average of unique values: (100 + 200 + 300) / 3 = 200
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Numeric(200.0));
}

#[test]
fn test_min_distinct() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT MIN(DISTINCT amount) FROM sales
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "MIN".to_string(),
                distinct: true,
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // MIN should be 100 (same with or without DISTINCT)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(100));
}

#[test]
fn test_max_distinct() {
    let db = create_test_db_with_duplicates();
    let executor = SelectExecutor::new(&db);

    // SELECT MAX(DISTINCT amount) FROM sales
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "MAX".to_string(),
                distinct: true,
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // MAX should be 300 (same with or without DISTINCT)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(300));
}

#[test]
fn test_count_distinct_with_nulls() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            true, // nullable
        )],
    );
    db.create_table(schema).unwrap();

    // Insert values including NULLs: 1, 1, 2, NULL, NULL
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(DISTINCT val) FROM test
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: true,
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "val".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Should count only unique non-NULL values: 1, 2 = 2 distinct values
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
}

#[test]
fn test_distinct_all_same_value() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert same value 3 times
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(42)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(42)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(42)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(DISTINCT val), SUM(DISTINCT val) FROM test
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: true,
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: true,
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // COUNT(DISTINCT): 1 unique value
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    // SUM(DISTINCT): 42 (only counted once)
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(42));
}

#[test]
fn test_distinct_empty_table() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "empty_test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            true,
        )],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT COUNT(DISTINCT val), SUM(DISTINCT val) FROM empty_test
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: true,
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: true,
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "empty_test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // COUNT on empty table should be 0
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(0));
    // SUM on empty table should be NULL (SQL standard)
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Null);
}
