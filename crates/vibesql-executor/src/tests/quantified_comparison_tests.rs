//! Quantified comparison tests (ALL, ANY, SOME)
//!
//! Tests for MySQL quantified comparison operators:
//! - value > ALL (subquery)
//! - value < ANY (subquery)
//! - value = SOME (subquery)
//! - NULL handling
//! - Empty subquery handling

use super::super::*;

#[test]
fn test_all_greater_than_basic() {
    // Test: SELECT * FROM t1 WHERE x > ALL (SELECT y FROM t2)
    // Returns rows where x is greater than EVERY value in subquery
    let mut db = vibesql_storage::Database::new();

    // Create tables
    let schema1 = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema1).unwrap();

    let schema2 = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "y".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema2).unwrap();

    // Insert data: t1 has [5, 10, 15, 20], t2 has [8, 12]
    for val in [5, 10, 15, 20] {
        db.insert_row("t1", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(val)]))
            .unwrap();
    }
    for val in [8, 12] {
        db.insert_row("t2", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(val)]))
            .unwrap();
    }

    // Build query: SELECT x FROM t1 WHERE x > ALL (SELECT y FROM t2)
    // Only 15 and 20 are greater than ALL values in t2 (8, 12)
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "y".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t2".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::QuantifiedComparison {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            quantifier: vibesql_ast::Quantifier::All,
            subquery,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return 15 and 20
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(15));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(20));
}

#[test]
fn test_any_less_than_basic() {
    // Test: SELECT * FROM t1 WHERE x < ANY (SELECT y FROM t2)
    // Returns rows where x is less than AT LEAST ONE value in subquery
    let mut db = vibesql_storage::Database::new();

    // Create tables
    let schema1 = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema1).unwrap();

    let schema2 = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "y".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema2).unwrap();

    // Insert data: t1 has [5, 10, 15], t2 has [12]
    for val in [5, 10, 15] {
        db.insert_row("t1", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(val)]))
            .unwrap();
    }
    db.insert_row("t2", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(12)]))
        .unwrap();

    // Build query: SELECT x FROM t1 WHERE x < ANY (SELECT y FROM t2)
    // 5 and 10 are less than 12
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "y".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t2".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::QuantifiedComparison {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::LessThan,
            quantifier: vibesql_ast::Quantifier::Any,
            subquery,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return 5 and 10
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(10));
}

#[test]
fn test_some_equals_basic() {
    // Test: SOME is synonym for ANY
    // SELECT * FROM t1 WHERE x = SOME (SELECT y FROM t2)
    let mut db = vibesql_storage::Database::new();

    // Create tables
    let schema1 = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema1).unwrap();

    let schema2 = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "y".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema2).unwrap();

    // Insert data: t1 has [1, 2, 3, 4], t2 has [2, 4]
    for val in [1, 2, 3, 4] {
        db.insert_row("t1", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(val)]))
            .unwrap();
    }
    for val in [2, 4] {
        db.insert_row("t2", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(val)]))
            .unwrap();
    }

    // Build query: SELECT x FROM t1 WHERE x = SOME (SELECT y FROM t2)
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "y".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t2".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::QuantifiedComparison {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            quantifier: vibesql_ast::Quantifier::Some,
            subquery,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return 2 and 4
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(4));
}

#[test]
fn test_all_with_empty_subquery() {
    // MySQL behavior: x > ALL (empty set) returns TRUE
    // This is because: "for all values in empty set, x > value" is vacuously true
    let mut db = vibesql_storage::Database::new();

    let schema1 = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema1).unwrap();

    let schema2 = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "y".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema2).unwrap();

    // Insert data: t1 has [5, 10], t2 is empty
    for val in [5, 10] {
        db.insert_row("t1", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(val)]))
            .unwrap();
    }

    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "y".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t2".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::QuantifiedComparison {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            quantifier: vibesql_ast::Quantifier::All,
            subquery,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return ALL rows (5 and 10) because empty ALL is TRUE
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(10));
}

#[test]
fn test_any_with_empty_subquery() {
    // MySQL behavior: x > ANY (empty set) returns FALSE
    // This is because: "there exists a value in empty set where x > value" is false
    let mut db = vibesql_storage::Database::new();

    let schema1 = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema1).unwrap();

    let schema2 = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "y".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema2).unwrap();

    // Insert data: t1 has [5, 10], t2 is empty
    for val in [5, 10] {
        db.insert_row("t1", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(val)]))
            .unwrap();
    }

    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "y".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t2".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::QuantifiedComparison {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            quantifier: vibesql_ast::Quantifier::Any,
            subquery,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return NO rows because empty ANY is FALSE
    assert_eq!(result.len(), 0);
}

#[test]
fn test_all_with_null_in_subquery() {
    // MySQL behavior: If subquery contains NULL and comparison fails for any value,
    // return NULL (not FALSE)
    let mut db = vibesql_storage::Database::new();

    let schema1 = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema1).unwrap();

    let schema2 = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "y".to_string(),
            vibesql_types::DataType::Integer,
            true,
        )],
    );
    db.create_table(schema2).unwrap();

    // Insert data: t1 has [10], t2 has [5, NULL, 20]
    db.insert_row("t1", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]))
        .unwrap();

    for val in [
        vibesql_types::SqlValue::Integer(5),
        vibesql_types::SqlValue::Null,
        vibesql_types::SqlValue::Integer(20),
    ] {
        db.insert_row("t2", vibesql_storage::Row::new(vec![val])).unwrap();
    }

    // Query: SELECT x FROM t1 WHERE x > ALL (SELECT y FROM t2)
    // 10 > 5 (TRUE), 10 > NULL (NULL), 10 > 20 (FALSE)
    // Result should be FALSE (not NULL) because we have a definite FALSE
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "y".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t2".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::QuantifiedComparison {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            quantifier: vibesql_ast::Quantifier::All,
            subquery,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return no rows because 10 is NOT greater than all values (fails for 20)
    assert_eq!(result.len(), 0);
}

#[test]
fn test_all_with_null_left_value() {
    // MySQL behavior: NULL > ALL (anything) returns NULL
    let mut db = vibesql_storage::Database::new();

    let schema1 = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            true,
        )],
    );
    db.create_table(schema1).unwrap();

    let schema2 = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "y".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema2).unwrap();

    // Insert data: t1 has [NULL], t2 has [5]
    db.insert_row("t1", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null])).unwrap();
    db.insert_row("t2", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();

    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "y".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t2".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::QuantifiedComparison {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            quantifier: vibesql_ast::Quantifier::All,
            subquery,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return no rows because NULL comparisons filter out in WHERE clause
    assert_eq!(result.len(), 0);
}
