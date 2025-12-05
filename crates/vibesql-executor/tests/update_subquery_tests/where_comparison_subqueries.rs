use vibesql_ast::{Assignment, Expression};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::UpdateExecutor;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_update_where_scalar_subquery_equal() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000)]))
        .unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(60000)]))
        .unwrap();

    // Create config table
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("min_salary".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();
    db.insert_row("config", Row::new(vec![SqlValue::Integer(50000)])).unwrap();

    // Subquery: SELECT min_salary FROM config
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "min_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "config".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = 55000 WHERE salary = (SELECT min_salary FROM config)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(55000)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 1

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(55000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(60000)); // Not updated
}

#[test]
fn test_update_where_scalar_subquery_less_than() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("bonus".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(40000), SqlValue::Integer(0)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(70000), SqlValue::Integer(0)]),
    )
    .unwrap();

    // Subquery: SELECT AVG(salary) FROM employees
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "AVG".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "salary".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET bonus = 5000 WHERE salary < (SELECT AVG(salary) FROM employees)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "bonus".to_string(),
            value: Expression::Literal(SqlValue::Integer(5000)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
            op: vibesql_ast::BinaryOperator::LessThan,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 1 (40000 < 55000)

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Integer(5000)); // Updated
    assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Integer(0)); // Not updated
}

#[test]
fn test_update_where_subquery_returns_null() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000)]))
        .unwrap();

    // Create config table with no rows
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("max_salary".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();

    // Subquery returns NULL (empty result)
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "max_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "config".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = 60000 WHERE salary < (SELECT max_salary FROM config)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(60000)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
            op: vibesql_ast::BinaryOperator::LessThan,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 0); // No rows updated (NULL comparison is always FALSE/UNKNOWN)

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(50000)); // Not updated
}

#[test]
fn test_update_where_subquery_with_aggregate() {
    let mut db = Database::new();

    // Create items table
    let schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, false),
            ColumnSchema::new("discounted".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "items",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Boolean(false)]),
    )
    .unwrap();
    db.insert_row(
        "items",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(50), SqlValue::Boolean(false)]),
    )
    .unwrap();
    db.insert_row(
        "items",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(200), SqlValue::Boolean(false)]),
    )
    .unwrap();

    // Subquery: SELECT MAX(price) FROM items
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "MAX".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "price".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "items".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE items SET discounted = TRUE WHERE price = (SELECT MAX(price) FROM items)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "items".to_string(),
        assignments: vec![Assignment {
            column: "discounted".to_string(),
            value: Expression::Literal(SqlValue::Boolean(true)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Item 3 with price 200

    let table = db.get_table("items").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Boolean(false));
    assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Boolean(false));
    assert_eq!(rows[2].get(2).unwrap(), &SqlValue::Boolean(true)); // Updated
}

#[test]
fn test_update_where_and_set_both_use_subqueries() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(60000), SqlValue::Integer(20)]),
    )
    .unwrap();

    // Create salary_targets table
    let targets_schema = TableSchema::new(
        "salary_targets".to_string(),
        vec![ColumnSchema::new("target".to_string(), DataType::Integer, false)],
    );
    db.create_table(targets_schema).unwrap();
    db.insert_row("salary_targets", Row::new(vec![SqlValue::Integer(70000)])).unwrap();

    // Create active_depts table
    let dept_schema = TableSchema::new(
        "active_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    // SET subquery
    let set_subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "target".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "salary_targets".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // WHERE subquery
    let where_subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "active_depts".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = (SELECT target FROM salary_targets) WHERE dept_id IN (SELECT
    // dept_id FROM active_depts)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(set_subquery),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery: where_subquery,
            negated: false,
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 1

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(70000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(60000)); // Not updated
}
