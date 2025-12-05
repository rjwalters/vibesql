use vibesql_ast::{Assignment, Expression, UpdateStmt};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::UpdateExecutor;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// Helper functions for test setup and common operations

fn create_employees_table(db: &mut Database) {
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
}

fn create_employees_table_numeric_salary(db: &mut Database) {
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "salary".to_string(),
                DataType::Numeric { precision: 15, scale: 2 },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
}

fn create_products_table(db: &mut Database) {
    let schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
}

fn create_data_table(db: &mut Database, name: &str, column_name: &str, nullable: bool) {
    let schema = TableSchema::new(
        name.to_string(),
        vec![ColumnSchema::new(column_name.to_string(), DataType::Integer, nullable)],
    );
    db.create_table(schema).unwrap();
}

fn create_scalar_subquery(table_name: &str, column_name: &str) -> Box<vibesql_ast::SelectStmt> {
    Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: column_name.to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: table_name.to_string(),
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
    })
}

fn create_aggregate_subquery(
    table_name: &str,
    column_name: &str,
    func_name: &str,
) -> Box<vibesql_ast::SelectStmt> {
    Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::Function {
                name: func_name.to_string(),
                args: vec![Expression::ColumnRef { table: None, column: column_name.to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: table_name.to_string(),
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
    })
}

fn create_update_stmt(
    table_name: &str,
    column: &str,
    value: Expression,
    where_clause: Option<vibesql_ast::WhereClause>,
) -> UpdateStmt {
    UpdateStmt {
        table_name: table_name.to_string(),
        assignments: vec![Assignment { column: column.to_string(), value }],
        where_clause,
    }
}

fn create_multi_column_update_stmt(
    table_name: &str,
    assignments: Vec<(&str, Expression)>,
) -> UpdateStmt {
    UpdateStmt {
        table_name: table_name.to_string(),
        assignments: assignments
            .into_iter()
            .map(|(col, val)| Assignment { column: col.to_string(), value: val })
            .collect(),
        where_clause: None,
    }
}

fn insert_employee(db: &mut Database, id: i64, salary: SqlValue) {
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(id), salary])).unwrap();
}

fn insert_product(db: &mut Database, id: i64, price: SqlValue) {
    db.insert_row("products", Row::new(vec![SqlValue::Integer(id), price])).unwrap();
}

fn insert_data_row(db: &mut Database, table: &str, value: SqlValue) {
    db.insert_row(table, Row::new(vec![value])).unwrap();
}

fn verify_single_row_update(db: &Database, table: &str, column_index: usize, expected: SqlValue) {
    let table = db.get_table(table).unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(column_index).unwrap(), &expected);
}

fn verify_all_rows_update(db: &Database, table: &str, column_index: usize, expected: SqlValue) {
    let table = db.get_table(table).unwrap();
    for row in table.scan() {
        assert_eq!(row.get(column_index).unwrap(), &expected);
    }
}

#[test]
fn test_update_with_scalar_subquery_single_value() {
    let mut db = Database::new();
    create_employees_table(&mut db);
    create_data_table(&mut db, "config", "max_salary", false);

    insert_employee(&mut db, 1, SqlValue::Integer(45000));
    insert_data_row(&mut db, "config", SqlValue::Integer(100000));

    let subquery = create_scalar_subquery("config", "max_salary");
    let stmt =
        create_update_stmt("employees", "salary", Expression::ScalarSubquery(subquery), None);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
    verify_single_row_update(&db, "employees", 1, SqlValue::Integer(100000));
}

#[test]
fn test_update_with_scalar_subquery_max_aggregate() {
    let mut db = Database::new();
    create_employees_table(&mut db);
    create_data_table(&mut db, "salaries", "amount", false);

    insert_employee(&mut db, 1, SqlValue::Integer(45000));
    insert_data_row(&mut db, "salaries", SqlValue::Integer(60000));
    insert_data_row(&mut db, "salaries", SqlValue::Integer(75000));
    insert_data_row(&mut db, "salaries", SqlValue::Integer(50000));

    let subquery = create_aggregate_subquery("salaries", "amount", "MAX");
    let stmt =
        create_update_stmt("employees", "salary", Expression::ScalarSubquery(subquery), None);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
    verify_single_row_update(&db, "employees", 1, SqlValue::Integer(75000));
}

#[test]
fn test_update_with_scalar_subquery_min_aggregate() {
    let mut db = Database::new();
    create_products_table(&mut db);
    create_data_table(&mut db, "prices", "amount", false);

    insert_product(&mut db, 1, SqlValue::Integer(100));
    insert_data_row(&mut db, "prices", SqlValue::Integer(50));
    insert_data_row(&mut db, "prices", SqlValue::Integer(25));
    insert_data_row(&mut db, "prices", SqlValue::Integer(75));

    let subquery = create_aggregate_subquery("prices", "amount", "MIN");
    let stmt = create_update_stmt("products", "price", Expression::ScalarSubquery(subquery), None);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
    verify_single_row_update(&db, "products", 1, SqlValue::Integer(25));
}

#[test]
fn test_update_with_scalar_subquery_avg_aggregate() {
    let mut db = Database::new();
    create_employees_table_numeric_salary(&mut db);
    create_data_table(&mut db, "salaries", "amount", false);

    insert_employee(&mut db, 1, SqlValue::Numeric(10000.0));
    insert_data_row(&mut db, "salaries", SqlValue::Integer(60000));
    insert_data_row(&mut db, "salaries", SqlValue::Integer(70000));
    insert_data_row(&mut db, "salaries", SqlValue::Integer(50000));

    let subquery = create_aggregate_subquery("salaries", "amount", "AVG");
    let stmt =
        create_update_stmt("employees", "salary", Expression::ScalarSubquery(subquery), None);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
    verify_single_row_update(&db, "employees", 1, SqlValue::Numeric(60000.0));
}

#[test]
fn test_update_with_scalar_subquery_returns_null() {
    let mut db = Database::new();
    create_employees_table(&mut db);
    create_data_table(&mut db, "config", "max_salary", true);

    insert_employee(&mut db, 1, SqlValue::Integer(45000));
    insert_data_row(&mut db, "config", SqlValue::Null);

    let subquery = create_scalar_subquery("config", "max_salary");
    let stmt =
        create_update_stmt("employees", "salary", Expression::ScalarSubquery(subquery), None);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
    verify_single_row_update(&db, "employees", 1, SqlValue::Null);
}

#[test]
fn test_update_with_scalar_subquery_empty_result() {
    let mut db = Database::new();
    create_employees_table(&mut db);
    create_data_table(&mut db, "config", "max_salary", false);

    insert_employee(&mut db, 1, SqlValue::Integer(45000));
    // No config rows inserted - empty result set

    let subquery = create_scalar_subquery("config", "max_salary");
    let stmt =
        create_update_stmt("employees", "salary", Expression::ScalarSubquery(subquery), None);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
    verify_single_row_update(&db, "employees", 1, SqlValue::Null);
}

#[test]
fn test_update_with_multiple_subqueries() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, min_sal INT, max_sal INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("min_sal".to_string(), DataType::Integer, true),
            ColumnSchema::new("max_sal".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();
    create_data_table(&mut db, "salaries", "amount", false);

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Null]),
    )
    .unwrap();
    insert_data_row(&mut db, "salaries", SqlValue::Integer(40000));
    insert_data_row(&mut db, "salaries", SqlValue::Integer(80000));
    insert_data_row(&mut db, "salaries", SqlValue::Integer(60000));

    let min_subquery = create_aggregate_subquery("salaries", "amount", "MIN");
    let max_subquery = create_aggregate_subquery("salaries", "amount", "MAX");
    let stmt = create_multi_column_update_stmt(
        "employees",
        vec![
            ("min_sal", Expression::ScalarSubquery(min_subquery)),
            ("max_sal", Expression::ScalarSubquery(max_subquery)),
        ],
    );

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify both columns were updated
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Integer(40000)); // min_sal
    assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(80000)); // max_sal
}

#[test]
fn test_update_with_subquery_updates_multiple_rows() {
    let mut db = Database::new();
    create_employees_table(&mut db);
    create_data_table(&mut db, "config", "base_salary", false);

    insert_employee(&mut db, 1, SqlValue::Integer(40000));
    insert_employee(&mut db, 2, SqlValue::Integer(45000));
    insert_employee(&mut db, 3, SqlValue::Integer(50000));
    insert_data_row(&mut db, "config", SqlValue::Integer(55000));

    let subquery = create_scalar_subquery("config", "base_salary");
    let stmt =
        create_update_stmt("employees", "salary", Expression::ScalarSubquery(subquery), None);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 3);
    verify_all_rows_update(&db, "employees", 1, SqlValue::Integer(55000));
}

#[test]
fn test_update_with_subquery_and_where_clause() {
    let mut db = Database::new();
    create_employees_table(&mut db);
    create_data_table(&mut db, "config", "max_salary", false);

    insert_employee(&mut db, 1, SqlValue::Integer(40000));
    insert_employee(&mut db, 2, SqlValue::Integer(50000));
    insert_data_row(&mut db, "config", SqlValue::Integer(45000));

    let subquery = create_scalar_subquery("config", "max_salary");
    let where_clause = vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
        op: vibesql_ast::BinaryOperator::Equal,
        right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    });
    let stmt = create_update_stmt(
        "employees",
        "salary",
        Expression::ScalarSubquery(subquery),
        Some(where_clause),
    );

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify only employee 1 was updated
    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(45000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(50000)); // Not updated
}
