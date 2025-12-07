//! DELETE with subquery tests (Issue #353)

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

mod common {
    use super::*;

    pub fn create_employees_table(db: &mut Database) {
        let schema = TableSchema::new(
            "employees".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
    }

    pub fn insert_employee(db: &mut Database, id: i64, name: &str, dept_id: i64) {
        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Varchar(arcstr::ArcStr::from(name)),
                SqlValue::Integer(dept_id),
            ]),
        )
        .unwrap();
    }

    pub fn create_dept_table(db: &mut Database, table_name: &str) {
        let schema = TableSchema::new(
            table_name.to_string(),
            vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();
    }

    pub fn insert_dept(db: &mut Database, table_name: &str, dept_id: i64) {
        db.insert_row(table_name, Row::new(vec![SqlValue::Integer(dept_id)])).unwrap();
    }

    pub fn create_items_table(db: &mut Database) {
        let schema = TableSchema::new(
            "items".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new("price".to_string(), DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
    }

    pub fn insert_item(db: &mut Database, id: i64, name: &str, price: i64) {
        db.insert_row(
            "items",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Varchar(arcstr::ArcStr::from(name)),
                SqlValue::Integer(price),
            ]),
        )
        .unwrap();
    }

    pub fn create_orders_table(db: &mut Database) {
        let schema = TableSchema::new(
            "orders".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("customer_id".to_string(), DataType::Integer, false),
                ColumnSchema::new("amount".to_string(), DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
    }

    pub fn insert_order(db: &mut Database, id: i64, customer_id: i64, amount: i64) {
        db.insert_row(
            "orders",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Integer(customer_id),
                SqlValue::Integer(amount),
            ]),
        )
        .unwrap();
    }

    pub fn create_customers_table(db: &mut Database, table_name: &str) {
        let schema = TableSchema::new(
            table_name.to_string(),
            vec![
                ColumnSchema::new("customer_id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "status".to_string(),
                    DataType::Varchar { max_length: Some(20) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();
    }

    pub fn insert_customer(db: &mut Database, table_name: &str, customer_id: i64, status: &str) {
        db.insert_row(
            table_name,
            Row::new(vec![SqlValue::Integer(customer_id), SqlValue::Varchar(arcstr::ArcStr::from(status))]),
        )
        .unwrap();
    }

    pub fn create_salary_employees_table(db: &mut Database) {
        let schema = TableSchema::new(
            "employees".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
    }

    pub fn insert_salary_employee(db: &mut Database, id: i64, name: &str, salary: i64) {
        db.insert_row(
            "employees",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Varchar(arcstr::ArcStr::from(name)),
                SqlValue::Integer(salary),
            ]),
        )
        .unwrap();
    }

    pub fn create_config_table(db: &mut Database) {
        let schema = TableSchema::new(
            "config".to_string(),
            vec![ColumnSchema::new("threshold".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();
    }
}

mod in_subquery {
    use vibesql_ast::{DeleteStmt, Expression, WhereClause};
    use vibesql_storage::Database;
    use vibesql_types::SqlValue;

    use super::common::*;
    use crate::DeleteExecutor;

    #[test]
    fn test_delete_where_in_subquery() {
        let mut db = Database::new();

        create_employees_table(&mut db);

        insert_employee(&mut db, 1, "Alice", 10);
        insert_employee(&mut db, 2, "Bob", 20);
        insert_employee(&mut db, 3, "Charlie", 10);

        create_dept_table(&mut db, "inactive_depts");
        insert_dept(&mut db, "inactive_depts", 10);

        // Subquery: SELECT dept_id FROM inactive_depts
        let subquery = Box::new(vibesql_ast::SelectStmt {
            into_table: None,
            into_variables: None,
            with_clause: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
                alias: None,
            }],
            from: Some(vibesql_ast::FromClause::Table {
                name: "inactive_depts".to_string(),
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

        // DELETE FROM employees WHERE dept_id IN (SELECT dept_id FROM inactive_depts)
        let stmt = DeleteStmt {
            only: false,
            table_name: "employees".to_string(),
            where_clause: Some(WhereClause::Condition(Expression::In {
                expr: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "dept_id".to_string(),
                }),
                subquery,
                negated: false,
            })),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 2); // Alice and Charlie

        // Verify only Bob remains
        let table = db.get_table("employees").unwrap();
        assert_eq!(table.row_count(), 1);
        let remaining = &table.scan()[0];
        assert_eq!(remaining.get(1).unwrap(), &SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    }

    #[test]
    fn test_delete_where_not_in_subquery() {
        let mut db = Database::new();

        create_employees_table(&mut db);

        insert_employee(&mut db, 1, "Alice", 10);
        insert_employee(&mut db, 2, "Bob", 20);

        create_dept_table(&mut db, "active_depts");
        insert_dept(&mut db, "active_depts", 10);

        // Subquery
        let subquery = Box::new(vibesql_ast::SelectStmt {
            into_table: None,
            into_variables: None,
            with_clause: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
                alias: None,
            }],
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

        // DELETE FROM employees WHERE dept_id NOT IN (SELECT dept_id FROM active_depts)
        let stmt = DeleteStmt {
            only: false,
            table_name: "employees".to_string(),
            where_clause: Some(WhereClause::Condition(Expression::In {
                expr: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "dept_id".to_string(),
                }),
                subquery,
                negated: true,
            })),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 1); // Bob in inactive dept

        // Verify only Alice remains
        let table = db.get_table("employees").unwrap();
        assert_eq!(table.row_count(), 1);
        let remaining = &table.scan()[0];
        assert_eq!(remaining.get(1).unwrap(), &SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    }
}

mod scalar_subquery {
    use vibesql_ast::{DeleteStmt, Expression, WhereClause};
    use vibesql_storage::Database;
    use vibesql_types::SqlValue;

    use super::common::*;
    use crate::DeleteExecutor;

    #[test]
    fn test_delete_where_scalar_subquery_comparison() {
        let mut db = Database::new();

        create_salary_employees_table(&mut db);

        insert_salary_employee(&mut db, 1, "Alice", 40000);
        insert_salary_employee(&mut db, 2, "Bob", 60000);
        insert_salary_employee(&mut db, 3, "Charlie", 70000);

        // Subquery: SELECT AVG(salary) FROM employees
        let subquery = Box::new(vibesql_ast::SelectStmt {
            into_table: None,
            into_variables: None,
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

        // DELETE FROM employees WHERE salary < (SELECT AVG(salary) FROM employees)
        let stmt = DeleteStmt {
            only: false,
            table_name: "employees".to_string(),
            where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
                op: vibesql_ast::BinaryOperator::LessThan,
                right: Box::new(Expression::ScalarSubquery(subquery)),
            })),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 1); // Alice (40000 < avg 56666)

        // Verify Bob and Charlie remain
        let table = db.get_table("employees").unwrap();
        assert_eq!(table.row_count(), 2);
        let names: Vec<arcstr::ArcStr> = table
            .scan()
            .iter()
            .map(|row| {
                if let SqlValue::Varchar(name) = row.get(1).unwrap() {
                    name.clone()
                } else { arcstr::ArcStr::from("") }
            })
            .collect();
        assert!(names.iter().any(|n| n.as_str() == "Bob"));
        assert!(names.iter().any(|n| n.as_str() == "Charlie"));
    }

    #[test]
    fn test_delete_where_subquery_with_aggregate_max() {
        let mut db = Database::new();

        create_items_table(&mut db);

        insert_item(&mut db, 1, "Widget", 100);
        insert_item(&mut db, 2, "Gadget", 200);
        insert_item(&mut db, 3, "Doohickey", 150);

        // Subquery: SELECT MAX(price) FROM items
        let subquery = Box::new(vibesql_ast::SelectStmt {
            into_table: None,
            into_variables: None,
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

        // DELETE FROM items WHERE price = (SELECT MAX(price) FROM items)
        let stmt = DeleteStmt {
            only: false,
            table_name: "items".to_string(),
            where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(Expression::ScalarSubquery(subquery)),
            })),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 1); // Gadget with price 200

        // Verify Widget and Doohickey remain
        let table = db.get_table("items").unwrap();
        assert_eq!(table.row_count(), 2);
        let prices: Vec<i64> = table
            .scan()
            .iter()
            .map(|row| if let SqlValue::Integer(price) = row.get(2).unwrap() { *price } else { 0 })
            .collect();
        assert!(prices.contains(&100));
        assert!(prices.contains(&150));
    }

    #[test]
    fn test_delete_where_subquery_returns_null() {
        let mut db = Database::new();

        create_salary_employees_table(&mut db);

        insert_salary_employee(&mut db, 1, "Alice", 50000);

        create_config_table(&mut db);

        // Subquery returns NULL (empty result)
        let subquery = Box::new(vibesql_ast::SelectStmt {
            into_table: None,
            into_variables: None,
            with_clause: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "threshold".to_string() },
                alias: None,
            }],
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

        // DELETE FROM employees WHERE salary > (SELECT threshold FROM config)
        let stmt = DeleteStmt {
            only: false,
            table_name: "employees".to_string(),
            where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
                op: vibesql_ast::BinaryOperator::GreaterThan,
                right: Box::new(Expression::ScalarSubquery(subquery)),
            })),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 0); // No rows deleted (NULL comparison always FALSE/UNKNOWN)

        let table = db.get_table("employees").unwrap();
        assert_eq!(table.row_count(), 1); // Alice still exists
    }
}

mod empty_subquery {
    use vibesql_ast::{DeleteStmt, Expression, WhereClause};
    use vibesql_storage::Database;

    use super::common::*;
    use crate::DeleteExecutor;

    #[test]
    fn test_delete_where_subquery_empty_result() {
        let mut db = Database::new();

        create_employees_table(&mut db);

        insert_employee(&mut db, 1, "Alice", 10);

        create_dept_table(&mut db, "old_depts");

        // Subquery returns empty result
        let subquery = Box::new(vibesql_ast::SelectStmt {
            into_table: None,
            into_variables: None,
            with_clause: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
                alias: None,
            }],
            from: Some(vibesql_ast::FromClause::Table {
                name: "old_depts".to_string(),
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

        // DELETE FROM employees WHERE dept_id IN (SELECT dept_id FROM old_depts)
        let stmt = DeleteStmt {
            only: false,
            table_name: "employees".to_string(),
            where_clause: Some(WhereClause::Condition(Expression::In {
                expr: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "dept_id".to_string(),
                }),
                subquery,
                negated: false,
            })),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 0); // No rows deleted

        let table = db.get_table("employees").unwrap();
        assert_eq!(table.row_count(), 1); // Alice still exists
    }
}

mod complex_subquery {
    use vibesql_ast::{DeleteStmt, Expression, WhereClause};
    use vibesql_storage::Database;
    use vibesql_types::SqlValue;

    use super::common::*;
    use crate::DeleteExecutor;

    #[test]
    fn test_delete_where_complex_subquery_with_filter() {
        let mut db = Database::new();

        create_orders_table(&mut db);

        insert_order(&mut db, 1, 101, 50);
        insert_order(&mut db, 2, 102, 75);
        insert_order(&mut db, 3, 103, 120);

        create_customers_table(&mut db, "inactive_customers");

        insert_customer(&mut db, "inactive_customers", 101, "inactive");
        insert_customer(&mut db, "inactive_customers", 102, "inactive");

        // Subquery: SELECT customer_id FROM inactive_customers WHERE status = 'inactive'
        let subquery = Box::new(vibesql_ast::SelectStmt {
            into_table: None,
            into_variables: None,
            with_clause: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "customer_id".to_string() },
                alias: None,
            }],
            from: Some(vibesql_ast::FromClause::Table {
                name: "inactive_customers".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "status".to_string() }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("inactive")))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        });

        // DELETE FROM orders WHERE customer_id IN (SELECT customer_id FROM inactive_customers WHERE
        // status = 'inactive')
        let stmt = DeleteStmt {
            only: false,
            table_name: "orders".to_string(),
            where_clause: Some(WhereClause::Condition(Expression::In {
                expr: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "customer_id".to_string(),
                }),
                subquery,
                negated: false,
            })),
        };

        let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(deleted, 2); // Orders 1 and 2

        // Verify only order 3 remains
        let table = db.get_table("orders").unwrap();
        assert_eq!(table.row_count(), 1);
        let remaining = &table.scan()[0];
        assert_eq!(remaining.get(0).unwrap(), &SqlValue::Integer(3));
    }
}
