use vibesql_ast::{Assignment, BinaryOperator, Expression};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Helper to create a users table with primary key
pub fn create_users_table_with_primary_key(db: &mut Database) {
    let schema = TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();
}

/// Helper to create a users table with unique email constraint
pub fn create_users_table_with_unique_email(db: &mut Database) {
    let schema = TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["email".to_string()]],
    );
    db.create_table(schema).unwrap();
}

/// Helper to create a users table with composite unique constraint
pub fn create_users_table_with_composite_unique(db: &mut Database) {
    let schema = TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "first_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
            ColumnSchema::new(
                "last_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["first_name".to_string(), "last_name".to_string()]],
    );
    db.create_table(schema).unwrap();
}

/// Helper to create a products table with price >= 0 check constraint
pub fn create_products_table_with_check_price(db: &mut Database) {
    let schema = TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, false),
        ],
        None,
        Vec::new(),
        vec![(
            "price_positive".to_string(),
            Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expression::Literal(SqlValue::Integer(0))),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();
}

/// Helper to create a products table with nullable price and check constraint
pub fn create_products_table_with_nullable_price(db: &mut Database) {
    let schema = TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, true), // nullable
        ],
        None,
        Vec::new(),
        vec![(
            "price_positive".to_string(),
            Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expression::Literal(SqlValue::Integer(0))),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();
}

/// Helper to create an employees table with bonus < salary check constraint
pub fn create_employees_table_with_check_bonus(db: &mut Database) {
    let schema = TableSchema::with_all_constraint_types(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("bonus".to_string(), DataType::Integer, false),
        ],
        None,
        Vec::new(),
        vec![(
            "bonus_less_than_salary".to_string(),
            Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "bonus".to_string() }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();
}

/// Helper to insert a user row with optional email
#[allow(dead_code)]
pub fn insert_user_row(
    db: &mut Database,
    id: i64,
    name: &str,
    email: Option<&str>,
) -> Result<(), vibesql_storage::StorageError> {
    let email_value = email.map(|e| SqlValue::Varchar(arcstr::ArcStr::from(e))).unwrap_or(SqlValue::Null);
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(id), SqlValue::Varchar(arcstr::ArcStr::from(name)), email_value]),
    )
}

/// Helper to insert a user row without email column (for PK tests)
pub fn insert_user_row_simple(
    db: &mut Database,
    id: i64,
    name: &str,
) -> Result<(), vibesql_storage::StorageError> {
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(id), SqlValue::Varchar(arcstr::ArcStr::from(name))]),
    )
}

/// Helper to create an update statement with a where clause matching id
pub fn create_update_with_id_clause(
    table_name: &str,
    column: &str,
    value: SqlValue,
    id: i64,
) -> vibesql_ast::UpdateStmt {
    vibesql_ast::UpdateStmt {
        table_name: table_name.to_string(),
        assignments: vec![Assignment {
            column: column.to_string(),
            value: Expression::Literal(value),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(id))),
        })),
    }
}
