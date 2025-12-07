use vibesql_ast::{CreateIndexStmt, CreateTableStmt, IndexColumn, OrderDirection, SelectStmt};
use vibesql_executor::SelectExecutor;
use vibesql_storage::Database;
use vibesql_types::DataType;

#[test]
fn test_index_ordering() {
    let mut db = Database::new();

    // Create table
    let create_table_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            vibesql_ast::ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            vibesql_ast::ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    vibesql_executor::CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Insert data
    let insert_stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create index
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_users_name".to_string(),
        if_not_exists: false,
        table_name: "users".to_string(),
        index_type: vibesql_ast::IndexType::BTree { unique: false },
        columns: vec![IndexColumn {
            column_name: "name".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    };

    vibesql_executor::IndexExecutor::execute(&create_index_stmt, &mut db).unwrap();

    // Query with ORDER BY
    let select_stmt = SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "name".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "name".to_string() },
            direction: OrderDirection::Asc,
        }]),
        limit: None,
        offset: None,
        set_operation: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();

    // Check that results are ordered
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));
}
