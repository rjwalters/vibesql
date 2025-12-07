use vibesql_ast::*;
use vibesql_types::{SqlValue, StringValue};

// ============================================================================
// Statement Tests - Top-level SQL statements
// ============================================================================

#[test]
fn test_create_select_statement() {
    let stmt = Statement::Select(Box::new(SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    }));

    match stmt {
        Statement::Select(_) => {} // Success
        _ => panic!("Expected Select statement"),
    }
}

#[test]
fn test_create_insert_statement() {
    let stmt = Statement::Insert(InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["name".to_string()],
        source: InsertSource::Values(vec![vec![Expression::Literal(SqlValue::Varchar(
            StringValue::from("Alice"),
        ))]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    });

    match stmt {
        Statement::Insert(_) => {} // Success
        _ => panic!("Expected Insert statement"),
    }
}

#[test]
fn test_create_update_statement() {
    let stmt = Statement::Update(UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: Expression::Literal(SqlValue::Varchar(StringValue::from("Bob"))),
        }],
        where_clause: None,
    });

    match stmt {
        Statement::Update(_) => {} // Success
        _ => panic!("Expected Update statement"),
    }
}

#[test]
fn test_create_delete_statement() {
    let stmt = Statement::Delete(DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: None,
    });

    match stmt {
        Statement::Delete(_) => {} // Success
        _ => panic!("Expected Delete statement"),
    }
}
