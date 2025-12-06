//! Tests for WHEN clause conditional trigger execution

use super::{count_audit_rows, create_audit_table};
use crate::{CreateTableExecutor, InsertExecutor};
use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;

#[test]
fn test_when_clause_filters_firing() {
    let mut db = Database::new();

    // Create table with amount column
    let table_stmt = vibesql_ast::CreateTableStmt {
        if_not_exists: false,
        table_name: "TRANSACTIONS".to_string(),
        columns: vec![
            vibesql_ast::ColumnDef {
                name: "id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            vibesql_ast::ColumnDef {
                name: "amount".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&table_stmt, &mut db)
        .expect("Failed to create transactions table");
    create_audit_table(&mut db);

    // Create trigger with WHEN (amount > 100) condition
    let trigger_stmt = CreateTriggerStmt {
        trigger_name: "log_high_amount".to_string(),
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "TRANSACTIONS".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: Some(Box::new(vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::GreaterThan,
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                column: "amount".to_string(),
                table: None,
            }),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                100,
            ))),
        })),
        triggered_action: TriggerAction::RawSql(
            "INSERT INTO audit_log (event) VALUES ('High amount')".to_string(),
        ),
    };
    crate::advanced_objects::execute_create_trigger(&trigger_stmt, &mut db)
        .expect("Failed to create trigger");

    // Insert row with amount=50 (should NOT fire)
    let insert1 = vibesql_ast::InsertStmt {
        table_name: "TRANSACTIONS".to_string(),
        columns: vec!["id".to_string(), "amount".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(50)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert1).expect("Failed to insert");

    // Verify trigger did NOT fire
    assert_eq!(count_audit_rows(&db), 0);

    // Insert row with amount=150 (should fire)
    let insert2 = vibesql_ast::InsertStmt {
        table_name: "TRANSACTIONS".to_string(),
        columns: vec!["id".to_string(), "amount".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(150)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert2).expect("Failed to insert");

    // Verify trigger fired only once (for amount=150)
    assert_eq!(count_audit_rows(&db), 1);
}
