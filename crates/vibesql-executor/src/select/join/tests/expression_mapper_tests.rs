use super::super::expression_mapper::*;
use std::collections::HashSet;
use vibesql_ast::{self, Expression};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::DataType;

fn create_test_schema(name: &str, columns: Vec<(&str, DataType)>) -> TableSchema {
    let cols =
        columns.into_iter().map(|(n, t)| ColumnSchema::new(n.to_string(), t, false)).collect();
    TableSchema::new(name.to_string(), cols)
}

#[test]
fn test_single_table_resolution() {
    let mut mapper = ExpressionMapper::new();
    let varchar_100 = DataType::Varchar { max_length: Some(100) };
    let schema =
        create_test_schema("users", vec![("id", DataType::Integer), ("name", varchar_100.clone())]);
    mapper.add_table("users", &schema);

    assert_eq!(mapper.resolve_column(Some("users"), "id"), Some(0));
    assert_eq!(mapper.resolve_column(Some("users"), "name"), Some(1));
    assert_eq!(mapper.resolve_column(Some("users"), "missing"), None);
}

#[test]
fn test_single_table_unqualified_column() {
    let mut mapper = ExpressionMapper::new();
    let varchar_100 = DataType::Varchar { max_length: Some(100) };
    let schema =
        create_test_schema("users", vec![("id", DataType::Integer), ("name", varchar_100.clone())]);
    mapper.add_table("users", &schema);

    // Unqualified columns should resolve to first match
    assert_eq!(mapper.resolve_column(None, "id"), Some(0));
    assert_eq!(mapper.resolve_column(None, "name"), Some(1));
}

#[test]
fn test_two_table_resolution() {
    let mut mapper = ExpressionMapper::new();
    let varchar_100 = DataType::Varchar { max_length: Some(100) };

    let users_schema =
        create_test_schema("users", vec![("id", DataType::Integer), ("name", varchar_100.clone())]);
    mapper.add_table("users", &users_schema);

    let orders_schema = create_test_schema(
        "orders",
        vec![("id", DataType::Integer), ("user_id", DataType::Integer)],
    );
    mapper.add_table("orders", &orders_schema);

    // Users columns at 0-1
    assert_eq!(mapper.resolve_column(Some("users"), "id"), Some(0));
    assert_eq!(mapper.resolve_column(Some("users"), "name"), Some(1));

    // Orders columns at 2-3
    assert_eq!(mapper.resolve_column(Some("orders"), "id"), Some(2));
    assert_eq!(mapper.resolve_column(Some("orders"), "user_id"), Some(3));
}

#[test]
fn test_case_insensitive_resolution() {
    let mut mapper = ExpressionMapper::new();
    let varchar_100 = DataType::Varchar { max_length: Some(100) };
    let schema =
        create_test_schema("users", vec![("ID", DataType::Integer), ("Name", varchar_100.clone())]);
    mapper.add_table("USERS", &schema);

    // Case variations should resolve
    assert_eq!(mapper.resolve_column(Some("users"), "id"), Some(0));
    assert_eq!(mapper.resolve_column(Some("USERS"), "ID"), Some(0));
    assert_eq!(mapper.resolve_column(Some("Users"), "name"), Some(1));
}

#[test]
fn test_total_columns() {
    let mut mapper = ExpressionMapper::new();

    assert_eq!(mapper.total_columns(), 0);

    let schema1 =
        create_test_schema("t1", vec![("a", DataType::Integer), ("b", DataType::Integer)]);
    mapper.add_table("t1", &schema1);
    assert_eq!(mapper.total_columns(), 2);

    let schema2 = create_test_schema("t2", vec![("c", DataType::Integer)]);
    mapper.add_table("t2", &schema2);
    assert_eq!(mapper.total_columns(), 3);
}

#[test]
fn test_table_offset() {
    let mut mapper = ExpressionMapper::new();

    let schema1 =
        create_test_schema("t1", vec![("a", DataType::Integer), ("b", DataType::Integer)]);
    mapper.add_table("t1", &schema1);

    let schema2 = create_test_schema("t2", vec![("c", DataType::Integer)]);
    mapper.add_table("t2", &schema2);

    assert_eq!(mapper.table_offset("t1"), Some(0));
    assert_eq!(mapper.table_offset("t2"), Some(2));
}

#[test]
fn test_expression_analysis_single_table() {
    let mut mapper = ExpressionMapper::new();
    let schema = create_test_schema("users", vec![("id", DataType::Integer)]);
    mapper.add_table("users", &schema);

    // Single column reference
    let expr = Expression::ColumnRef { table: Some("users".to_string()), column: "id".to_string() };

    let analysis = mapper.analyze_expression(&expr);
    assert!(analysis.all_resolvable);
    assert_eq!(analysis.tables_referenced.len(), 1);
    assert_eq!(analysis.single_table(), Some("users".to_string()));
}

#[test]
fn test_expression_analysis_two_tables() {
    let mut mapper = ExpressionMapper::new();
    mapper.add_table("users", &create_test_schema("users", vec![("id", DataType::Integer)]));
    mapper.add_table("orders", &create_test_schema("orders", vec![("user_id", DataType::Integer)]));

    // Binary operation: users.id = orders.user_id
    let expr = Expression::BinaryOp {
        op: vibesql_ast::BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: Some("users".to_string()),
            column: "id".to_string(),
        }),
        right: Box::new(Expression::ColumnRef {
            table: Some("orders".to_string()),
            column: "user_id".to_string(),
        }),
    };

    let analysis = mapper.analyze_expression(&expr);
    assert!(analysis.all_resolvable);
    assert_eq!(analysis.tables_referenced.len(), 2);
    assert!(!analysis.is_single_table());
}

#[test]
fn test_expression_analysis_unresolvable() {
    let mapper = ExpressionMapper::new();

    // Try to reference non-existent column
    let expr = Expression::ColumnRef { table: Some("users".to_string()), column: "id".to_string() };

    let analysis = mapper.analyze_expression(&expr);
    assert!(!analysis.all_resolvable);
}

#[test]
fn test_expression_refs_only_tables() {
    let mut mapper = ExpressionMapper::new();
    mapper.add_table("users", &create_test_schema("users", vec![("id", DataType::Integer)]));
    mapper.add_table("orders", &create_test_schema("orders", vec![("user_id", DataType::Integer)]));
    mapper.add_table("items", &create_test_schema("items", vec![("order_id", DataType::Integer)]));

    let expr = Expression::BinaryOp {
        op: vibesql_ast::BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef {
            table: Some("users".to_string()),
            column: "id".to_string(),
        }),
        right: Box::new(Expression::ColumnRef {
            table: Some("orders".to_string()),
            column: "user_id".to_string(),
        }),
    };

    let mut allowed = HashSet::new();
    allowed.insert("users".to_string());
    allowed.insert("orders".to_string());

    assert!(mapper.expression_refs_only_tables(&expr, &allowed));

    // Remove one table from allowed set
    allowed.remove("orders");
    assert!(!mapper.expression_refs_only_tables(&expr, &allowed));
}

#[test]
fn test_mapper_clone() {
    let mut mapper1 = ExpressionMapper::new();
    let schema = create_test_schema("users", vec![("id", DataType::Integer)]);
    mapper1.add_table("users", &schema);

    let mapper2 = mapper1.clone();
    assert_eq!(
        mapper1.resolve_column(Some("users"), "id"),
        mapper2.resolve_column(Some("users"), "id")
    );
}
