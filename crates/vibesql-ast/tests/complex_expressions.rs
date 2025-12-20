use vibesql_ast::*;
use vibesql_types::{SqlValue, StringValue};

// ============================================================================
// Complex Expression Tests
// ============================================================================

#[test]
fn test_case_expression_simple() {
    let expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
            "status", false,
        )))),
        when_clauses: vec![
            CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Integer(1))],
                result: Expression::Literal(SqlValue::Varchar(StringValue::from("active"))),
            },
            CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Integer(2))],
                result: Expression::Literal(SqlValue::Varchar(StringValue::from("inactive"))),
            },
        ],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(StringValue::from(
            "unknown",
        ))))),
    };
    match expr {
        Expression::Case { .. } => {} // Success
        _ => panic!("Expected CASE expression"),
    }
}

#[test]
fn test_case_expression_searched() {
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "age", false,
                ))),
                right: Box::new(Expression::Literal(SqlValue::Integer(18))),
            }],
            result: Expression::Literal(SqlValue::Varchar(StringValue::from("adult"))),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(StringValue::from(
            "minor",
        ))))),
    };
    match expr {
        Expression::Case { operand: None, .. } => {} // Success
        _ => panic!("Expected searched CASE expression"),
    }
}

#[test]
fn test_scalar_subquery() {
    let subquery = SelectStmt {
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::Function {
                name: vibesql_ast::FunctionIdentifier::new("AVG"),
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "salary", false,
                ))],
                character_unit: None,
            },
            alias: None,
            source_text: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let expr = Expression::ScalarSubquery(Box::new(subquery));
    match expr {
        Expression::ScalarSubquery(_) => {} // Success
        _ => panic!("Expected scalar subquery"),
    }
}

#[test]
fn test_in_expression() {
    let subquery = SelectStmt {
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Expression {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "department_id",
                false,
            )),
            alias: None,
            source_text: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "departments".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let expr = Expression::In {
        expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
            "dept_id", false,
        ))),
        subquery: Box::new(subquery),
        negated: false,
    };
    match expr {
        Expression::In { negated: false, .. } => {} // Success
        _ => panic!("Expected IN expression"),
    }
}

#[test]
fn test_not_in_expression() {
    let subquery = SelectStmt {
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(FromClause::Table {
            name: "excluded".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let expr = Expression::In {
        expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("id", false))),
        subquery: Box::new(subquery),
        negated: true,
    };
    match expr {
        Expression::In { negated: true, .. } => {} // Success
        _ => panic!("Expected NOT IN expression"),
    }
}
