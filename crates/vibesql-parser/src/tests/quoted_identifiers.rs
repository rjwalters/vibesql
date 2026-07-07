//! Tests for SQLite-compatible acceptance of single-quoted strings as
//! identifiers in DDL/DML name positions, string/double-quoted constraint
//! names, and the `IS TRUE/FALSE` truth-value predicate with a trailing
//! COLLATE postfix (issue #5841, quote.test / istrue.test recovery).

use crate::Parser;

// --- Single-quoted strings as identifiers (quote.test quote-1.x) ---

#[test]
fn test_create_table_single_quoted_name_and_columns() {
    // quote-1.0: CREATE TABLE '@abc' ( '#xyz' int, '!pqr' text )
    let sql = "CREATE TABLE '@abc' ( '#xyz' int, '!pqr' text )";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            assert_eq!(stmt.table_name, "@abc");
            assert_eq!(stmt.columns.len(), 2);
            assert_eq!(stmt.columns[0].name, "#xyz");
            assert_eq!(stmt.columns[1].name, "!pqr");
        }
        other => panic!("Expected CreateTable, got: {:?}", other),
    }
}

#[test]
fn test_qualified_single_quoted_name_in_select() {
    // quote-1.3: SELECT '@abc'.'!pqr' FROM '@abc'
    let sql = "SELECT '@abc'.'!pqr' FROM '@abc'";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    assert!(matches!(result.unwrap(), vibesql_ast::Statement::Select(_)));
}

#[test]
fn test_bare_single_quoted_string_stays_a_literal() {
    // A single-quoted string NOT followed by `.` must remain a string literal,
    // never be reinterpreted as a column reference.
    let sql = "SELECT '!pqr'";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Select(select) => {
            let e = match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => expr,
                other => panic!("Expected expression projection, got: {:?}", other),
            };
            assert!(
                matches!(e, vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(_))),
                "Expected string literal, got: {:?}",
                e
            );
        }
        other => panic!("Expected Select, got: {:?}", other),
    }
}

#[test]
fn test_update_set_single_quoted_column() {
    // quote-1.4: UPDATE '@abc' SET '#xyz'=11
    let sql = "UPDATE '@abc' SET '#xyz'=11";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::Update(stmt) => {
            assert_eq!(stmt.assignments[0].column, "#xyz");
        }
        other => panic!("Expected Update, got: {:?}", other),
    }
}

// --- Constraint names: string literal and double-quoted (item 4) ---

#[test]
fn test_column_constraint_string_name() {
    let sql = "CREATE TABLE t ( x INTEGER CONSTRAINT 'c1' NOT NULL )";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            let c = &stmt.columns[0].constraints[0];
            assert_eq!(c.name.as_deref(), Some("c1"));
        }
        other => panic!("Expected CreateTable, got: {:?}", other),
    }
}

#[test]
fn test_column_constraint_double_quoted_name() {
    let sql = "CREATE TABLE t ( x INTEGER CONSTRAINT \"c1\" NOT NULL )";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            let c = &stmt.columns[0].constraints[0];
            assert_eq!(c.name.as_deref(), Some("c1"));
        }
        other => panic!("Expected CreateTable, got: {:?}", other),
    }
}

#[test]
fn test_table_constraint_string_name() {
    let sql = "CREATE TABLE t ( x, CONSTRAINT 'ck' CHECK (x > 0) )";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            assert_eq!(stmt.table_constraints[0].name.as_deref(), Some("ck"));
        }
        other => panic!("Expected CreateTable, got: {:?}", other),
    }
}

#[test]
fn test_table_constraint_double_quoted_name() {
    let sql = "CREATE TABLE t ( x, CONSTRAINT \"ck\" CHECK (x > 0) )";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            assert_eq!(stmt.table_constraints[0].name.as_deref(), Some("ck"));
        }
        other => panic!("Expected CreateTable, got: {:?}", other),
    }
}

// --- IS TRUE/FALSE with a trailing COLLATE postfix (item 6) ---

fn parse_single_select_expr(sql: &str) -> vibesql_ast::Expression {
    match Parser::parse_sql(sql).expect("parse ok") {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => expr.clone(),
            other => panic!("Expected expression projection, got: {:?}", other),
        },
        other => panic!("Expected Select, got: {:?}", other),
    }
}

#[test]
fn test_is_true_collate_parses_as_truth_value() {
    // istrue-710: `0.5 IS TRUE COLLATE NOCASE` == `0.5 IS TRUE` (COLLATE on the
    // boolean operand is a no-op for the truth-value predicate). It must parse
    // (not error) and produce IsTruthValue(TRUE), not an equality comparison.
    let expr = parse_single_select_expr("SELECT 0.5 IS TRUE COLLATE NOCASE");
    match expr {
        vibesql_ast::Expression::IsTruthValue { truth_value, negated, .. } => {
            assert_eq!(truth_value, vibesql_ast::TruthValue::True);
            assert!(!negated);
        }
        other => panic!("Expected IsTruthValue(True), got: {:?}", other),
    }
}

#[test]
fn test_is_false_collate_parses_as_truth_value() {
    let expr = parse_single_select_expr("SELECT 0.0 IS FALSE COLLATE RTRIM");
    match expr {
        vibesql_ast::Expression::IsTruthValue { truth_value, negated, .. } => {
            assert_eq!(truth_value, vibesql_ast::TruthValue::False);
            assert!(!negated);
        }
        other => panic!("Expected IsTruthValue(False), got: {:?}", other),
    }
}

#[test]
fn test_is_not_true_collate_parses_as_negated_truth_value() {
    let expr = parse_single_select_expr("SELECT 1 IS NOT TRUE COLLATE BINARY");
    match expr {
        vibesql_ast::Expression::IsTruthValue { truth_value, negated, .. } => {
            assert_eq!(truth_value, vibesql_ast::TruthValue::True);
            assert!(negated);
        }
        other => panic!("Expected IsTruthValue(True, negated), got: {:?}", other),
    }
}

#[test]
fn test_is_column_collate_still_a_comparison() {
    // COLLATE on a non-literal right operand must be preserved as an ordinary
    // NULL-safe comparison (IsDistinctFrom), not folded into a truth predicate.
    let expr = parse_single_select_expr("SELECT a IS b COLLATE NOCASE FROM t");
    assert!(
        matches!(expr, vibesql_ast::Expression::IsDistinctFrom { .. }),
        "Expected IsDistinctFrom, got: {:?}",
        expr
    );
}
