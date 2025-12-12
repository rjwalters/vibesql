//! Tests for ASSERTION DDL statements (CREATE ASSERTION, DROP ASSERTION)

use crate::Parser;

#[test]
fn test_create_assertion_simple() {
    let sql = "CREATE ASSERTION valid_balance CHECK (balance >= 0)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateAssertion(stmt)) = result {
        assert_eq!(stmt.assertion_name, "valid_balance");
        // Check that we have a condition (basic sanity check)
        assert!(matches!(*stmt.check_condition, vibesql_ast::Expression::BinaryOp { .. }));
    } else {
        panic!("Expected CreateAssertion statement");
    }
}

#[test]
fn test_create_assertion_with_not_exists() {
    let sql = "CREATE ASSERTION valid_balance CHECK (NOT EXISTS (SELECT * FROM accounts WHERE balance < 0))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateAssertion(stmt)) = result {
        assert_eq!(stmt.assertion_name, "valid_balance");
        // Check that we have a NOT EXISTS expression (could be UnaryOp or Exists depending on
        // implementation) Just verify it parsed successfully - the exact expression type
        // may vary
    } else {
        panic!("Expected CreateAssertion statement");
    }
}

#[test]
fn test_create_assertion_with_complex_condition() {
    let sql = "CREATE ASSERTION emp_dept_consistency CHECK ((SELECT COUNT(*) FROM employees) = (SELECT SUM(emp_count) FROM departments))";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateAssertion(stmt)) = result {
        assert_eq!(stmt.assertion_name, "emp_dept_consistency");
        // Check that we have a binary comparison between two subqueries
        assert!(matches!(*stmt.check_condition, vibesql_ast::Expression::BinaryOp { .. }));
    } else {
        panic!("Expected CreateAssertion statement");
    }
}

#[test]
fn test_create_assertion_with_and_condition() {
    let sql = "CREATE ASSERTION age_range CHECK (age >= 18 AND age <= 65)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateAssertion(stmt)) = result {
        assert_eq!(stmt.assertion_name, "age_range");
        // Check that we have an AND condition
        assert!(matches!(*stmt.check_condition, vibesql_ast::Expression::BinaryOp { .. }));
    } else {
        panic!("Expected CreateAssertion statement");
    }
}

#[test]
fn test_drop_assertion_simple() {
    let sql = "DROP ASSERTION valid_balance";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropAssertion(stmt)) = result {
        assert_eq!(stmt.assertion_name, "valid_balance");
        assert!(!stmt.cascade); // Default is RESTRICT (false)
    } else {
        panic!("Expected DropAssertion statement");
    }
}

#[test]
fn test_drop_assertion_cascade() {
    let sql = "DROP ASSERTION valid_balance CASCADE";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropAssertion(stmt)) = result {
        assert_eq!(stmt.assertion_name, "valid_balance");
        assert!(stmt.cascade);
    } else {
        panic!("Expected DropAssertion statement");
    }
}

#[test]
fn test_drop_assertion_restrict() {
    let sql = "DROP ASSERTION valid_balance RESTRICT";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropAssertion(stmt)) = result {
        assert_eq!(stmt.assertion_name, "valid_balance");
        assert!(!stmt.cascade); // RESTRICT means cascade is false
    } else {
        panic!("Expected DropAssertion statement");
    }
}
