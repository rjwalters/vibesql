use super::*;

// ========================================================================
// JSON `->` / `->>` operator parsing tests (issue #5827)
//
// Per the SQLite grammar the JSON extraction operators bind one tier
// tighter than `* / % DIV` and one tier looser than unary operators, and
// are left-associative: `a -> b -> c` parses as `(a -> b) -> c`.
// ========================================================================

/// Parse a `SELECT <expr>;` statement and return the first select-list expression.
fn parse_select_expr(sql: &str) -> vibesql_ast::Expression {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("should parse: {}: {:?}", sql, e));
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            return expr.clone();
        }
    }
    panic!("expected SELECT with expression select list: {}", sql);
}

fn unwrap_binop(
    expr: vibesql_ast::Expression,
    context: &str,
) -> (vibesql_ast::BinaryOperator, vibesql_ast::Expression, vibesql_ast::Expression) {
    if let vibesql_ast::Expression::BinaryOp { op, left, right } = expr {
        (op, *left, *right)
    } else {
        panic!("{}: expected BinaryOp node, got {:?}", context, expr);
    }
}

#[test]
fn test_json_extract_operator_parses() {
    let expr = parse_select_expr("SELECT c -> '$.x' FROM t;");
    let (op, _, _) = unwrap_binop(expr, "->");
    assert_eq!(op, vibesql_ast::BinaryOperator::JsonExtract);
}

#[test]
fn test_json_extract_text_operator_parses() {
    let expr = parse_select_expr("SELECT c ->> '$.x' FROM t;");
    let (op, _, _) = unwrap_binop(expr, "->>");
    assert_eq!(op, vibesql_ast::BinaryOperator::JsonExtractText);
}

#[test]
fn test_json_operator_integer_shorthand() {
    // Integer RHS is array-index shorthand and must parse.
    let expr = parse_select_expr("SELECT c ->> 2 FROM t;");
    let (op, _, right) = unwrap_binop(expr, "->> 2");
    assert_eq!(op, vibesql_ast::BinaryOperator::JsonExtractText);
    matches!(right, vibesql_ast::Expression::Literal(_));
}

#[test]
fn test_json_operator_left_associative() {
    // a -> b -> c parses as (a -> b) -> c
    let expr = parse_select_expr("SELECT a -> 'b' -> 'c';");
    let (op, left, _) = unwrap_binop(expr, "outer ->");
    assert_eq!(op, vibesql_ast::BinaryOperator::JsonExtract);
    let (inner_op, _, _) = unwrap_binop(left, "inner ->");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::JsonExtract);
}

#[test]
fn test_json_operator_binds_tighter_than_multiply() {
    // a -> b * c parses as (a -> b) * c
    let expr = parse_select_expr("SELECT a -> 'b' * c;");
    let (op, left, _) = unwrap_binop(expr, "outer *");
    assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
    let (inner_op, _, _) = unwrap_binop(left, "inner ->");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::JsonExtract);
}

#[test]
fn test_json_operator_looser_than_comparison() {
    // a -> b = c parses as (a -> b) = c
    let expr = parse_select_expr("SELECT a -> 'b' = c;");
    let (op, left, _) = unwrap_binop(expr, "outer =");
    assert_eq!(op, vibesql_ast::BinaryOperator::Equal);
    let (inner_op, _, _) = unwrap_binop(left, "inner ->");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::JsonExtract);
}
