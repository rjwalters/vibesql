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

// ========================================================================
// `||` (Concat) precedence tests (issue #5839)
//
// Per the SQLite grammar `||`, `->`, and `->>` share the tightest binary
// tier, so `||` binds TIGHTER than `* / %` and `+ -`. The canonical
// reproducer is `SELECT 22||45*66` which must parse as `(22||45)*66` and
// evaluate to `"2245"*66 = 148170` (not `22||(45*66) = 222970`).
// ========================================================================

#[test]
fn test_concat_binds_tighter_than_multiply() {
    // 22 || 45 * 66 parses as (22 || 45) * 66
    let expr = parse_select_expr("SELECT 22 || 45 * 66;");
    let (op, left, _) = unwrap_binop(expr, "outer *");
    assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
    let (inner_op, _, _) = unwrap_binop(left, "inner ||");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::Concat);
}

#[test]
fn test_concat_binds_tighter_than_multiply_on_rhs() {
    // 22 * 45 || 66 parses as 22 * (45 || 66)
    let expr = parse_select_expr("SELECT 22 * 45 || 66;");
    let (op, _, right) = unwrap_binop(expr, "outer *");
    assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
    let (inner_op, _, _) = unwrap_binop(right, "inner ||");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::Concat);
}

#[test]
fn test_concat_binds_tighter_than_divide_and_modulo() {
    // 22 || 45 / 3 parses as (22 || 45) / 3
    let expr = parse_select_expr("SELECT 22 || 45 / 3;");
    let (op, left, _) = unwrap_binop(expr, "outer /");
    assert_eq!(op, vibesql_ast::BinaryOperator::Divide);
    let (inner_op, _, _) = unwrap_binop(left, "inner ||");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::Concat);

    // 22 || 45 % 7 parses as (22 || 45) % 7
    let expr = parse_select_expr("SELECT 22 || 45 % 7;");
    let (op, left, _) = unwrap_binop(expr, "outer %");
    assert_eq!(op, vibesql_ast::BinaryOperator::Modulo);
    let (inner_op, _, _) = unwrap_binop(left, "inner ||");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::Concat);
}

#[test]
fn test_concat_binds_tighter_than_plus() {
    // 22 || 45 + 66 parses as (22 || 45) + 66
    let expr = parse_select_expr("SELECT 22 || 45 + 66;");
    let (op, left, _) = unwrap_binop(expr, "outer +");
    assert_eq!(op, vibesql_ast::BinaryOperator::Plus);
    let (inner_op, _, _) = unwrap_binop(left, "inner ||");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::Concat);
}

#[test]
fn test_concat_chain_then_multiply() {
    // 1 || 2 || 3 * 4 parses as ((1 || 2 || 3) * 4); the left-associative
    // concat chain is the tighter operand of the multiply.
    let expr = parse_select_expr("SELECT 1 || 2 || 3 * 4;");
    let (op, left, _) = unwrap_binop(expr, "outer *");
    assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
    let (concat_op, concat_left, _) = unwrap_binop(left, "outer ||");
    assert_eq!(concat_op, vibesql_ast::BinaryOperator::Concat);
    // Left of the outer || is itself a || (left-associative chain).
    let (inner_op, _, _) = unwrap_binop(concat_left, "inner ||");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::Concat);
}

#[test]
fn test_concat_and_json_share_tier_left_associative() {
    // a -> 'k' || 'x' parses as (a -> 'k') || 'x' — `||` and `->` share one
    // left-associative tier.
    let expr = parse_select_expr("SELECT a -> 'k' || 'x';");
    let (op, left, _) = unwrap_binop(expr, "outer ||");
    assert_eq!(op, vibesql_ast::BinaryOperator::Concat);
    let (inner_op, _, _) = unwrap_binop(left, "inner ->");
    assert_eq!(inner_op, vibesql_ast::BinaryOperator::JsonExtract);
}
