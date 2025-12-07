use vibesql_ast::*;
use vibesql_types::{SqlValue, StringValue};

// ============================================================================
// BinaryOperator Tests - All SQL operators
// ============================================================================

#[test]
fn test_arithmetic_operators() {
    let _plus = BinaryOperator::Plus;
    let _minus = BinaryOperator::Minus;
    let _multiply = BinaryOperator::Multiply;
    let _divide = BinaryOperator::Divide;
    // If these compile, the operators exist
}

#[test]
fn test_comparison_operators() {
    let _eq = BinaryOperator::Equal;
    let _ne = BinaryOperator::NotEqual;
    let _lt = BinaryOperator::LessThan;
    let _le = BinaryOperator::LessThanOrEqual;
    let _gt = BinaryOperator::GreaterThan;
    let _ge = BinaryOperator::GreaterThanOrEqual;
    // If these compile, the operators exist
}

#[test]
fn test_logical_operators() {
    let _and = BinaryOperator::And;
    let _or = BinaryOperator::Or;
    // If these compile, the operators exist
}

#[test]
fn test_modulo_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Modulo,
        left: Box::new(Expression::Literal(SqlValue::Integer(10))),
        right: Box::new(Expression::Literal(SqlValue::Integer(3))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Modulo, .. } => {} // Success
        _ => panic!("Expected modulo operation"),
    }
}

#[test]
fn test_concat_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Concat,
        left: Box::new(Expression::Literal(SqlValue::Varchar(StringValue::from("Hello")))),
        right: Box::new(Expression::Literal(SqlValue::Varchar(StringValue::from("World")))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Concat, .. } => {} // Success
        _ => panic!("Expected concat operation"),
    }
}

#[test]
fn test_not_equal_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::NotEqual,
        left: Box::new(Expression::ColumnRef { table: None, column: "status".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Varchar(StringValue::from("active")))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::NotEqual, .. } => {} // Success
        _ => panic!("Expected not equal operation"),
    }
}

#[test]
fn test_less_than_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::LessThan,
        left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(18))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::LessThan, .. } => {} // Success
        _ => panic!("Expected less than operation"),
    }
}

#[test]
fn test_greater_than_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::GreaterThan,
        left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(50000))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::GreaterThan, .. } => {} // Success
        _ => panic!("Expected greater than operation"),
    }
}

#[test]
fn test_and_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        right: Box::new(Expression::Literal(SqlValue::Boolean(false))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, .. } => {} // Success
        _ => panic!("Expected AND operation"),
    }
}

#[test]
fn test_or_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        right: Box::new(Expression::Literal(SqlValue::Boolean(false))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Or, .. } => {} // Success
        _ => panic!("Expected OR operation"),
    }
}

// ============================================================================
// UnaryOperator Tests
// ============================================================================

#[test]
fn test_unary_not_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expression::Literal(SqlValue::Boolean(true))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Not, .. } => {} // Success
        _ => panic!("Expected NOT operation"),
    }
}

#[test]
fn test_unary_minus_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Minus,
        expr: Box::new(Expression::Literal(SqlValue::Integer(42))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Minus, .. } => {} // Success
        _ => panic!("Expected unary minus operation"),
    }
}

#[test]
fn test_unary_plus_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Plus,
        expr: Box::new(Expression::Literal(SqlValue::Integer(42))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Plus, .. } => {} // Success
        _ => panic!("Expected unary plus operation"),
    }
}

// ============================================================================
// Vector Distance Operator Tests (pgvector compatible)
// ============================================================================

#[test]
fn test_cosine_distance_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::CosineDistance,
        left: Box::new(Expression::Literal(SqlValue::Vector(vec![1.0, 0.0, 0.0]))),
        right: Box::new(Expression::Literal(SqlValue::Vector(vec![0.0, 1.0, 0.0]))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::CosineDistance, .. } => {} // Success
        _ => panic!("Expected cosine distance operation"),
    }
}

#[test]
fn test_negative_inner_product_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::NegativeInnerProduct,
        left: Box::new(Expression::Literal(SqlValue::Vector(vec![1.0, 2.0, 3.0]))),
        right: Box::new(Expression::Literal(SqlValue::Vector(vec![4.0, 5.0, 6.0]))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::NegativeInnerProduct, .. } => {} // Success
        _ => panic!("Expected negative inner product operation"),
    }
}

#[test]
fn test_l2_distance_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::L2Distance,
        left: Box::new(Expression::Literal(SqlValue::Vector(vec![0.0, 0.0]))),
        right: Box::new(Expression::Literal(SqlValue::Vector(vec![3.0, 4.0]))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::L2Distance, .. } => {} // Success
        _ => panic!("Expected L2 distance operation"),
    }
}

#[test]
fn test_vector_distance_operators_exist() {
    let _cosine = BinaryOperator::CosineDistance;
    let _neg_ip = BinaryOperator::NegativeInnerProduct;
    let _l2 = BinaryOperator::L2Distance;
    // If these compile, the operators exist
}
