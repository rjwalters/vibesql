// ============================================================================
// Operator Serialization
// ============================================================================
//
// Handles serialization of binary and unary operators.

use vibesql_ast::{BinaryOperator, UnaryOperator};

impl_simple_enum_serialization!(
    BinaryOperator,
    write_binary_operator,
    read_binary_operator,
    "binary operator",
    {
        BinaryOperator::Plus => 0,
        BinaryOperator::Minus => 1,
        BinaryOperator::Multiply => 2,
        BinaryOperator::Divide => 3,
        BinaryOperator::IntegerDivide => 4,
        BinaryOperator::Modulo => 5,
        BinaryOperator::Equal => 10,
        BinaryOperator::NotEqual => 11,
        BinaryOperator::LessThan => 12,
        BinaryOperator::LessThanOrEqual => 13,
        BinaryOperator::GreaterThan => 14,
        BinaryOperator::GreaterThanOrEqual => 15,
        BinaryOperator::And => 20,
        BinaryOperator::Or => 21,
        BinaryOperator::Concat => 30,
        // Vector distance operators (pgvector compatible)
        BinaryOperator::CosineDistance => 40,
        BinaryOperator::NegativeInnerProduct => 41,
        BinaryOperator::L2Distance => 42,
    }
);

impl_simple_enum_serialization!(
    UnaryOperator,
    write_unary_operator,
    read_unary_operator,
    "unary operator",
    {
        UnaryOperator::Not => 0,
        UnaryOperator::Minus => 1,
        UnaryOperator::Plus => 2,
        UnaryOperator::IsNull => 3,
        UnaryOperator::IsNotNull => 4,
    }
);
