//! Operator enums for SQL expressions

/// Binary operators for SQL expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    IntegerDivide, // DIV (MySQL-specific integer division)
    Modulo,

    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Logical
    And,
    Or,

    // String
    Concat, /* || */

    // Vector distance operators (pgvector compatible)
    CosineDistance,       // <-> (1 - cosine_similarity)
    NegativeInnerProduct, // <#> (negative dot product for MIPS)
    L2Distance,           // <=> (Euclidean distance)

                          // Note: LIKE and IN are not simple binary operators. They are implemented
                          // as Expression variants in expression.rs due to their complex structure:
                          // - LIKE: Pattern matching with wildcards (%, _)
                          // - IN: Subquery or value list support
}

/// Unary operators for SQL expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,       // NOT
    Minus,     // - (negation)
    Plus,      // + (unary plus)
    IsNull,    // IS NULL
    IsNotNull, // IS NOT NULL
}
