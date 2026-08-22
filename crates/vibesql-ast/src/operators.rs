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

    // Bitwise
    BitwiseAnd, // &
    BitwiseOr,  // |
    LeftShift,  // <<
    RightShift, // >>

    // Vector distance operators (pgvector compatible)
    CosineDistance,       // <-> (1 - cosine_similarity)
    NegativeInnerProduct, // <#> (negative dot product for MIPS)
    L2Distance,           // <=> (Euclidean distance)

    // JSON operators (SQLite/PostgreSQL compatible)
    JsonExtract, // -> (extract JSON value)
    JsonExtractText, /* ->> (extract as text) */

                 /* Note: LIKE and IN are not simple binary operators. They are
                  * implemented as Expression variants in
                  * expression.rs due to their complex structure:
                  * - LIKE: Pattern matching with wildcards (%, _)
                  * - IN: Subquery or value list support */
}

impl BinaryOperator {
    /// Returns true if this operator is represented as a word (AND, OR, DIV)
    /// rather than a symbol (+, -, <, etc.)
    ///
    /// Word operators require spaces around them in SQL, while symbolic
    /// operators do not (though spaces are optional).
    pub fn is_word_operator(&self) -> bool {
        matches!(self, BinaryOperator::And | BinaryOperator::Or | BinaryOperator::IntegerDivide)
    }
}

/// Unary operators for SQL expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,        // NOT
    Minus,      // - (negation)
    Plus,       // + (unary plus)
    BitwiseNot, // ~ (bitwise NOT)
    IsNull,     // IS NULL
    IsNotNull,  // IS NOT NULL
}
