/// Operator behavior traits and types for different SQL modes
///
/// This module defines the traits and types that specify how operators
/// behave differently across SQL dialects (MySQL vs SQLite).
/// Behavior for division operations
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivisionBehavior {
    /// Return integer result (truncated) - SQLite
    ///
    /// Example: `83 / 6 = 13` (integer division)
    Integer,

    /// Return decimal/float result - MySQL
    ///
    /// Example: `83 / 6 = 13.8333` (floating-point division)
    Decimal,
}

/// String concatenation operator preference
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcatOperator {
    /// Use || operator (SQLite, PostgreSQL)
    PipePipe,

    /// Use CONCAT() function (MySQL default)
    Function,

    /// Both supported (MySQL with PIPES_AS_CONCAT mode)
    Both,
}

/// Trait for mode-specific operator behaviors
///
/// This trait defines how various operators behave in different SQL modes.
/// Each SQL mode (MySQL, SQLite) implements this trait to specify its
/// operator semantics.
///
/// ## Example
///
/// ```rust
/// use vibesql_types::{SqlMode, MySqlModeFlags, OperatorBehavior, DivisionBehavior};
///
/// let mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
/// assert_eq!(mode.integer_division_behavior(), DivisionBehavior::Decimal);
/// assert!(mode.supports_xor());
/// ```
#[allow(dead_code)]
pub trait OperatorBehavior {
    /// Division behavior for integer/integer operands
    ///
    /// Returns whether division of two integers should return:
    /// - `DivisionBehavior::Integer`: Truncated integer result (SQLite)
    /// - `DivisionBehavior::Decimal`: Floating-point result (MySQL)
    fn integer_division_behavior(&self) -> DivisionBehavior;

    /// Whether this mode supports the XOR operator
    ///
    /// - MySQL: supports `^` as XOR
    /// - SQLite: does not support XOR operator
    fn supports_xor(&self) -> bool;

    /// Whether this mode supports the DIV operator (integer division)
    ///
    /// - MySQL: supports `DIV` for integer division
    /// - SQLite: does not support `DIV` keyword
    fn supports_integer_div_operator(&self) -> bool;

    /// String concatenation operator preference
    ///
    /// Returns how this mode handles string concatenation:
    /// - `ConcatOperator::PipePipe`: Uses `||` operator
    /// - `ConcatOperator::Function`: Uses `CONCAT()` function
    /// - `ConcatOperator::Both`: Supports both approaches
    fn string_concat_operator(&self) -> ConcatOperator;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_division_behavior_variants() {
        // Test enum variants exist and can be compared
        assert_eq!(DivisionBehavior::Integer, DivisionBehavior::Integer);
        assert_eq!(DivisionBehavior::Decimal, DivisionBehavior::Decimal);
        assert_ne!(DivisionBehavior::Integer, DivisionBehavior::Decimal);
    }

    #[test]
    fn test_division_behavior_debug() {
        // Test Debug implementation
        let integer = DivisionBehavior::Integer;
        let decimal = DivisionBehavior::Decimal;

        assert_eq!(format!("{:?}", integer), "Integer");
        assert_eq!(format!("{:?}", decimal), "Decimal");
    }

    #[test]
    fn test_concat_operator_variants() {
        // Test enum variants exist and can be compared
        assert_eq!(ConcatOperator::PipePipe, ConcatOperator::PipePipe);
        assert_eq!(ConcatOperator::Function, ConcatOperator::Function);
        assert_eq!(ConcatOperator::Both, ConcatOperator::Both);
        assert_ne!(ConcatOperator::PipePipe, ConcatOperator::Function);
        assert_ne!(ConcatOperator::Function, ConcatOperator::Both);
    }

    #[test]
    fn test_concat_operator_debug() {
        // Test Debug implementation
        let pipe = ConcatOperator::PipePipe;
        let func = ConcatOperator::Function;
        let both = ConcatOperator::Both;

        assert_eq!(format!("{:?}", pipe), "PipePipe");
        assert_eq!(format!("{:?}", func), "Function");
        assert_eq!(format!("{:?}", both), "Both");
    }
}
