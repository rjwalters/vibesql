use crate::SqlValue;

/// Represents the value type category for type inference and coercion
///
/// This enum is used to determine the result type of operations based
/// on the SQL mode's type system behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueType {
    /// Integer type (whole numbers)
    Integer,

    /// Exact decimal type (MySQL DECIMAL/NUMERIC)
    ///
    /// This represents exact decimal arithmetic with fixed precision.
    /// MySQL uses this for division results to preserve precision.
    Numeric,

    /// Approximate floating-point type (MySQL FLOAT/DOUBLE, SQLite REAL)
    ///
    /// This represents IEEE 754 floating-point numbers with potential
    /// rounding errors but better performance for large ranges.
    Float,

    /// Text/String type
    Text,

    /// Binary blob type
    Blob,

    /// NULL value
    Null,
}

/// Type system behavior trait for SQL modes
///
/// This trait defines how different SQL modes handle type-related operations
/// such as type inference, coercion, and result type determination.
///
/// # Examples
///
/// ```
/// use vibesql_types::{SqlMode, SqlValue, MySqlModeFlags, TypeBehavior, ValueType};
///
/// let mysql_mode = SqlMode::MySQL { flags: MySqlModeFlags::default() };
/// let sqlite_mode = SqlMode::SQLite;
///
/// // MySQL always returns Numeric for division
/// assert_eq!(
///     mysql_mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Integer(2)),
///     ValueType::Numeric
/// );
///
/// // SQLite returns Integer for int/int division
/// assert_eq!(
///     sqlite_mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Integer(2)),
///     ValueType::Integer
/// );
///
/// // SQLite returns Float when any operand is real
/// assert_eq!(
///     sqlite_mode.division_result_type(&SqlValue::Float(5.0), &SqlValue::Integer(2)),
///     ValueType::Float
/// );
/// ```
pub trait TypeBehavior {
    /// Whether this mode has a distinct DECIMAL/NUMERIC type
    ///
    /// - **MySQL**: true - has separate DECIMAL type for exact arithmetic
    /// - **SQLite**: false - only has INTEGER and REAL types
    fn has_decimal_type(&self) -> bool;

    /// Whether this mode uses dynamic typing (type affinity)
    ///
    /// Dynamic typing means values can change types during operations
    /// and type checking is lenient.
    ///
    /// - **MySQL**: false - uses static typing with defined column types
    /// - **SQLite**: true - uses type affinity system, types are suggestions
    fn uses_dynamic_typing(&self) -> bool;

    /// Get the result type for division operation
    ///
    /// This determines what type will result from dividing two values,
    /// which varies significantly between SQL modes.
    ///
    /// # Arguments
    ///
    /// * `left` - The left operand (dividend)
    /// * `right` - The right operand (divisor)
    ///
    /// # Returns
    ///
    /// The `ValueType` that should result from this division
    ///
    /// # MySQL Behavior
    ///
    /// By default, MySQL returns Numeric (exact decimal) for division to preserve
    /// precision:
    /// - `INTEGER / INTEGER → Numeric` (e.g., 5 / 2 = 2.5000)
    /// - `FLOAT / INTEGER → Numeric`
    /// - Any division → Numeric (unless NULL)
    ///
    /// When `sqlite_division_semantics` flag is set in `MySqlModeFlags`,
    /// MySQL mode will use SQLite's integer-preserving division instead.
    /// This is useful for compatibility with test suites that use MySQL
    /// syntax but expect SQLite division results.
    ///
    /// # SQLite Behavior
    ///
    /// SQLite uses type affinity rules:
    /// - `INTEGER / INTEGER → Integer` (truncated, e.g., 5 / 2 = 2)
    /// - `REAL / INTEGER → Float` (any real operand makes result float)
    /// - `INTEGER / REAL → Float`
    /// - `REAL / REAL → Float`
    fn division_result_type(&self, left: &SqlValue, right: &SqlValue) -> ValueType;

    /// Whether implicit type coercion is permissive
    ///
    /// Permissive coercion allows implicit conversions between types
    /// (e.g., string to number, number to string).
    ///
    /// - **MySQL**: true (unless STRICT_TRANS_TABLES or similar flags set)
    /// - **SQLite**: true (always permissive, uses type affinity)
    fn permissive_type_coercion(&self) -> bool;
}

/// Helper to check if a SqlValue is a floating-point type
fn is_float_value(value: &SqlValue) -> bool {
    matches!(
        value,
        SqlValue::Float(_) | SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Numeric(_) // Numeric is stored as f64, treated as float-like in SQLite
    )
}

impl TypeBehavior for super::SqlMode {
    fn has_decimal_type(&self) -> bool {
        match self {
            super::SqlMode::MySQL { .. } => true,
            super::SqlMode::SQLite => false,
        }
    }

    fn uses_dynamic_typing(&self) -> bool {
        match self {
            super::SqlMode::MySQL { .. } => false,
            super::SqlMode::SQLite => true,
        }
    }

    fn division_result_type(&self, left: &SqlValue, right: &SqlValue) -> ValueType {
        // Handle NULL operands
        if left.is_null() || right.is_null() {
            return ValueType::Null;
        }

        match self {
            super::SqlMode::MySQL { flags } => {
                // Check if SQLite division semantics are requested
                // This is used when MySQL syntax is needed (e.g., CAST...AS SIGNED)
                // but SQLite division behavior is required for compatibility
                if flags.sqlite_division_semantics {
                    // Use SQLite's integer-preserving division
                    if is_float_value(left) || is_float_value(right) {
                        ValueType::Float
                    } else {
                        ValueType::Integer
                    }
                } else {
                    // Standard MySQL: always returns Numeric (exact decimal) for division
                    // to preserve precision regardless of operand types
                    ValueType::Numeric
                }
            }
            super::SqlMode::SQLite => {
                // SQLite uses type affinity:
                // - If either operand is a real/float type, result is Float
                // - Otherwise (both integer), result is Integer (truncated)
                if is_float_value(left) || is_float_value(right) {
                    ValueType::Float
                } else {
                    ValueType::Integer
                }
            }
        }
    }

    fn permissive_type_coercion(&self) -> bool {
        match self {
            // MySQL is permissive by default (would need to check mode flags for strict mode)
            // For now, return true as default MySQL behavior
            super::SqlMode::MySQL { .. } => true,

            // SQLite is always permissive with type affinity
            super::SqlMode::SQLite => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqlMode;

    #[test]
    fn test_has_decimal_type() {
        assert!(SqlMode::MySQL { flags: Default::default() }.has_decimal_type());
        assert!(!SqlMode::SQLite.has_decimal_type());
        // Default is MySQL
        assert!(SqlMode::default().has_decimal_type());
    }

    #[test]
    fn test_uses_dynamic_typing() {
        assert!(!SqlMode::MySQL { flags: Default::default() }.uses_dynamic_typing());
        assert!(SqlMode::SQLite.uses_dynamic_typing());
        // Default is MySQL
        assert!(!SqlMode::default().uses_dynamic_typing());
    }

    #[test]
    fn test_permissive_type_coercion() {
        assert!(SqlMode::MySQL { flags: Default::default() }.permissive_type_coercion());
        assert!(SqlMode::SQLite.permissive_type_coercion());
    }

    #[test]
    fn test_mysql_division_result_type() {
        let mode = SqlMode::MySQL { flags: Default::default() };

        // MySQL always returns Numeric for division
        assert_eq!(
            mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Integer(2)),
            ValueType::Numeric
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Bigint(10), &SqlValue::Bigint(3)),
            ValueType::Numeric
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Float(5.5), &SqlValue::Integer(2)),
            ValueType::Numeric
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Integer(100), &SqlValue::Float(2.5)),
            ValueType::Numeric
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Numeric(7.5), &SqlValue::Numeric(2.5)),
            ValueType::Numeric
        );

        // NULL handling
        assert_eq!(
            mode.division_result_type(&SqlValue::Null, &SqlValue::Integer(2)),
            ValueType::Null
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Null),
            ValueType::Null
        );
    }

    #[test]
    fn test_mysql_with_sqlite_division_semantics() {
        use crate::MySqlModeFlags;

        // MySQL with sqlite_division_semantics flag uses SQLite's integer division
        let flags = MySqlModeFlags { sqlite_division_semantics: true, ..Default::default() };
        let mode = SqlMode::MySQL { flags };

        // int / int → Integer (truncated, like SQLite)
        assert_eq!(
            mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Integer(2)),
            ValueType::Integer
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Bigint(10), &SqlValue::Bigint(3)),
            ValueType::Integer
        );

        // any real operand → Float (like SQLite)
        assert_eq!(
            mode.division_result_type(&SqlValue::Float(5.0), &SqlValue::Integer(2)),
            ValueType::Float
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Integer(10), &SqlValue::Float(2.0)),
            ValueType::Float
        );

        // NULL handling still works
        assert_eq!(
            mode.division_result_type(&SqlValue::Null, &SqlValue::Integer(2)),
            ValueType::Null
        );
    }

    #[test]
    fn test_sqlite_division_result_type() {
        let mode = SqlMode::SQLite;

        // SQLite: int / int → Integer (truncated)
        assert_eq!(
            mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Integer(2)),
            ValueType::Integer
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Bigint(10), &SqlValue::Bigint(3)),
            ValueType::Integer
        );

        // SQLite: any real operand → Float
        assert_eq!(
            mode.division_result_type(&SqlValue::Float(5.0), &SqlValue::Integer(2)),
            ValueType::Float
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Integer(10), &SqlValue::Float(2.0)),
            ValueType::Float
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Real(7.5), &SqlValue::Real(2.5)),
            ValueType::Float
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Double(5.0), &SqlValue::Integer(2)),
            ValueType::Float
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Numeric(5.0), &SqlValue::Integer(2)),
            ValueType::Float
        );

        // NULL handling
        assert_eq!(
            mode.division_result_type(&SqlValue::Null, &SqlValue::Integer(2)),
            ValueType::Null
        );

        assert_eq!(
            mode.division_result_type(&SqlValue::Integer(5), &SqlValue::Null),
            ValueType::Null
        );
    }

    #[test]
    fn test_value_type_enum() {
        // Ensure all ValueType variants are distinct
        assert_ne!(ValueType::Integer, ValueType::Numeric);
        assert_ne!(ValueType::Numeric, ValueType::Float);
        assert_ne!(ValueType::Integer, ValueType::Float);
        assert_ne!(ValueType::Integer, ValueType::Text);
        assert_ne!(ValueType::Integer, ValueType::Blob);
        assert_ne!(ValueType::Integer, ValueType::Null);
    }
}
