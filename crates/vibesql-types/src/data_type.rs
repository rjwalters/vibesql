//! SQL Data Type definitions

use crate::temporal::IntervalField;

/// SQL:1999 Data Types
///
/// Represents the type of a column or expression in SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    // Exact numeric types
    Integer,
    Smallint,
    Bigint,
    Unsigned, // 64-bit unsigned integer (MySQL compatibility)
    Numeric { precision: u8, scale: u8 },
    Decimal { precision: u8, scale: u8 },

    // Approximate numeric types
    Float { precision: u8 }, // SQL:1999 FLOAT(p), default 53 (double precision)
    Real,
    DoublePrecision,

    // Character string types
    Character { length: usize },
    Varchar { max_length: Option<usize> }, // None = default length (255)
    CharacterLargeObject,                  // CLOB
    Name,                                  /* NAME type for SQL identifiers (SQL:1999), maps to
                                            * VARCHAR(128) */

    // Boolean type (SQL:1999)
    Boolean,

    // Date/time types
    Date,
    Time { with_timezone: bool },
    Timestamp { with_timezone: bool },

    // Interval types
    // Single field: INTERVAL YEAR, INTERVAL MONTH, etc. (end_field is None)
    // Multi-field: INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, etc.
    Interval { start_field: IntervalField, end_field: Option<IntervalField> },

    // Binary types
    BinaryLargeObject,             // BLOB
    Bit { length: Option<usize> }, // BIT or BIT(n), MySQL compatibility, default length is 1

    // Vector types (for AI/ML workloads)
    Vector { dimensions: u32 },

    // User-defined types (SQL:1999)
    UserDefined { type_name: String },

    // Special type for NULL
    Null,
}

impl DataType {
    /// Returns the type precedence for SQL:1999 type coercion
    ///
    /// Higher precedence types are preferred in type coercion.
    /// Based on SQL:1999 Section 9.5 (Result of data type combinations).
    ///
    /// Precedence order (highest to lowest):
    /// 1. Character strings (VARCHAR, CHAR, CLOB, NAME)
    /// 2. Approximate numerics (DOUBLE PRECISION, REAL, FLOAT)
    /// 3. Exact numerics with scale (DECIMAL, NUMERIC)
    /// 4. Exact numerics without scale (BIGINT > INTEGER > SMALLINT, UNSIGNED)
    /// 5. Boolean
    /// 6. Temporal types (TIMESTAMP > TIME > DATE)
    /// 7. Interval types
    /// 8. Binary types (BLOB)
    fn type_precedence(&self) -> u8 {
        match self {
            // NULL has lowest precedence - coerces to anything
            DataType::Null => 0,

            // Binary types
            DataType::BinaryLargeObject => 10,
            DataType::Bit { .. } => 11, // BIT type, slightly higher than BLOB

            // Interval types
            DataType::Interval { .. } => 20,

            // Temporal types (ordered by precision)
            DataType::Date => 30,
            DataType::Time { .. } => 31,
            DataType::Timestamp { .. } => 32,

            // Boolean
            DataType::Boolean => 40,

            // Exact numerics without scale (ordered by size)
            DataType::Smallint => 50,
            DataType::Integer => 51,
            DataType::Bigint => 52,
            DataType::Unsigned => 52, // Same as BIGINT (both 64-bit)

            // Exact numerics with scale
            DataType::Decimal { .. } => 60,
            DataType::Numeric { .. } => 60,

            // Approximate numerics (ordered by precision)
            DataType::Real => 70,
            DataType::Float { .. } => 71,
            DataType::DoublePrecision => 72,

            // Character strings (highest precedence)
            DataType::Character { .. } => 80,
            DataType::Varchar { .. } => 81,
            DataType::Name => 81, // NAME is equivalent to VARCHAR
            DataType::CharacterLargeObject => 82,

            // Vector types (specialized for AI/ML operations, don't coerce with other types)
            DataType::Vector { .. } => 65,

            // User-defined types don't participate in implicit coercion
            DataType::UserDefined { .. } => 255,
        }
    }

    /// Determines if implicit type coercion is possible between two types
    ///
    /// Returns true if SQL:1999 allows implicit conversion between the types.
    /// This is more permissive than exact type equality.
    fn can_implicitly_coerce(&self, other: &DataType) -> bool {
        // NULL can coerce to/from anything
        if matches!(self, DataType::Null) || matches!(other, DataType::Null) {
            return true;
        }

        match (self, other) {
            // Same types can always coerce
            (a, b) if a == b => true,

            // DECIMAL/NUMERIC with different precision/scale can coerce
            (DataType::Decimal { .. }, DataType::Decimal { .. }) => true,
            (DataType::Numeric { .. }, DataType::Numeric { .. }) => true,

            // VARCHAR with different lengths can coerce
            (DataType::Varchar { .. }, DataType::Varchar { .. }) => true,

            // BIT types with different lengths can coerce
            (DataType::Bit { .. }, DataType::Bit { .. }) => true,

            // BIT can coerce to/from integer types (numeric interpretation)
            (
                DataType::Bit { .. },
                DataType::Integer | DataType::Bigint | DataType::Unsigned | DataType::Smallint,
            ) => true,
            (
                DataType::Integer | DataType::Bigint | DataType::Unsigned | DataType::Smallint,
                DataType::Bit { .. },
            ) => true,

            // BIT can coerce to/from binary types
            (DataType::Bit { .. }, DataType::BinaryLargeObject) => true,
            (DataType::BinaryLargeObject, DataType::Bit { .. }) => true,

            // Numeric types can coerce among themselves
            (DataType::Smallint, DataType::Integer | DataType::Bigint | DataType::Unsigned) => true,
            (DataType::Integer, DataType::Bigint | DataType::Unsigned) => true,
            (DataType::Bigint, DataType::Unsigned) => true,
            (DataType::Unsigned, DataType::Bigint) => true,
            (
                DataType::Integer | DataType::Bigint | DataType::Unsigned | DataType::Smallint,
                DataType::Decimal { .. } | DataType::Numeric { .. },
            ) => true,
            (
                DataType::Decimal { .. } | DataType::Numeric { .. },
                DataType::Real | DataType::Float { .. } | DataType::DoublePrecision,
            ) => true,
            (
                DataType::Integer | DataType::Bigint | DataType::Unsigned | DataType::Smallint,
                DataType::Real | DataType::Float { .. } | DataType::DoublePrecision,
            ) => true,

            // Numeric types are bidirectionally coercible (widening and narrowing allowed)
            (DataType::Bigint | DataType::Unsigned, DataType::Integer | DataType::Smallint) => true,
            (
                DataType::Real | DataType::Float { .. } | DataType::DoublePrecision,
                DataType::Decimal { .. } | DataType::Numeric { .. },
            ) => true,
            (
                DataType::Real | DataType::Float { .. } | DataType::DoublePrecision,
                DataType::Integer | DataType::Bigint | DataType::Unsigned | DataType::Smallint,
            ) => true,
            (
                DataType::Decimal { .. } | DataType::Numeric { .. },
                DataType::Integer | DataType::Bigint | DataType::Unsigned | DataType::Smallint,
            ) => true,

            // Character string types can coerce among themselves
            (
                DataType::Character { .. },
                DataType::Varchar { .. } | DataType::Name | DataType::CharacterLargeObject,
            ) => true,
            (
                DataType::Varchar { .. },
                DataType::Character { .. } | DataType::Name | DataType::CharacterLargeObject,
            ) => true,
            (
                DataType::Name,
                DataType::Character { .. }
                | DataType::Varchar { .. }
                | DataType::CharacterLargeObject,
            ) => true,
            (
                DataType::CharacterLargeObject,
                DataType::Character { .. } | DataType::Varchar { .. } | DataType::Name,
            ) => true,

            // Temporal types can coerce among themselves
            (DataType::Date, DataType::Timestamp { .. }) => true,
            (DataType::Time { .. }, DataType::Timestamp { .. }) => true,
            (DataType::Timestamp { .. }, DataType::Date | DataType::Time { .. }) => true,

            // Intervals with different fields can coerce if compatible
            (DataType::Interval { .. }, DataType::Interval { .. }) => true,

            // Vectors with matching dimensions can coerce
            (DataType::Vector { dimensions: d1 }, DataType::Vector { dimensions: d2 }) => d1 == d2,

            // User-defined types only coerce to themselves (checked above with ==)
            // All other combinations cannot coerce
            _ => false,
        }
    }

    /// Coerces two types to their common type according to SQL:1999 rules
    ///
    /// Returns the result type that should be used when combining values
    /// of the two input types, or None if no implicit coercion exists.
    ///
    /// Based on SQL:1999 Section 9.5 (Result of data type combinations).
    pub fn coerce_to_common(&self, other: &DataType) -> Option<DataType> {
        // NULL coerces to the other type
        if matches!(self, DataType::Null) {
            return Some(other.clone());
        }
        if matches!(other, DataType::Null) {
            return Some(self.clone());
        }

        // Check if coercion is possible
        if !self.can_implicitly_coerce(other) {
            return None;
        }

        // Same types return themselves
        if self == other {
            return Some(self.clone());
        }

        // Special handling for types with precision/scale parameters
        match (self, other) {
            // For DECIMAL/NUMERIC, combine precision and scale appropriately
            (
                DataType::Decimal { precision: p1, scale: s1 },
                DataType::Decimal { precision: p2, scale: s2 },
            ) => {
                let max_scale = (*s1).max(*s2);
                let max_precision = (*p1).max(*p2);
                Some(DataType::Decimal { precision: max_precision, scale: max_scale })
            }
            (
                DataType::Numeric { precision: p1, scale: s1 },
                DataType::Numeric { precision: p2, scale: s2 },
            ) => {
                let max_scale = (*s1).max(*s2);
                let max_precision = (*p1).max(*p2);
                Some(DataType::Numeric { precision: max_precision, scale: max_scale })
            }

            // For VARCHAR, use the larger length (or None for unlimited)
            (DataType::Varchar { max_length: l1 }, DataType::Varchar { max_length: l2 }) => {
                let max_length = match (l1, l2) {
                    (None, _) | (_, None) => None,
                    (Some(a), Some(b)) => Some((*a).max(*b)),
                };
                Some(DataType::Varchar { max_length })
            }

            // For BIT, use the larger length (or None for unlimited)
            (DataType::Bit { length: l1 }, DataType::Bit { length: l2 }) => {
                let max_length = match (l1, l2) {
                    (None, _) | (_, None) => None,
                    (Some(a), Some(b)) => Some((*a).max(*b)),
                };
                Some(DataType::Bit { length: max_length })
            }

            // Vectors with matching dimensions coerce to themselves
            (DataType::Vector { dimensions: d1 }, DataType::Vector { dimensions: d2 })
                if d1 == d2 =>
            {
                Some(self.clone())
            }

            // For all other cases, choose the type with higher precedence
            _ => {
                let result = if self.type_precedence() > other.type_precedence() {
                    self.clone()
                } else {
                    other.clone()
                };
                Some(result)
            }
        }
    }

    /// Check if this type is compatible with another type for operations
    ///
    /// This now uses SQL:1999 type coercion rules. Types are compatible if
    /// they can be implicitly coerced to a common type.
    pub fn is_compatible_with(&self, other: &DataType) -> bool {
        self.coerce_to_common(other).is_some()
    }
}
