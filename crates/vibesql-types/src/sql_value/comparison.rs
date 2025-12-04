//! Comparison implementations for SqlValue

use std::cmp::Ordering;

use crate::sql_value::SqlValue;

/// PartialEq implementation for SqlValue
///
/// For DISTINCT operations and BTreeMap keys, we need Eq semantics where:
/// - NULL == NULL (for grouping purposes, unlike SQL comparison)
/// - NaN == NaN (for grouping purposes, unlike IEEE 754)
/// - All other values use standard equality
///
/// This is consistent with the Ord implementation and satisfies BTreeMap requirements.
impl PartialEq for SqlValue {
    fn eq(&self, other: &Self) -> bool {
        use SqlValue::*;
        match (self, other) {
            // NULL equality (for grouping/DISTINCT)
            (Null, Null) => true,
            (Null, _) | (_, Null) => false,

            // Integer types
            (Integer(a), Integer(b)) => a == b,
            (Smallint(a), Smallint(b)) => a == b,
            (Bigint(a), Bigint(b)) => a == b,
            (Unsigned(a), Unsigned(b)) => a == b,

            // Floating point (NaN-aware: NaN == NaN for grouping)
            (Float(a), Float(b)) => {
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            }
            (Real(a), Real(b)) => {
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            }
            (Double(a), Double(b)) | (Numeric(a), Numeric(b)) => {
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            }

            // String types (Character and Varchar are interoperable)
            (Character(a), Character(b)) => a == b,
            (Varchar(a), Varchar(b)) => a == b,
            (Character(a), Varchar(b)) | (Varchar(a), Character(b)) => a == b,

            // Boolean
            (Boolean(a), Boolean(b)) => a == b,

            // Date/Time types
            (Date(a), Date(b)) => a == b,
            (Time(a), Time(b)) => a == b,
            (Timestamp(a), Timestamp(b)) => a == b,

            // Interval
            (Interval(a), Interval(b)) => a == b,

            // Type mismatch - not equal
            _ => false,
        }
    }
}

/// PartialOrd implementation for SQL value comparison
///
/// Implements SQL:1999 comparison semantics:
/// - NULL comparisons return None (SQL UNKNOWN)
/// - Type mismatches return None (incomparable)
/// - NaN in floating point returns None (IEEE 754 semantics)
/// - All other comparisons follow Rust's natural ordering
///
/// Note: This intentionally differs from Ord::cmp which provides total ordering
/// for sorting operations. PartialOrd represents SQL comparison semantics where
/// NULL and type mismatches are incomparable.
#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for SqlValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use SqlValue::*;
        match (self, other) {
            // NULL comparisons return None (SQL UNKNOWN semantics)
            (Null, _) | (_, Null) => None,

            // Integer types
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (Smallint(a), Smallint(b)) => a.partial_cmp(b),
            (Bigint(a), Bigint(b)) => a.partial_cmp(b),
            (Unsigned(a), Unsigned(b)) => a.partial_cmp(b),

            // Floating point (handles NaN properly via IEEE 754)
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Real(a), Real(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),

            // String types (lexicographic comparison, Character and Varchar are interoperable)
            (Character(a), Character(b)) => a.partial_cmp(b),
            (Varchar(a), Varchar(b)) => a.partial_cmp(b),
            (Character(a), Varchar(b)) | (Varchar(a), Character(b)) => a.partial_cmp(b),

            // Numeric (f64 - direct comparison)
            (Numeric(a), Numeric(b)) => a.partial_cmp(b),

            // Boolean (false < true in SQL)
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),

            // Date/Time types with proper temporal comparison
            (Date(a), Date(b)) => a.partial_cmp(b),
            (Time(a), Time(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),

            // Interval type comparison
            (Interval(a), Interval(b)) => a.partial_cmp(b),

            // Type mismatch - incomparable (SQL:1999 behavior)
            _ => None,
        }
    }
}

/// Eq implementation for SqlValue
///
/// For DISTINCT operations, we need Eq semantics where:
/// - NULL == NULL (for grouping purposes, unlike SQL comparison)
/// - NaN == NaN (for grouping purposes, unlike IEEE 754)
/// - All other values use standard equality
impl Eq for SqlValue {}

/// Ord implementation for SqlValue
///
/// Required for BTreeMap usage in indexes for efficient range queries.
///
/// For index storage and sorting purposes, we define a total ordering where:
/// - NULL is treated as "less than" all other values (NULLS FIRST semantics)
/// - NaNs are treated as "greater than" all other floats for consistency
/// - Type mismatches use a type-based ordering (e.g., integers < floats < strings)
/// - Within each type, use natural ordering
///
/// Note: This differs from SQL comparison semantics (which uses three-valued logic)
/// but is necessary for BTreeMap keys which require total ordering.
impl Ord for SqlValue {
    fn cmp(&self, other: &Self) -> Ordering {
        use SqlValue::*;

        // NULL ordering: NULL is less than everything else
        match (self, other) {
            (Null, Null) => return Ordering::Equal,
            (Null, _) => return Ordering::Less,
            (_, Null) => return Ordering::Greater,
            _ => {}
        }

        // Try partial comparison first
        if let Some(ordering) = self.partial_cmp(other) {
            return ordering;
        }

        // Handle NaN cases and type mismatches
        // For floats containing NaN: treat NaN as greater than all other floats
        match (self, other) {
            // Float NaN handling
            (Float(a), Float(b)) => {
                if a.is_nan() && b.is_nan() {
                    Ordering::Equal
                } else if a.is_nan() {
                    Ordering::Greater
                } else {
                    Ordering::Less // b must be NaN
                }
            }
            (Real(a), Real(b)) => {
                if a.is_nan() && b.is_nan() {
                    Ordering::Equal
                } else if a.is_nan() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Double(a), Double(b)) | (Numeric(a), Numeric(b)) => {
                if a.is_nan() && b.is_nan() {
                    Ordering::Equal
                } else if a.is_nan() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }

            // Type mismatch - use type tag ordering
            // This provides a stable sort order across different types
            _ => {
                fn type_tag(val: &SqlValue) -> u8 {
                    match val {
                        Integer(_) => 1,
                        Smallint(_) => 2,
                        Bigint(_) => 3,
                        Unsigned(_) => 4,
                        Numeric(_) => 5,
                        Float(_) => 6,
                        Real(_) => 7,
                        Double(_) => 8,
                        Character(_) => 9,
                        Varchar(_) => 10,
                        Boolean(_) => 11,
                        Date(_) => 12,
                        Time(_) => 13,
                        Timestamp(_) => 14,
                        Interval(_) => 15,
                        Vector(_) => 16,
                        Null => 0, // Already handled above, but for completeness
                    }
                }
                type_tag(self).cmp(&type_tag(other))
            }
        }
    }
}
