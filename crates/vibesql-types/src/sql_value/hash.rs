//! Hash implementation for SqlValue

use std::hash::{Hash, Hasher};

use crate::sql_value::SqlValue;

/// Hash implementation for SqlValue
///
/// Custom implementation to handle floating-point values correctly:
/// - NaN values are treated as equal (hash to same value)
/// - Uses to_bits() for floats to ensure consistent hashing
/// - NULL hashes to a specific value
impl Hash for SqlValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use SqlValue::*;

        // Hash discriminant first to distinguish variants
        std::mem::discriminant(self).hash(state);

        match self {
            Integer(i) => i.hash(state),
            Smallint(i) => i.hash(state),
            Bigint(i) => i.hash(state),
            Unsigned(u) => u.hash(state),
            Numeric(f) => {
                if f.is_nan() {
                    f64::NAN.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
            // For floats, use to_bits() to get consistent hash for NaN
            Float(f) => {
                if f.is_nan() {
                    // All NaN values hash the same
                    f32::NAN.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
            Real(f) => {
                if f.is_nan() {
                    f32::NAN.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
            Double(f) => {
                if f.is_nan() {
                    f64::NAN.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }

            Character(s) => s.hash(state),
            Varchar(s) => s.hash(state),
            Boolean(b) => b.hash(state),
            Date(s) => s.hash(state),
            Time(s) => s.hash(state),
            Timestamp(s) => s.hash(state),
            Interval(s) => s.hash(state),
            Vector(v) => {
                // Hash each f32 value using to_bits() for consistent NaN handling
                for f in v.iter() {
                    if f.is_nan() {
                        f32::NAN.to_bits().hash(state);
                    } else {
                        f.to_bits().hash(state);
                    }
                }
            }

            // NULL hashes to nothing (discriminant is enough)
            Null => {}
        }
    }
}
