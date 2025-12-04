//! Display implementation for SqlValue

use std::fmt;

use crate::sql_value::SqlValue;

/// Display implementation for SqlValue (how values are shown to users)
impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::Integer(i) => write!(f, "{}", i),
            SqlValue::Smallint(i) => write!(f, "{}", i),
            SqlValue::Bigint(i) => write!(f, "{}", i),
            SqlValue::Unsigned(u) => write!(f, "{}", u),
            // Format Numeric - SQLLogicTest compatibility
            // SQLLogicTest expects numeric results with 3 decimal places for consistency
            SqlValue::Numeric(n) => {
                if n.is_nan() {
                    write!(f, "NaN")
                } else if n.is_infinite() {
                    if *n > 0.0 {
                        write!(f, "Infinity")
                    } else {
                        write!(f, "-Infinity")
                    }
                } else {
                    // Always show 3 decimal places for SQLLogicTest compatibility
                    // This ensures results like "48.000" instead of "48"
                    write!(f, "{:.3}", n)
                }
            }
            // Format Float, Real, Double - SQLLogicTest compatibility
            // Same formatting as Numeric: 3 decimal places for consistency
            SqlValue::Float(n) => {
                let n64 = *n as f64;
                if n64.is_nan() {
                    write!(f, "NaN")
                } else if n64.is_infinite() {
                    if n64 > 0.0 {
                        write!(f, "Infinity")
                    } else {
                        write!(f, "-Infinity")
                    }
                } else {
                    // Always show 3 decimal places for SQLLogicTest compatibility
                    write!(f, "{:.3}", n64)
                }
            }
            SqlValue::Real(n) => {
                let n64 = *n as f64;
                if n64.is_nan() {
                    write!(f, "NaN")
                } else if n64.is_infinite() {
                    if n64 > 0.0 {
                        write!(f, "Infinity")
                    } else {
                        write!(f, "-Infinity")
                    }
                } else {
                    // Always show 3 decimal places for SQLLogicTest compatibility
                    write!(f, "{:.3}", n64)
                }
            }
            SqlValue::Double(n) => {
                if n.is_nan() {
                    write!(f, "NaN")
                } else if n.is_infinite() {
                    if *n > 0.0 {
                        write!(f, "Infinity")
                    } else {
                        write!(f, "-Infinity")
                    }
                } else {
                    // Always show 3 decimal places for SQLLogicTest compatibility
                    write!(f, "{:.3}", n)
                }
            }
            SqlValue::Character(s) => write!(f, "{}", s),
            SqlValue::Varchar(s) => write!(f, "{}", s),
            SqlValue::Boolean(true) => write!(f, "TRUE"),
            SqlValue::Boolean(false) => write!(f, "FALSE"),
            SqlValue::Date(s) => write!(f, "{}", s),
            SqlValue::Time(s) => write!(f, "{}", s),
            SqlValue::Timestamp(s) => write!(f, "{}", s),
            SqlValue::Interval(s) => write!(f, "{}", s),
            SqlValue::Vector(v) => {
                // Format vector as space-separated f32 values
                let formatted: Vec<String> = v.iter().map(|x| x.to_string()).collect();
                write!(f, "[{}]", formatted.join(", "))
            }
            SqlValue::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_display_whole_numbers() {
        // SQLLogicTest compatibility: whole numbers display with 3 decimal places
        assert_eq!(format!("{}", SqlValue::Numeric(32.0)), "32.000");
        assert_eq!(format!("{}", SqlValue::Numeric(-4373.0)), "-4373.000");
        assert_eq!(format!("{}", SqlValue::Numeric(0.0)), "0.000");
        assert_eq!(format!("{}", SqlValue::Numeric(164.0)), "164.000");
    }

    #[test]
    fn test_numeric_display_fractional() {
        // Fractional values display with 3 decimal places (rounded if needed)
        assert_eq!(format!("{}", SqlValue::Numeric(32.5)), "32.500");
        assert_eq!(format!("{}", SqlValue::Numeric(-4373.123)), "-4373.123");
        assert_eq!(format!("{}", SqlValue::Numeric(0.5)), "0.500");
    }

    #[test]
    fn test_numeric_display_special_values() {
        // Special values
        assert_eq!(format!("{}", SqlValue::Numeric(f64::NAN)), "NaN");
        assert_eq!(format!("{}", SqlValue::Numeric(f64::INFINITY)), "Infinity");
        assert_eq!(format!("{}", SqlValue::Numeric(f64::NEG_INFINITY)), "-Infinity");
    }

    #[test]
    fn test_float_display_whole_numbers() {
        // SQLLogicTest compatibility: Float type also displays with 3 decimal places
        assert_eq!(format!("{}", SqlValue::Float(32.0)), "32.000");
        assert_eq!(format!("{}", SqlValue::Float(-4373.0)), "-4373.000");
        assert_eq!(format!("{}", SqlValue::Float(0.0)), "0.000");
        assert_eq!(format!("{}", SqlValue::Float(127.75)), "127.750");
    }

    #[test]
    fn test_real_display_fractional() {
        // Real type also displays with 3 decimal places
        assert_eq!(format!("{}", SqlValue::Real(32.5)), "32.500");
        assert_eq!(format!("{}", SqlValue::Real(-4373.123)), "-4373.123");
        assert_eq!(format!("{}", SqlValue::Real(0.5)), "0.500");
    }

    #[test]
    fn test_double_display_special_values() {
        // Double type handles special values
        assert_eq!(format!("{}", SqlValue::Double(f64::NAN)), "NaN");
        assert_eq!(format!("{}", SqlValue::Double(f64::INFINITY)), "Infinity");
        assert_eq!(format!("{}", SqlValue::Double(f64::NEG_INFINITY)), "-Infinity");
        assert_eq!(format!("{}", SqlValue::Double(123.45)), "123.450");
    }
}
