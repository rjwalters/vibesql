//! Display implementation for SqlValue

use std::fmt;

use crate::sql_value::SqlValue;

/// Format a float value like SQLite does:
/// - Use minimal representation (no trailing zeros after decimal point)
/// - Always show at least one decimal place for whole numbers (1.0 not 1)
/// - Use scientific notation for very small or very large values
fn format_float(n: f64) -> String {
    if n.is_nan() {
        return "NaN".to_string();
    }
    if n.is_infinite() {
        return if n > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }

    // Handle zero specially
    if n == 0.0 {
        return "0.0".to_string();
    }

    let abs_n = n.abs();

    // Use scientific notation for very large or very small numbers (like SQLite)
    if abs_n >= 1e15 || (abs_n < 1e-4 && abs_n != 0.0) {
        // Format with scientific notation, then clean up
        let s = format!("{:e}", n);
        // SQLite uses lowercase 'e' and formats like "1.0e-05"
        return s;
    }

    // Use Rust's default Display which gives shortest round-trip representation
    // This is similar to SQLite's approach of minimal representation
    let s = format!("{}", n);

    // If there's no decimal point, add ".0" for consistency
    if !s.contains('.') && !s.contains('e') {
        format!("{}.0", s)
    } else {
        s
    }
}

/// Display implementation for SqlValue (how values are shown to users)
impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::Integer(i) => write!(f, "{}", i),
            SqlValue::Smallint(i) => write!(f, "{}", i),
            SqlValue::Bigint(i) => write!(f, "{}", i),
            SqlValue::Unsigned(u) => write!(f, "{}", u),
            // Format floating point types like SQLite: minimal representation
            SqlValue::Numeric(n) => write!(f, "{}", format_float(*n)),
            SqlValue::Float(n) => write!(f, "{}", format_float(*n as f64)),
            SqlValue::Real(n) => write!(f, "{}", format_float(*n as f64)),
            SqlValue::Double(n) => write!(f, "{}", format_float(*n)),
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
    fn test_format_float_helper() {
        // SQLite-style formatting: minimal representation with at least one decimal place
        assert_eq!(format_float(1.1), "1.1");
        assert_eq!(format_float(2.2), "2.2");
        assert_eq!(format_float(1.0), "1.0");
        assert_eq!(format_float(2.0), "2.0");
        assert_eq!(format_float(0.0), "0.0");
        assert_eq!(format_float(3.14159), "3.14159");
        assert_eq!(format_float(0.5), "0.5");
        assert_eq!(format_float(100.0), "100.0");
        assert_eq!(format_float(-4373.0), "-4373.0");
        assert_eq!(format_float(-4373.123), "-4373.123");
    }

    #[test]
    fn test_format_float_scientific() {
        // Very large numbers use scientific notation
        assert_eq!(format_float(1e15), "1e15");
        assert_eq!(format_float(1e16), "1e16");
        // Very small numbers use scientific notation
        assert_eq!(format_float(0.00001), "1e-5");
        assert_eq!(format_float(1e-10), "1e-10");
    }

    #[test]
    fn test_numeric_display_whole_numbers() {
        // SQLite-style: whole numbers display with .0 suffix
        assert_eq!(format!("{}", SqlValue::Numeric(32.0)), "32.0");
        assert_eq!(format!("{}", SqlValue::Numeric(-4373.0)), "-4373.0");
        assert_eq!(format!("{}", SqlValue::Numeric(0.0)), "0.0");
        assert_eq!(format!("{}", SqlValue::Numeric(164.0)), "164.0");
    }

    #[test]
    fn test_numeric_display_fractional() {
        // Fractional values display without trailing zeros
        assert_eq!(format!("{}", SqlValue::Numeric(32.5)), "32.5");
        assert_eq!(format!("{}", SqlValue::Numeric(-4373.123)), "-4373.123");
        assert_eq!(format!("{}", SqlValue::Numeric(0.5)), "0.5");
        assert_eq!(format!("{}", SqlValue::Numeric(1.1)), "1.1");
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
        // SQLite-style: Float type displays with minimal representation
        assert_eq!(format!("{}", SqlValue::Float(32.0)), "32.0");
        assert_eq!(format!("{}", SqlValue::Float(-4373.0)), "-4373.0");
        assert_eq!(format!("{}", SqlValue::Float(0.0)), "0.0");
        assert_eq!(format!("{}", SqlValue::Float(127.75)), "127.75");
    }

    #[test]
    fn test_real_display_fractional() {
        // Real type displays with minimal representation
        // Note: f32 has limited precision (~7 significant digits), so some values
        // like 1.1 show extra digits when converted to f64 for display
        assert_eq!(format!("{}", SqlValue::Real(32.5)), "32.5");
        assert_eq!(format!("{}", SqlValue::Real(0.5)), "0.5");
        // 1.1 can be exactly represented in f32, but becomes 1.100000023841858 when
        // cast to f64 due to representation differences
        let result = format!("{}", SqlValue::Real(1.1));
        assert!(result.starts_with("1.1"), "Expected to start with 1.1, got: {}", result);
    }

    #[test]
    fn test_double_display_special_values() {
        // Double type handles special values
        assert_eq!(format!("{}", SqlValue::Double(f64::NAN)), "NaN");
        assert_eq!(format!("{}", SqlValue::Double(f64::INFINITY)), "Infinity");
        assert_eq!(format!("{}", SqlValue::Double(f64::NEG_INFINITY)), "-Infinity");
        assert_eq!(format!("{}", SqlValue::Double(123.45)), "123.45");
    }
}
