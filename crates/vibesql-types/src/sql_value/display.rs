//! Display implementation for SqlValue

use std::fmt;

use crate::sql_value::SqlValue;

/// Format a f64 value like SQLite does:
/// - Use minimal representation (shortest round-trip safe string)
/// - KEEP ".0" for whole numbers to distinguish REAL from INTEGER (SQLite behavior)
/// - Use scientific notation for very small or very large values
fn format_f64(n: f64) -> String {
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

    let abs_n = n.abs();

    // Use scientific notation for very large or very small numbers (like SQLite)
    if abs_n >= 1e15 || (abs_n < 1e-4 && abs_n != 0.0) {
        let s = format!("{:e}", n);
        return s;
    }

    // Use ryu for shortest round-trip representation
    let mut buffer = ryu::Buffer::new();
    let s = buffer.format(n);

    // SQLite behavior: KEEP ".0" suffix for whole numbers to distinguish REAL from INTEGER
    // Example: SUM on mixed-type column returns 44.0 (REAL), not 44 (INTEGER)
    s.to_string()
}

/// Format a f32 value like SQLite does.
/// IMPORTANT: Format at f32 precision, not f64, to avoid exposing
/// representation differences (e.g., 1.1f32 becomes 1.100000023841858 as f64)
fn format_f32(n: f32) -> String {
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

    let abs_n = n.abs();

    // Use scientific notation for very large or very small numbers (like SQLite)
    if abs_n >= 1e15 || (abs_n < 1e-4 && abs_n != 0.0) {
        let s = format!("{:e}", n);
        return s;
    }

    // Use ryu for shortest round-trip representation at f32 precision
    let mut buffer = ryu::Buffer::new();
    let s = buffer.format(n);

    // SQLite behavior: KEEP ".0" suffix for whole numbers to distinguish REAL from INTEGER
    s.to_string()
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
            // Use f32-specific formatting for Float/Real to avoid precision artifacts
            SqlValue::Numeric(n) => write!(f, "{}", format_f64(*n)),
            SqlValue::Float(n) => write!(f, "{}", format_f32(*n)),
            SqlValue::Real(n) => write!(f, "{}", format_f32(*n)),
            SqlValue::Double(n) => write!(f, "{}", format_f64(*n)),
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
    fn test_format_f64_helper() {
        // SQLite-style formatting: minimal representation with at least one decimal place
        assert_eq!(format_f64(1.1), "1.1");
        assert_eq!(format_f64(2.2), "2.2");
        assert_eq!(format_f64(1.0), "1.0");
        assert_eq!(format_f64(2.0), "2.0");
        assert_eq!(format_f64(0.0), "0.0");
        assert_eq!(format_f64(123.456), "123.456");
        assert_eq!(format_f64(0.5), "0.5");
        assert_eq!(format_f64(100.0), "100.0");
        assert_eq!(format_f64(-4373.0), "-4373.0");
        assert_eq!(format_f64(-4373.123), "-4373.123");
    }

    #[test]
    fn test_format_f32_helper() {
        // f32 formatting: minimal representation at f32 precision
        // This is the key fix: 1.1f32 should display as "1.1", not "1.100000023841858"
        assert_eq!(format_f32(1.1f32), "1.1");
        assert_eq!(format_f32(2.2f32), "2.2");
        assert_eq!(format_f32(1.0f32), "1.0");
        assert_eq!(format_f32(0.0f32), "0.0");
        assert_eq!(format_f32(123.456f32), "123.456");
        assert_eq!(format_f32(0.5f32), "0.5");
        assert_eq!(format_f32(100.0f32), "100.0");
        assert_eq!(format_f32(-4373.0f32), "-4373.0");
    }

    #[test]
    fn test_format_f64_scientific() {
        // Very large numbers use scientific notation
        assert_eq!(format_f64(1e15), "1e15");
        assert_eq!(format_f64(1e16), "1e16");
        // Very small numbers use scientific notation
        assert_eq!(format_f64(0.00001), "1e-5");
        assert_eq!(format_f64(1e-10), "1e-10");
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
        // Real type displays with minimal representation at f32 precision
        // This is the key fix for issue #4362
        assert_eq!(format!("{}", SqlValue::Real(32.5)), "32.5");
        assert_eq!(format!("{}", SqlValue::Real(0.5)), "0.5");
        // 1.1f32 should display as "1.1", not "1.100000023841858"
        assert_eq!(format!("{}", SqlValue::Real(1.1)), "1.1");
        assert_eq!(format!("{}", SqlValue::Real(2.2)), "2.2");
    }

    #[test]
    fn test_double_display_special_values() {
        // Double type handles special values
        assert_eq!(format!("{}", SqlValue::Double(f64::NAN)), "NaN");
        assert_eq!(format!("{}", SqlValue::Double(f64::INFINITY)), "Infinity");
        assert_eq!(format!("{}", SqlValue::Double(f64::NEG_INFINITY)), "-Infinity");
        assert_eq!(format!("{}", SqlValue::Double(123.45)), "123.45");
    }

    #[test]
    fn test_format_f64_whole_numbers() {
        // SQLite behavior: whole numbers formatted WITH ".0" to distinguish from INTEGER
        assert_eq!(format_f64(45.0), "45.0");
        assert_eq!(format_f64(100.0), "100.0");
        assert_eq!(format_f64(0.0), "0.0");
        assert_eq!(format_f64(45.5), "45.5");
        assert_eq!(format_f64(123.456), "123.456");
    }
}
