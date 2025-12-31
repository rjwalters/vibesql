//! Display implementation for SqlValue

use std::fmt;

use crate::sql_value::SqlValue;

/// Format a f64 value like SQLite does:
/// - Use minimal representation (shortest round-trip safe string)
/// - KEEP ".0" for whole numbers to distinguish REAL from INTEGER (SQLite behavior)
/// - Use scientific notation for very small or very large values
/// - Normalize -0.0 to 0.0 (SQLite behavior)
fn format_f64(n: f64) -> String {
    if n.is_nan() {
        return "NaN".to_string();
    }
    // Normal display uses inf/-inf - quote() function formats as 9.0e+999
    if n.is_infinite() {
        return if n > 0.0 { "inf".to_string() } else { "-inf".to_string() };
    }

    // Normalize negative zero to positive zero (SQLite behavior)
    let n = if n == 0.0 { 0.0 } else { n };

    let abs_n = n.abs();

    // Use scientific notation for very large or very small numbers (like SQLite)
    // SQLite uses ~15 significant figures (IEEE 754 double precision)
    // Format: 1.0e+15, 1.0e-05 (lowercase e, explicit +/-, 2-digit exponent)
    if abs_n >= 1e15 || (abs_n < 1e-4 && abs_n != 0.0) {
        // Use 14 decimal places in mantissa (15 total significant figures)
        let s = format!("{:.14e}", n);
        return format_scientific_sqlite(&s);
    }

    // Use ryu for shortest round-trip representation
    let mut buffer = ryu::Buffer::new();
    let s = buffer.format(n);

    // SQLite behavior: KEEP ".0" suffix for whole numbers to distinguish REAL from INTEGER
    // Example: SUM on mixed-type column returns 44.0 (REAL), not 44 (INTEGER)
    s.to_string()
}

/// Format scientific notation like SQLite: 1.0e+15, 1.0e-05
/// - Lowercase 'e'
/// - Explicit + or - sign for exponent
/// - Two-digit exponent (padded with leading zero if needed)
/// - Strip trailing zeros from mantissa (but keep at least one decimal place)
fn format_scientific_sqlite(s: &str) -> String {
    // Input format from Rust: "1.50000000000000e10" or "1.5e-5"
    // Output format for SQLite: "1.5e+10" or "1.5e-05"
    if let Some(e_pos) = s.find('e') {
        let (mantissa, exp_part) = s.split_at(e_pos);
        let exp_str = &exp_part[1..]; // Skip the 'e'

        // Strip trailing zeros from mantissa and trailing decimal point
        let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');

        let (sign, exp_digits) = if let Some(stripped) = exp_str.strip_prefix('-') {
            ("-", stripped)
        } else if let Some(stripped) = exp_str.strip_prefix('+') {
            ("+", stripped)
        } else {
            ("+", exp_str)
        };

        // Pad exponent to 2 digits
        let exp_num: i32 = exp_digits.parse().unwrap_or(0);
        format!("{}e{}{:02}", mantissa, sign, exp_num.abs())
    } else {
        s.to_string()
    }
}

/// Format a f32 value like SQLite does.
/// IMPORTANT: Format at f32 precision, not f64, to avoid exposing
/// representation differences (e.g., 1.1f32 becomes 1.100000023841858 as f64)
/// - Normalize -0.0 to 0.0 (SQLite behavior)
fn format_f32(n: f32) -> String {
    if n.is_nan() {
        return "NaN".to_string();
    }
    // Normal display uses inf/-inf - quote() function formats as 9.0e+999
    if n.is_infinite() {
        return if n > 0.0 { "inf".to_string() } else { "-inf".to_string() };
    }

    // Normalize negative zero to positive zero (SQLite behavior)
    let n = if n == 0.0 { 0.0 } else { n };

    let abs_n = n.abs();

    // Use scientific notation for very large or very small numbers (like SQLite)
    // SQLite uses ~15 significant figures (IEEE 754 double precision)
    // Format: 1.0e+15, 1.0e-05 (lowercase e, explicit +/-, 2-digit exponent)
    if abs_n >= 1e15 || (abs_n < 1e-4 && abs_n != 0.0) {
        // Use 14 decimal places in mantissa (15 total significant figures)
        let s = format!("{:.14e}", n);
        return format_scientific_sqlite(&s);
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
            SqlValue::Real(n) => write!(f, "{}", format_f64(*n)),
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
            SqlValue::Blob(b) => {
                // SQLite compatibility: Display BLOB as text if it contains valid UTF-8,
                // otherwise display as hex string (without x'' prefix)
                if let Ok(s) = std::str::from_utf8(b) {
                    write!(f, "{}", s)
                } else {
                    // Not valid UTF-8, display as hex
                    for byte in b {
                        write!(f, "{:02X}", byte)?;
                    }
                    Ok(())
                }
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
        // Very large numbers use scientific notation (SQLite format: e+XX or e-XX)
        assert_eq!(format_f64(1e15), "1e+15");
        assert_eq!(format_f64(1e16), "1e+16");
        // Very small numbers use scientific notation
        assert_eq!(format_f64(0.00001), "1e-05");
        assert_eq!(format_f64(1e-10), "1e-10");
    }

    #[test]
    fn test_format_scientific_sqlite() {
        // Test the SQLite-compatible scientific notation formatter
        assert_eq!(format_scientific_sqlite("1e15"), "1e+15");
        assert_eq!(format_scientific_sqlite("1e5"), "1e+05");
        assert_eq!(format_scientific_sqlite("1.5e-5"), "1.5e-05");
        assert_eq!(format_scientific_sqlite("1.5e-15"), "1.5e-15");
        assert_eq!(format_scientific_sqlite("9.22337203685478e18"), "9.22337203685478e+18");
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
        // Normal display uses inf/-inf - quote() function formats as 9.0e+999
        assert_eq!(format!("{}", SqlValue::Numeric(f64::INFINITY)), "inf");
        assert_eq!(format!("{}", SqlValue::Numeric(f64::NEG_INFINITY)), "-inf");
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
        // Real type (now f64) displays with minimal representation
        // SQLite REAL is an 8-byte IEEE float (same as f64)
        assert_eq!(format!("{}", SqlValue::Real(32.5)), "32.5");
        assert_eq!(format!("{}", SqlValue::Real(0.5)), "0.5");
        assert_eq!(format!("{}", SqlValue::Real(1.1)), "1.1");
        assert_eq!(format!("{}", SqlValue::Real(2.2)), "2.2");
    }

    #[test]
    fn test_double_display_special_values() {
        // Double type handles special values
        assert_eq!(format!("{}", SqlValue::Double(f64::NAN)), "NaN");
        // Normal display uses inf/-inf - quote() function formats as 9.0e+999
        assert_eq!(format!("{}", SqlValue::Double(f64::INFINITY)), "inf");
        assert_eq!(format!("{}", SqlValue::Double(f64::NEG_INFINITY)), "-inf");
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

    #[test]
    fn test_format_negative_zero() {
        // SQLite behavior: -0.0 should be normalized to 0.0
        assert_eq!(format_f64(-0.0), "0.0");
        assert_eq!(format_f32(-0.0f32), "0.0");
        // Result of 0.0 * -1.0 should be "0.0", not "-0.0"
        assert_eq!(format_f64(0.0 * -1.0), "0.0");
    }

    #[test]
    fn test_blob_display_utf8() {
        // SQLite compatibility: BLOBs containing valid UTF-8 display as text
        // x'616263' = "abc"
        assert_eq!(
            format!("{}", SqlValue::Blob(vec![0x61, 0x62, 0x63])),
            "abc"
        );
        // x'68617265' = "hare"
        assert_eq!(
            format!("{}", SqlValue::Blob(vec![0x68, 0x61, 0x72, 0x65])),
            "hare"
        );
        // x'68656c6c6f' = "hello"
        assert_eq!(
            format!("{}", SqlValue::Blob(vec![0x68, 0x65, 0x6c, 0x6c, 0x6f])),
            "hello"
        );
    }

    #[test]
    fn test_blob_display_invalid_utf8() {
        // Non-UTF8 bytes display as hex
        assert_eq!(
            format!("{}", SqlValue::Blob(vec![0xFF, 0xFE])),
            "FFFE"
        );
        // Invalid UTF-8 sequence
        assert_eq!(
            format!("{}", SqlValue::Blob(vec![0x80, 0x81, 0x82])),
            "808182"
        );
    }

    #[test]
    fn test_blob_display_empty() {
        // Empty blob displays as empty string
        assert_eq!(format!("{}", SqlValue::Blob(vec![])), "");
    }
}
