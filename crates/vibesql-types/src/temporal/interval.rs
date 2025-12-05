//! SQL INTERVAL type implementation
//!
//! Implements SQL:1999 interval semantics following PostgreSQL's approach:
//! - Intervals are internally stored as (months, days, microseconds)
//! - Comparison uses linear representation with approximations (1 month = 30 days)
//! - Supports year-month intervals and day-time intervals

use std::{cmp::Ordering, fmt, str::FromStr};

/// SQL INTERVAL type - represents a duration
///
/// Formats vary: '5 YEAR', '1-6 YEAR TO MONTH', '5 12:30:45 DAY TO SECOND', etc.
///
/// Internal representation follows PostgreSQL:
/// - months: Total months (years * 12 + months)
/// - days: Total days
/// - microseconds: Total time in microseconds (hours, minutes, seconds, fractions)
///
/// Note: Equality and ordering are based on the internal representation (months, days, microseconds),
/// not the string value. This means "1 YEAR" equals "12 MONTH" even though the strings differ.
#[derive(Debug, Clone)]
pub struct Interval {
    /// The original interval string (e.g., "5 YEAR", "1-6 YEAR TO MONTH")
    pub value: String,
    /// Total months (used for comparison)
    months: i32,
    /// Total days (used for comparison)
    days: i32,
    /// Total microseconds (used for comparison)
    microseconds: i64,
}

impl Interval {
    /// Create a new Interval
    pub fn new(value: String) -> Self {
        // Parse the interval string to extract internal representation
        let (months, days, microseconds) = Self::parse_interval(&value);
        Interval { value, months, days, microseconds }
    }

    /// Parse an interval string into (months, days, microseconds)
    ///
    /// This is a simplified parser that handles common interval formats.
    /// For more complex parsing, this should be enhanced.
    fn parse_interval(s: &str) -> (i32, i32, i64) {
        let mut months = 0;
        let mut days = 0;
        let mut microseconds = 0i64;

        // Split into value and unit parts
        // Format examples: "5 YEAR", "1-6 YEAR TO MONTH", "30 DAY", "12:30:45 HOUR TO SECOND"
        let parts: Vec<&str> = s.split_whitespace().collect();

        if parts.is_empty() {
            return (0, 0, 0);
        }

        // Check if it's a compound interval (contains "TO")
        if let Some(to_pos) = parts.iter().position(|&p| p.eq_ignore_ascii_case("TO")) {
            // Compound interval like "1-6 YEAR TO MONTH" or "5 12:30:45 DAY TO SECOND"
            if to_pos >= 2 {
                let value_part = parts[0];
                let from_unit = parts[to_pos - 1];
                let to_unit = parts[to_pos + 1];

                // Handle YEAR TO MONTH (format: "Y-M" or "Y")
                if from_unit.eq_ignore_ascii_case("YEAR") && to_unit.eq_ignore_ascii_case("MONTH") {
                    if let Some(dash_pos) = value_part.find('-') {
                        let years: i32 = value_part[..dash_pos].parse().unwrap_or(0);
                        let month_part: i32 = value_part[dash_pos + 1..].parse().unwrap_or(0);
                        months = years * 12 + month_part;
                    } else {
                        let years: i32 = value_part.parse().unwrap_or(0);
                        months = years * 12;
                    }
                }
                // Handle DAY TO HOUR, DAY TO SECOND, etc.
                else if from_unit.eq_ignore_ascii_case("DAY") {
                    if let Some(space_pos) = value_part.find(' ') {
                        days = value_part[..space_pos].parse().unwrap_or(0);
                        // Parse time component
                        let time_part = value_part[space_pos + 1..].trim();
                        microseconds = Self::parse_time_to_microseconds(time_part);
                    } else {
                        days = value_part.parse().unwrap_or(0);
                    }
                }
                // Handle HOUR TO SECOND, etc.
                else if from_unit.eq_ignore_ascii_case("HOUR")
                    || from_unit.eq_ignore_ascii_case("MINUTE")
                    || from_unit.eq_ignore_ascii_case("SECOND")
                {
                    microseconds = Self::parse_time_to_microseconds(value_part);
                }
            }
        } else {
            // Simple interval like "5 YEAR", "30 DAY", "24 HOUR"
            if parts.len() >= 2 {
                let value_part = parts[0];
                let unit = parts[1];

                match unit.to_uppercase().as_str() {
                    "YEAR" | "YEARS" => {
                        let years: i32 = value_part.parse().unwrap_or(0);
                        months = years * 12;
                    }
                    "MONTH" | "MONTHS" => {
                        months = value_part.parse().unwrap_or(0);
                    }
                    "DAY" | "DAYS" => {
                        days = value_part.parse().unwrap_or(0);
                    }
                    "HOUR" | "HOURS" => {
                        let hours: i64 = value_part.parse().unwrap_or(0);
                        microseconds = hours * 3600 * 1_000_000;
                    }
                    "MINUTE" | "MINUTES" => {
                        let minutes: i64 = value_part.parse().unwrap_or(0);
                        microseconds = minutes * 60 * 1_000_000;
                    }
                    "SECOND" | "SECONDS" => {
                        microseconds = Self::parse_seconds_to_microseconds(value_part);
                    }
                    _ => {}
                }
            }
        }

        (months, days, microseconds)
    }

    /// Parse a time string (HH:MM:SS or HH:MM:SS.ffffff) to microseconds
    fn parse_time_to_microseconds(s: &str) -> i64 {
        let parts: Vec<&str> = s.split(':').collect();
        let mut total_microseconds = 0i64;

        if !parts.is_empty() {
            // Hours
            if let Ok(hours) = parts[0].parse::<i64>() {
                total_microseconds += hours * 3600 * 1_000_000;
            }
        }

        if parts.len() > 1 {
            // Minutes
            if let Ok(minutes) = parts[1].parse::<i64>() {
                total_microseconds += minutes * 60 * 1_000_000;
            }
        }

        if parts.len() > 2 {
            // Seconds (may have fractional part)
            total_microseconds += Self::parse_seconds_to_microseconds(parts[2]);
        }

        total_microseconds
    }

    /// Parse seconds string (possibly with fractional part) to microseconds
    fn parse_seconds_to_microseconds(s: &str) -> i64 {
        if let Some(dot_pos) = s.find('.') {
            let whole: i64 = s[..dot_pos].parse().unwrap_or(0);
            let frac_str = &s[dot_pos + 1..];
            // Pad or truncate to 6 digits for microseconds
            let frac_str_padded = format!("{:0<6}", frac_str);
            let frac: i64 = frac_str_padded[..6].parse().unwrap_or(0);
            whole * 1_000_000 + frac
        } else {
            s.parse::<i64>().unwrap_or(0) * 1_000_000
        }
    }

    /// Compute a linear comparison value for this interval
    ///
    /// Following PostgreSQL's approach:
    /// - Convert months to days (1 month = 30 days approximation)
    /// - Add days
    /// - Convert to microseconds (1 day = 24 hours = 86400 seconds)
    /// - Add interval's microseconds
    ///
    /// Note: This uses approximations and may not be accurate for all intervals,
    /// particularly when comparing year-month intervals with day-time intervals.
    fn cmp_value(&self) -> i128 {
        // Convert months to days (approximation: 1 month = 30 days)
        let total_days = (self.months as i64) * 30 + (self.days as i64);

        // Convert days to microseconds (1 day = 86400 seconds = 86400000000 microseconds)
        let days_in_microseconds = total_days as i128 * 86_400_000_000i128;

        // Add the time component
        days_in_microseconds + (self.microseconds as i128)
    }
}

impl FromStr for Interval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // For now, just store the string representation
        // Can be enhanced later to parse and validate interval format
        Ok(Interval::new(s.to_string()))
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_value().cmp(&other.cmp_value())
    }
}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        // Compare based on internal representation, not string value
        // This allows "1 YEAR" to equal "12 MONTH"
        self.months == other.months
            && self.days == other.days
            && self.microseconds == other.microseconds
    }
}

impl Eq for Interval {}

impl std::hash::Hash for Interval {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Only hash the fields used in equality comparison
        // This ensures that if a == b, then hash(a) == hash(b)
        self.months.hash(state);
        self.days.hash(state);
        self.microseconds.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_comparison_simple_years() {
        let i1 = Interval::new("1 YEAR".to_string());
        let i2 = Interval::new("2 YEAR".to_string());
        assert!(i1 < i2, "1 YEAR should be less than 2 YEAR");
    }

    #[test]
    fn test_interval_comparison_simple_months() {
        let i1 = Interval::new("1 MONTH".to_string());
        let i2 = Interval::new("6 MONTH".to_string());
        assert!(i1 < i2, "1 MONTH should be less than 6 MONTH");
    }

    #[test]
    fn test_interval_comparison_simple_days() {
        let i1 = Interval::new("1 DAY".to_string());
        let i2 = Interval::new("30 DAY".to_string());
        assert!(i1 < i2, "1 DAY should be less than 30 DAY");
    }

    #[test]
    fn test_interval_comparison_month_vs_days() {
        // 1 month is approximated as 30 days in comparison (ordering)
        // Note: They are NOT equal (different internal representation)
        // but they have the same comparison value
        let i1 = Interval::new("1 MONTH".to_string());
        let i2 = Interval::new("30 DAY".to_string());
        assert_ne!(i1, i2, "1 MONTH and 30 DAY have different representations");
        assert_eq!(
            i1.cmp(&i2),
            Ordering::Equal,
            "1 MONTH should compare equal to 30 DAY (approximation)"
        );
    }

    #[test]
    fn test_interval_comparison_year_vs_months() {
        // 1 year = 12 months
        let i1 = Interval::new("1 YEAR".to_string());
        let i2 = Interval::new("12 MONTH".to_string());
        assert_eq!(i1, i2, "1 YEAR should equal 12 MONTH");
    }

    #[test]
    fn test_interval_comparison_year_vs_days() {
        // 1 year = 12 months = 360 days (approximation for ordering)
        // Note: They are NOT equal (different internal representation)
        // but they have the same comparison value
        let i1 = Interval::new("1 YEAR".to_string());
        let i2 = Interval::new("360 DAY".to_string());
        assert_ne!(i1, i2, "1 YEAR and 360 DAY have different representations");
        assert_eq!(
            i1.cmp(&i2),
            Ordering::Equal,
            "1 YEAR should compare equal to 360 DAY (approximation)"
        );
    }

    #[test]
    fn test_interval_comparison_hours() {
        let i1 = Interval::new("1 HOUR".to_string());
        let i2 = Interval::new("2 HOUR".to_string());
        assert!(i1 < i2, "1 HOUR should be less than 2 HOUR");
    }

    #[test]
    fn test_interval_comparison_minutes() {
        let i1 = Interval::new("30 MINUTE".to_string());
        let i2 = Interval::new("90 MINUTE".to_string());
        assert!(i1 < i2, "30 MINUTE should be less than 90 MINUTE");
    }

    #[test]
    fn test_interval_comparison_seconds() {
        let i1 = Interval::new("45 SECOND".to_string());
        let i2 = Interval::new("90 SECOND".to_string());
        assert!(i1 < i2, "45 SECOND should be less than 90 SECOND");
    }

    #[test]
    fn test_interval_comparison_year_to_month() {
        // "1-6 YEAR TO MONTH" = 1 year + 6 months = 18 months
        let i1 = Interval::new("1-6 YEAR TO MONTH".to_string());
        let i2 = Interval::new("18 MONTH".to_string());
        assert_eq!(i1, i2, "1-6 YEAR TO MONTH should equal 18 MONTH");
    }

    #[test]
    fn test_interval_comparison_year_to_month_ordering() {
        let i1 = Interval::new("1-0 YEAR TO MONTH".to_string());
        let i2 = Interval::new("1-6 YEAR TO MONTH".to_string());
        let i3 = Interval::new("2-0 YEAR TO MONTH".to_string());
        assert!(i1 < i2, "1-0 should be less than 1-6");
        assert!(i2 < i3, "1-6 should be less than 2-0");
    }

    #[test]
    fn test_interval_comparison_cross_type_month_vs_day() {
        // 1 month (approximated as 30 days) < 31 days
        let i1 = Interval::new("1 MONTH".to_string());
        let i2 = Interval::new("31 DAY".to_string());
        assert!(i1 < i2, "1 MONTH should be less than 31 DAY");
    }

    #[test]
    fn test_interval_comparison_cross_type_month_vs_day_greater() {
        // 1 month (approximated as 30 days) > 29 days
        let i1 = Interval::new("1 MONTH".to_string());
        let i2 = Interval::new("29 DAY".to_string());
        assert!(i1 > i2, "1 MONTH should be greater than 29 DAY");
    }

    #[test]
    fn test_interval_comparison_equality() {
        let i1 = Interval::new("5 YEAR".to_string());
        let i2 = Interval::new("5 YEAR".to_string());
        assert_eq!(i1, i2, "Same intervals should be equal");
    }

    #[test]
    fn test_interval_parsing_zero() {
        let i = Interval::new("0 DAY".to_string());
        assert_eq!(i.days, 0);
        assert_eq!(i.months, 0);
        assert_eq!(i.microseconds, 0);
    }

    #[test]
    fn test_interval_ordering_total() {
        let mut intervals = [
            Interval::new("2 YEAR".to_string()),
            Interval::new("1 MONTH".to_string()),
            Interval::new("1 DAY".to_string()),
            Interval::new("1 YEAR".to_string()),
            Interval::new("30 DAY".to_string()),
        ];

        intervals.sort();

        // Expected order: 1 DAY < 1 MONTH == 30 DAY < 1 YEAR < 2 YEAR
        assert_eq!(intervals[0].value, "1 DAY");
        // 1 MONTH and 30 DAY are equivalent, either could be at index 1 or 2
        assert!(
            (intervals[1].value == "1 MONTH" && intervals[2].value == "30 DAY")
                || (intervals[1].value == "30 DAY" && intervals[2].value == "1 MONTH")
        );
        assert_eq!(intervals[3].value, "1 YEAR");
        assert_eq!(intervals[4].value, "2 YEAR");
    }

    #[test]
    fn test_interval_cmp_value_calculation() {
        // Test internal cmp_value calculation
        let i1 = Interval::new("1 YEAR".to_string());
        // 1 year = 12 months = 360 days = 360 * 86400000000 microseconds
        let expected = 360i128 * 86_400_000_000i128;
        assert_eq!(i1.cmp_value(), expected);

        let i2 = Interval::new("1 DAY".to_string());
        // 1 day = 86400000000 microseconds
        let expected = 86_400_000_000i128;
        assert_eq!(i2.cmp_value(), expected);

        let i3 = Interval::new("1 HOUR".to_string());
        // 1 hour = 3600 seconds = 3600000000 microseconds
        let expected = 3_600_000_000i128;
        assert_eq!(i3.cmp_value(), expected);
    }

    #[test]
    fn test_interval_from_str() {
        let i: Interval = "5 YEAR".parse().unwrap();
        assert_eq!(i.value, "5 YEAR");
        assert_eq!(i.months, 60); // 5 * 12
    }

    #[test]
    fn test_interval_display() {
        let i = Interval::new("5 YEAR".to_string());
        assert_eq!(format!("{}", i), "5 YEAR");
    }

    #[test]
    fn test_interval_parsing_fractional_seconds() {
        let i = Interval::new("1.5 SECOND".to_string());
        assert_eq!(i.microseconds, 1_500_000); // 1.5 seconds = 1500000 microseconds
    }

    #[test]
    fn test_interval_comparison_with_fractional_seconds() {
        let i1 = Interval::new("1.5 SECOND".to_string());
        let i2 = Interval::new("2 SECOND".to_string());
        assert!(i1 < i2, "1.5 SECOND should be less than 2 SECOND");
    }

    #[test]
    fn test_interval_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Helper function to compute hash
        fn calculate_hash<T: Hash>(t: &T) -> u64 {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        }

        // Test that equal intervals have equal hashes
        let i1 = Interval::new("1 YEAR".to_string());
        let i2 = Interval::new("12 MONTH".to_string());

        // These should be equal
        assert_eq!(i1, i2, "1 YEAR should equal 12 MONTH");

        // And they should have the same hash
        assert_eq!(
            calculate_hash(&i1),
            calculate_hash(&i2),
            "Equal intervals must have equal hash values"
        );

        // Test another case: year to month
        let i3 = Interval::new("1-6 YEAR TO MONTH".to_string());
        let i4 = Interval::new("18 MONTH".to_string());

        assert_eq!(i3, i4, "1-6 YEAR TO MONTH should equal 18 MONTH");
        assert_eq!(
            calculate_hash(&i3),
            calculate_hash(&i4),
            "Equal intervals must have equal hash values"
        );

        // Test that different intervals have different hashes (usually)
        let i5 = Interval::new("1 YEAR".to_string());
        let i6 = Interval::new("2 YEAR".to_string());

        assert_ne!(i5, i6, "1 YEAR should not equal 2 YEAR");
        // Note: We can't guarantee different hashes for different values,
        // but we can verify the Hash/Eq invariant holds
    }
}
