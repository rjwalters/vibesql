//! SQL DATE type implementation

use std::{cmp::Ordering, fmt, str::FromStr};

/// SQL DATE type - represents a date without time
///
/// Format: YYYY-MM-DD (e.g., '2024-01-01')
/// Stored as year, month, day components for correct comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Date {
    pub year: i32,
    pub month: u8, // 1-12
    pub day: u8,   // 1-31
}

impl Date {
    /// Create a new Date (validation is basic)
    pub fn new(year: i32, month: u8, day: u8) -> Result<Self, String> {
        if !(1..=12).contains(&month) {
            return Err(format!("Invalid month: {}", month));
        }
        if !(1..=31).contains(&day) {
            return Err(format!("Invalid day: {}", day));
        }
        Ok(Date { year, month, day })
    }
}

impl FromStr for Date {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse format: YYYY-MM-DD
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid date format: '{}' (expected YYYY-MM-DD)", s));
        }

        let year = parts[0].parse::<i32>().map_err(|_| format!("Invalid year: '{}'", parts[0]))?;
        let month = parts[1].parse::<u8>().map_err(|_| format!("Invalid month: '{}'", parts[1]))?;
        let day = parts[2].parse::<u8>().map_err(|_| format!("Invalid day: '{}'", parts[2]))?;

        Date::new(year, month, day)
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Zero-pad the year to 4 digits excluding the sign: SQLite renders
        // negative (astronomical) years as e.g. '-0900-02-28', but Rust's
        // `{:04}` counts the '-' toward the width (giving '-900').
        if self.year < 0 {
            write!(f, "-{:04}-{:02}-{:02}", -self.year, self.month, self.day)
        } else {
            write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
        }
    }
}

impl PartialOrd for Date {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Date {
    fn cmp(&self, other: &Self) -> Ordering {
        self.year
            .cmp(&other.year)
            .then_with(|| self.month.cmp(&other.month))
            .then_with(|| self.day.cmp(&other.day))
    }
}
