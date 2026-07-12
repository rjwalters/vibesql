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
    /// Create a new Date with full calendar validation.
    ///
    /// Rejects not only out-of-range months/days but also calendar-impossible
    /// combinations such as `2024-02-30`, `2023-02-29` (non-leap February 29),
    /// and `2024-04-31` (a 31st in a 30-day month). This is the single
    /// construction choke point for user-facing input (`FromStr`, INSERT
    /// validation, CAST, and columnar filter string parsing all route through
    /// here), so validating here fixes every user-facing path at once.
    ///
    /// For the persistence/deserialization path (WAL/checkpoint replay), which
    /// must accept whatever was previously written, use
    /// [`Date::from_parts_unchecked`] instead — see its docs for the
    /// compatibility rationale.
    pub fn new(year: i32, month: u8, day: u8) -> Result<Self, String> {
        if !(1..=12).contains(&month) {
            return Err(format!("Invalid month: {}", month));
        }
        if !(1..=31).contains(&day) {
            return Err(format!("Invalid day: {}", day));
        }
        let max_day = days_in_month(year, month);
        if day > max_day {
            return Err(format!(
                "Invalid date: {:04}-{:02}-{:02} (month {} has {} day(s) in year {})",
                year, month, day, month, max_day, year
            ));
        }
        Ok(Date { year, month, day })
    }

    /// Construct a `Date` from raw components **without calendar validation**.
    ///
    /// This exists solely for the persistence/deserialization path
    /// (`row_serialization.rs`, WAL/checkpoint replay, in-memory row
    /// round-trips). Older VibeSQL builds — or the pre-fix version of this
    /// crate — accepted calendar-invalid dates such as `2024-02-30` at INSERT
    /// time and persisted them to disk. If deserialization routed through the
    /// now-strict [`Date::new`], those already-persisted databases would become
    /// unreadable (a hard recovery failure) after upgrading. To preserve
    /// backward read compatibility we deliberately keep the stored-byte path
    /// lenient: previously-written invalid dates load exactly as they were
    /// stored, while every *new* user-facing construction is rejected by
    /// [`Date::new`].
    ///
    /// Do not use this for user-facing input — only for reconstructing a value
    /// that was already validated (or intentionally tolerated) at write time.
    pub fn from_parts_unchecked(year: i32, month: u8, day: u8) -> Self {
        Date { year, month, day }
    }
}

/// Return the number of days in the given month of the given (proleptic
/// Gregorian) year. `month` must be 1..=12; callers range-check before calling.
fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Proleptic Gregorian leap-year rule (matches `chrono::NaiveDate`).
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leap_year_rule() {
        assert!(is_leap_year(2024)); // divisible by 4
        assert!(is_leap_year(2000)); // divisible by 400
        assert!(!is_leap_year(1900)); // divisible by 100, not 400
        assert!(!is_leap_year(2023)); // not divisible by 4
    }

    #[test]
    fn days_per_month() {
        assert_eq!(days_in_month(2024, 1), 31);
        assert_eq!(days_in_month(2024, 4), 30);
        assert_eq!(days_in_month(2024, 2), 29); // leap
        assert_eq!(days_in_month(2023, 2), 28); // non-leap
        assert_eq!(days_in_month(2024, 12), 31);
    }

    #[test]
    fn new_rejects_calendar_invalid_dates() {
        assert!(Date::new(2024, 2, 30).is_err());
        assert!(Date::new(2023, 2, 29).is_err());
        assert!(Date::new(2024, 4, 31).is_err());
        assert!(Date::new(2024, 13, 1).is_err());
        assert!(Date::new(2024, 1, 0).is_err());
    }

    #[test]
    fn new_accepts_valid_dates() {
        assert!(Date::new(2024, 2, 29).is_ok());
        assert!(Date::new(2023, 2, 28).is_ok());
        assert!(Date::new(2024, 12, 31).is_ok());
        assert!(Date::new(1970, 1, 1).is_ok());
    }

    #[test]
    fn from_parts_unchecked_is_lenient() {
        // The persistence/deserialization path must reconstruct whatever was
        // stored, including calendar-invalid dates written by older builds.
        let d = Date::from_parts_unchecked(2024, 2, 30);
        assert_eq!((d.year, d.month, d.day), (2024, 2, 30));
    }
}
