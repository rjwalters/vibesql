//! SQL TIMESTAMP type implementation

use std::{cmp::Ordering, fmt, str::FromStr};

use super::{Date, Time};

/// SQL TIMESTAMP type - represents a date and time
///
/// Supports multiple formats:
/// - ISO 8601: '2024-01-01T14:30:00' or '2024-01-01T14:30:00.123456'
/// - Space-separated: '2024-01-01 14:30:00' or '2024-01-01 14:30:00.123456'
/// - With timezone: '2024-01-01T14:30:00Z' or '2024-01-01T14:30:00+05:00'
/// - Date only: '2024-01-01' (assumes midnight)
///
/// Stored as Date and Time components for correct comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timestamp {
    pub date: Date,
    pub time: Time,
}

impl Timestamp {
    /// Create a new Timestamp
    pub fn new(date: Date, time: Time) -> Self {
        Timestamp { date, time }
    }
}

impl FromStr for Timestamp {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Trim whitespace
        let trimmed = s.trim();

        // Strip timezone suffix if present (Z, +HH:MM, -HH:MM)
        let timestamp_part = strip_timezone_suffix(trimmed);

        // Try parsing with 'T' separator (ISO 8601)
        if let Some(t_pos) = timestamp_part.find('T') {
            let date_str = &timestamp_part[..t_pos];
            let time_str = &timestamp_part[t_pos + 1..];

            let date = Date::from_str(date_str)?;
            let time = Time::from_str(time_str)?;

            return Ok(Timestamp::new(date, time));
        }

        // Try parsing with space separator
        let parts: Vec<&str> = timestamp_part.split_whitespace().collect();

        if parts.len() == 2 {
            // Standard format: YYYY-MM-DD HH:MM:SS[.ffffff]
            let date = Date::from_str(parts[0])?;
            let time = Time::from_str(parts[1])?;

            return Ok(Timestamp::new(date, time));
        } else if parts.len() == 1 {
            // Could be date-only format
            if let Ok(date) = Date::from_str(parts[0]) {
                // Date only - use midnight time
                let midnight = Time::new(0, 0, 0, 0).unwrap();
                return Ok(Timestamp::new(date, midnight));
            }
        }

        // If all attempts failed, return helpful error message
        Err(format!(
            "Invalid timestamp format: '{}'. Supported formats: \
            ISO 8601 (2025-11-10T08:24:34), \
            space-separated (2025-11-10 08:24:34), \
            or date only (2025-11-10)",
            s
        ))
    }
}

/// Strip timezone suffix from timestamp string
/// Handles: Z, +HH:MM, -HH:MM, +HHMM, -HHMM
fn strip_timezone_suffix(s: &str) -> &str {
    // Check for 'Z' suffix (UTC)
    if s.ends_with('Z') || s.ends_with('z') {
        return &s[..s.len() - 1];
    }

    // Check for +/- timezone offset
    // Look for last occurrence of + or - (could be in date part, so we look from the right)
    if let Some(pos) = s.rfind(['+', '-']) {
        // Make sure this is actually a timezone indicator, not part of the date
        // Timezone offsets appear after the time, so position should be > 10 (YYYY-MM-DD)
        if pos > 10 {
            // Check if what follows looks like a timezone offset
            let potential_tz = &s[pos..];
            if is_timezone_offset(potential_tz) {
                return &s[..pos];
            }
        }
    }

    s
}

/// Check if a string looks like a timezone offset (+HH:MM, -HH:MM, +HHMM, -HHMM)
fn is_timezone_offset(s: &str) -> bool {
    if s.len() < 3 {
        return false;
    }

    let sign = s.chars().next().unwrap();
    if sign != '+' && sign != '-' {
        return false;
    }

    let rest = &s[1..];

    // Check for +HH:MM or -HH:MM format (6 chars: +HH:MM)
    if rest.len() == 5 && rest.chars().nth(2) == Some(':') {
        return rest[..2].chars().all(|c| c.is_ascii_digit())
            && rest[3..].chars().all(|c| c.is_ascii_digit());
    }

    // Check for +HHMM or -HHMM format (4 chars: +HHMM)
    if rest.len() == 4 {
        return rest.chars().all(|c| c.is_ascii_digit());
    }

    // Check for +HH format (2 chars: +HH)
    if rest.len() == 2 {
        return rest.chars().all(|c| c.is_ascii_digit());
    }

    false
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.date, self.time)
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.date.cmp(&other.date).then_with(|| self.time.cmp(&other.time))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_pads_fraction_to_minimum_three_digits() {
        // Timestamp delegates fractional rendering to Time (issue #5332):
        // `.5` must render as `.500` to match SQLite's subsec output.
        let ts = Timestamp::from_str("2024-01-01 12:00:00.5").unwrap();
        assert_eq!(ts.to_string(), "2024-01-01 12:00:00.500");
    }

    #[test]
    fn display_preserves_sub_millisecond_digits() {
        let ts = Timestamp::from_str("2024-01-01 14:30:00.123456").unwrap();
        assert_eq!(ts.to_string(), "2024-01-01 14:30:00.123456");
    }

    #[test]
    fn display_parse_round_trip_preserves_value() {
        for s in [
            "2024-01-01 12:00:00",
            "2024-01-01 12:00:00.5",
            "2024-01-01 12:00:00.500",
            "2024-01-01 14:30:00.123456",
            "2024-01-01 14:30:00.123456789",
        ] {
            let ts = Timestamp::from_str(s).unwrap();
            let reparsed = Timestamp::from_str(&ts.to_string()).unwrap();
            assert_eq!(reparsed, ts, "round-trip failed for '{}' (rendered '{}')", s, ts);
        }
    }
}
