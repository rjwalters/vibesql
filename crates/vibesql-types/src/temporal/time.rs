//! SQL TIME type implementation

use std::{cmp::Ordering, fmt, str::FromStr};

/// SQL TIME type - represents a time without date
///
/// Format: HH:MM:SS or HH:MM:SS.fff (e.g., '14:30:00' or '14:30:00.123')
/// Stored as hour, minute, second, nanosecond components for correct comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Time {
    pub hour: u8,        // 0-23
    pub minute: u8,      // 0-59
    pub second: u8,      // 0-59
    pub nanosecond: u32, // 0-999999999
}

impl Time {
    /// Create a new Time (validation is basic)
    pub fn new(hour: u8, minute: u8, second: u8, nanosecond: u32) -> Result<Self, String> {
        if hour > 23 {
            return Err(format!("Invalid hour: {}", hour));
        }
        if minute > 59 {
            return Err(format!("Invalid minute: {}", minute));
        }
        if second > 59 {
            return Err(format!("Invalid second: {}", second));
        }
        if nanosecond > 999_999_999 {
            return Err(format!("Invalid nanosecond: {}", nanosecond));
        }
        Ok(Time { hour, minute, second, nanosecond })
    }
}

impl FromStr for Time {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse format: HH:MM:SS or HH:MM:SS.fff
        let (time_part, frac_part) = if let Some(dot_pos) = s.find('.') {
            (&s[..dot_pos], Some(&s[dot_pos + 1..]))
        } else {
            (s, None)
        };

        let parts: Vec<&str> = time_part.split(':').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid time format: '{}' (expected HH:MM:SS)", s));
        }

        let hour = parts[0].parse::<u8>().map_err(|_| format!("Invalid hour: '{}'", parts[0]))?;
        let minute =
            parts[1].parse::<u8>().map_err(|_| format!("Invalid minute: '{}'", parts[1]))?;
        let second =
            parts[2].parse::<u8>().map_err(|_| format!("Invalid second: '{}'", parts[2]))?;

        // Parse fractional seconds if present
        let nanosecond = if let Some(frac) = frac_part {
            // Pad or truncate to 9 digits (nanoseconds)
            let padded = format!("{:0<9}", frac);
            let truncated = &padded[..9.min(padded.len())];
            truncated
                .parse::<u32>()
                .map_err(|_| format!("Invalid fractional seconds: '{}'", frac))?
        } else {
            0
        };

        Time::new(hour, minute, second, nanosecond)
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nanosecond == 0 {
            write!(f, "{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
        } else {
            // Display fractional seconds, trimming trailing zeros but never
            // below 3 digits. SQLite's subsec rendering is always exactly 3
            // digits (`HH:MM:SS.SSS`); padding to a minimum of 3 keeps
            // `.5` → `.500` aligned with SQLite while preserving stored
            // sub-millisecond precision (e.g. `.123456`) so Display → parse
            // round-trips hold (issue #5332).
            let frac = format!("{:09}", self.nanosecond);
            let trimmed_len = frac.trim_end_matches('0').len().max(3);
            write!(
                f,
                "{:02}:{:02}:{:02}.{}",
                self.hour,
                self.minute,
                self.second,
                &frac[..trimmed_len]
            )
        }
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Time {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hour
            .cmp(&other.hour)
            .then_with(|| self.minute.cmp(&other.minute))
            .then_with(|| self.second.cmp(&other.second))
            .then_with(|| self.nanosecond.cmp(&other.nanosecond))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time_with_ns(nanosecond: u32) -> Time {
        Time::new(13, 15, 44, nanosecond).unwrap()
    }

    #[test]
    fn display_no_fraction_when_nanosecond_zero() {
        assert_eq!(time_with_ns(0).to_string(), "13:15:44");
    }

    #[test]
    fn display_pads_fraction_to_minimum_three_digits() {
        // SQLite renders subsec with exactly 3 digits: `.5` must become `.500`.
        assert_eq!(time_with_ns(500_000_000).to_string(), "13:15:44.500");
        assert_eq!(time_with_ns(120_000_000).to_string(), "13:15:44.120");
        assert_eq!(time_with_ns(1_000_000).to_string(), "13:15:44.001");
    }

    #[test]
    fn display_keeps_three_digit_fractions_unchanged() {
        assert_eq!(time_with_ns(123_000_000).to_string(), "13:15:44.123");
    }

    #[test]
    fn display_preserves_sub_millisecond_digits() {
        // No hard truncation: stored sub-ms precision must stay visible.
        assert_eq!(time_with_ns(123_456_000).to_string(), "13:15:44.123456");
        assert_eq!(time_with_ns(123_456_789).to_string(), "13:15:44.123456789");
    }

    #[test]
    fn display_parse_round_trip_preserves_nanoseconds() {
        for ns in [
            0u32,
            500_000_000,
            123_000_000,
            120_000_000,
            123_456_000,
            1_000_000,
            123_456_789,
            999_999_999,
            1, // renders as .000000001
        ] {
            let t = time_with_ns(ns);
            let parsed = Time::from_str(&t.to_string()).unwrap();
            assert_eq!(parsed, t, "round-trip failed for ns={} (rendered '{}')", ns, t);
        }
    }
}
