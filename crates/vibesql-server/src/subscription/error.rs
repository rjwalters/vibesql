//! Error classification for subscription retry logic
//!
//! Categorizes errors as transient (retry) or permanent (don't retry)
//! to enable smart retry behavior with circuit breaker pattern.

use std::fmt::Display;

/// Classification of subscription query errors
///
/// Used to determine retry strategy:
/// - Transient: Retry with exponential backoff (lock timeouts, connection issues)
/// - Permanent: Don't retry, send error to client (table dropped, syntax error)
/// - Unknown: Retry with caution (unrecognized errors)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionErrorKind {
    /// Transient error - safe to retry
    /// Examples: database lock timeout, connection reset, temporary unavailability
    Transient,

    /// Permanent error - don't retry
    /// Examples: table dropped, syntax error, permission denied, invalid column
    Permanent,

    /// Unknown error - retry with caution
    /// Examples: errors we haven't classified yet
    Unknown,
}

impl Display for SubscriptionErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient => write!(f, "transient"),
            Self::Permanent => write!(f, "permanent"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Classify an error from query execution
///
/// Examines error messages and types to determine if the error is likely
/// transient (safe to retry) or permanent (should not be retried).
pub fn classify_error(error: &dyn std::error::Error) -> SubscriptionErrorKind {
    let error_str = error.to_string().to_lowercase();

    // Transient errors
    if error_str.contains("lock") || error_str.contains("timeout") {
        return SubscriptionErrorKind::Transient;
    }
    if error_str.contains("connection") || error_str.contains("reset") {
        return SubscriptionErrorKind::Transient;
    }
    if error_str.contains("unavailable") || error_str.contains("busy") {
        return SubscriptionErrorKind::Transient;
    }
    if error_str.contains("temporarily") || error_str.contains("temporary") {
        return SubscriptionErrorKind::Transient;
    }

    // Permanent errors
    if error_str.contains("table") && error_str.contains("not found")
        || error_str.contains("table") && error_str.contains("doesn't exist")
        || error_str.contains("table") && error_str.contains("does not exist")
    {
        return SubscriptionErrorKind::Permanent;
    }
    if error_str.contains("column") && error_str.contains("not found")
        || error_str.contains("column") && error_str.contains("doesn't exist")
        || error_str.contains("column") && error_str.contains("does not exist")
    {
        return SubscriptionErrorKind::Permanent;
    }
    if error_str.contains("syntax") || error_str.contains("parse") {
        return SubscriptionErrorKind::Permanent;
    }
    if error_str.contains("permission") || error_str.contains("denied") {
        return SubscriptionErrorKind::Permanent;
    }
    if error_str.contains("invalid") {
        return SubscriptionErrorKind::Permanent;
    }

    // Unknown - default to retrying with caution
    SubscriptionErrorKind::Unknown
}

/// Classify an error from a string representation
pub fn classify_error_str(error_msg: &str) -> SubscriptionErrorKind {
    let error_str = error_msg.to_lowercase();

    // Transient errors
    if error_str.contains("lock") || error_str.contains("timeout") {
        return SubscriptionErrorKind::Transient;
    }
    if error_str.contains("connection") || error_str.contains("reset") {
        return SubscriptionErrorKind::Transient;
    }
    if error_str.contains("unavailable") || error_str.contains("busy") {
        return SubscriptionErrorKind::Transient;
    }
    if error_str.contains("temporarily") || error_str.contains("temporary") {
        return SubscriptionErrorKind::Transient;
    }

    // Permanent errors
    if (error_str.contains("table") && error_str.contains("not found"))
        || (error_str.contains("table") && error_str.contains("doesn't exist"))
        || (error_str.contains("table") && error_str.contains("does not exist"))
    {
        return SubscriptionErrorKind::Permanent;
    }
    if (error_str.contains("column") && error_str.contains("not found"))
        || (error_str.contains("column") && error_str.contains("doesn't exist"))
        || (error_str.contains("column") && error_str.contains("does not exist"))
    {
        return SubscriptionErrorKind::Permanent;
    }
    if error_str.contains("syntax") || error_str.contains("parse") {
        return SubscriptionErrorKind::Permanent;
    }
    if error_str.contains("permission") || error_str.contains("denied") {
        return SubscriptionErrorKind::Permanent;
    }
    if error_str.contains("invalid") {
        return SubscriptionErrorKind::Permanent;
    }

    // Unknown - default to retrying with caution
    SubscriptionErrorKind::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_transient_timeout() {
        let kind = classify_error_str("Query timeout");
        assert_eq!(kind, SubscriptionErrorKind::Transient);
    }

    #[test]
    fn test_classify_transient_lock() {
        let kind = classify_error_str("Database lock timeout");
        assert_eq!(kind, SubscriptionErrorKind::Transient);
    }

    #[test]
    fn test_classify_transient_connection() {
        let kind = classify_error_str("Connection reset by peer");
        assert_eq!(kind, SubscriptionErrorKind::Transient);
    }

    #[test]
    fn test_classify_permanent_table_not_found() {
        let kind = classify_error_str("Table 'users' not found");
        assert_eq!(kind, SubscriptionErrorKind::Permanent);
    }

    #[test]
    fn test_classify_permanent_column_not_found() {
        let kind = classify_error_str("Column 'name' doesn't exist");
        assert_eq!(kind, SubscriptionErrorKind::Permanent);
    }

    #[test]
    fn test_classify_permanent_syntax() {
        let kind = classify_error_str("Syntax error in SELECT");
        assert_eq!(kind, SubscriptionErrorKind::Permanent);
    }

    #[test]
    fn test_classify_permanent_permission() {
        let kind = classify_error_str("Permission denied");
        assert_eq!(kind, SubscriptionErrorKind::Permanent);
    }

    #[test]
    fn test_classify_unknown() {
        let kind = classify_error_str("Some other error");
        assert_eq!(kind, SubscriptionErrorKind::Unknown);
    }

    #[test]
    fn test_error_kind_display() {
        assert_eq!(format!("{}", SubscriptionErrorKind::Transient), "transient");
        assert_eq!(format!("{}", SubscriptionErrorKind::Permanent), "permanent");
        assert_eq!(format!("{}", SubscriptionErrorKind::Unknown), "unknown");
    }
}
