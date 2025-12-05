//! Parsing and handling of retry configuration.

use std::time::Duration;

use crate::ParseErrorKind;

/// Configuration for retry behavior
#[derive(Debug, Clone, PartialEq)]
pub struct RetryConfig {
    /// Number of retry attempts
    pub attempts: usize,
    /// Duration to wait between retries
    pub backoff: Duration,
}

/// Parse retry configuration from tokens
///
/// The retry configuration is optional and can be specified as:
///
/// ```text
/// ... retry 3 backoff 1s
/// ```
pub(crate) fn parse_retry_config(tokens: &[&str]) -> Result<Option<RetryConfig>, ParseErrorKind> {
    if tokens.is_empty() {
        return Ok(None);
    }

    let mut iter = tokens.iter().peekable();

    // Check if we have retry clause
    match iter.next() {
        Some(&"retry") => {}
        Some(token) => return Err(ParseErrorKind::UnexpectedToken(token.to_string())),
        None => return Ok(None),
    }

    // Parse number of attempts
    let attempts = match iter.next() {
        Some(attempts_str) => attempts_str
            .parse::<usize>()
            .map_err(|_| ParseErrorKind::InvalidNumber(attempts_str.to_string()))?,
        None => {
            return Err(ParseErrorKind::InvalidRetryConfig(
                "expected a positive number of attempts".to_string(),
            ))
        }
    };

    if attempts == 0 {
        return Err(ParseErrorKind::InvalidRetryConfig(
            "attempt must be greater than 0".to_string(),
        ));
    }

    // Expect "backoff" keyword
    match iter.next() {
        Some(&"backoff") => {}
        Some(token) => return Err(ParseErrorKind::UnexpectedToken(token.to_string())),
        None => {
            return Err(ParseErrorKind::InvalidRetryConfig("expected keyword backoff".to_string()))
        }
    }

    // Parse backoff duration
    let duration_str = match iter.next() {
        Some(s) => s,
        None => {
            return Err(ParseErrorKind::InvalidRetryConfig("expected backoff duration".to_string()))
        }
    };

    let backoff = humantime::parse_duration(duration_str)
        .map_err(|_| ParseErrorKind::InvalidDuration(duration_str.to_string()))?;

    // No more tokens should be present
    if iter.next().is_some() {
        return Err(ParseErrorKind::UnexpectedToken("extra tokens".to_string()));
    }

    Ok(Some(RetryConfig { attempts, backoff }))
}
