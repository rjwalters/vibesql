//! Locale detection from environment variables.
//!
//! This module detects the user's preferred locale by checking environment
//! variables in the following order:
//!
//! 1. `VIBESQL_LANG` - VibeSQL-specific override
//! 2. `LC_ALL` - POSIX locale override
//! 3. `LC_MESSAGES` - POSIX messages locale
//! 4. `LANG` - POSIX default locale
//!
//! If no locale is found, defaults to "en-US".

use std::env;

/// List of available locales (embedded in the binary)
const AVAILABLE_LOCALES: &[&str] = &["en-US", "es", "pt-BR", "zh-CN", "ja", "de"];

/// Detect the user's preferred locale from environment variables.
///
/// Checks environment variables in priority order:
/// 1. `VIBESQL_LANG`
/// 2. `LC_ALL`
/// 3. `LC_MESSAGES`
/// 4. `LANG`
///
/// The detected locale is normalized and validated against available locales.
/// If no match is found, defaults to "en-US".
///
/// # Example
///
/// ```text
/// use vibesql_l10n::detect_locale;
///
/// let locale = detect_locale();
/// println!("Detected locale: {}", locale);
/// ```
pub fn detect_locale() -> String {
    // Check environment variables in priority order
    let env_vars = ["VIBESQL_LANG", "LC_ALL", "LC_MESSAGES", "LANG"];

    for var in &env_vars {
        if let Ok(value) = env::var(var) {
            if let Some(locale) = normalize_locale(&value) {
                return locale;
            }
        }
    }

    // Default to English
    "en-US".to_string()
}

/// Normalize a locale string and match it to an available locale.
///
/// Handles various locale formats:
/// - "en_US.UTF-8" -> "en-US"
/// - "en-US" -> "en-US"
/// - "en" -> "en-US" (if en-US is available)
/// - "C" or "POSIX" -> None (skip these)
fn normalize_locale(input: &str) -> Option<String> {
    // Skip POSIX/C locale
    if input == "C" || input == "POSIX" || input.is_empty() {
        return None;
    }

    // Extract the language and region, ignoring encoding suffix
    // e.g., "en_US.UTF-8" -> "en_US"
    let base = input.split('.').next().unwrap_or(input);

    // Normalize separator: underscore to hyphen
    let normalized = base.replace('_', "-");

    // Try exact match first
    if AVAILABLE_LOCALES.contains(&normalized.as_str()) {
        return Some(normalized);
    }

    // Try language-only match (e.g., "en" matches "en-US")
    let language = normalized.split('-').next().unwrap_or(&normalized);
    for locale in AVAILABLE_LOCALES {
        if locale.starts_with(language) {
            return Some(locale.to_string());
        }
    }

    // No match found - will fall back to default
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_locale_exact_match() {
        assert_eq!(normalize_locale("en-US"), Some("en-US".to_string()));
    }

    #[test]
    fn test_normalize_locale_underscore() {
        assert_eq!(normalize_locale("en_US"), Some("en-US".to_string()));
    }

    #[test]
    fn test_normalize_locale_with_encoding() {
        assert_eq!(normalize_locale("en_US.UTF-8"), Some("en-US".to_string()));
    }

    #[test]
    fn test_normalize_locale_language_only() {
        assert_eq!(normalize_locale("en"), Some("en-US".to_string()));
    }

    #[test]
    fn test_normalize_locale_posix() {
        assert_eq!(normalize_locale("C"), None);
        assert_eq!(normalize_locale("POSIX"), None);
    }

    #[test]
    fn test_normalize_locale_empty() {
        assert_eq!(normalize_locale(""), None);
    }

    #[test]
    fn test_normalize_locale_unknown() {
        // Unknown locales return None (will fall back to default)
        assert_eq!(normalize_locale("xx-YY"), None);
    }

    #[test]
    fn test_detect_locale_default() {
        // When no env vars are set, should return default
        // Note: This test may be affected by the actual environment
        let locale = detect_locale();
        assert!(!locale.is_empty());
    }
}
