//! Structured debug output infrastructure
//!
//! This module provides a unified interface for debug/profiling output that can emit
//! either human-readable text (default) or machine-parseable JSON.
//!
//! # Environment Variables
//!
//! - `VIBESQL_DEBUG_FORMAT=json` - Output JSON to stderr (for agents/CI)
//! - `VIBESQL_DEBUG_FORMAT=text` - Output human-readable text (default)
//!
//! # JSON Schema
//!
//! All JSON output follows this structure:
//! ```json
//! {
//!   "timestamp": "2024-01-15T10:30:00.123Z",
//!   "category": "optimizer",
//!   "event": "join_reorder",
//!   "data": { ... }
//! }
//! ```
//!
//! # Categories
//!
//! - `optimizer` - Query optimization decisions (join reorder, table elimination, etc.)
//! - `execution` - Execution timing and statistics
//! - `index` - Index selection and usage
//! - `dml` - DML operation timing (insert, update, delete)
//! - `profile` - General profiling output

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Output format for debug messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum DebugFormat {
    /// Human-readable text output (default)
    #[default]
    Text = 0,
    /// Machine-parseable JSON output
    Json = 1,
}

/// Global format setting (0 = Text, 1 = Json)
static DEBUG_FORMAT: AtomicU8 = AtomicU8::new(0);

/// Initialize debug format from environment variable.
/// Call this once at program start.
pub fn init() {
    if let Ok(format) = std::env::var("VIBESQL_DEBUG_FORMAT") {
        match format.to_lowercase().as_str() {
            "json" => DEBUG_FORMAT.store(1, Ordering::Relaxed),
            "text" | "" => DEBUG_FORMAT.store(0, Ordering::Relaxed),
            _ => {
                eprintln!(
                    "[WARNING] Unknown VIBESQL_DEBUG_FORMAT='{}', using 'text'",
                    format
                );
                DEBUG_FORMAT.store(0, Ordering::Relaxed);
            }
        }
    }
}

/// Get the current debug output format
pub fn get_format() -> DebugFormat {
    match DEBUG_FORMAT.load(Ordering::Relaxed) {
        1 => DebugFormat::Json,
        _ => DebugFormat::Text,
    }
}

/// Check if JSON output is enabled
pub fn is_json() -> bool {
    DEBUG_FORMAT.load(Ordering::Relaxed) == 1
}

/// Debug output categories
#[derive(Debug, Clone, Copy)]
pub enum Category {
    /// Query optimization decisions
    Optimizer,
    /// Execution timing and statistics
    Execution,
    /// Index selection and usage
    Index,
    /// DML operation timing
    Dml,
    /// General profiling
    Profile,
}

impl Category {
    pub fn as_str(&self) -> &'static str {
        match self {
            Category::Optimizer => "optimizer",
            Category::Execution => "execution",
            Category::Index => "index",
            Category::Dml => "dml",
            Category::Profile => "profile",
        }
    }
}

/// Get current timestamp in ISO 8601 format
fn iso_timestamp() -> String {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();

    // Convert to date/time components (simplified UTC)
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simplified date calculation (good enough for debugging purposes)
    // This handles dates from 1970-2099 reasonably well
    let mut year = 1970;
    let mut remaining_days = days_since_epoch as i64;

    loop {
        let days_in_year = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
            366
        } else {
            365
        };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_months: [i64; 12] = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days in &days_in_months {
        if remaining_days < days {
            break;
        }
        remaining_days -= days;
        month += 1;
    }
    let day = remaining_days + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, hours, minutes, seconds, millis
    )
}

/// Escape a string for JSON output
fn json_escape(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result
}

/// A builder for constructing debug output with optional JSON fields
pub struct DebugEvent {
    category: Category,
    event: &'static str,
    tag: &'static str,
    text_parts: Vec<String>,
    json_fields: Vec<(String, JsonValue)>,
}

/// JSON value types (simplified, no external dependency)
pub enum JsonValue {
    String(String),
    Number(f64),
    Int(i64),
    Bool(bool),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
    Null,
}

impl JsonValue {
    /// Format as JSON string
    pub fn to_json(&self) -> String {
        match self {
            JsonValue::String(s) => format!("\"{}\"", json_escape(s)),
            JsonValue::Number(n) => {
                if n.is_finite() {
                    format!("{}", n)
                } else {
                    "null".to_string()
                }
            }
            JsonValue::Int(n) => format!("{}", n),
            JsonValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            JsonValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_json()).collect();
                format!("[{}]", items.join(","))
            }
            JsonValue::Object(fields) => {
                let items: Vec<String> = fields
                    .iter()
                    .map(|(k, v)| format!("\"{}\":{}", json_escape(k), v.to_json()))
                    .collect();
                format!("{{{}}}", items.join(","))
            }
            JsonValue::Null => "null".to_string(),
        }
    }
}

impl DebugEvent {
    /// Create a new debug event
    pub fn new(category: Category, event: &'static str, tag: &'static str) -> Self {
        Self {
            category,
            event,
            tag,
            text_parts: Vec::new(),
            json_fields: Vec::new(),
        }
    }

    /// Add a text message (for human-readable output)
    pub fn text(mut self, message: impl Into<String>) -> Self {
        self.text_parts.push(message.into());
        self
    }

    /// Add a JSON field (for machine-readable output)
    pub fn field(mut self, name: impl Into<String>, value: JsonValue) -> Self {
        self.json_fields.push((name.into(), value));
        self
    }

    /// Add a string field
    pub fn field_str(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.field(name, JsonValue::String(value.into()))
    }

    /// Add an integer field
    pub fn field_int(self, name: impl Into<String>, value: i64) -> Self {
        self.field(name, JsonValue::Int(value))
    }

    /// Add a float field
    pub fn field_float(self, name: impl Into<String>, value: f64) -> Self {
        self.field(name, JsonValue::Number(value))
    }

    /// Add a boolean field
    pub fn field_bool(self, name: impl Into<String>, value: bool) -> Self {
        self.field(name, JsonValue::Bool(value))
    }

    /// Add duration in microseconds
    pub fn field_duration_us(self, name: impl Into<String>, duration: std::time::Duration) -> Self {
        self.field(name, JsonValue::Int(duration.as_micros() as i64))
    }

    /// Add duration in milliseconds (as float)
    pub fn field_duration_ms(self, name: impl Into<String>, duration: std::time::Duration) -> Self {
        self.field(
            name,
            JsonValue::Number(duration.as_secs_f64() * 1000.0),
        )
    }

    /// Add a string array field
    pub fn field_str_array(self, name: impl Into<String>, values: &[String]) -> Self {
        let arr: Vec<JsonValue> = values
            .iter()
            .map(|s| JsonValue::String(s.clone()))
            .collect();
        self.field(name, JsonValue::Array(arr))
    }

    /// Emit the debug event to stderr
    pub fn emit(self) {
        match get_format() {
            DebugFormat::Text => {
                // Traditional text output: [TAG] messages
                let message = self.text_parts.join(" ");
                eprintln!("[{}] {}", self.tag, message);
            }
            DebugFormat::Json => {
                // JSON output
                let timestamp = iso_timestamp();
                let mut fields = vec![
                    ("timestamp".to_string(), JsonValue::String(timestamp)),
                    (
                        "category".to_string(),
                        JsonValue::String(self.category.as_str().to_string()),
                    ),
                    (
                        "event".to_string(),
                        JsonValue::String(self.event.to_string()),
                    ),
                ];

                // Add data object with all custom fields
                if !self.json_fields.is_empty() {
                    fields.push(("data".to_string(), JsonValue::Object(self.json_fields)));
                }

                let json = JsonValue::Object(fields).to_json();
                eprintln!("{}", json);
            }
        }
    }
}

/// Convenience function to create a debug event
pub fn debug_event(category: Category, event: &'static str, tag: &'static str) -> DebugEvent {
    DebugEvent::new(category, event, tag)
}

/// Macro for creating debug events with both text and JSON output
///
/// # Example
///
/// ```text
/// debug_emit!(
///     optimizer, "join_reorder", "JOIN_REORDER",
///     text: "Optimal order: {:?}", optimal_order,
///     fields: {
///         "original_order" => json_str_array(&original_order),
///         "optimal_order" => json_str_array(&optimal_order),
///         "optimizer_time_us" => JsonValue::Int(time.as_micros() as i64)
///     }
/// );
/// ```
#[macro_export]
macro_rules! debug_emit {
    (
        $category:ident, $event:expr, $tag:expr,
        text: $($text_fmt:expr),* $(,)?
        $(, fields: { $($field_name:expr => $field_value:expr),* $(,)? })?
    ) => {{
        let mut event = $crate::debug_output::debug_event(
            $crate::debug_output::Category::$category,
            $event,
            $tag
        );
        event = event.text(format!($($text_fmt),*));
        $($(
            event = event.field($field_name, $field_value);
        )*)?
        event.emit();
    }};
}

// Re-export for use in macros
pub use crate::debug_emit;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_escape() {
        assert_eq!(json_escape("hello"), "hello");
        assert_eq!(json_escape("hello\"world"), "hello\\\"world");
        assert_eq!(json_escape("line\nbreak"), "line\\nbreak");
        assert_eq!(json_escape("tab\there"), "tab\\there");
    }

    #[test]
    fn test_json_value_formatting() {
        assert_eq!(JsonValue::String("test".to_string()).to_json(), "\"test\"");
        assert_eq!(JsonValue::Int(42).to_json(), "42");
        assert_eq!(JsonValue::Number(3.5).to_json(), "3.5");
        assert_eq!(JsonValue::Bool(true).to_json(), "true");
        assert_eq!(JsonValue::Null.to_json(), "null");

        let arr = JsonValue::Array(vec![JsonValue::Int(1), JsonValue::Int(2)]);
        assert_eq!(arr.to_json(), "[1,2]");

        let obj = JsonValue::Object(vec![
            ("name".to_string(), JsonValue::String("test".to_string())),
            ("value".to_string(), JsonValue::Int(42)),
        ]);
        assert_eq!(obj.to_json(), "{\"name\":\"test\",\"value\":42}");
    }

    #[test]
    fn test_debug_event_builder() {
        // Just test that the builder compiles and doesn't panic
        let event = DebugEvent::new(Category::Optimizer, "test_event", "TEST")
            .text("Test message")
            .field_str("key", "value")
            .field_int("count", 42)
            .field_float("ratio", 0.5)
            .field_bool("enabled", true);

        // Don't emit in tests to avoid polluting output
        drop(event);
    }

    #[test]
    fn test_iso_timestamp_format() {
        let ts = iso_timestamp();
        // Should match pattern: YYYY-MM-DDTHH:MM:SS.mmmZ
        assert!(ts.len() == 24, "Timestamp should be 24 chars: {}", ts);
        assert!(ts.ends_with('Z'), "Timestamp should end with Z: {}", ts);
        assert!(ts.contains('T'), "Timestamp should contain T: {}", ts);
    }
}
