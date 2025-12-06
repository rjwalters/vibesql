//! Localization support for VibeSQL using Project Fluent.
//!
//! This crate provides internationalization (i18n) and localization (l10n) support
//! for VibeSQL CLI and related tools.
//!
//! # Usage
//!
//! ```rust,ignore
//! use vibesql_l10n::{init, format, vibe_msg};
//!
//! // Initialize with system locale detection
//! init(None).unwrap();
//!
//! // Or specify a locale explicitly
//! init(Some("es")).unwrap();
//!
//! // Format a simple message
//! let banner = format("cli-banner", None);
//!
//! // Format a message with arguments using the macro
//! let rows = vibe_msg!("rows-with-time", count = 42, time = "0.123");
//! ```

mod detection;
mod loader;

pub use detection::detect_locale;
pub use loader::L10nError;

// Re-export fluent for use by the vibe_msg! macro in downstream crates
pub use fluent;

use std::cell::RefCell;
use std::sync::RwLock;

use fluent::{FluentArgs, FluentBundle, FluentResource};
use once_cell::sync::Lazy;
use rust_embed::RustEmbed;
use unic_langid::LanguageIdentifier;

/// Embedded Fluent translation resources
#[derive(RustEmbed)]
#[folder = "resources/"]
#[prefix = ""]
struct Resources;

/// Global locale setting (thread-safe string)
static LOCALE: Lazy<RwLock<String>> = Lazy::new(|| {
    RwLock::new(detection::detect_locale())
});

// Thread-local FluentBundle (not Sync, so we use thread-local storage)
thread_local! {
    static BUNDLE: RefCell<Option<FluentBundle<FluentResource>>> = const { RefCell::new(None) };
}

/// Load or create the Fluent bundle for the current thread
fn get_or_init_bundle<F, R>(f: F) -> R
where
    F: FnOnce(&FluentBundle<FluentResource>) -> R,
{
    BUNDLE.with(|bundle| {
        let mut bundle_ref = bundle.borrow_mut();
        if bundle_ref.is_none() {
            let locale = LOCALE.read().map(|l| l.clone()).unwrap_or_else(|_| "en-US".to_string());
            match create_bundle(&locale) {
                Ok(new_bundle) => {
                    *bundle_ref = Some(new_bundle);
                }
                Err(_) => {
                    // Fall back to English if the requested locale fails
                    if let Ok(fallback) = create_bundle("en-US") {
                        *bundle_ref = Some(fallback);
                    }
                }
            }
        }
        match bundle_ref.as_ref() {
            Some(b) => f(b),
            None => {
                // Create an empty bundle as last resort
                let empty = FluentBundle::new(vec!["en-US".parse().unwrap()]);
                f(&empty)
            }
        }
    })
}

/// List of FTL resource files to load for each locale.
/// These files are loaded in order and merged into a single bundle.
const RESOURCE_FILES: &[&str] = &["cli.ftl", "parser.ftl", "storage.ftl", "catalog.ftl", "executor.ftl"];

/// Create a new FluentBundle for the given locale
fn create_bundle(locale_str: &str) -> Result<FluentBundle<FluentResource>, L10nError> {
    let locale: LanguageIdentifier = locale_str
        .parse()
        .map_err(|_| L10nError::InvalidLocale(locale_str.to_string()))?;

    let mut bundle = FluentBundle::new(vec![locale]);
    // Disable Unicode isolation characters (used for bidirectional text support)
    // Not needed for database error messages and causes test failures
    bundle.set_use_isolating(false);

    // Load all resource files for this locale
    for file_name in RESOURCE_FILES {
        let resource_path = format!("{}/{}", locale_str, file_name);
        let fallback_path = format!("en-US/{}", file_name);

        // Try the requested locale first, then fall back to en-US
        let ftl_content = match Resources::get(&resource_path) {
            Some(content) => content,
            None => {
                // Try fallback locale
                match Resources::get(&fallback_path) {
                    Some(content) => content,
                    None => {
                        // Skip if neither locale has this file (it's optional)
                        continue;
                    }
                }
            }
        };

        let ftl_string = std::str::from_utf8(ftl_content.data.as_ref())
            .map_err(|e| L10nError::ParseError(e.to_string()))?
            .to_string();

        let resource = FluentResource::try_new(ftl_string)
            .map_err(|(_, errors)| L10nError::ParseError(format!("{:?}", errors)))?;

        // Ignore errors from duplicate message IDs (later files override earlier ones)
        let _ = bundle.add_resource(resource);
    }

    Ok(bundle)
}

/// Initialize the localization system with an optional locale override.
///
/// If `locale` is `None`, the system locale will be detected from environment
/// variables (`VIBESQL_LANG`, `LC_ALL`, `LC_MESSAGES`, `LANG`).
///
/// # Arguments
///
/// * `locale` - Optional locale identifier (e.g., "en-US", "es", "ja")
///
/// # Errors
///
/// Returns `L10nError` if the locale is invalid or resources cannot be loaded.
///
/// # Example
///
/// ```rust,ignore
/// use vibesql_l10n::init;
///
/// // Use system locale
/// init(None)?;
///
/// // Or specify explicitly
/// init(Some("es"))?;
/// ```
pub fn init(locale: Option<&str>) -> Result<(), L10nError> {
    let locale_str = locale.map(String::from).unwrap_or_else(detection::detect_locale);

    // Validate the locale can be parsed
    let _: LanguageIdentifier = locale_str
        .parse()
        .map_err(|_| L10nError::InvalidLocale(locale_str.clone()))?;

    // Update global locale setting
    let mut global_locale = LOCALE.write().map_err(|_| L10nError::LockError)?;
    *global_locale = locale_str.clone();

    // Clear the thread-local bundle so it gets recreated with new locale
    BUNDLE.with(|bundle| {
        *bundle.borrow_mut() = None;
    });

    Ok(())
}

/// Format a localized message with optional arguments.
///
/// If the message ID is not found, the message ID itself is returned as a fallback.
///
/// # Arguments
///
/// * `msg_id` - The Fluent message identifier
/// * `args` - Optional arguments for message placeholders
///
/// # Example
///
/// ```rust,ignore
/// use vibesql_l10n::format;
/// use fluent::FluentArgs;
///
/// // Simple message
/// let goodbye = format("cli-goodbye", None);
///
/// // Message with arguments
/// let mut args = FluentArgs::new();
/// args.set("count", 42);
/// let rows = format("rows-count", Some(&args));
/// ```
pub fn format(msg_id: &str, args: Option<&FluentArgs>) -> String {
    get_or_init_bundle(|bundle| {
        let msg = match bundle.get_message(msg_id) {
            Some(m) => m,
            None => return msg_id.to_string(),
        };

        let pattern = match msg.value() {
            Some(p) => p,
            None => return msg_id.to_string(),
        };

        let mut errors = Vec::new();
        let result = bundle.format_pattern(pattern, args, &mut errors);

        if !errors.is_empty() {
            // Log errors in debug mode but still return the result
            #[cfg(debug_assertions)]
            eprintln!("l10n format errors for '{}': {:?}", msg_id, errors);
        }

        result.to_string()
    })
}

/// Get the currently active locale identifier.
///
/// # Example
///
/// ```rust,ignore
/// use vibesql_l10n::current_locale;
///
/// let locale = current_locale();
/// println!("Current locale: {}", locale);
/// ```
pub fn current_locale() -> LanguageIdentifier {
    LOCALE.read()
        .map(|l| l.parse().unwrap_or_else(|_| "en-US".parse().unwrap()))
        .unwrap_or_else(|_| "en-US".parse().unwrap())
}

/// Convenience macro for formatting localized messages.
///
/// # Examples
///
/// ```rust,ignore
/// use vibesql_l10n::vibe_msg;
///
/// // Simple message without arguments
/// let msg = vibe_msg!("cli-goodbye");
///
/// // Message with named arguments
/// let msg = vibe_msg!("rows-with-time", count = 42, time = "0.123");
/// ```
#[macro_export]
macro_rules! vibe_msg {
    ($id:literal) => {
        $crate::format($id, None)
    };
    ($id:literal, $($key:ident = $value:expr),+ $(,)?) => {{
        let mut args = $crate::fluent::FluentArgs::new();
        $(
            args.set(stringify!($key), $value);
        )+
        $crate::format($id, Some(&args))
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_initialization() {
        // The lazy static should initialize without panicking
        let locale = current_locale();
        assert!(!locale.to_string().is_empty());
    }

    #[test]
    fn test_resources_embedded() {
        // Check that resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(!files.is_empty(), "No resources embedded");
        assert!(files.iter().any(|f| f.contains("cli.ftl")), "cli.ftl not found in: {:?}", files);
    }

    #[test]
    fn test_format_simple_message() {
        // Initialize with English
        init(Some("en-US")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "Goodbye!");
    }

    #[test]
    fn test_format_message_with_args() {
        init(Some("en-US")).unwrap();

        let mut args = FluentArgs::new();
        args.set("count", 42);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("42"));
    }

    #[test]
    fn test_fallback_for_unknown_message() {
        init(Some("en-US")).unwrap();

        let result = format("nonexistent-message-id", None);
        assert_eq!(result, "nonexistent-message-id");
    }

    #[test]
    fn test_vibe_msg_macro() {
        init(Some("en-US")).unwrap();

        let goodbye = vibe_msg!("cli-goodbye");
        assert_eq!(goodbye, "Goodbye!");
    }

    #[test]
    fn test_vibe_msg_macro_with_args() {
        init(Some("en-US")).unwrap();

        let result = vibe_msg!("rows-count", count = 5);
        assert!(result.contains("5"));
    }

    #[test]
    fn test_parser_resources_loaded() {
        init(Some("en-US")).unwrap();

        // Test parser.ftl messages are accessible
        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "Unterminated string literal");

        let result = format("lexer-empty-delimited-identifier", None);
        assert_eq!(result, "Empty delimited identifier is not allowed");
    }

    #[test]
    fn test_parser_message_with_args() {
        init(Some("en-US")).unwrap();

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("Unexpected character"));
    }

    #[test]
    fn test_spanish_locale() {
        init(Some("es")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "¡Hasta luego!");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("filas"));
    }

    #[test]
    fn test_spanish_parser_messages() {
        init(Some("es")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "Literal de cadena sin terminar");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("Carácter inesperado"));
    }

    #[test]
    fn test_spanish_resources_embedded() {
        // Check that Spanish resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("es/")), "Spanish resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "es/cli.ftl"), "es/cli.ftl not found");
        assert!(files.iter().any(|f| f == "es/parser.ftl"), "es/parser.ftl not found");
    }

    #[test]
    fn test_portuguese_locale() {
        init(Some("pt-BR")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "Até logo!");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("linhas"));
    }

    #[test]
    fn test_portuguese_parser_messages() {
        init(Some("pt-BR")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "Literal de string não terminado");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("Caractere inesperado"));
    }

    #[test]
    fn test_portuguese_resources_embedded() {
        // Check that Portuguese (Brazilian) resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("pt-BR/")), "Portuguese resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "pt-BR/cli.ftl"), "pt-BR/cli.ftl not found");
        assert!(files.iter().any(|f| f == "pt-BR/parser.ftl"), "pt-BR/parser.ftl not found");
        assert!(files.iter().any(|f| f == "pt-BR/executor.ftl"), "pt-BR/executor.ftl not found");
        assert!(files.iter().any(|f| f == "pt-BR/storage.ftl"), "pt-BR/storage.ftl not found");
        assert!(files.iter().any(|f| f == "pt-BR/catalog.ftl"), "pt-BR/catalog.ftl not found");
    }
}
