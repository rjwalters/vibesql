//! Localization support for VibeSQL using Project Fluent.
//!
//! This crate provides internationalization (i18n) and localization (l10n) support
//! for VibeSQL CLI and related tools.
//!
//! # Usage
//!
//! ```text
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
/// ```text
/// use vibesql_l10n::init;
///
/// // Use system locale
/// init(None).unwrap();
///
/// // Or specify explicitly
/// init(Some("es")).unwrap();
/// ```
pub fn init(locale: Option<&str>) -> Result<(), L10nError> {
    let locale_str = locale.map(String::from).unwrap_or_else(detection::detect_locale);

    // Validate the locale can be parsed
    let _: LanguageIdentifier = locale_str
        .parse()
        .map_err(|_| L10nError::InvalidLocale(locale_str.clone()))?;

    // Create the bundle eagerly to avoid race conditions in parallel tests.
    // This ensures we use the locale passed to init() rather than reading
    // the global LOCALE later (which another thread may have changed).
    let new_bundle = create_bundle(&locale_str)?;

    // Update global locale setting
    let mut global_locale = LOCALE.write().map_err(|_| L10nError::LockError)?;
    *global_locale = locale_str.clone();

    // Set the thread-local bundle with the new bundle created above
    BUNDLE.with(|bundle| {
        *bundle.borrow_mut() = Some(new_bundle);
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
/// ```text
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
/// ```text
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
/// ```text
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

    #[test]
    fn test_chinese_locale() {
        init(Some("zh-CN")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "再见！");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("行"));
    }

    #[test]
    fn test_chinese_parser_messages() {
        init(Some("zh-CN")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "未终止的字符串字面量");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("意外的字符"));
    }

    #[test]
    fn test_chinese_resources_embedded() {
        // Check that Chinese resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("zh-CN/")), "Chinese resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "zh-CN/cli.ftl"), "zh-CN/cli.ftl not found");
        assert!(files.iter().any(|f| f == "zh-CN/parser.ftl"), "zh-CN/parser.ftl not found");
        assert!(files.iter().any(|f| f == "zh-CN/executor.ftl"), "zh-CN/executor.ftl not found");
        assert!(files.iter().any(|f| f == "zh-CN/storage.ftl"), "zh-CN/storage.ftl not found");
        assert!(files.iter().any(|f| f == "zh-CN/catalog.ftl"), "zh-CN/catalog.ftl not found");
    }

    #[test]
    fn test_chinese_executor_messages() {
        init(Some("zh-CN")).unwrap();

        let result = vibe_msg!("executor-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("未找到表"));

        let result = vibe_msg!("executor-division-by-zero");
        assert_eq!(result, "除以零");
    }

    #[test]
    fn test_chinese_storage_messages() {
        init(Some("zh-CN")).unwrap();

        let result = vibe_msg!("storage-column-count-mismatch", expected = 3, actual = 5);
        assert!(result.contains("3"));
        assert!(result.contains("5"));
        assert!(result.contains("列数不匹配"));
    }

    #[test]
    fn test_chinese_catalog_messages() {
        init(Some("zh-CN")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "orders");
        assert!(result.contains("orders"));
        assert!(result.contains("已存在"));
    }

    #[test]
    fn test_japanese_locale() {
        init(Some("ja")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "さようなら！");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("行"));
    }

    #[test]
    fn test_japanese_parser_messages() {
        init(Some("ja")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "文字列リテラルが閉じられていません");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("予期しない文字"));
    }

    #[test]
    fn test_japanese_resources_embedded() {
        // Check that Japanese resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("ja/")), "Japanese resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "ja/cli.ftl"), "ja/cli.ftl not found");
        assert!(files.iter().any(|f| f == "ja/parser.ftl"), "ja/parser.ftl not found");
        assert!(files.iter().any(|f| f == "ja/executor.ftl"), "ja/executor.ftl not found");
        assert!(files.iter().any(|f| f == "ja/storage.ftl"), "ja/storage.ftl not found");
        assert!(files.iter().any(|f| f == "ja/catalog.ftl"), "ja/catalog.ftl not found");
    }

    #[test]
    fn test_japanese_executor_messages() {
        init(Some("ja")).unwrap();

        let result = vibe_msg!("executor-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("見つかりません"));

        let result = vibe_msg!("executor-division-by-zero");
        assert_eq!(result, "ゼロによる除算");
    }

    #[test]
    fn test_japanese_catalog_messages() {
        init(Some("ja")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("すでに存在します"));
    }

    #[test]
    fn test_japanese_storage_messages() {
        init(Some("ja")).unwrap();

        let result = vibe_msg!("storage-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("見つかりません"));
    }

    #[test]
    fn test_french_locale() {
        init(Some("fr")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "Au revoir !");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("lignes"));
    }

    #[test]
    fn test_french_parser_messages() {
        init(Some("fr")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "Chaîne littérale non terminée");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("Caractère inattendu"));
    }

    #[test]
    fn test_french_resources_embedded() {
        // Check that French resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("fr/")), "French resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "fr/cli.ftl"), "fr/cli.ftl not found");
        assert!(files.iter().any(|f| f == "fr/parser.ftl"), "fr/parser.ftl not found");
        assert!(files.iter().any(|f| f == "fr/executor.ftl"), "fr/executor.ftl not found");
        assert!(files.iter().any(|f| f == "fr/storage.ftl"), "fr/storage.ftl not found");
        assert!(files.iter().any(|f| f == "fr/catalog.ftl"), "fr/catalog.ftl not found");
    }

    #[test]
    fn test_french_executor_messages() {
        init(Some("fr")).unwrap();

        let result = vibe_msg!("executor-division-by-zero");
        assert_eq!(result, "Division par zéro");

        let result = vibe_msg!("executor-table-not-found", name = "utilisateurs");
        assert!(result.contains("utilisateurs"));
        assert!(result.contains("introuvable"));
    }

    #[test]
    fn test_french_storage_messages() {
        init(Some("fr")).unwrap();

        let result = vibe_msg!("storage-row-not-found");
        assert_eq!(result, "Ligne introuvable");

        let result = vibe_msg!("storage-column-count-mismatch", expected = 3, actual = 5);
        assert!(result.contains("3"));
        assert!(result.contains("5"));
    }

    #[test]
    fn test_french_catalog_messages() {
        init(Some("fr")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "produits");
        assert!(result.contains("produits"));
        assert!(result.contains("existe déjà"));
    }

    #[test]
    fn test_german_locale() {
        init(Some("de")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "Auf Wiedersehen!");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("Zeilen"));
    }

    #[test]
    fn test_german_parser_messages() {
        init(Some("de")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "Nicht abgeschlossenes Zeichenkettenliteral");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("Unerwartetes Zeichen"));
    }

    #[test]
    fn test_german_executor_messages() {
        init(Some("de")).unwrap();

        let result = vibe_msg!("executor-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("nicht gefunden"));

        let result = format("executor-division-by-zero", None);
        assert_eq!(result, "Division durch Null");
    }

    #[test]
    fn test_german_storage_messages() {
        init(Some("de")).unwrap();

        let result = vibe_msg!("storage-table-not-found", name = "products");
        assert!(result.contains("products"));
        assert!(result.contains("nicht gefunden"));

        let result = format("storage-row-not-found", None);
        assert_eq!(result, "Zeile nicht gefunden");
    }

    #[test]
    fn test_german_catalog_messages() {
        init(Some("de")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "orders");
        assert!(result.contains("orders"));
        assert!(result.contains("existiert bereits"));

        let result = vibe_msg!("catalog-schema-not-found", name = "myschema");
        assert!(result.contains("myschema"));
        assert!(result.contains("nicht gefunden"));
    }

    #[test]
    fn test_german_resources_embedded() {
        // Check that German resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("de/")), "German resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "de/cli.ftl"), "de/cli.ftl not found");
        assert!(files.iter().any(|f| f == "de/parser.ftl"), "de/parser.ftl not found");
        assert!(files.iter().any(|f| f == "de/executor.ftl"), "de/executor.ftl not found");
        assert!(files.iter().any(|f| f == "de/storage.ftl"), "de/storage.ftl not found");
        assert!(files.iter().any(|f| f == "de/catalog.ftl"), "de/catalog.ftl not found");
    }

    #[test]
    fn test_german_umlaut_handling() {
        init(Some("de")).unwrap();

        // Test messages with umlauts are correctly handled
        let result = vibe_msg!("executor-schema-not-empty", name = "öffentlich");
        assert!(result.contains("öffentlich"));
        assert!(result.contains("kann nicht gelöscht werden"));

        // Test the help hint with German characters
        let result = format("cli-help-hint", None);
        assert!(result.contains("Geben Sie"));
    }

    #[test]
    fn test_korean_locale() {
        init(Some("ko")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "안녕히 가세요!");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("행"));
    }

    #[test]
    fn test_korean_parser_messages() {
        init(Some("ko")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "종료되지 않은 문자열 리터럴");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("예기치 않은 문자"));
    }

    #[test]
    fn test_korean_resources_embedded() {
        // Check that Korean resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("ko/")), "Korean resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "ko/cli.ftl"), "ko/cli.ftl not found");
        assert!(files.iter().any(|f| f == "ko/parser.ftl"), "ko/parser.ftl not found");
        assert!(files.iter().any(|f| f == "ko/executor.ftl"), "ko/executor.ftl not found");
        assert!(files.iter().any(|f| f == "ko/storage.ftl"), "ko/storage.ftl not found");
        assert!(files.iter().any(|f| f == "ko/catalog.ftl"), "ko/catalog.ftl not found");
    }

    #[test]
    fn test_korean_executor_messages() {
        init(Some("ko")).unwrap();

        let result = vibe_msg!("executor-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("찾을 수 없습니다"));

        let result = vibe_msg!("executor-division-by-zero");
        assert_eq!(result, "0으로 나누기");
    }

    #[test]
    fn test_korean_storage_messages() {
        init(Some("ko")).unwrap();

        let result = vibe_msg!("storage-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("찾을 수 없습니다"));

        let result = format("storage-row-not-found", None);
        assert_eq!(result, "행을 찾을 수 없습니다");
    }

    #[test]
    fn test_korean_catalog_messages() {
        init(Some("ko")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "orders");
        assert!(result.contains("orders"));
        assert!(result.contains("이미 존재합니다"));

        let result = vibe_msg!("catalog-schema-not-found", name = "myschema");
        assert!(result.contains("myschema"));
        assert!(result.contains("찾을 수 없습니다"));
    }

    #[test]
    fn test_korean_hangul_handling() {
        init(Some("ko")).unwrap();

        // Test messages with Hangul are correctly handled
        let result = vibe_msg!("executor-schema-not-empty", name = "공개");
        assert!(result.contains("공개"));
        assert!(result.contains("삭제할 수 없습니다"));

        // Test the help hint with Korean characters
        let result = format("cli-help-hint", None);
        assert!(result.contains("\\help"));
        assert!(result.contains("도움말"));
    }

    #[test]
    fn test_indonesian_locale() {
        init(Some("id")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "Sampai jumpa!");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("baris"));
    }

    #[test]
    fn test_indonesian_parser_messages() {
        init(Some("id")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "Literal string tidak diakhiri");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("Karakter tidak terduga"));
    }

    #[test]
    fn test_indonesian_resources_embedded() {
        // Check that Indonesian resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("id/")), "Indonesian resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "id/cli.ftl"), "id/cli.ftl not found");
        assert!(files.iter().any(|f| f == "id/parser.ftl"), "id/parser.ftl not found");
        assert!(files.iter().any(|f| f == "id/executor.ftl"), "id/executor.ftl not found");
        assert!(files.iter().any(|f| f == "id/storage.ftl"), "id/storage.ftl not found");
        assert!(files.iter().any(|f| f == "id/catalog.ftl"), "id/catalog.ftl not found");
    }

    #[test]
    fn test_indonesian_executor_messages() {
        init(Some("id")).unwrap();

        let result = vibe_msg!("executor-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("tidak ditemukan"));

        let result = vibe_msg!("executor-division-by-zero");
        assert_eq!(result, "Pembagian dengan nol");
    }

    #[test]
    fn test_indonesian_storage_messages() {
        init(Some("id")).unwrap();

        let result = vibe_msg!("storage-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("tidak ditemukan"));

        let result = format("storage-row-not-found", None);
        assert_eq!(result, "Baris tidak ditemukan");
    }

    #[test]
    fn test_indonesian_catalog_messages() {
        init(Some("id")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "orders");
        assert!(result.contains("orders"));
        assert!(result.contains("sudah ada"));

        let result = vibe_msg!("catalog-schema-not-found", name = "myschema");
        assert!(result.contains("myschema"));
        assert!(result.contains("tidak ditemukan"));
    }

    #[test]
    fn test_swedish_locale() {
        init(Some("sv")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "Hej då!");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("rader"));
    }

    #[test]
    fn test_swedish_parser_messages() {
        init(Some("sv")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "Oavslutad stränglitteral");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("Oväntat tecken"));
    }

    #[test]
    fn test_swedish_resources_embedded() {
        // Check that Swedish resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("sv/")), "Swedish resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "sv/cli.ftl"), "sv/cli.ftl not found");
        assert!(files.iter().any(|f| f == "sv/parser.ftl"), "sv/parser.ftl not found");
        assert!(files.iter().any(|f| f == "sv/executor.ftl"), "sv/executor.ftl not found");
        assert!(files.iter().any(|f| f == "sv/storage.ftl"), "sv/storage.ftl not found");
        assert!(files.iter().any(|f| f == "sv/catalog.ftl"), "sv/catalog.ftl not found");
    }

    #[test]
    fn test_swedish_executor_messages() {
        init(Some("sv")).unwrap();

        let result = vibe_msg!("executor-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("hittades inte"));

        let result = vibe_msg!("executor-division-by-zero");
        assert_eq!(result, "Division med noll");
    }

    #[test]
    fn test_swedish_storage_messages() {
        init(Some("sv")).unwrap();

        let result = vibe_msg!("storage-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("hittades inte"));

        let result = format("storage-row-not-found", None);
        assert_eq!(result, "Rad hittades inte");
    }

    #[test]
    fn test_swedish_catalog_messages() {
        init(Some("sv")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "orders");
        assert!(result.contains("orders"));
        assert!(result.contains("finns redan"));

        let result = vibe_msg!("catalog-schema-not-found", name = "myschema");
        assert!(result.contains("myschema"));
        assert!(result.contains("hittades inte"));
    }

    #[test]
    fn test_swedish_special_characters() {
        init(Some("sv")).unwrap();

        // Test messages with Swedish characters (å, ä, ö) are correctly handled
        let result = vibe_msg!("executor-schema-not-empty", name = "offentlig");
        assert!(result.contains("offentlig"));
        assert!(result.contains("är inte tomt"));

        // Test the help hint with Swedish characters
        let result = format("cli-help-hint", None);
        assert!(result.contains("\\help"));
        assert!(result.contains("hjälp"));
    }

    #[test]
    fn test_thai_locale() {
        init(Some("th")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "ลาก่อน!");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("แถว"));
    }

    #[test]
    fn test_thai_parser_messages() {
        init(Some("th")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "String literal ไม่ถูกปิด");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("อักขระไม่คาดคิด"));
    }

    #[test]
    fn test_thai_resources_embedded() {
        // Check that Thai resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("th/")), "Thai resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "th/cli.ftl"), "th/cli.ftl not found");
        assert!(files.iter().any(|f| f == "th/parser.ftl"), "th/parser.ftl not found");
        assert!(files.iter().any(|f| f == "th/executor.ftl"), "th/executor.ftl not found");
        assert!(files.iter().any(|f| f == "th/storage.ftl"), "th/storage.ftl not found");
        assert!(files.iter().any(|f| f == "th/catalog.ftl"), "th/catalog.ftl not found");
    }

    #[test]
    fn test_thai_executor_messages() {
        init(Some("th")).unwrap();

        let result = vibe_msg!("executor-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("ไม่พบตาราง"));

        let result = vibe_msg!("executor-division-by-zero");
        assert_eq!(result, "หารด้วยศูนย์");
    }

    #[test]
    fn test_thai_storage_messages() {
        init(Some("th")).unwrap();

        let result = vibe_msg!("storage-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("ไม่พบตาราง"));

        let result = format("storage-row-not-found", None);
        assert_eq!(result, "ไม่พบแถว");
    }

    #[test]
    fn test_thai_catalog_messages() {
        init(Some("th")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "orders");
        assert!(result.contains("orders"));
        assert!(result.contains("มีอยู่แล้ว"));

        let result = vibe_msg!("catalog-schema-not-found", name = "myschema");
        assert!(result.contains("myschema"));
        assert!(result.contains("ไม่พบ Schema"));
    }

    #[test]
    fn test_thai_script_handling() {
        init(Some("th")).unwrap();

        // Test messages with Thai script are correctly handled
        let result = vibe_msg!("executor-schema-not-empty", name = "สาธารณะ");
        assert!(result.contains("สาธารณะ"));
        assert!(result.contains("ไม่สามารถลบ"));

        // Test the help hint with Thai characters
        let result = format("cli-help-hint", None);
        assert!(result.contains("\\help"));
        assert!(result.contains("ความช่วยเหลือ"));
    }

    #[test]
    fn test_vietnamese_locale() {
        init(Some("vi")).unwrap();

        let goodbye = format("cli-goodbye", None);
        assert_eq!(goodbye, "Tạm biệt!");

        let mut args = FluentArgs::new();
        args.set("count", 5);
        let result = format("rows-count", Some(&args));
        assert!(result.contains("5"));
        assert!(result.contains("hàng"));
    }

    #[test]
    fn test_vietnamese_parser_messages() {
        init(Some("vi")).unwrap();

        let result = format("lexer-unterminated-string", None);
        assert_eq!(result, "Chuỗi ký tự chưa kết thúc");

        let result = vibe_msg!("lexer-unexpected-character", character = "~");
        assert!(result.contains("~"));
        assert!(result.contains("Ký tự không mong đợi"));
    }

    #[test]
    fn test_vietnamese_resources_embedded() {
        // Check that Vietnamese resources are embedded
        let files: Vec<_> = Resources::iter().collect();
        assert!(files.iter().any(|f| f.starts_with("vi/")), "Vietnamese resources not found in: {:?}", files);
        assert!(files.iter().any(|f| f == "vi/cli.ftl"), "vi/cli.ftl not found");
        assert!(files.iter().any(|f| f == "vi/parser.ftl"), "vi/parser.ftl not found");
        assert!(files.iter().any(|f| f == "vi/executor.ftl"), "vi/executor.ftl not found");
        assert!(files.iter().any(|f| f == "vi/storage.ftl"), "vi/storage.ftl not found");
        assert!(files.iter().any(|f| f == "vi/catalog.ftl"), "vi/catalog.ftl not found");
    }

    #[test]
    fn test_vietnamese_executor_messages() {
        init(Some("vi")).unwrap();

        let result = vibe_msg!("executor-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("Không tìm thấy bảng"));

        let result = vibe_msg!("executor-division-by-zero");
        assert_eq!(result, "Chia cho số không");
    }

    #[test]
    fn test_vietnamese_storage_messages() {
        init(Some("vi")).unwrap();

        let result = vibe_msg!("storage-table-not-found", name = "users");
        assert!(result.contains("users"));
        assert!(result.contains("Không tìm thấy bảng"));

        let result = format("storage-row-not-found", None);
        assert_eq!(result, "Không tìm thấy hàng");
    }

    #[test]
    fn test_vietnamese_catalog_messages() {
        init(Some("vi")).unwrap();

        let result = vibe_msg!("catalog-table-already-exists", name = "orders");
        assert!(result.contains("orders"));
        assert!(result.contains("đã tồn tại"));

        let result = vibe_msg!("catalog-schema-not-found", name = "myschema");
        assert!(result.contains("myschema"));
        assert!(result.contains("Không tìm thấy schema"));
    }

    #[test]
    fn test_vietnamese_diacritics_handling() {
        init(Some("vi")).unwrap();

        // Test messages with Vietnamese diacritics are correctly handled
        let result = vibe_msg!("executor-schema-not-empty", name = "công_khai");
        assert!(result.contains("công_khai"));
        assert!(result.contains("không trống"));

        // Test the help hint with Vietnamese characters
        let result = format("cli-help-hint", None);
        assert!(result.contains("\\help"));
        assert!(result.contains("trợ giúp"));
    }
}
