use serde::{Deserialize, Serialize};
use wasm_bindgen::{prelude::*, JsError};

// Module declarations
pub mod examples;
pub mod execute;
pub mod query;
pub mod schema;
#[cfg(test)]
mod tests;

/// Initializes the WASM module and sets up panic hooks.
///
/// When the `i18n` feature is enabled, this also detects the browser's
/// language setting via `navigator.language` and initializes the localization
/// system accordingly.
#[wasm_bindgen(start)]
pub fn init_wasm() {
    // Better error messages in browser console
    console_error_panic_hook::set_once();

    // Initialize l10n with browser locale detection when i18n feature is enabled
    #[cfg(feature = "i18n")]
    {
        let locale = detect_browser_locale();
        let _ = vibesql_l10n::init(locale.as_deref());
    }
}

/// Detect the browser's preferred locale from navigator.language.
///
/// Returns `Some(locale)` if detection succeeds, `None` to use default.
#[cfg(feature = "i18n")]
fn detect_browser_locale() -> Option<String> {
    web_sys::window()
        .and_then(|w| w.navigator().language())
}

/// Sets the locale for localized error messages at runtime.
///
/// This allows changing the language without reloading the page.
/// Supported locales include: en-US, es, pt-BR, zh-CN, ja, de, fr, ko,
/// id, sv, th, vi.
#[cfg(feature = "i18n")]
#[wasm_bindgen]
pub fn set_locale(locale: &str) {
    let _ = vibesql_l10n::init(Some(locale));
}

/// Gets the currently active locale code.
#[cfg(feature = "i18n")]
#[wasm_bindgen]
pub fn get_locale() -> String {
    vibesql_l10n::current_locale().to_string()
}

/// Gets a localized "language changed" message for the current locale.
///
/// This message confirms the locale change in the user's new language.
/// Example: "Language changed to Japanese" (in Japanese: "言語を日本語に変更しました")
#[cfg(feature = "i18n")]
#[wasm_bindgen]
pub fn get_locale_changed_message(locale_name: &str) -> String {
    use vibesql_l10n::fluent::FluentArgs;
    let mut args = FluentArgs::new();
    args.set("locale", locale_name);
    vibesql_l10n::format("locale-changed", Some(&args))
}

/// Result of a query execution
#[derive(Serialize, Deserialize)]
#[wasm_bindgen(getter_with_clone)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Row data as JSON strings
    pub rows: Vec<String>,
    /// Number of rows
    pub row_count: usize,
}

/// Result of an execute (DDL/DML) operation
#[derive(Serialize, Deserialize)]
#[wasm_bindgen(getter_with_clone)]
pub struct ExecuteResult {
    /// Number of rows affected (for INSERT, UPDATE, DELETE)
    pub rows_affected: usize,
    /// Success message
    pub message: String,
}

/// Table column metadata
#[derive(Clone, Serialize, Deserialize)]
#[wasm_bindgen(getter_with_clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Data type (as string)
    pub data_type: String,
    /// Whether column can be NULL
    pub nullable: bool,
}

/// Table schema information
#[derive(Clone, Serialize, Deserialize)]
#[wasm_bindgen(getter_with_clone)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnInfo>,
}

/// In-memory SQL database with WASM bindings
#[wasm_bindgen]
pub struct Database {
    db: vibesql_storage::Database,
}

#[wasm_bindgen]
impl Database {
    /// Creates a new empty database instance with in-memory storage
    #[wasm_bindgen(constructor)]
    pub fn new() -> Database {
        Database { db: vibesql_storage::Database::new() }
    }

    /// Creates a new database instance configured for browser environment
    ///
    /// This uses browser-appropriate defaults:
    /// - 512MB memory budget for indexes
    /// - 2GB disk budget (OPFS)
    /// - SpillToDisk policy for automatic memory management
    /// - OPFS-backed persistent storage for indexes
    ///
    /// Data persists across browser sessions using Origin Private File System (OPFS).
    ///
    /// Returns a Promise that resolves to a Database instance.
    ///
    /// # Browser Compatibility
    /// - Chrome 86+
    /// - Firefox 111+
    /// - Safari 15.2+
    ///
    /// # Example
    /// ```javascript
    /// const db = await Database.newWithPersistence();
    /// ```
    #[wasm_bindgen(js_name = newWithPersistence)]
    pub async fn new_with_persistence() -> Result<Database, JsError> {
        // Use browser-default configuration with OPFS persistence
        let config = vibesql_storage::DatabaseConfig::browser_default();
        let path = std::path::PathBuf::from("/vibesql-data");

        let db = vibesql_storage::Database::with_path_and_config(path, config);

        Ok(Database { db })
    }

    /// Returns the version string
    pub fn version(&self) -> String {
        "vibesql-wasm 0.1.0".to_string()
    }

    /// Lists all table names in the database
    pub fn list_tables(&self) -> Vec<String> {
        self.db.list_tables()
    }

    /// Executes a SELECT query and returns results as JSON
    pub fn query(&self, sql: String) -> Result<JsValue, JsError> {
        query::execute_query(self, &sql).map_err(|e| JsError::new(&format!("{:?}", e)))
    }

    /// Gets the schema for a specific table
    pub fn describe_table(&self, table_name: String) -> Result<JsValue, JsError> {
        schema::describe_table_impl(self, &table_name)
            .map_err(|e| JsError::new(&format!("{:?}", e)))
    }

    /// Executes a DDL or DML statement (CREATE TABLE, INSERT, UPDATE, DELETE)
    /// Returns a JSON string with the result
    pub fn execute(&mut self, sql: String) -> Result<JsValue, JsError> {
        execute::execute_statement(self, &sql).map_err(|e| JsError::new(&format!("{:?}", e)))
    }

    /// Load the Employees example database
    pub fn load_employees(&mut self) -> Result<JsValue, JsError> {
        examples::load_employees_impl(self).map_err(|e| JsError::new(&format!("{:?}", e)))
    }

    /// Load the Northwind example database
    pub fn load_northwind(&mut self) -> Result<JsValue, JsError> {
        examples::load_northwind_impl(self).map_err(|e| JsError::new(&format!("{:?}", e)))
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
