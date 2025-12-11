// ============================================================================
// Database Lifecycle Management
// ============================================================================

use std::collections::HashMap;

use super::transactions::TransactionManager;
use crate::Table;

/// Manages database lifecycle: creation, opening, closing, and state reset
#[derive(Debug, Clone)]
pub struct Lifecycle {
    tables: HashMap<String, Table>,
    transaction_manager: TransactionManager,
    current_role: Option<String>,
    security_enabled: bool,
}

impl Lifecycle {
    /// Create a new database lifecycle manager
    ///
    /// Note: Security is disabled by default for backward compatibility with existing code.
    /// Call `enable_security()` to turn on access control enforcement.
    pub fn new() -> Self {
        Lifecycle {
            tables: HashMap::new(),
            transaction_manager: TransactionManager::new(),
            current_role: None,
            security_enabled: false,
        }
    }

    /// Reset the database to empty state (more efficient than creating a new instance).
    ///
    /// Clears all tables and clears all transactions.
    /// Useful for test scenarios where you need to reuse a Database instance.
    pub fn reset(&mut self) {
        self.tables.clear();
        self.transaction_manager = TransactionManager::new();
        self.current_role = None;
        self.security_enabled = false;
    }

    // Getters for internal access
    #[allow(dead_code)]
    pub fn tables(&self) -> &HashMap<String, Table> {
        &self.tables
    }

    #[allow(dead_code)]
    pub fn tables_mut(&mut self) -> &mut HashMap<String, Table> {
        &mut self.tables
    }

    pub fn transaction_manager(&self) -> &TransactionManager {
        &self.transaction_manager
    }

    pub fn transaction_manager_mut(&mut self) -> &mut TransactionManager {
        &mut self.transaction_manager
    }

    /// Perform rollback with catalog access (called from Database)
    pub fn perform_rollback(
        &mut self,
        catalog: &mut vibesql_catalog::Catalog,
        tables: &mut HashMap<String, Table>,
    ) -> Result<(), crate::StorageError> {
        self.transaction_manager.rollback_transaction(catalog, tables)
    }

    pub fn current_role(&self) -> Option<&str> {
        self.current_role.as_deref()
    }

    pub fn set_role(&mut self, role: Option<String>) {
        self.current_role = role;
    }

    pub fn is_security_enabled(&self) -> bool {
        self.security_enabled
    }

    pub fn disable_security(&mut self) {
        self.security_enabled = false;
    }

    pub fn enable_security(&mut self) {
        self.security_enabled = true;
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}
