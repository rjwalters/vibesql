//! Assertion management methods (SQL:1999 Feature F671/F672).

use crate::{advanced_objects::Assertion, errors::CatalogError};

impl super::super::Catalog {
    /// Create an ASSERTION (SQL:1999 Feature F671/F672)
    pub fn create_assertion(&mut self, assertion: Assertion) -> Result<(), CatalogError> {
        let name = assertion.name.clone();
        if self.assertions.contains_key(&name) {
            return Err(CatalogError::AssertionAlreadyExists(name));
        }
        self.assertions.insert(name, assertion);
        Ok(())
    }

    /// Get an ASSERTION definition by name
    pub fn get_assertion(&self, name: &str) -> Option<&Assertion> {
        self.assertions.get(name)
    }

    /// Drop an ASSERTION
    pub fn drop_assertion(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // For now, assertions don't have dependencies, so cascade is ignored
        // In the future, cascade might be used if assertions can reference other assertions
        let _ = cascade;

        self.assertions
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::AssertionNotFound(name.to_string()))
    }

    /// Check if an assertion exists
    pub fn assertion_exists(&self, name: &str) -> bool {
        self.assertions.contains_key(name)
    }

    /// List all assertion names
    pub fn list_assertions(&self) -> Vec<String> {
        self.assertions.keys().cloned().collect()
    }

    /// Get all assertions (for runtime enforcement)
    pub fn get_all_assertions(&self) -> impl Iterator<Item = &Assertion> {
        self.assertions.values()
    }
}
