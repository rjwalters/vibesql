//! Execution context for procedural statements
//!
//! Manages:
//! - Local variables (DECLARE)
//! - Parameters (IN, OUT, INOUT)
//! - Scope management (nested blocks)
//! - Label tracking (for LEAVE/ITERATE)
//! - Recursion depth limiting

use std::collections::HashMap;
use vibesql_types::SqlValue;

/// Maximum recursion depth for function/procedure calls
const MAX_RECURSION_DEPTH: usize = 100;

/// Control flow state returned by procedural statement execution
#[derive(Debug, Clone, PartialEq)]
pub enum ControlFlow {
    /// Continue to next statement
    Continue,
    /// Return from function/procedure with value
    Return(SqlValue),
    /// Leave a labeled block/loop
    Leave(String),
    /// Iterate (continue) a labeled loop
    Iterate(String),
}

/// Execution context for procedural statements
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Local variables (DECLARE)
    variables: HashMap<String, SqlValue>,
    /// Parameters (IN, OUT, INOUT)
    parameters: HashMap<String, SqlValue>,
    /// Active labels for LEAVE/ITERATE
    labels: HashMap<String, bool>,
    /// Current recursion depth
    recursion_depth: usize,
    /// Maximum allowed recursion depth
    max_recursion: usize,
    /// Whether this is a function context (read-only, cannot modify data)
    pub(crate) is_function: bool,
    /// Track which parameters are OUT/INOUT and their target variable names
    /// Key: parameter name (uppercase), Value: target variable name (as specified in CALL)
    out_parameters: HashMap<String, String>,
}

impl ExecutionContext {
    /// Create a new execution context
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
            parameters: HashMap::new(),
            labels: HashMap::new(),
            recursion_depth: 0,
            max_recursion: MAX_RECURSION_DEPTH,
            is_function: false,
            out_parameters: HashMap::new(),
        }
    }

    /// Create a new context with specified recursion depth
    pub fn with_recursion_depth(depth: usize) -> Self {
        Self {
            variables: HashMap::new(),
            parameters: HashMap::new(),
            labels: HashMap::new(),
            recursion_depth: depth,
            max_recursion: MAX_RECURSION_DEPTH,
            is_function: false,
            out_parameters: HashMap::new(),
        }
    }

    /// Set a local variable value
    pub fn set_variable(&mut self, name: &str, value: SqlValue) {
        self.variables.insert(name.to_uppercase(), value);
    }

    /// Get a local variable value
    pub fn get_variable(&self, name: &str) -> Option<&SqlValue> {
        self.variables.get(&name.to_uppercase())
    }

    /// Check if a variable exists
    pub fn has_variable(&self, name: &str) -> bool {
        self.variables.contains_key(&name.to_uppercase())
    }

    /// Set a parameter value
    pub fn set_parameter(&mut self, name: &str, value: SqlValue) {
        self.parameters.insert(name.to_uppercase(), value);
    }

    /// Get a parameter value
    pub fn get_parameter(&self, name: &str) -> Option<&SqlValue> {
        self.parameters.get(&name.to_uppercase())
    }

    /// Get a mutable reference to a parameter value
    pub fn get_parameter_mut(&mut self, name: &str) -> Option<&mut SqlValue> {
        self.parameters.get_mut(&name.to_uppercase())
    }

    /// Check if a parameter exists
    pub fn has_parameter(&self, name: &str) -> bool {
        self.parameters.contains_key(&name.to_uppercase())
    }

    /// Get a value (variable or parameter)
    pub fn get_value(&self, name: &str) -> Option<&SqlValue> {
        self.get_variable(name).or_else(|| self.get_parameter(name))
    }

    /// Push a label onto the stack
    pub fn push_label(&mut self, label: &str) {
        self.labels.insert(label.to_uppercase(), true);
    }

    /// Pop a label from the stack
    pub fn pop_label(&mut self, label: &str) {
        self.labels.remove(&label.to_uppercase());
    }

    /// Check if a label is active
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.contains_key(&label.to_uppercase())
    }

    /// Increment recursion depth and check limit
    pub fn enter_recursion(&mut self) -> Result<(), String> {
        self.recursion_depth += 1;
        if self.recursion_depth > self.max_recursion {
            Err(format!("Maximum recursion depth ({}) exceeded", self.max_recursion))
        } else {
            Ok(())
        }
    }

    /// Decrement recursion depth
    pub fn exit_recursion(&mut self) {
        if self.recursion_depth > 0 {
            self.recursion_depth -= 1;
        }
    }

    /// Get current recursion depth
    pub fn recursion_depth(&self) -> usize {
        self.recursion_depth
    }

    /// Get all parameters (for OUT/INOUT return)
    pub fn get_all_parameters(&self) -> &HashMap<String, SqlValue> {
        &self.parameters
    }

    /// Register an OUT or INOUT parameter with its target variable name
    pub fn register_out_parameter(&mut self, param_name: &str, target_var_name: String) {
        self.out_parameters.insert(param_name.to_uppercase(), target_var_name);
    }

    /// Get all OUT/INOUT parameters for return
    /// Returns a HashMap of parameter name -> target variable name
    pub fn get_out_parameters(&self) -> &HashMap<String, String> {
        &self.out_parameters
    }

    /// Get all available variable and parameter names (for error messages)
    pub fn get_available_names(&self) -> Vec<String> {
        let mut names: Vec<String> =
            self.variables.keys().chain(self.parameters.keys()).cloned().collect();
        names.sort();
        names
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_storage() {
        let mut ctx = ExecutionContext::new();

        ctx.set_variable("x", SqlValue::Integer(42));
        assert_eq!(ctx.get_variable("x"), Some(&SqlValue::Integer(42)));
        assert_eq!(ctx.get_variable("X"), Some(&SqlValue::Integer(42))); // Case insensitive
        assert!(ctx.has_variable("x"));
    }

    #[test]
    fn test_parameter_storage() {
        let mut ctx = ExecutionContext::new();

        ctx.set_parameter("param1", SqlValue::Integer(100));
        assert_eq!(ctx.get_parameter("param1"), Some(&SqlValue::Integer(100)));
        assert!(ctx.has_parameter("PARAM1"));
    }

    #[test]
    fn test_get_value_precedence() {
        let mut ctx = ExecutionContext::new();

        // Variables take precedence over parameters
        ctx.set_parameter("x", SqlValue::Integer(1));
        ctx.set_variable("x", SqlValue::Integer(2));

        assert_eq!(ctx.get_value("x"), Some(&SqlValue::Integer(2)));
    }

    #[test]
    fn test_label_management() {
        let mut ctx = ExecutionContext::new();

        ctx.push_label("loop1");
        assert!(ctx.has_label("loop1"));
        assert!(ctx.has_label("LOOP1")); // Case insensitive

        ctx.pop_label("loop1");
        assert!(!ctx.has_label("loop1"));
    }

    #[test]
    fn test_recursion_limit() {
        let mut ctx = ExecutionContext::new();

        for _ in 0..100 {
            assert!(ctx.enter_recursion().is_ok());
        }

        // 101st call should fail
        assert!(ctx.enter_recursion().is_err());
    }
}
