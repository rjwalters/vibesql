//! Trigger definitions for SQL triggers

use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};

/// Trigger definition stored in the catalog
#[derive(Debug, Clone)]
pub struct TriggerDefinition {
    /// Name of the trigger
    pub name: String,
    /// Trigger timing (BEFORE, AFTER, INSTEAD OF)
    pub timing: TriggerTiming,
    /// Trigger event (INSERT, UPDATE, DELETE)
    pub event: TriggerEvent,
    /// Table name the trigger is on
    pub table_name: String,
    /// Granularity (ROW or STATEMENT)
    pub granularity: TriggerGranularity,
    /// Optional WHEN condition
    pub when_condition: Option<Box<vibesql_ast::Expression>>,
    /// Triggered action (procedural SQL)
    pub triggered_action: TriggerAction,
    /// Whether trigger is enabled (default: true)
    pub enabled: bool,
    /// Optional original SQL definition string (for persistence/serialization).
    /// When `Some`, the SQL-dump persistence path will emit this verbatim so the
    /// trigger can be reconstructed across CLI invocations. When `None`, the
    /// SQL-dump path has no reliable way to round-trip the trigger.
    pub sql_definition: Option<String>,
}

impl TriggerDefinition {
    /// Create a new trigger definition (without preserved SQL text)
    pub fn new(
        name: String,
        timing: TriggerTiming,
        event: TriggerEvent,
        table_name: String,
        granularity: TriggerGranularity,
        when_condition: Option<Box<vibesql_ast::Expression>>,
        triggered_action: TriggerAction,
    ) -> Self {
        TriggerDefinition {
            name,
            timing,
            event,
            table_name,
            granularity,
            when_condition,
            triggered_action,
            enabled: true, // Default to enabled
            sql_definition: None,
        }
    }

    /// Create a new trigger definition with the original SQL text preserved.
    ///
    /// The `sql_definition` is used by the SQL-dump persistence path to emit a
    /// reconstructible `CREATE TRIGGER` statement. Mirrors
    /// [`crate::ViewDefinition::new_with_sql`].
    pub fn new_with_sql(
        name: String,
        timing: TriggerTiming,
        event: TriggerEvent,
        table_name: String,
        granularity: TriggerGranularity,
        when_condition: Option<Box<vibesql_ast::Expression>>,
        triggered_action: TriggerAction,
        sql_definition: String,
    ) -> Self {
        TriggerDefinition {
            name,
            timing,
            event,
            table_name,
            granularity,
            when_condition,
            triggered_action,
            enabled: true,
            sql_definition: Some(sql_definition),
        }
    }

    /// Check if the trigger is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enable the trigger
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable the trigger
    pub fn disable(&mut self) {
        self.enabled = false;
    }
}
