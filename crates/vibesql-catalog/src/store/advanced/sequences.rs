//! Sequence management methods.

#![allow(clippy::only_used_in_recursion)]

use crate::{advanced_objects::Sequence, errors::CatalogError};

impl super::super::Catalog {
    // ============================================================================
    // Sequence Management Methods
    // ============================================================================

    /// Create a SEQUENCE
    pub fn create_sequence(
        &mut self,
        name: String,
        start_with: Option<i64>,
        increment_by: i64,
        min_value: Option<i64>,
        max_value: Option<i64>,
        cycle: bool,
    ) -> Result<(), CatalogError> {
        if self.sequences.contains_key(&name) {
            return Err(CatalogError::SequenceAlreadyExists(name));
        }
        self.sequences.insert(
            name.clone(),
            Sequence::new(name, start_with, increment_by, min_value, max_value, cycle),
        );
        Ok(())
    }

    /// Drop a SEQUENCE
    pub fn drop_sequence(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // Check if sequence exists
        if !self.sequences.contains_key(name) {
            return Err(CatalogError::SequenceNotFound(name.to_string()));
        }

        // Check if any columns use this sequence in their default values
        let mut columns_using_sequence = Vec::new();
        for schema in self.schemas.values() {
            for table_name in schema.list_tables() {
                if let Some(table) = schema.get_table(&table_name, false) {
                    for column in &table.columns {
                        if let Some(default_expr) = &column.default_value {
                            if self.expression_uses_sequence(default_expr, name) {
                                columns_using_sequence
                                    .push((table_name.clone(), column.name.clone()));
                            }
                        }
                    }
                }
            }
        }

        // If RESTRICT and sequence is in use, return error
        if !cascade && !columns_using_sequence.is_empty() {
            return Err(CatalogError::SequenceInUse {
                sequence_name: name.to_string(),
                dependent_columns: columns_using_sequence,
            });
        }

        // If CASCADE, remove sequence dependencies from columns
        if cascade && !columns_using_sequence.is_empty() {
            for (table_name, column_name) in columns_using_sequence {
                // Get mutable reference to the schema containing the table
                for schema in self.schemas.values_mut() {
                    if let Some(table) = schema.get_table(&table_name, false) {
                        // Find the column and remove its default value
                        if table.columns.iter().any(|c| c.name == column_name) {
                            // We need to reconstruct the table to modify it
                            // This is a limitation of the current architecture
                            // For now, we'll handle this by removing and reinserting the table
                            let mut modified_table = table.clone();
                            for col in &mut modified_table.columns {
                                if col.name == column_name {
                                    col.default_value = None;
                                }
                            }
                            // Drop and recreate table with modified columns
                            schema.drop_table(&table_name, true)?;
                            schema.create_table(modified_table)?;
                            break;
                        }
                    }
                }
            }
        }

        // Finally, drop the sequence
        self.sequences.remove(name);
        Ok(())
    }

    /// Check if an expression uses a specific sequence
    fn expression_uses_sequence(
        &self,
        expr: &vibesql_ast::Expression,
        sequence_name: &str,
    ) -> bool {
        use vibesql_ast::Expression;
        match expr {
            Expression::NextValue { sequence_name: seq_name } => seq_name == sequence_name,
            Expression::BinaryOp { left, right, .. } => {
                self.expression_uses_sequence(left, sequence_name)
                    || self.expression_uses_sequence(right, sequence_name)
            }
            Expression::UnaryOp { expr, .. } => self.expression_uses_sequence(expr, sequence_name),
            Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
                args.iter().any(|arg| self.expression_uses_sequence(arg, sequence_name))
            }
            Expression::IsNull { expr, .. } => self.expression_uses_sequence(expr, sequence_name),
            Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().is_some_and(|e| self.expression_uses_sequence(e, sequence_name))
                    || when_clauses.iter().any(|when| {
                        when.conditions
                            .iter()
                            .any(|c| self.expression_uses_sequence(c, sequence_name))
                            || self.expression_uses_sequence(&when.result, sequence_name)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| self.expression_uses_sequence(e, sequence_name))
            }
            Expression::ScalarSubquery(_)
            | Expression::In { .. }
            | Expression::InList { .. }
            | Expression::Between { .. }
            | Expression::Cast { .. }
            | Expression::Position { .. }
            | Expression::Trim { .. }
            | Expression::Like { .. }
            | Expression::Exists { .. }
            | Expression::QuantifiedComparison { .. }
            | Expression::WindowFunction { .. } => {
                // These could theoretically contain sequence references in subexpressions
                // For now, we'll do a simple check
                false
            }
            _ => false,
        }
    }

    /// Alter a SEQUENCE
    pub fn alter_sequence(
        &mut self,
        name: &str,
        restart_with: Option<i64>,
        increment_by: Option<i64>,
        min_value: Option<Option<i64>>,
        max_value: Option<Option<i64>>,
        cycle: Option<bool>,
    ) -> Result<(), CatalogError> {
        let seq = self
            .sequences
            .get_mut(name)
            .ok_or_else(|| CatalogError::SequenceNotFound(name.to_string()))?;

        if let Some(restart) = restart_with {
            seq.restart(Some(restart));
        }
        if let Some(incr) = increment_by {
            seq.increment_by = incr;
        }
        if let Some(min) = min_value {
            seq.min_value = min;
        }
        if let Some(max) = max_value {
            seq.max_value = max;
        }
        if let Some(cyc) = cycle {
            seq.cycle = cyc;
        }

        Ok(())
    }

    /// Get a mutable reference to a SEQUENCE for NEXT VALUE FOR
    pub fn get_sequence_mut(&mut self, name: &str) -> Result<&mut Sequence, CatalogError> {
        self.sequences.get_mut(name).ok_or_else(|| CatalogError::SequenceNotFound(name.to_string()))
    }

    /// Get an immutable reference to a SEQUENCE
    pub fn get_sequence(&self, name: &str) -> Result<&Sequence, CatalogError> {
        self.sequences.get(name).ok_or_else(|| CatalogError::SequenceNotFound(name.to_string()))
    }

    /// List all sequence names
    pub fn list_sequences(&self) -> Vec<&str> {
        self.sequences.keys().map(|s| s.as_str()).collect()
    }

    /// Insert a pre-constructed sequence (for persistence restoration)
    pub fn insert_sequence(&mut self, name: String, sequence: Sequence) {
        self.sequences.insert(name, sequence);
    }
}
