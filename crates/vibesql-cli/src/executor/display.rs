use vibesql_storage::Database;

use super::SqlExecutor;

impl SqlExecutor {
    pub fn describe_table(&self, table_name: &str) -> anyhow::Result<()> {
        // 1. Normalize table name to uppercase (SQL standard for unquoted identifiers)
        let normalized_name = table_name.to_uppercase();

        // 2. Validate table exists
        let table = self.db.get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", table_name))?;

        // 2. Get schema name
        let schema_name = self.db.catalog.get_current_schema();

        // 3. Print table header
        println!("                Table \"{}.{}\"", schema_name, table_name);

        // 4. Print column information
        println!(" {:<20} | {:<25} | {:<8} | {:<10}",
                 "Column", "Type", "Nullable", "Default");
        println!("{}", "-".repeat(70));

        for column in &table.schema.columns {
            let nullable = if column.nullable { "" } else { "not null" };
            let default_val = column.default_value
                .as_ref()
                .map(|v| format!("{:?}", v))
                .unwrap_or_default();

            println!(" {:<20} | {:<25} | {:<8} | {:<10}",
                     column.name,
                     format_data_type(&column.data_type),
                     nullable,
                     truncate_for_display(&default_val, 10));
        }

        // 5. Print constraints
        print_constraints(&table.schema)?;

        // 6. Print indexes
        print_indexes(&self.db, &normalized_name)?;

        Ok(())
    }

    pub fn list_tables(&self) -> anyhow::Result<()> {
        // Get all tables from database
        let tables = self.db.list_tables();
        if tables.is_empty() {
            println!("No tables found");
        } else {
            println!("Tables:");
            for table_name in tables {
                println!("  {}", table_name);
            }
        }
        Ok(())
    }

    pub fn list_schemas(&self) -> anyhow::Result<()> {
        let schemas = self.db.catalog.list_schemas();
        let current_schema = self.db.catalog.get_current_schema();

        if schemas.is_empty() {
            println!("No schemas found");
        } else {
            println!("List of schemas");
            println!("{:<20} {:<10}", "Name", "");
            println!("{}", "-".repeat(30));
            for schema_name in schemas {
                let marker = if schema_name == current_schema { "(current)" } else { "" };
                println!("{:<20} {:<10}", schema_name, marker);
            }
        }
        Ok(())
    }

    pub fn list_indexes(&self) -> anyhow::Result<()> {
        let index_names = self.db.list_indexes();

        if index_names.is_empty() {
            println!("No indexes found");
        } else {
            println!("List of indexes");
            println!("{:<20} {:<20} {:<15} {:<10}", "Name", "Table", "Columns", "Type");
            println!("{}", "-".repeat(70));

            for index_name in index_names {
                if let Some(index_meta) = self.db.get_index(&index_name) {
                    let columns_str = index_meta
                        .columns
                        .iter()
                        .map(|col| col.column_name.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let index_type = if index_meta.unique { "UNIQUE" } else { "BTREE" };

                    println!(
                        "{:<20} {:<20} {:<15} {:<10}",
                        index_meta.index_name, index_meta.table_name, columns_str, index_type
                    );
                }
            }
        }
        Ok(())
    }

    pub fn list_roles(&self) -> anyhow::Result<()> {
        let roles = self.db.catalog.list_roles();
        let current_role = self.db.get_current_role();

        if roles.is_empty() {
            // If no roles defined, show default PUBLIC role
            println!("List of roles");
            println!("{:<20} {:<15}", "Name", "Attributes");
            println!("{}", "-".repeat(35));
            println!("{:<20} {:<15}", "PUBLIC", "(default)");
        } else {
            println!("List of roles");
            println!("{:<20} {:<15}", "Name", "Attributes");
            println!("{}", "-".repeat(35));

            for role_name in roles {
                let marker = if role_name == current_role { "(current)" } else { "" };
                println!("{:<20} {:<15}", role_name, marker);
            }
        }
        Ok(())
    }
}

/// Truncate a string for display in error messages
pub fn truncate_for_display(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

/// Format DataType for table description output (PostgreSQL-style)
pub fn format_data_type(data_type: &vibesql_types::DataType) -> String {
    match data_type {
        vibesql_types::DataType::Integer => "integer".to_string(),
        vibesql_types::DataType::Smallint => "smallint".to_string(),
        vibesql_types::DataType::Bigint => "bigint".to_string(),
        vibesql_types::DataType::Unsigned => "unsigned bigint".to_string(),
        vibesql_types::DataType::Numeric { precision, scale } => format!("numeric({}, {})", precision, scale),
        vibesql_types::DataType::Decimal { precision, scale } => format!("numeric({}, {})", precision, scale),
        vibesql_types::DataType::Float { precision } => format!("float({})", precision),
        vibesql_types::DataType::Real => "real".to_string(),
        vibesql_types::DataType::DoublePrecision => "double precision".to_string(),
        vibesql_types::DataType::Character { length } => format!("character({})", length),
        vibesql_types::DataType::Varchar { max_length } => {
            match max_length {
                Some(len) => format!("character varying({})", len),
                None => "character varying".to_string(),
            }
        }
        vibesql_types::DataType::CharacterLargeObject => "text".to_string(),
        vibesql_types::DataType::Name => "name".to_string(),
        vibesql_types::DataType::Boolean => "boolean".to_string(),
        vibesql_types::DataType::Date => "date".to_string(),
        vibesql_types::DataType::Time { with_timezone } => {
            if *with_timezone {
                "time with time zone".to_string()
            } else {
                "time".to_string()
            }
        }
        vibesql_types::DataType::Timestamp { with_timezone } => {
            if *with_timezone {
                "timestamp with time zone".to_string()
            } else {
                "timestamp".to_string()
            }
        }
        vibesql_types::DataType::Interval { .. } => "interval".to_string(),
        vibesql_types::DataType::BinaryLargeObject => "bytea".to_string(),
        vibesql_types::DataType::Bit { length } => {
            match length {
                Some(len) => format!("bit({})", len),
                None => "bit".to_string(),
            }
        }
        vibesql_types::DataType::UserDefined { type_name } => type_name.clone(),
        vibesql_types::DataType::Null => "null".to_string(),
    }
}

/// Print constraints for a table schema
fn print_constraints(schema: &vibesql_catalog::TableSchema) -> anyhow::Result<()> {
    let mut has_constraints = false;

    // Print primary key
    if let Some(pk_cols) = &schema.primary_key {
        if !has_constraints {
            println!("\nConstraints:");
            has_constraints = true;
        }
        println!("    \"{}_pkey\" PRIMARY KEY, btree ({})",
                 schema.name,
                 pk_cols.join(", "));
    }

    // Print unique constraints
    for (idx, unique_cols) in schema.unique_constraints.iter().enumerate() {
        if !has_constraints {
            println!("\nConstraints:");
            has_constraints = true;
        }
        println!("    \"{}_{}_key\" UNIQUE CONSTRAINT, btree ({})",
                 schema.name,
                 idx + 1,
                 unique_cols.join(", "));
    }

    // Print foreign key constraints
    for (idx, fk) in schema.foreign_keys.iter().enumerate() {
        if !has_constraints {
            println!("\nConstraints:");
            has_constraints = true;
        }
        println!("    \"{}_{}_fkey\" FOREIGN KEY ({}) REFERENCES {}({})",
                 schema.name,
                 idx + 1,
                 fk.column_names.join(", "),
                 fk.parent_table,
                 fk.parent_column_names.join(", "));
    }

    // Print check constraints
    for (name, _expr) in &schema.check_constraints {
        if !has_constraints {
            println!("\nConstraints:");
            has_constraints = true;
        }
        println!("    \"{}\" CHECK", name);
    }

    Ok(())
}

/// Print indexes for a table
fn print_indexes(db: &Database, table_name: &str) -> anyhow::Result<()> {
    let index_names = db.list_indexes();
    let indexes: Vec<_> = index_names
        .into_iter()
        .filter_map(|idx_name| {
            db.get_index(&idx_name).and_then(|idx| {
                if idx.table_name == table_name {
                    Some(idx)
                } else {
                    None
                }
            })
        })
        .collect();

    if !indexes.is_empty() {
        println!("\nIndexes:");
        for index in indexes {
            let idx_type = if index.unique { "UNIQUE, btree" } else { "btree" };
            let columns = index.columns.iter()
                .map(|c| c.column_name.clone())
                .collect::<Vec<_>>()
                .join(", ");

            println!("    \"{}\" {}, ({})",
                     index.index_name,
                     idx_type,
                     columns);
        }
    }

    Ok(())
}
