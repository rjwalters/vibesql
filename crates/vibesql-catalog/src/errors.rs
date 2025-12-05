/// Errors returned by catalog operations.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogError {
    TableAlreadyExists(String),
    TableNotFound {
        table_name: String,
    },
    ColumnAlreadyExists(String),
    ColumnNotFound {
        column_name: String,
        table_name: String,
    },
    SchemaAlreadyExists(String),
    SchemaNotFound(String),
    SchemaNotEmpty(String),
    RoleAlreadyExists(String),
    RoleNotFound(String),
    // Advanced SQL:1999 objects
    DomainAlreadyExists(String),
    DomainNotFound(String),
    DomainInUse {
        domain_name: String,
        dependent_columns: Vec<(String, String)>, // (table_name, column_name)
    },
    SequenceAlreadyExists(String),
    SequenceNotFound(String),
    SequenceInUse {
        sequence_name: String,
        dependent_columns: Vec<(String, String)>, // (table_name, column_name)
    },
    TypeAlreadyExists(String),
    TypeNotFound(String),
    TypeInUse(String),
    CollationAlreadyExists(String),
    CollationNotFound(String),
    CharacterSetAlreadyExists(String),
    CharacterSetNotFound(String),
    TranslationAlreadyExists(String),
    TranslationNotFound(String),
    ViewAlreadyExists(String),
    ViewNotFound(String),
    ViewInUse {
        view_name: String,
        dependent_views: Vec<String>,
    },
    TriggerAlreadyExists(String),
    TriggerNotFound(String),
    AssertionAlreadyExists(String),
    AssertionNotFound(String),
    FunctionAlreadyExists(String),
    FunctionNotFound(String),
    ProcedureAlreadyExists(String),
    ProcedureNotFound(String),
    ConstraintAlreadyExists(String),
    ConstraintNotFound(String),
    IndexAlreadyExists {
        index_name: String,
        table_name: String,
    },
    IndexNotFound {
        index_name: String,
        table_name: String,
    },
    CircularForeignKey {
        table_name: String,
        message: String,
    },
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::TableAlreadyExists(name) => {
                write!(f, "Table '{}' already exists", name)
            }
            CatalogError::TableNotFound { table_name } => {
                write!(f, "Table '{}' not found", table_name)
            }
            CatalogError::ColumnAlreadyExists(name) => {
                write!(f, "Column '{}' already exists", name)
            }
            CatalogError::ColumnNotFound { column_name, table_name } => {
                write!(f, "Column '{}' not found in table '{}'", column_name, table_name)
            }
            CatalogError::SchemaAlreadyExists(name) => {
                write!(f, "Schema '{}' already exists", name)
            }
            CatalogError::SchemaNotFound(name) => write!(f, "Schema '{}' not found", name),
            CatalogError::SchemaNotEmpty(name) => {
                write!(f, "Schema '{}' is not empty", name)
            }
            CatalogError::RoleAlreadyExists(name) => {
                write!(f, "Role '{}' already exists", name)
            }
            CatalogError::RoleNotFound(name) => write!(f, "Role '{}' not found", name),
            CatalogError::DomainAlreadyExists(name) => {
                write!(f, "Domain '{}' already exists", name)
            }
            CatalogError::DomainNotFound(name) => write!(f, "Domain '{}' not found", name),
            CatalogError::DomainInUse { domain_name, dependent_columns } => {
                write!(
                    f,
                    "Domain '{}' is still in use by {} column(s): {}",
                    domain_name,
                    dependent_columns.len(),
                    dependent_columns
                        .iter()
                        .map(|(t, c)| format!("{}.{}", t, c))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            CatalogError::SequenceAlreadyExists(name) => {
                write!(f, "Sequence '{}' already exists", name)
            }
            CatalogError::SequenceNotFound(name) => write!(f, "Sequence '{}' not found", name),
            CatalogError::SequenceInUse { sequence_name, dependent_columns } => {
                write!(
                    f,
                    "Sequence '{}' is still in use by {} column(s): {}",
                    sequence_name,
                    dependent_columns.len(),
                    dependent_columns
                        .iter()
                        .map(|(t, c)| format!("{}.{}", t, c))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            CatalogError::TypeAlreadyExists(name) => {
                write!(f, "Type '{}' already exists", name)
            }
            CatalogError::TypeNotFound(name) => write!(f, "Type '{}' not found", name),
            CatalogError::TypeInUse(name) => {
                write!(f, "Type '{}' is still in use by one or more tables", name)
            }
            CatalogError::CollationAlreadyExists(name) => {
                write!(f, "Collation '{}' already exists", name)
            }
            CatalogError::CollationNotFound(name) => {
                write!(f, "Collation '{}' not found", name)
            }
            CatalogError::CharacterSetAlreadyExists(name) => {
                write!(f, "Character set '{}' already exists", name)
            }
            CatalogError::CharacterSetNotFound(name) => {
                write!(f, "Character set '{}' not found", name)
            }
            CatalogError::TranslationAlreadyExists(name) => {
                write!(f, "Translation '{}' already exists", name)
            }
            CatalogError::TranslationNotFound(name) => {
                write!(f, "Translation '{}' not found", name)
            }
            CatalogError::ViewAlreadyExists(name) => {
                write!(f, "View '{}' already exists", name)
            }
            CatalogError::ViewNotFound(name) => {
                write!(f, "View '{}' not found", name)
            }
            CatalogError::ViewInUse { view_name, dependent_views } => {
                write!(
                    f,
                    "View or table '{}' is still in use by {} view(s): {}",
                    view_name,
                    dependent_views.len(),
                    dependent_views.join(", ")
                )
            }
            CatalogError::TriggerAlreadyExists(name) => {
                write!(f, "Trigger '{}' already exists", name)
            }
            CatalogError::TriggerNotFound(name) => {
                write!(f, "Trigger '{}' not found", name)
            }
            CatalogError::AssertionAlreadyExists(name) => {
                write!(f, "Assertion '{}' already exists", name)
            }
            CatalogError::AssertionNotFound(name) => {
                write!(f, "Assertion '{}' not found", name)
            }
            CatalogError::FunctionAlreadyExists(name) => {
                write!(f, "Function '{}' already exists", name)
            }
            CatalogError::FunctionNotFound(name) => {
                write!(f, "Function '{}' not found", name)
            }
            CatalogError::ProcedureAlreadyExists(name) => {
                write!(f, "Procedure '{}' already exists", name)
            }
            CatalogError::ProcedureNotFound(name) => {
                write!(f, "Procedure '{}' not found", name)
            }
            CatalogError::ConstraintAlreadyExists(name) => {
                write!(f, "Constraint '{}' already exists", name)
            }
            CatalogError::ConstraintNotFound(name) => {
                write!(f, "Constraint '{}' not found", name)
            }
            CatalogError::IndexAlreadyExists { index_name, table_name } => {
                write!(f, "Index '{}' on table '{}' already exists", index_name, table_name)
            }
            CatalogError::IndexNotFound { index_name, table_name } => {
                write!(f, "Index '{}' on table '{}' not found", index_name, table_name)
            }
            CatalogError::CircularForeignKey { table_name, message } => {
                write!(
                    f,
                    "Circular foreign key dependency detected for table '{}': {}",
                    table_name, message
                )
            }
        }
    }
}

impl std::error::Error for CatalogError {}
