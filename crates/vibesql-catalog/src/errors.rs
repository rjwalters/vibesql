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
        use vibesql_l10n::vibe_msg;
        match self {
            CatalogError::TableAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-table-already-exists", name = name.as_str()))
            }
            CatalogError::TableNotFound { table_name } => {
                write!(f, "{}", vibe_msg!("catalog-table-not-found", table_name = table_name.as_str()))
            }
            CatalogError::ColumnAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-column-already-exists", name = name.as_str()))
            }
            CatalogError::ColumnNotFound { column_name, table_name } => {
                write!(f, "{}", vibe_msg!("catalog-column-not-found", column_name = column_name.as_str(), table_name = table_name.as_str()))
            }
            CatalogError::SchemaAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-schema-already-exists", name = name.as_str()))
            }
            CatalogError::SchemaNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-schema-not-found", name = name.as_str()))
            }
            CatalogError::SchemaNotEmpty(name) => {
                write!(f, "{}", vibe_msg!("catalog-schema-not-empty", name = name.as_str()))
            }
            CatalogError::RoleAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-role-already-exists", name = name.as_str()))
            }
            CatalogError::RoleNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-role-not-found", name = name.as_str()))
            }
            CatalogError::DomainAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-domain-already-exists", name = name.as_str()))
            }
            CatalogError::DomainNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-domain-not-found", name = name.as_str()))
            }
            CatalogError::DomainInUse { domain_name, dependent_columns } => {
                let columns = dependent_columns
                    .iter()
                    .map(|(t, c)| format!("{}.{}", t, c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{}", vibe_msg!("catalog-domain-in-use", domain_name = domain_name.as_str(), count = dependent_columns.len() as i64, columns = columns.as_str()))
            }
            CatalogError::SequenceAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-sequence-already-exists", name = name.as_str()))
            }
            CatalogError::SequenceNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-sequence-not-found", name = name.as_str()))
            }
            CatalogError::SequenceInUse { sequence_name, dependent_columns } => {
                let columns = dependent_columns
                    .iter()
                    .map(|(t, c)| format!("{}.{}", t, c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{}", vibe_msg!("catalog-sequence-in-use", sequence_name = sequence_name.as_str(), count = dependent_columns.len() as i64, columns = columns.as_str()))
            }
            CatalogError::TypeAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-type-already-exists", name = name.as_str()))
            }
            CatalogError::TypeNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-type-not-found", name = name.as_str()))
            }
            CatalogError::TypeInUse(name) => {
                write!(f, "{}", vibe_msg!("catalog-type-in-use", name = name.as_str()))
            }
            CatalogError::CollationAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-collation-already-exists", name = name.as_str()))
            }
            CatalogError::CollationNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-collation-not-found", name = name.as_str()))
            }
            CatalogError::CharacterSetAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-character-set-already-exists", name = name.as_str()))
            }
            CatalogError::CharacterSetNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-character-set-not-found", name = name.as_str()))
            }
            CatalogError::TranslationAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-translation-already-exists", name = name.as_str()))
            }
            CatalogError::TranslationNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-translation-not-found", name = name.as_str()))
            }
            CatalogError::ViewAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-view-already-exists", name = name.as_str()))
            }
            CatalogError::ViewNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-view-not-found", name = name.as_str()))
            }
            CatalogError::ViewInUse { view_name, dependent_views } => {
                let views = dependent_views.join(", ");
                write!(f, "{}", vibe_msg!("catalog-view-in-use", view_name = view_name.as_str(), count = dependent_views.len() as i64, views = views.as_str()))
            }
            CatalogError::TriggerAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-trigger-already-exists", name = name.as_str()))
            }
            CatalogError::TriggerNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-trigger-not-found", name = name.as_str()))
            }
            CatalogError::AssertionAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-assertion-already-exists", name = name.as_str()))
            }
            CatalogError::AssertionNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-assertion-not-found", name = name.as_str()))
            }
            CatalogError::FunctionAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-function-already-exists", name = name.as_str()))
            }
            CatalogError::FunctionNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-function-not-found", name = name.as_str()))
            }
            CatalogError::ProcedureAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-procedure-already-exists", name = name.as_str()))
            }
            CatalogError::ProcedureNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-procedure-not-found", name = name.as_str()))
            }
            CatalogError::ConstraintAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("catalog-constraint-already-exists", name = name.as_str()))
            }
            CatalogError::ConstraintNotFound(name) => {
                write!(f, "{}", vibe_msg!("catalog-constraint-not-found", name = name.as_str()))
            }
            CatalogError::IndexAlreadyExists { index_name, table_name } => {
                write!(f, "{}", vibe_msg!("catalog-index-already-exists", index_name = index_name.as_str(), table_name = table_name.as_str()))
            }
            CatalogError::IndexNotFound { index_name, table_name } => {
                write!(f, "{}", vibe_msg!("catalog-index-not-found", index_name = index_name.as_str(), table_name = table_name.as_str()))
            }
            CatalogError::CircularForeignKey { table_name, message } => {
                write!(f, "{}", vibe_msg!("catalog-circular-foreign-key", table_name = table_name.as_str(), message = message.as_str()))
            }
        }
    }
}

impl std::error::Error for CatalogError {}
