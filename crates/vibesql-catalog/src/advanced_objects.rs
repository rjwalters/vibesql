//! Advanced SQL:1999 object definitions
//! Minimal stub implementations for: DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER SET, TRANSLATION

/// Domain - User-defined data type with constraints
#[derive(Debug, Clone, Default)]
pub struct Domain {
    pub name: String,
    // TODO: Add base_type, constraints when implementing full functionality
}

impl Domain {
    pub fn new(name: String) -> Self {
        Domain { name }
    }
}

/// Sequence - Auto-incrementing number generator
#[derive(Debug, Clone)]
pub struct Sequence {
    pub name: String,
    pub start_with: i64, // Original start value (for RESTART without WITH)
    pub increment_by: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool,
    pub current_value: i64, // Current sequence value
    pub exhausted: bool,    // true if reached limit with NO CYCLE
}

impl Sequence {
    pub fn new(
        name: String,
        start_with: Option<i64>,
        increment_by: i64,
        min_value: Option<i64>,
        max_value: Option<i64>,
        cycle: bool,
    ) -> Self {
        let start = start_with.unwrap_or(if increment_by >= 0 { 1 } else { -1 });
        Sequence {
            name,
            start_with: start,
            increment_by,
            min_value,
            max_value,
            cycle,
            current_value: start,
            exhausted: false,
        }
    }

    /// Get the next value from the sequence
    pub fn next_value(&mut self) -> Result<i64, String> {
        if self.exhausted {
            return Err(format!("Sequence {} is exhausted", self.name));
        }

        let value = self.current_value;

        // Advance the sequence
        let next = value
            .checked_add(self.increment_by)
            .ok_or_else(|| format!("Sequence {} overflow", self.name))?;

        // Check bounds
        if let Some(max) = self.max_value {
            if self.increment_by > 0 && next > max {
                if self.cycle {
                    self.current_value = self.min_value.unwrap_or(self.start_with);
                } else {
                    self.exhausted = true;
                }
                return Ok(value);
            }
        }

        if let Some(min) = self.min_value {
            if self.increment_by < 0 && next < min {
                if self.cycle {
                    self.current_value = self.max_value.unwrap_or(self.start_with);
                } else {
                    self.exhausted = true;
                }
                return Ok(value);
            }
        }

        self.current_value = next;
        Ok(value)
    }

    /// Restart the sequence
    pub fn restart(&mut self, value: Option<i64>) {
        self.current_value = value.unwrap_or(self.start_with);
        self.exhausted = false;
    }
}

impl Default for Sequence {
    fn default() -> Self {
        Sequence {
            name: String::new(),
            start_with: 1,
            increment_by: 1,
            min_value: None,
            max_value: None,
            cycle: false,
            current_value: 1,
            exhausted: false,
        }
    }
}

/// User-defined Type (UDT)
#[derive(Debug, Clone, Default)]
pub struct UserDefinedType {
    pub name: String,
    // TODO: Add fields when implementing full functionality
}

impl UserDefinedType {
    pub fn new(name: String) -> Self {
        UserDefinedType { name }
    }
}

/// Collation - String comparison rules
#[derive(Debug, Clone, Default)]
pub struct Collation {
    pub name: String,
    pub character_set: Option<String>,    // FOR character_set
    pub source_collation: Option<String>, // FROM source_collation
    pub pad_space: Option<bool>,          // PAD SPACE (true) | NO PAD (false)
}

impl Collation {
    pub fn new(
        name: String,
        character_set: Option<String>,
        source_collation: Option<String>,
        pad_space: Option<bool>,
    ) -> Self {
        Collation { name, character_set, source_collation, pad_space }
    }
}

/// Character Set - Character encoding
#[derive(Debug, Clone, Default)]
pub struct CharacterSet {
    pub name: String,
    pub source: Option<String>,    // GET source
    pub collation: Option<String>, // COLLATE FROM collation
}

impl CharacterSet {
    pub fn new(name: String, source: Option<String>, collation: Option<String>) -> Self {
        CharacterSet { name, source, collation }
    }
}

/// Translation - Character set translation
#[derive(Debug, Clone, Default)]
pub struct Translation {
    pub name: String,
    pub source_charset: Option<String>,     // FOR source_charset
    pub target_charset: Option<String>,     // TO target_charset
    pub translation_source: Option<String>, // FROM translation_source
}

impl Translation {
    pub fn new(
        name: String,
        source_charset: Option<String>,
        target_charset: Option<String>,
        translation_source: Option<String>,
    ) -> Self {
        Translation { name, source_charset, target_charset, translation_source }
    }
}

/// Assertion - Named constraint that checks a boolean condition (SQL:1999 Feature F671/F672)
///
/// Assertions are schema-level constraints that can reference multiple tables
/// and enforce complex integrity rules across the database.
#[derive(Debug, Clone)]
pub struct Assertion {
    pub name: String,
    pub check_condition: vibesql_ast::Expression,
}

impl Assertion {
    pub fn new(name: String, check_condition: vibesql_ast::Expression) -> Self {
        Assertion { name, check_condition }
    }
}

/// Function - Stored function (SQL:1999 Feature P001)
#[derive(Debug, Clone)]
pub struct Function {
    pub name: String,
    pub schema: String,
    pub parameters: Vec<FunctionParam>,
    pub return_type: vibesql_types::DataType,
    pub body: FunctionBody,
    // Characteristics (Phase 6)
    pub deterministic: bool,
    pub sql_security: SqlSecurity,
    pub comment: Option<String>,
    pub language: String,
}

impl Function {
    pub fn new(
        name: String,
        schema: String,
        parameters: Vec<FunctionParam>,
        return_type: vibesql_types::DataType,
        body: FunctionBody,
    ) -> Self {
        Function {
            name,
            schema,
            parameters,
            return_type,
            body,
            // Default characteristics
            deterministic: false,
            sql_security: SqlSecurity::Definer,
            comment: None,
            language: "SQL".to_string(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_characteristics(
        name: String,
        schema: String,
        parameters: Vec<FunctionParam>,
        return_type: vibesql_types::DataType,
        body: FunctionBody,
        deterministic: bool,
        sql_security: SqlSecurity,
        comment: Option<String>,
        language: String,
    ) -> Self {
        Function {
            name,
            schema,
            parameters,
            return_type,
            body,
            deterministic,
            sql_security,
            comment,
            language,
        }
    }
}

/// Parameter in a function definition
#[derive(Debug, Clone)]
pub struct FunctionParam {
    pub name: String,
    pub data_type: vibesql_types::DataType,
}

/// Body of a function
#[derive(Debug, Clone)]
pub enum FunctionBody {
    /// SQL procedural block: BEGIN ... END
    BeginEnd(String), // Stored as raw SQL for now
    /// Raw SQL for simpler cases
    RawSql(String),
}

/// Procedure - Stored procedure (SQL:1999 Feature P001)
#[derive(Debug, Clone)]
pub struct Procedure {
    pub name: String,
    pub schema: String,
    pub parameters: Vec<ProcedureParam>,
    pub body: ProcedureBody,
    // Characteristics (Phase 6)
    pub sql_security: SqlSecurity,
    pub comment: Option<String>,
    pub language: String,
}

impl Procedure {
    pub fn new(
        name: String,
        schema: String,
        parameters: Vec<ProcedureParam>,
        body: ProcedureBody,
    ) -> Self {
        Procedure {
            name,
            schema,
            parameters,
            body,
            // Default characteristics
            sql_security: SqlSecurity::Definer,
            comment: None,
            language: "SQL".to_string(),
        }
    }

    pub fn with_characteristics(
        name: String,
        schema: String,
        parameters: Vec<ProcedureParam>,
        body: ProcedureBody,
        sql_security: SqlSecurity,
        comment: Option<String>,
        language: String,
    ) -> Self {
        Procedure { name, schema, parameters, body, sql_security, comment, language }
    }
}

/// Parameter in a procedure definition
#[derive(Debug, Clone)]
pub struct ProcedureParam {
    pub mode: ParameterMode,
    pub name: String,
    pub data_type: vibesql_types::DataType,
}

/// Parameter mode: IN, OUT, or INOUT
#[derive(Debug, Clone, PartialEq)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
}

/// SQL SECURITY characteristic for procedures and functions (Phase 6)
#[derive(Debug, Clone, PartialEq)]
pub enum SqlSecurity {
    Definer,
    Invoker,
}

/// Body of a procedure
#[derive(Debug, Clone)]
pub enum ProcedureBody {
    /// SQL procedural block: BEGIN ... END
    /// Stores the parsed AST of procedural statements for execution
    BeginEnd(Vec<vibesql_ast::ProceduralStatement>),
    /// Raw SQL for simpler cases (legacy/fallback)
    RawSql(String),
}
