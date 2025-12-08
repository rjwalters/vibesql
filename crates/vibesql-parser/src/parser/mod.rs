use std::fmt;

use crate::{keywords::Keyword, lexer::Lexer, token::Token};

mod advanced_objects;
mod alter;
mod create;
mod cursor;
mod delete;
mod domain;
mod drop;
mod expressions;
mod grant;
mod helpers;
mod index;
mod insert;
mod introspection;
mod prepared;
mod revoke;
mod role;
mod schema;
mod select;
mod table_options;
mod transaction;
mod trigger;
mod truncate;
mod update;
mod view;

/// Parser error
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parse error: {}", self.message)
    }
}

/// SQL Parser - converts tokens into AST
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
    /// Counter for placeholder parameters (?)
    /// Incremented each time a placeholder is parsed, providing 0-indexed parameter positions
    placeholder_count: usize,
}

impl Parser {
    /// Create a new parser from tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, position: 0, placeholder_count: 0 }
    }

    /// Parse a comma-separated list of items using a provided parser function
    ///
    /// This is a generic helper that consolidates the common pattern of parsing
    /// comma-separated lists throughout the parser (e.g., GROUP BY expressions,
    /// ORDER BY items, identifier lists, etc.)
    ///
    /// # Arguments
    /// * `parse_item` - Closure that parses a single item of type T
    ///
    /// # Returns
    /// * `Ok(Vec<T>)` - Successfully parsed list of items
    /// * `Err(ParseError)` - Error parsing an item
    ///
    /// # Example
    /// ```text
    /// // Parse comma-separated expressions (for GROUP BY)
    /// let exprs = self.parse_comma_separated_list(|p| p.parse_expression())?;
    ///
    /// // Parse comma-separated identifiers
    /// let ids = self.parse_comma_separated_list(|p| p.parse_identifier())?;
    /// ```
    pub fn parse_comma_separated_list<T, F>(&mut self, parse_item: F) -> Result<Vec<T>, ParseError>
    where
        F: Fn(&mut Self) -> Result<T, ParseError>,
    {
        let mut items = Vec::new();

        // Parse first item
        items.push(parse_item(self)?);

        // Parse remaining items (preceded by commas)
        while matches!(self.peek(), Token::Comma) {
            self.advance(); // consume comma
            items.push(parse_item(self)?);
        }

        Ok(items)
    }

    /// Parse SQL input string into a Statement
    pub fn parse_sql(input: &str) -> Result<vibesql_ast::Statement, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = Parser::new(tokens);
        parser.parse_statement()
    }

    /// Parse a statement
    pub fn parse_statement(&mut self) -> Result<vibesql_ast::Statement, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::With) => {
                let select_stmt = self.parse_select_statement()?;
                Ok(vibesql_ast::Statement::Select(Box::new(select_stmt)))
            }
            Token::Keyword(Keyword::Insert) => {
                let insert_stmt = self.parse_insert_statement()?;
                Ok(vibesql_ast::Statement::Insert(insert_stmt))
            }
            Token::Keyword(Keyword::Replace) => {
                let insert_stmt = self.parse_replace_statement()?;
                Ok(vibesql_ast::Statement::Insert(insert_stmt))
            }
            Token::Keyword(Keyword::Update) => {
                let update_stmt = self.parse_update_statement()?;
                Ok(vibesql_ast::Statement::Update(update_stmt))
            }
            Token::Keyword(Keyword::Delete) => {
                let delete_stmt = self.parse_delete_statement()?;
                Ok(vibesql_ast::Statement::Delete(delete_stmt))
            }
            Token::Keyword(Keyword::Create) => {
                // Check for CREATE OR REPLACE VIEW and CREATE OR REPLACE TEMP/TEMPORARY VIEW
                if self.peek_next_keyword(Keyword::Or)
                    && matches!(self.peek_at_offset(2), Token::Keyword(Keyword::Replace))
                {
                    // Could be CREATE OR REPLACE VIEW or CREATE OR REPLACE TEMP/TEMPORARY VIEW
                    if matches!(self.peek_at_offset(3), Token::Keyword(Keyword::View))
                        || matches!(self.peek_at_offset(3), Token::Keyword(Keyword::Temp))
                        || matches!(self.peek_at_offset(3), Token::Keyword(Keyword::Temporary))
                    {
                        return Ok(vibesql_ast::Statement::CreateView(
                            self.parse_create_view_statement()?,
                        ));
                    }
                }
                if self.peek_next_keyword(Keyword::Table) {
                    Ok(vibesql_ast::Statement::CreateTable(self.parse_create_table_statement()?))
                } else if self.peek_next_keyword(Keyword::Schema) {
                    Ok(vibesql_ast::Statement::CreateSchema(self.parse_create_schema_statement()?))
                } else if self.peek_next_keyword(Keyword::Role) {
                    Ok(vibesql_ast::Statement::CreateRole(self.parse_create_role_statement()?))
                } else if self.peek_next_keyword(Keyword::Domain) {
                    Ok(vibesql_ast::Statement::CreateDomain(self.parse_create_domain_statement()?))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    Ok(vibesql_ast::Statement::CreateSequence(
                        self.parse_create_sequence_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Type) {
                    Ok(vibesql_ast::Statement::CreateType(self.parse_create_type_statement()?))
                } else if self.peek_next_keyword(Keyword::Collation) {
                    Ok(vibesql_ast::Statement::CreateCollation(
                        self.parse_create_collation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Character) {
                    Ok(vibesql_ast::Statement::CreateCharacterSet(
                        self.parse_create_character_set_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Translation) {
                    Ok(vibesql_ast::Statement::CreateTranslation(
                        self.parse_create_translation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::View) {
                    Ok(vibesql_ast::Statement::CreateView(self.parse_create_view_statement()?))
                } else if self.peek_next_keyword(Keyword::Temp)
                    || self.peek_next_keyword(Keyword::Temporary)
                {
                    // CREATE TEMP VIEW or CREATE TEMPORARY VIEW
                    Ok(vibesql_ast::Statement::CreateView(self.parse_create_view_statement()?))
                } else if self.peek_next_keyword(Keyword::Trigger) {
                    Ok(vibesql_ast::Statement::CreateTrigger(
                        self.parse_create_trigger_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Index)
                    || self.peek_next_keyword(Keyword::Unique)
                    || self.peek_next_keyword(Keyword::Fulltext)
                    || self.peek_next_keyword(Keyword::Spatial)
                {
                    Ok(vibesql_ast::Statement::CreateIndex(self.parse_create_index_statement()?))
                } else if self.peek_next_keyword(Keyword::Assertion) {
                    Ok(vibesql_ast::Statement::CreateAssertion(
                        self.parse_create_assertion_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Procedure) {
                    Ok(vibesql_ast::Statement::CreateProcedure(
                        self.parse_create_procedure_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Function) {
                    Ok(vibesql_ast::Statement::CreateFunction(
                        self.parse_create_function_statement()?,
                    ))
                } else {
                    Err(ParseError {
                        message:
                            "Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, VIEW, TRIGGER, INDEX, ASSERTION, PROCEDURE, or FUNCTION after CREATE"
                                .to_string(),
                    })
                }
            }
            Token::Keyword(Keyword::Drop) => {
                if self.peek_next_keyword(Keyword::Table) {
                    Ok(vibesql_ast::Statement::DropTable(self.parse_drop_table_statement()?))
                } else if self.peek_next_keyword(Keyword::Schema) {
                    Ok(vibesql_ast::Statement::DropSchema(self.parse_drop_schema_statement()?))
                } else if self.peek_next_keyword(Keyword::Role) {
                    Ok(vibesql_ast::Statement::DropRole(self.parse_drop_role_statement()?))
                } else if self.peek_next_keyword(Keyword::Domain) {
                    Ok(vibesql_ast::Statement::DropDomain(self.parse_drop_domain_statement()?))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    Ok(vibesql_ast::Statement::DropSequence(self.parse_drop_sequence_statement()?))
                } else if self.peek_next_keyword(Keyword::Type) {
                    Ok(vibesql_ast::Statement::DropType(self.parse_drop_type_statement()?))
                } else if self.peek_next_keyword(Keyword::Collation) {
                    Ok(vibesql_ast::Statement::DropCollation(
                        self.parse_drop_collation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Character) {
                    Ok(vibesql_ast::Statement::DropCharacterSet(
                        self.parse_drop_character_set_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Translation) {
                    Ok(vibesql_ast::Statement::DropTranslation(
                        self.parse_drop_translation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::View) {
                    Ok(vibesql_ast::Statement::DropView(self.parse_drop_view_statement()?))
                } else if self.peek_next_keyword(Keyword::Trigger) {
                    Ok(vibesql_ast::Statement::DropTrigger(self.parse_drop_trigger_statement()?))
                } else if self.peek_next_keyword(Keyword::Index) {
                    Ok(vibesql_ast::Statement::DropIndex(self.parse_drop_index_statement()?))
                } else if self.peek_next_keyword(Keyword::Assertion) {
                    Ok(vibesql_ast::Statement::DropAssertion(
                        self.parse_drop_assertion_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Procedure) {
                    Ok(vibesql_ast::Statement::DropProcedure(
                        self.parse_drop_procedure_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Function) {
                    Ok(vibesql_ast::Statement::DropFunction(self.parse_drop_function_statement()?))
                } else {
                    Err(ParseError {
                        message:
                            "Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, VIEW, TRIGGER, INDEX, ASSERTION, PROCEDURE, or FUNCTION after DROP"
                                .to_string(),
                    })
                }
            }
            Token::Keyword(Keyword::Truncate) => {
                let truncate_stmt = self.parse_truncate_table_statement()?;
                Ok(vibesql_ast::Statement::TruncateTable(truncate_stmt))
            }
            Token::Keyword(Keyword::Alter) => {
                if self.peek_next_keyword(Keyword::Table) {
                    let alter_stmt = self.parse_alter_table_statement()?;
                    Ok(vibesql_ast::Statement::AlterTable(alter_stmt))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    let alter_stmt = self.parse_alter_sequence_statement()?;
                    Ok(vibesql_ast::Statement::AlterSequence(alter_stmt))
                } else if self.peek_next_keyword(Keyword::Trigger) {
                    let alter_stmt = self.parse_alter_trigger_statement()?;
                    Ok(vibesql_ast::Statement::AlterTrigger(alter_stmt))
                } else {
                    Err(ParseError {
                        message: "Expected TABLE, SEQUENCE, or TRIGGER after ALTER".to_string(),
                    })
                }
            }
            Token::Keyword(Keyword::Reindex) => {
                let reindex_stmt = self.parse_reindex_statement()?;
                Ok(vibesql_ast::Statement::Reindex(reindex_stmt))
            }
            Token::Keyword(Keyword::Analyze) => {
                let analyze_stmt = self.parse_analyze_statement()?;
                Ok(vibesql_ast::Statement::Analyze(analyze_stmt))
            }
            Token::Keyword(Keyword::Explain) => {
                let explain_stmt = self.parse_explain_statement()?;
                Ok(vibesql_ast::Statement::Explain(explain_stmt))
            }
            Token::Keyword(Keyword::Begin) | Token::Keyword(Keyword::Start) => {
                let begin_stmt = self.parse_begin_statement()?;
                Ok(vibesql_ast::Statement::BeginTransaction(begin_stmt))
            }
            Token::Keyword(Keyword::Commit) => {
                let commit_stmt = self.parse_commit_statement()?;
                Ok(vibesql_ast::Statement::Commit(commit_stmt))
            }
            Token::Keyword(Keyword::Rollback) => {
                // Check if this is ROLLBACK TO SAVEPOINT by looking ahead
                let saved_position = self.position;
                self.advance(); // consume ROLLBACK
                if self.peek_keyword(Keyword::To) {
                    // Reset and parse as ROLLBACK TO SAVEPOINT
                    self.position = saved_position;
                    let rollback_to_stmt = self.parse_rollback_to_savepoint_statement()?;
                    Ok(vibesql_ast::Statement::RollbackToSavepoint(rollback_to_stmt))
                } else {
                    // Reset and parse as regular ROLLBACK
                    self.position = saved_position;
                    let rollback_stmt = self.parse_rollback_statement()?;
                    Ok(vibesql_ast::Statement::Rollback(rollback_stmt))
                }
            }
            Token::Keyword(Keyword::Savepoint) => {
                let savepoint_stmt = self.parse_savepoint_statement()?;
                Ok(vibesql_ast::Statement::Savepoint(savepoint_stmt))
            }
            Token::Keyword(Keyword::Release) => {
                let release_stmt = self.parse_release_savepoint_statement()?;
                Ok(vibesql_ast::Statement::ReleaseSavepoint(release_stmt))
            }
            Token::Keyword(Keyword::Set) => {
                // Look ahead to determine which SET statement this is
                if self.peek_next_keyword(Keyword::Schema) {
                    let set_stmt = self.parse_set_schema_statement()?;
                    Ok(vibesql_ast::Statement::SetSchema(set_stmt))
                } else if self.peek_next_keyword(Keyword::Catalog) {
                    let set_stmt = schema::parse_set_catalog(self)?;
                    Ok(vibesql_ast::Statement::SetCatalog(set_stmt))
                } else if self.peek_next_keyword(Keyword::Names) {
                    let set_stmt = schema::parse_set_names(self)?;
                    Ok(vibesql_ast::Statement::SetNames(set_stmt))
                } else if self.peek_next_keyword(Keyword::Time) {
                    let set_stmt = schema::parse_set_time_zone(self)?;
                    Ok(vibesql_ast::Statement::SetTimeZone(set_stmt))
                } else if self.peek_next_keyword(Keyword::Transaction) {
                    let set_stmt = self.parse_set_transaction_statement()?;
                    Ok(vibesql_ast::Statement::SetTransaction(set_stmt))
                } else if self.peek_next_keyword(Keyword::Local) {
                    // SET LOCAL TRANSACTION
                    let set_stmt = self.parse_set_transaction_statement()?;
                    Ok(vibesql_ast::Statement::SetTransaction(set_stmt))
                } else {
                    // Try to parse as SET variable statement (SESSION/GLOBAL or direct variable)
                    let set_stmt = schema::parse_set_variable(self)?;
                    Ok(vibesql_ast::Statement::SetVariable(set_stmt))
                }
            }
            Token::Keyword(Keyword::Grant) => {
                let grant_stmt = self.parse_grant_statement()?;
                Ok(vibesql_ast::Statement::Grant(grant_stmt))
            }
            Token::Keyword(Keyword::Revoke) => {
                let revoke_stmt = self.parse_revoke_statement()?;
                Ok(vibesql_ast::Statement::Revoke(revoke_stmt))
            }
            Token::Keyword(Keyword::Declare) => {
                let declare_cursor_stmt = self.parse_declare_cursor_statement()?;
                Ok(vibesql_ast::Statement::DeclareCursor(declare_cursor_stmt))
            }
            Token::Keyword(Keyword::Open) => {
                let open_cursor_stmt = self.parse_open_cursor_statement()?;
                Ok(vibesql_ast::Statement::OpenCursor(open_cursor_stmt))
            }
            Token::Keyword(Keyword::Fetch) => {
                let fetch_stmt = self.parse_fetch_statement()?;
                Ok(vibesql_ast::Statement::Fetch(fetch_stmt))
            }
            Token::Keyword(Keyword::Close) => {
                let close_cursor_stmt = self.parse_close_cursor_statement()?;
                Ok(vibesql_ast::Statement::CloseCursor(close_cursor_stmt))
            }
            Token::Keyword(Keyword::Call) => {
                let call_stmt = self.parse_call_statement()?;
                Ok(vibesql_ast::Statement::Call(call_stmt))
            }
            Token::Keyword(Keyword::Show) => self.parse_show_statement(),
            Token::Keyword(Keyword::Describe) => {
                let describe_stmt = self.parse_describe_statement()?;
                Ok(vibesql_ast::Statement::Describe(describe_stmt))
            }
            Token::Keyword(Keyword::Prepare) => {
                let prepare_stmt = self.parse_prepare_statement()?;
                Ok(vibesql_ast::Statement::Prepare(prepare_stmt))
            }
            Token::Keyword(Keyword::Execute) => {
                let execute_stmt = self.parse_execute_statement()?;
                Ok(vibesql_ast::Statement::Execute(execute_stmt))
            }
            Token::Keyword(Keyword::Deallocate) => {
                let deallocate_stmt = self.parse_deallocate_statement()?;
                Ok(vibesql_ast::Statement::Deallocate(deallocate_stmt))
            }
            _ => {
                Err(ParseError { message: format!("Expected statement, found {:?}", self.peek()) })
            }
        }
    }

    /// Parse BEGIN [TRANSACTION] statement
    pub fn parse_begin_statement(&mut self) -> Result<vibesql_ast::BeginStmt, ParseError> {
        transaction::parse_begin_statement(self)
    }

    /// Parse COMMIT statement
    pub fn parse_commit_statement(&mut self) -> Result<vibesql_ast::CommitStmt, ParseError> {
        transaction::parse_commit_statement(self)
    }

    /// Parse ROLLBACK statement
    pub fn parse_rollback_statement(&mut self) -> Result<vibesql_ast::RollbackStmt, ParseError> {
        transaction::parse_rollback_statement(self)
    }

    /// Parse ALTER TABLE statement
    pub fn parse_alter_table_statement(
        &mut self,
    ) -> Result<vibesql_ast::AlterTableStmt, ParseError> {
        alter::parse_alter_table(self)
    }

    /// Parse SAVEPOINT statement
    pub fn parse_savepoint_statement(&mut self) -> Result<vibesql_ast::SavepointStmt, ParseError> {
        transaction::parse_savepoint_statement(self)
    }

    /// Parse ROLLBACK TO SAVEPOINT statement
    pub fn parse_rollback_to_savepoint_statement(
        &mut self,
    ) -> Result<vibesql_ast::RollbackToSavepointStmt, ParseError> {
        transaction::parse_rollback_to_savepoint_statement(self)
    }

    /// Parse RELEASE SAVEPOINT statement
    pub fn parse_release_savepoint_statement(
        &mut self,
    ) -> Result<vibesql_ast::ReleaseSavepointStmt, ParseError> {
        transaction::parse_release_savepoint_statement(self)
    }

    /// Parse CREATE SCHEMA statement
    pub fn parse_create_schema_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateSchemaStmt, ParseError> {
        schema::parse_create_schema(self)
    }

    /// Parse DROP SCHEMA statement
    pub fn parse_drop_schema_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropSchemaStmt, ParseError> {
        schema::parse_drop_schema(self)
    }

    /// Parse SET SCHEMA statement
    pub fn parse_set_schema_statement(&mut self) -> Result<vibesql_ast::SetSchemaStmt, ParseError> {
        schema::parse_set_schema(self)
    }

    /// Parse GRANT statement
    pub fn parse_grant_statement(&mut self) -> Result<vibesql_ast::GrantStmt, ParseError> {
        grant::parse_grant(self)
    }

    /// Parse REVOKE statement
    pub fn parse_revoke_statement(&mut self) -> Result<vibesql_ast::RevokeStmt, ParseError> {
        revoke::parse_revoke(self)
    }

    /// Parse CREATE ROLE statement
    pub fn parse_create_role_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateRoleStmt, ParseError> {
        role::parse_create_role(self)
    }

    /// Parse DROP ROLE statement
    pub fn parse_drop_role_statement(&mut self) -> Result<vibesql_ast::DropRoleStmt, ParseError> {
        role::parse_drop_role(self)
    }

    // ========================================================================
    // Advanced SQL Object Parsers (SQL:1999)
    // ========================================================================

    /// Parse CREATE DOMAIN statement (uses full implementation from domain module)
    pub fn parse_create_domain_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateDomainStmt, ParseError> {
        domain::parse_create_domain(self)
    }

    /// Parse DROP DOMAIN statement (uses full implementation from domain module)
    pub fn parse_drop_domain_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropDomainStmt, ParseError> {
        domain::parse_drop_domain(self)
    }

    /// Parse CREATE SEQUENCE statement
    pub fn parse_create_sequence_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateSequenceStmt, ParseError> {
        advanced_objects::parse_create_sequence(self)
    }

    /// Parse DROP SEQUENCE statement
    pub fn parse_drop_sequence_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropSequenceStmt, ParseError> {
        advanced_objects::parse_drop_sequence(self)
    }

    /// Parse ALTER SEQUENCE statement
    pub fn parse_alter_sequence_statement(
        &mut self,
    ) -> Result<vibesql_ast::AlterSequenceStmt, ParseError> {
        advanced_objects::parse_alter_sequence(self)
    }

    /// Parse CREATE TYPE statement
    pub fn parse_create_type_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateTypeStmt, ParseError> {
        advanced_objects::parse_create_type(self)
    }

    /// Parse SET TRANSACTION statement
    pub fn parse_set_transaction_statement(
        &mut self,
    ) -> Result<vibesql_ast::SetTransactionStmt, ParseError> {
        // SET keyword
        self.expect_keyword(Keyword::Set)?;

        // Optional LOCAL keyword
        let local = self.try_consume_keyword(Keyword::Local);

        // TRANSACTION keyword
        self.expect_keyword(Keyword::Transaction)?;

        // Parse optional characteristics
        let mut isolation_level = None;
        let mut access_mode = None;

        loop {
            if self.try_consume_keyword(Keyword::Serializable) {
                isolation_level = Some(vibesql_ast::IsolationLevel::Serializable);
            } else if self.try_consume_keyword(Keyword::Read) {
                if self.try_consume_keyword(Keyword::Only) {
                    access_mode = Some(vibesql_ast::TransactionAccessMode::ReadOnly);
                } else if self.try_consume_keyword(Keyword::Write) {
                    access_mode = Some(vibesql_ast::TransactionAccessMode::ReadWrite);
                } else {
                    return Err(ParseError {
                        message: "Expected ONLY or WRITE after READ".to_string(),
                    });
                }
            } else if self.try_consume_keyword(Keyword::Isolation) {
                self.expect_keyword(Keyword::Level)?;
                if self.try_consume_keyword(Keyword::Serializable) {
                    isolation_level = Some(vibesql_ast::IsolationLevel::Serializable);
                } else {
                    return Err(ParseError {
                        message: "Expected SERIALIZABLE after ISOLATION LEVEL".to_string(),
                    });
                }
            } else {
                break;
            }

            // Check for comma (more characteristics)
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(vibesql_ast::SetTransactionStmt { local, isolation_level, access_mode })
    }

    /// Parse DROP TYPE statement
    pub fn parse_drop_type_statement(&mut self) -> Result<vibesql_ast::DropTypeStmt, ParseError> {
        advanced_objects::parse_drop_type(self)
    }

    /// Parse CREATE COLLATION statement
    pub fn parse_create_collation_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateCollationStmt, ParseError> {
        advanced_objects::parse_create_collation(self)
    }

    /// Parse DROP COLLATION statement
    pub fn parse_drop_collation_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropCollationStmt, ParseError> {
        advanced_objects::parse_drop_collation(self)
    }

    /// Parse CREATE CHARACTER SET statement
    pub fn parse_create_character_set_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateCharacterSetStmt, ParseError> {
        advanced_objects::parse_create_character_set(self)
    }

    /// Parse DROP CHARACTER SET statement
    pub fn parse_drop_character_set_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropCharacterSetStmt, ParseError> {
        advanced_objects::parse_drop_character_set(self)
    }

    /// Parse CREATE TRANSLATION statement
    pub fn parse_create_translation_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateTranslationStmt, ParseError> {
        advanced_objects::parse_create_translation(self)
    }

    /// Parse DROP TRANSLATION statement
    pub fn parse_drop_translation_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropTranslationStmt, ParseError> {
        advanced_objects::parse_drop_translation(self)
    }

    /// Parse CREATE ASSERTION statement
    pub fn parse_create_assertion_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateAssertionStmt, ParseError> {
        advanced_objects::parse_create_assertion(self)
    }

    /// Parse DROP ASSERTION statement
    pub fn parse_drop_assertion_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropAssertionStmt, ParseError> {
        advanced_objects::parse_drop_assertion(self)
    }

    /// Parse CREATE PROCEDURE statement
    pub fn parse_create_procedure_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateProcedureStmt, ParseError> {
        self.advance(); // consume CREATE
        self.advance(); // consume PROCEDURE
        self.parse_create_procedure()
    }

    /// Parse DROP PROCEDURE statement
    pub fn parse_drop_procedure_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropProcedureStmt, ParseError> {
        self.advance(); // consume DROP
        self.advance(); // consume PROCEDURE
        self.parse_drop_procedure()
    }

    /// Parse CREATE FUNCTION statement
    pub fn parse_create_function_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateFunctionStmt, ParseError> {
        self.advance(); // consume CREATE
        self.advance(); // consume FUNCTION
        self.parse_create_function()
    }

    /// Parse DROP FUNCTION statement
    pub fn parse_drop_function_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropFunctionStmt, ParseError> {
        self.advance(); // consume DROP
        self.advance(); // consume FUNCTION
        self.parse_drop_function()
    }

    /// Parse CALL statement
    pub fn parse_call_statement(&mut self) -> Result<vibesql_ast::CallStmt, ParseError> {
        self.advance(); // consume CALL
        self.parse_call()
    }
}
