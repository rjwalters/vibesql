//! Prepared statement parsing (SQL:1999 Feature E141)
//!
//! This module implements parsing for:
//! - PREPARE statement
//! - EXECUTE statement
//! - DEALLOCATE statement

use vibesql_ast::{
    DeallocateStmt, DeallocateTarget, ExecuteStmt, PrepareStmt, PreparedStatementBody,
};

use crate::{
    keywords::Keyword,
    parser::{ParseError, Parser},
    token::Token,
};

impl Parser {
    /// Parse PREPARE statement
    ///
    /// Syntax variants:
    /// ```sql
    /// -- SQL Standard syntax
    /// PREPARE statement_name FROM 'sql_string'
    ///
    /// -- PostgreSQL extended syntax (inline statement)
    /// PREPARE statement_name [(data_type, ...)] AS preparable_stmt
    /// ```
    ///
    /// Examples:
    /// ```sql
    /// PREPARE my_select FROM 'SELECT * FROM users WHERE id = $1'
    /// PREPARE my_insert(int, text) AS INSERT INTO users VALUES ($1, $2)
    /// PREPARE stmt AS SELECT * FROM users WHERE id = ?
    /// ```
    pub(super) fn parse_prepare_statement(&mut self) -> Result<PrepareStmt, ParseError> {
        // PREPARE keyword
        self.expect_keyword(Keyword::Prepare)?;

        // Statement name
        let name = self.parse_identifier()?;

        // Check for optional parameter types (PostgreSQL extended syntax)
        let param_types = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume '('
            let types = self.parse_comma_separated_list(|p| p.parse_data_type_name())?;
            self.expect_token(Token::RParen)?;
            Some(types)
        } else {
            None
        };

        // Parse statement body (FROM 'string' or AS statement)
        let statement = if self.try_consume_keyword(Keyword::From) {
            // SQL Standard: FROM 'sql_string'
            let sql_string = self.parse_string_literal()?;
            PreparedStatementBody::SqlString(sql_string)
        } else if self.try_consume_keyword(Keyword::As) {
            // PostgreSQL: AS preparable_stmt
            // Parse the inner statement (recursively)
            let inner_stmt = self.parse_preparable_statement()?;
            PreparedStatementBody::ParsedStatement(Box::new(inner_stmt))
        } else {
            return Err(ParseError {
                message: "Expected FROM or AS after PREPARE statement_name".to_string(),
            });
        };

        Ok(PrepareStmt { name, param_types, statement })
    }

    /// Parse EXECUTE statement
    ///
    /// Syntax variants:
    /// ```sql
    /// -- SQL Standard syntax
    /// EXECUTE statement_name [USING value, ...]
    ///
    /// -- PostgreSQL extended syntax
    /// EXECUTE statement_name [(param_value, ...)]
    /// ```
    ///
    /// Examples:
    /// ```sql
    /// EXECUTE my_select
    /// EXECUTE my_insert USING 1, 'Alice'
    /// EXECUTE my_insert(1, 'Alice')
    /// ```
    pub(super) fn parse_execute_statement(&mut self) -> Result<ExecuteStmt, ParseError> {
        // EXECUTE keyword
        self.expect_keyword(Keyword::Execute)?;

        // Statement name
        let name = self.parse_identifier()?;

        // Parse parameters (USING clause or parenthesized list)
        let params = if self.try_consume_keyword(Keyword::Using) {
            // SQL Standard: USING value, ...
            self.parse_comma_separated_list(|p| p.parse_expression())?
        } else if matches!(self.peek(), Token::LParen) {
            // PostgreSQL: (value, ...)
            self.advance(); // consume '('
            let exprs = if matches!(self.peek(), Token::RParen) {
                Vec::new()
            } else {
                self.parse_comma_separated_list(|p| p.parse_expression())?
            };
            self.expect_token(Token::RParen)?;
            exprs
        } else {
            Vec::new()
        };

        Ok(ExecuteStmt { name, params })
    }

    /// Parse DEALLOCATE statement
    ///
    /// Syntax:
    /// ```sql
    /// DEALLOCATE [PREPARE] statement_name
    /// DEALLOCATE ALL
    /// ```
    ///
    /// Examples:
    /// ```sql
    /// DEALLOCATE my_select
    /// DEALLOCATE PREPARE my_insert
    /// DEALLOCATE ALL
    /// ```
    pub(super) fn parse_deallocate_statement(&mut self) -> Result<DeallocateStmt, ParseError> {
        // DEALLOCATE keyword
        self.expect_keyword(Keyword::Deallocate)?;

        // Optional PREPARE keyword
        self.try_consume_keyword(Keyword::Prepare);

        // Target: ALL or statement_name
        let target = if self.try_consume_keyword(Keyword::All) {
            DeallocateTarget::All
        } else {
            let name = self.parse_identifier()?;
            DeallocateTarget::Name(name)
        };

        Ok(DeallocateStmt { target })
    }

    /// Parse a "preparable" statement - statements that can be prepared
    ///
    /// This includes SELECT, INSERT, UPDATE, DELETE, and VALUES
    fn parse_preparable_statement(&mut self) -> Result<vibesql_ast::Statement, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::With) => {
                let select_stmt = self.parse_select_statement()?;
                Ok(vibesql_ast::Statement::Select(Box::new(select_stmt)))
            }
            Token::Keyword(Keyword::Insert) => {
                let insert_stmt = self.parse_insert_statement()?;
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
            _ => Err(ParseError {
                message: format!(
                    "Expected SELECT, INSERT, UPDATE, or DELETE for preparable statement, found {:?}",
                    self.peek()
                ),
            }),
        }
    }

    /// Parse a data type name (for parameter types in PREPARE)
    fn parse_data_type_name(&mut self) -> Result<String, ParseError> {
        // Parse identifier (possibly multi-part like CHARACTER VARYING)
        let mut type_name = self.parse_identifier()?;

        // Handle multi-word types like "CHARACTER VARYING"
        while matches!(self.peek(), Token::Keyword(Keyword::Varying)) {
            self.advance();
            type_name.push_str(" VARYING");
        }

        // Handle optional size/precision like VARCHAR(100) or DECIMAL(10,2)
        if matches!(self.peek(), Token::LParen) {
            self.advance();
            type_name.push('(');

            // Parse first number
            if let Token::Number(n) = self.peek().clone() {
                self.advance();
                type_name.push_str(&n);
            }

            // Parse optional second number (for precision)
            if matches!(self.peek(), Token::Comma) {
                self.advance();
                type_name.push(',');
                if let Token::Number(n) = self.peek().clone() {
                    self.advance();
                    type_name.push_str(&n);
                }
            }

            self.expect_token(Token::RParen)?;
            type_name.push(')');
        }

        Ok(type_name)
    }

    /// Parse a string literal
    fn parse_string_literal(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            Token::String(s) => {
                self.advance();
                Ok(s)
            }
            _ => Err(ParseError {
                message: format!("Expected string literal, found {:?}", self.peek()),
            }),
        }
    }
}
