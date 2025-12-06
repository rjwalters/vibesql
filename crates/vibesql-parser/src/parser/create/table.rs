//! CREATE TABLE parsing

use super::super::*;

impl Parser {
    /// Parse CREATE TABLE statement
    pub(in crate::parser) fn parse_create_table_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateTableStmt, ParseError> {
        self.expect_keyword(Keyword::Create)?;
        self.expect_keyword(Keyword::Table)?;

        // Check for optional IF NOT EXISTS clause
        let if_not_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse table name (supports schema.table)
        let table_name = self.parse_qualified_identifier()?;

        // Parse column definitions and table constraints
        self.expect_token(Token::LParen)?;
        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();

        loop {
            // Check if this is a table-level constraint (including CONSTRAINT keyword)
            if self.peek_keyword(Keyword::Constraint)
                || self.peek_keyword(Keyword::Primary)
                || self.peek_keyword(Keyword::Foreign)
                || self.peek_keyword(Keyword::Unique)
                || self.peek_keyword(Keyword::Check)
                || self.peek_keyword(Keyword::Fulltext)
            {
                table_constraints.push(self.parse_table_constraint()?);
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                    continue;
                } else {
                    break;
                }
            }

            // Parse column name
            let name = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                // Allow unreserved keywords (like TIMESTAMP, DATE, TIME, INTERVAL) as column names
                Token::Keyword(kw) if kw.can_be_identifier() => {
                    let col_name = format!("{}", kw); // Already uppercase from Display impl
                    self.advance();
                    col_name
                }
                _ => return Err(ParseError { message: "Expected column name".to_string() }),
            };

            // Parse data type
            let data_type = self.parse_data_type()?;

            // Parse optional DEFAULT clause (before COMMENT, per MySQL standard)
            let default_value = if self.peek_keyword(Keyword::Default) {
                self.advance(); // consume DEFAULT
                Some(Box::new(self.parse_expression()?))
            } else {
                None
            };

            // Parse optional COMMENT clause (after DEFAULT, per MySQL standard)
            let comment = if self.peek_keyword(Keyword::Comment) {
                self.advance(); // consume COMMENT
                match self.peek() {
                    Token::String(s) => {
                        let c = s.clone();
                        self.advance();
                        Some(c)
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected string literal after COMMENT".to_string(),
                        })
                    }
                }
            } else {
                None
            };

            // Parse column constraints (which may include NOT NULL)
            let constraints = self.parse_column_constraints()?;

            // Determine nullability based on constraints
            let nullable = !constraints
                .iter()
                .any(|c| matches!(&c.kind, vibesql_ast::ColumnConstraintKind::NotNull));

            columns.push(vibesql_ast::ColumnDef {
                name,
                data_type,
                nullable,
                constraints,
                default_value,
                comment,
            });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect_token(Token::RParen)?;

        // Parse optional table options (MySQL extensions)
        let table_options = self.parse_table_options()?;

        // Parse optional WITH OIDS / WITHOUT OIDS clause
        // This is a PostgreSQL extension that we parse but ignore in execution
        if self.peek_keyword(Keyword::With) {
            self.advance(); // consume WITH
            self.expect_keyword(Keyword::Oids)?;
            // We parse it but don't store it - just for compatibility
        } else if self.peek_keyword(Keyword::Without) {
            self.advance(); // consume WITHOUT
            self.expect_keyword(Keyword::Oids)?;
            // We parse it but don't store it - just for compatibility
        }

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::CreateTableStmt {
            if_not_exists,
            table_name,
            columns,
            table_constraints,
            table_options,
        })
    }
}
