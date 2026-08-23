use super::*;

// Submodules
mod functions;
mod identifiers;
mod literals;
mod operators;
mod special_forms;
mod subqueries;

impl Parser {
    /// Parse an expression (entry point)
    pub(super) fn parse_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        self.parse_or_expression()
    }

    /// Parse primary expression (literals, identifiers, parenthesized expressions)
    pub(super) fn parse_primary_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        // Try to parse as a placeholder (?)
        if matches!(self.peek(), Token::Placeholder) {
            self.advance();
            let index = self.placeholder_count;
            self.placeholder_count += 1;
            return Ok(vibesql_ast::Expression::Placeholder(index));
        }

        // Try to parse as a numbered placeholder ($1, $2, etc.)
        if let Token::NumberedPlaceholder(n) = self.peek() {
            let index = *n;
            self.advance();
            return Ok(vibesql_ast::Expression::NumberedPlaceholder(index));
        }

        // Try to parse as a named placeholder (:name)
        if let Token::NamedPlaceholder(name) = self.peek() {
            let param_name = name.clone();
            self.advance();
            return Ok(vibesql_ast::Expression::NamedPlaceholder(param_name));
        }

        // Try to parse as an "at" named placeholder (@name). SQLite documents
        // `@AAAA` as working "exactly like a colon [`:AAAA`]" parameter
        // (R-49783-61279): the lexer already tokenizes `@name` as
        // `Token::UserVariable` (shared with the MySQL-style `SELECT ... INTO
        // @var` procedural clause), but until now nothing consumed that token
        // in ordinary expression position, so `SELECT @hello` failed with a
        // generic `near "@hello": syntax error` instead of being treated as a
        // bind parameter. `CREATE VIEW`/`CREATE TRIGGER` bodies still reject
        // `@name` via their own raw-token scan (`token_is_variable`), which is
        // unaffected by this change since it runs before expression parsing.
        if let Token::UserVariable(name) = self.peek() {
            let param_name = name.clone();
            self.advance();
            return Ok(vibesql_ast::Expression::NamedPlaceholder(param_name));
        }

        // Try to parse as a literal
        if let Some(expr) = self.parse_literal()? {
            return Ok(expr);
        }

        // Try to parse as a session variable (@@sql_mode, @@session.variable, etc.)
        if let Token::SessionVariable(name) = self.peek() {
            let var_name = name.clone();
            self.advance();
            return Ok(vibesql_ast::Expression::SessionVariable { name: var_name });
        }

        // Try to parse as a special form (CAST, EXISTS, NOT EXISTS)
        if let Some(expr) = self.parse_special_form()? {
            return Ok(expr);
        }

        // Try to parse as current date/time functions
        if let Some(expr) = self.parse_current_datetime_function()? {
            return Ok(expr);
        }

        // Try to parse as NEXT VALUE FOR sequence expression
        if let Some(expr) = self.parse_sequence_value_function()? {
            return Ok(expr);
        }

        // Try to parse as a function call
        if let Some(expr) = self.parse_function_call()? {
            return Ok(expr);
        }

        // Try to parse as an identifier (column reference or qualified name)
        if let Some(expr) = self.parse_identifier_expression()? {
            return Ok(expr);
        }

        // Try to parse as a parenthesized expression or scalar subquery
        if let Some(expr) = self.parse_parenthesized()? {
            return Ok(expr);
        }

        Err(ParseError { message: self.peek().syntax_error() })
    }
}
