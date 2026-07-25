use super::*;

impl Parser {
    /// Parse SELECT list (items after SELECT keyword)
    pub(crate) fn parse_select_list(&mut self) -> Result<Vec<vibesql_ast::SelectItem>, ParseError> {
        let mut items = Vec::new();

        loop {
            let item = self.parse_select_item()?;
            items.push(item);

            // Check if there's a comma (more items)
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(items)
    }

    /// Parse derived column list: AS (col1, col2, ...)
    /// SQL:1999 Feature E051-07/08
    fn parse_derived_column_list(&mut self) -> Result<Option<Vec<String>>, ParseError> {
        if !self.peek_keyword(Keyword::As) {
            return Ok(None);
        }

        self.consume_keyword(Keyword::As)?;
        self.expect_token(Token::LParen)?;

        let mut columns = Vec::new();
        loop {
            match self.peek() {
                Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                    columns.push(id.clone());
                    self.advance();
                }
                // Allow unreserved keywords as column names
                Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                    columns.push(format!("{}", kw).to_lowercase());
                    self.advance();
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in derived column list".to_string(),
                    })
                }
            }

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect_token(Token::RParen)?;

        if columns.is_empty() {
            return Err(ParseError { message: "Derived column list cannot be empty".to_string() });
        }

        Ok(Some(columns))
    }

    /// Parse a single SELECT item
    pub(crate) fn parse_select_item(&mut self) -> Result<vibesql_ast::SelectItem, ParseError> {
        // Check for qualified wildcard (table.* or alias.*)
        let saved_position = self.position;
        let qualifier = match self.peek() {
            Token::Identifier(ref qualifier) | Token::DelimitedIdentifier(ref qualifier) => {
                Some(qualifier.clone())
            }
            // Allow contextual keywords (M, YEAR, WINDOW, ...) as wildcard qualifiers,
            // matching the keyword-as-identifier handling in parse_identifier_expression.
            // SQL:1999 normalizes unquoted identifiers to lowercase.
            Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                Some(format!("{}", kw).to_lowercase())
            }
            _ => None,
        };

        if let Some(qualifier) = qualifier {
            self.advance(); // consume identifier

            if matches!(self.peek(), Token::Symbol('.')) {
                self.advance(); // consume dot

                if matches!(self.peek(), Token::Symbol('*')) {
                    self.advance(); // consume asterisk
                    let alias = self.parse_derived_column_list()?;
                    return Ok(vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias });
                }
            }
        }

        // Not a qualified wildcard, backtrack
        self.position = saved_position;

        // Check for wildcard (*)
        if matches!(self.peek(), Token::Symbol('*')) {
            self.advance();
            let alias = self.parse_derived_column_list()?;
            return Ok(vibesql_ast::SelectItem::Wildcard { alias });
        }

        // Record start position before parsing expression
        let start_pos = self.position;

        // Parse expression
        let expr = self.parse_expression()?;

        // Record end position after parsing expression
        let end_pos = self.position;

        // Reconstruct source text from tokens consumed during expression parsing.
        // This preserves the original identifier case and operator style (e.g., `f1+F2`
        // instead of `(F1 + F2)`) for use as column names when no alias is provided.
        let source_text = self.reconstruct_source_text(start_pos, end_pos);

        // Check for optional AS alias (MySQL allows aliases without AS keyword)
        // Keywords are allowed as aliases after AS (e.g., d_year AS year)
        let alias = if self.peek_keyword(Keyword::As) {
            self.consume_keyword(Keyword::As)?;
            Some(self.parse_alias_name()?)
        } else if matches!(self.peek(), Token::Identifier(_) | Token::DelimitedIdentifier(_)) {
            // MySQL compatibility: allow aliases without AS keyword
            match self.peek() {
                Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                    let alias = id.clone();
                    self.advance();
                    Some(alias)
                }
                _ => None,
            }
        } else if let Token::Keyword { keyword, original, .. } = self.peek() {
            // A keyword usable as a fallback identifier is a valid AS-less alias,
            // e.g. `SELECT sum(x) over FROM over` aliases the aggregate as `over`
            // (window6 5.0). Clause/operator keywords (FROM, WHERE, ORDER, ...) are
            // not `can_be_identifier()` and so continue to terminate the item.
            //
            // RETURNING and WINDOW are excluded even though they are fallback
            // identifiers: each introduces a trailing clause that can directly
            // follow the final select item (`INSERT ... SELECT 1 RETURNING a`,
            // `SELECT 1 WINDOW w AS ()`), so grabbing them as an AS-less alias would
            // swallow the clause (regression on insert-compound-returning, #5271).
            if keyword.can_be_identifier()
                && !matches!(keyword, Keyword::Returning | Keyword::Window)
            {
                let alias = original.clone();
                self.advance();
                Some(alias)
            } else {
                None
            }
        } else {
            None
        };

        Ok(vibesql_ast::SelectItem::Expression { expr, alias, source_text })
    }
}
