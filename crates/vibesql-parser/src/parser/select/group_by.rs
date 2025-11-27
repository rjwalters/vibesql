//! GROUP BY clause parsing with ROLLUP, CUBE, and GROUPING SETS support
//!
//! SQL:1999 OLAP extensions for multi-dimensional aggregation:
//! - ROLLUP: Creates hierarchical subtotals
//! - CUBE: Creates all dimension combinations
//! - GROUPING SETS: Explicitly specifies groupings

use super::*;

impl Parser {
    /// Parse GROUP BY clause after "GROUP BY" keywords have been consumed
    ///
    /// Supports:
    /// - Simple: `GROUP BY a, b, c`
    /// - ROLLUP: `GROUP BY ROLLUP(a, b)`
    /// - CUBE: `GROUP BY CUBE(a, b)`
    /// - GROUPING SETS: `GROUP BY GROUPING SETS((a, b), (a), ())`
    pub(crate) fn parse_group_by_clause(
        &mut self,
    ) -> Result<vibesql_ast::GroupByClause, ParseError> {
        // Check for ROLLUP, CUBE, or GROUPING SETS
        if self.peek_keyword(Keyword::Rollup) {
            self.consume_keyword(Keyword::Rollup)?;
            self.expect_token(Token::LParen)?;
            let elements = self.parse_grouping_element_list()?;
            self.expect_token(Token::RParen)?;
            Ok(vibesql_ast::GroupByClause::Rollup(elements))
        } else if self.peek_keyword(Keyword::Cube) {
            self.consume_keyword(Keyword::Cube)?;
            self.expect_token(Token::LParen)?;
            let elements = self.parse_grouping_element_list()?;
            self.expect_token(Token::RParen)?;
            Ok(vibesql_ast::GroupByClause::Cube(elements))
        } else if self.peek_keyword(Keyword::Grouping) {
            self.consume_keyword(Keyword::Grouping)?;
            self.expect_keyword(Keyword::Sets)?;
            self.expect_token(Token::LParen)?;
            let sets = self.parse_grouping_sets_list()?;
            self.expect_token(Token::RParen)?;
            Ok(vibesql_ast::GroupByClause::GroupingSets(sets))
        } else {
            // Simple GROUP BY
            let group_exprs = self.parse_comma_separated_list(|p| p.parse_expression())?;
            Ok(vibesql_ast::GroupByClause::Simple(group_exprs))
        }
    }

    /// Parse comma-separated list of grouping elements for ROLLUP/CUBE
    ///
    /// Each element can be:
    /// - Single expression: `a`
    /// - Composite: `(a, b)` treated as one grouping unit
    fn parse_grouping_element_list(
        &mut self,
    ) -> Result<Vec<vibesql_ast::GroupingElement>, ParseError> {
        self.parse_comma_separated_list(|p| p.parse_grouping_element())
    }

    /// Parse a single grouping element
    ///
    /// Can be a single expression or a composite (multiple expressions in parentheses)
    fn parse_grouping_element(&mut self) -> Result<vibesql_ast::GroupingElement, ParseError> {
        if matches!(self.peek(), Token::LParen) {
            // Could be composite grouping or parenthesized expression
            // We need to look ahead to determine which
            self.advance(); // consume '('

            // Check if empty - that would be invalid for ROLLUP/CUBE element
            if matches!(self.peek(), Token::RParen) {
                return Err(ParseError {
                    message: "Empty grouping element in ROLLUP/CUBE".to_string(),
                });
            }

            // Parse first expression
            let first = self.parse_expression()?;

            if matches!(self.peek(), Token::Comma) {
                // This is a composite: (a, b, ...)
                let mut elements = vec![first];
                while matches!(self.peek(), Token::Comma) {
                    self.advance(); // consume ','
                    elements.push(self.parse_expression()?);
                }
                self.expect_token(Token::RParen)?;
                Ok(vibesql_ast::GroupingElement::Composite(elements))
            } else {
                // This is just a parenthesized single expression
                self.expect_token(Token::RParen)?;
                Ok(vibesql_ast::GroupingElement::Single(first))
            }
        } else {
            // Single expression without parentheses
            let expr = self.parse_expression()?;
            Ok(vibesql_ast::GroupingElement::Single(expr))
        }
    }

    /// Parse comma-separated list of grouping sets for GROUPING SETS
    ///
    /// Each set is enclosed in parentheses: `(a, b)`, `(a)`, or `()` for grand total
    fn parse_grouping_sets_list(&mut self) -> Result<Vec<vibesql_ast::GroupingSet>, ParseError> {
        self.parse_comma_separated_list(|p| p.parse_grouping_set())
    }

    /// Parse a single grouping set
    ///
    /// Must be enclosed in parentheses: `(a, b)`, `(a)`, or `()` for grand total
    fn parse_grouping_set(&mut self) -> Result<vibesql_ast::GroupingSet, ParseError> {
        self.expect_token(Token::LParen)?;

        // Check for empty set (grand total)
        if matches!(self.peek(), Token::RParen) {
            self.advance(); // consume ')'
            return Ok(vibesql_ast::GroupingSet { columns: vec![] });
        }

        // Parse comma-separated list of expressions
        let columns = self.parse_comma_separated_list(|p| p.parse_expression())?;
        self.expect_token(Token::RParen)?;

        Ok(vibesql_ast::GroupingSet { columns })
    }
}
