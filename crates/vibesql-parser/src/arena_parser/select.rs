//! Arena-allocated SELECT statement parsing.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{
    CommonTableExpr, Expression, FromClause, GroupByClause, GroupingElement, GroupingSet, JoinType,
    OrderByItem, OrderDirection, SelectItem, SelectStmt, SetOperation, SetOperator, Symbol,
};

use super::ArenaParser;
use crate::keywords::Keyword;
use crate::token::Token;
use crate::ParseError;

impl<'arena> ArenaParser<'arena> {
    /// Parse a SELECT statement.
    pub(crate) fn parse_select_statement(
        &mut self,
    ) -> Result<&'arena SelectStmt<'arena>, ParseError> {
        // Parse optional WITH clause
        let with_clause = if self.try_consume_keyword(Keyword::With) {
            Some(self.parse_with_clause()?)
        } else {
            None
        };

        self.consume_keyword(Keyword::Select)?;

        // Parse DISTINCT
        let distinct = self.try_consume_keyword(Keyword::Distinct);
        self.try_consume_keyword(Keyword::All);

        // Parse select list
        let select_list = self.parse_select_list()?;

        // Parse optional INTO
        let (into_table, into_variables) = self.parse_into_clause()?;

        // Parse FROM
        let from = if self.try_consume_keyword(Keyword::From) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        // Parse WHERE
        let where_clause = if self.try_consume_keyword(Keyword::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY
        let group_by = if self.try_consume_keyword(Keyword::Group) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_group_by_clause()?)
        } else {
            None
        };

        // Parse HAVING
        let having = if self.try_consume_keyword(Keyword::Having) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse ORDER BY
        let order_by = if self.try_consume_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_order_by_clause()?)
        } else {
            None
        };

        // Parse LIMIT
        let limit = if self.try_consume_keyword(Keyword::Limit) {
            if let Token::Number(n) = self.peek() {
                let n = n
                    .parse::<usize>()
                    .map_err(|_| ParseError { message: "Invalid LIMIT value".to_string() })?;
                self.advance();
                Some(n)
            } else {
                return Err(ParseError { message: "Expected integer after LIMIT".to_string() });
            }
        } else {
            None
        };

        // Parse OFFSET
        let offset = if self.try_consume_keyword(Keyword::Offset) {
            if let Token::Number(n) = self.peek() {
                let n = n
                    .parse::<usize>()
                    .map_err(|_| ParseError { message: "Invalid OFFSET value".to_string() })?;
                self.advance();
                Some(n)
            } else {
                return Err(ParseError { message: "Expected integer after OFFSET".to_string() });
            }
        } else {
            None
        };

        // Parse set operations (UNION, INTERSECT, EXCEPT)
        let set_operation = self.parse_set_operation()?;

        let stmt = SelectStmt {
            with_clause,
            distinct,
            select_list,
            into_table,
            into_variables,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            set_operation,
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse WITH clause (CTEs).
    fn parse_with_clause(
        &mut self,
    ) -> Result<BumpVec<'arena, CommonTableExpr<'arena>>, ParseError> {
        let mut ctes = BumpVec::new_in(self.arena);

        loop {
            let cte = self.parse_cte()?;
            ctes.push(cte);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(ctes)
    }

    /// Parse a single CTE.
    fn parse_cte(&mut self) -> Result<CommonTableExpr<'arena>, ParseError> {
        // Parse CTE name
        let name = if let Token::Identifier(name) = self.peek() {
            let name = name.clone();
            self.advance();
            self.intern(&name)
        } else {
            return Err(ParseError { message: "Expected CTE name".to_string() });
        };

        // Parse optional column list
        let columns = if self.try_consume(&Token::LParen) {
            let mut cols = BumpVec::new_in(self.arena);
            while let Token::Identifier(col) = self.peek() {
                let col = col.clone();
                self.advance();
                cols.push(self.intern(&col));
                if !self.try_consume(&Token::Comma) {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.consume_keyword(Keyword::As)?;
        self.expect_token(Token::LParen)?;
        let query = self.parse_select_statement()?;
        self.expect_token(Token::RParen)?;

        Ok(CommonTableExpr { name, columns, query })
    }

    /// Parse select list.
    fn parse_select_list(&mut self) -> Result<BumpVec<'arena, SelectItem<'arena>>, ParseError> {
        let mut items = BumpVec::new_in(self.arena);

        loop {
            let item = self.parse_select_item()?;
            items.push(item);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(items)
    }

    /// Parse a single select item.
    fn parse_select_item(&mut self) -> Result<SelectItem<'arena>, ParseError> {
        // Check for * wildcard
        if matches!(self.peek(), Token::Symbol('*')) {
            self.advance();
            return Ok(SelectItem::Wildcard { alias: None });
        }

        // Parse expression
        let expr = self.parse_expression()?;

        // Check for qualified wildcard (table.*)
        // Note: We compare the symbol directly now, which means we need to intern "*" for comparison
        // But actually, the Wildcard expression is separate - column reference with "*" becomes Expression::Wildcard
        // So this check shouldn't match anymore since Expression::ColumnRef won't have "*" as column.
        // The wildcard case is handled in expression parsing.
        if let Expression::ColumnRef { table: Some(t), column: _ } = &expr {
            // Check if it's a qualified wildcard - but this won't happen since
            // we parse table.* as Wildcard expression, not as ColumnRef with "*" column
            // Keep this code path but it likely won't be hit
            let _ = t; // silence warning
        }

        // Check for alias
        let alias = if self.try_consume_keyword(Keyword::As) {
            if let Token::Identifier(name) = self.peek() {
                let name = name.clone();
                self.advance();
                Some(self.intern(&name))
            } else {
                return Err(ParseError { message: "Expected alias after AS".to_string() });
            }
        } else if let Token::Identifier(name) = self.peek() {
            // Implicit alias (no AS keyword)
            let name = name.clone();
            self.advance();
            Some(self.intern(&name))
        } else {
            None
        };

        Ok(SelectItem::Expression { expr, alias })
    }

    /// Parse INTO clause.
    fn parse_into_clause(
        &mut self,
    ) -> Result<(Option<Symbol>, Option<BumpVec<'arena, Symbol>>), ParseError> {
        if !self.try_consume_keyword(Keyword::Into) {
            return Ok((None, None));
        }

        // Check if it's SELECT INTO table_name or SELECT INTO @var1, @var2
        if let Token::Identifier(name) = self.peek() {
            let name = name.clone();
            self.advance();
            let name_sym = self.intern(&name);
            return Ok((Some(name_sym), None));
        }

        // Parse variable list
        let mut vars = BumpVec::new_in(self.arena);
        while let Token::Identifier(var) = self.peek() {
            let var = var.clone();
            self.advance();
            vars.push(self.intern(&var));
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        if vars.is_empty() {
            return Err(ParseError {
                message: "Expected table name or variables after INTO".to_string(),
            });
        }

        Ok((None, Some(vars)))
    }

    /// Parse FROM clause.
    fn parse_from_clause(&mut self) -> Result<FromClause<'arena>, ParseError> {
        let mut from = self.parse_table_reference()?;

        // Parse JOINs
        loop {
            let join_type = if self.try_consume_keyword(Keyword::Inner) {
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::Inner)
            } else if self.try_consume_keyword(Keyword::Left) {
                self.try_consume_keyword(Keyword::Outer);
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::LeftOuter)
            } else if self.try_consume_keyword(Keyword::Right) {
                self.try_consume_keyword(Keyword::Outer);
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::RightOuter)
            } else if self.try_consume_keyword(Keyword::Full) {
                self.try_consume_keyword(Keyword::Outer);
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::FullOuter)
            } else if self.try_consume_keyword(Keyword::Cross) {
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::Cross)
            } else if self.try_consume_keyword(Keyword::Join) {
                Some(JoinType::Inner)
            } else if self.try_consume(&Token::Comma) {
                Some(JoinType::Cross)
            } else {
                None
            };

            if let Some(jt) = join_type {
                let natural = self.try_consume_keyword(Keyword::Natural);
                let right = self.parse_table_reference()?;

                let condition = if jt != JoinType::Cross && !natural {
                    if self.try_consume_keyword(Keyword::On) {
                        Some(self.parse_expression()?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                let left_ref = self.arena.alloc(from);
                let right_ref = self.arena.alloc(right);

                from = FromClause::Join {
                    left: left_ref,
                    right: right_ref,
                    join_type: jt,
                    condition,
                    natural,
                };
            } else {
                break;
            }
        }

        Ok(from)
    }

    /// Parse a table reference.
    fn parse_table_reference(&mut self) -> Result<FromClause<'arena>, ParseError> {
        // Check for subquery
        if self.try_consume(&Token::LParen) {
            let query = self.parse_select_statement()?;
            self.expect_token(Token::RParen)?;

            // Subquery requires alias
            self.try_consume_keyword(Keyword::As);
            let alias = if let Token::Identifier(name) = self.peek() {
                let name = name.clone();
                self.advance();
                self.intern(&name)
            } else {
                return Err(ParseError { message: "Subquery requires alias".to_string() });
            };

            // Parse optional column aliases: (col1, col2, ...)
            let column_aliases = self.parse_column_alias_list()?;

            return Ok(FromClause::Subquery { query, alias, column_aliases });
        }

        // Regular table reference
        let name = if let Token::Identifier(name) = self.peek() {
            let name = name.clone();
            self.advance();
            self.intern(&name)
        } else {
            return Err(ParseError {
                message: format!("Expected table name, found {:?}", self.peek()),
            });
        };

        // Check for alias
        self.try_consume_keyword(Keyword::As);
        let alias = if let Token::Identifier(alias) = self.peek() {
            // Make sure it's not a keyword that would start a new clause
            if !matches!(
                self.peek(),
                Token::Keyword(Keyword::Where)
                    | Token::Keyword(Keyword::Join)
                    | Token::Keyword(Keyword::Inner)
                    | Token::Keyword(Keyword::Left)
                    | Token::Keyword(Keyword::Right)
                    | Token::Keyword(Keyword::Full)
                    | Token::Keyword(Keyword::Cross)
                    | Token::Keyword(Keyword::On)
                    | Token::Keyword(Keyword::Group)
                    | Token::Keyword(Keyword::Order)
                    | Token::Keyword(Keyword::Having)
                    | Token::Keyword(Keyword::Limit)
                    | Token::Keyword(Keyword::Union)
                    | Token::Keyword(Keyword::Intersect)
                    | Token::Keyword(Keyword::Except)
            ) {
                let alias = alias.clone();
                self.advance();
                Some(self.intern(&alias))
            } else {
                None
            }
        } else {
            None
        };

        // Parse optional column aliases: (col1, col2, ...)
        let column_aliases = if alias.is_some() { self.parse_column_alias_list()? } else { None };

        Ok(FromClause::Table { name, alias, column_aliases })
    }

    /// Parse GROUP BY clause.
    fn parse_group_by_clause(&mut self) -> Result<GroupByClause<'arena>, ParseError> {
        // Check for ROLLUP, CUBE, GROUPING SETS
        if self.peek_keyword(Keyword::Rollup) {
            self.advance();
            self.expect_token(Token::LParen)?;
            let elements = self.parse_grouping_elements()?;
            self.expect_token(Token::RParen)?;
            return Ok(GroupByClause::Rollup(elements));
        }

        if self.peek_keyword(Keyword::Cube) {
            self.advance();
            self.expect_token(Token::LParen)?;
            let elements = self.parse_grouping_elements()?;
            self.expect_token(Token::RParen)?;
            return Ok(GroupByClause::Cube(elements));
        }

        if self.peek_keyword(Keyword::Grouping) {
            self.advance();
            self.consume_keyword(Keyword::Sets)?;
            self.expect_token(Token::LParen)?;
            let sets = self.parse_grouping_sets()?;
            self.expect_token(Token::RParen)?;
            return Ok(GroupByClause::GroupingSets(sets));
        }

        // Simple GROUP BY
        let mut exprs = BumpVec::new_in(self.arena);
        loop {
            exprs.push(self.parse_expression()?);
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(GroupByClause::Simple(exprs))
    }

    /// Parse grouping elements.
    fn parse_grouping_elements(
        &mut self,
    ) -> Result<BumpVec<'arena, GroupingElement<'arena>>, ParseError> {
        let mut elements = BumpVec::new_in(self.arena);

        loop {
            if self.try_consume(&Token::LParen) {
                // Composite element
                let mut exprs = BumpVec::new_in(self.arena);
                loop {
                    exprs.push(self.parse_expression()?);
                    if !self.try_consume(&Token::Comma) {
                        break;
                    }
                }
                self.expect_token(Token::RParen)?;
                elements.push(GroupingElement::Composite(exprs));
            } else {
                // Single element
                let expr = self.parse_expression()?;
                elements.push(GroupingElement::Single(expr));
            }

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(elements)
    }

    /// Parse grouping sets.
    fn parse_grouping_sets(&mut self) -> Result<BumpVec<'arena, GroupingSet<'arena>>, ParseError> {
        let mut sets = BumpVec::new_in(self.arena);

        loop {
            self.expect_token(Token::LParen)?;
            let mut columns = BumpVec::new_in(self.arena);

            if !matches!(self.peek(), Token::RParen) {
                loop {
                    columns.push(self.parse_expression()?);
                    if !self.try_consume(&Token::Comma) {
                        break;
                    }
                }
            }

            self.expect_token(Token::RParen)?;
            sets.push(GroupingSet { columns });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(sets)
    }

    /// Parse ORDER BY clause.
    fn parse_order_by_clause(
        &mut self,
    ) -> Result<BumpVec<'arena, OrderByItem<'arena>>, ParseError> {
        let mut items = BumpVec::new_in(self.arena);

        loop {
            let expr = self.parse_expression()?;
            let direction = if self.try_consume_keyword(Keyword::Desc) {
                OrderDirection::Desc
            } else {
                self.try_consume_keyword(Keyword::Asc);
                OrderDirection::Asc
            };

            items.push(OrderByItem { expr, direction });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(items)
    }

    /// Parse set operation (UNION, INTERSECT, EXCEPT).
    fn parse_set_operation(&mut self) -> Result<Option<SetOperation<'arena>>, ParseError> {
        let op = if self.try_consume_keyword(Keyword::Union) {
            SetOperator::Union
        } else if self.try_consume_keyword(Keyword::Intersect) {
            SetOperator::Intersect
        } else if self.try_consume_keyword(Keyword::Except) {
            SetOperator::Except
        } else {
            return Ok(None);
        };

        let all = self.try_consume_keyword(Keyword::All);
        self.try_consume_keyword(Keyword::Distinct);

        let right = self.parse_select_statement()?;

        Ok(Some(SetOperation { op, all, right }))
    }
}
