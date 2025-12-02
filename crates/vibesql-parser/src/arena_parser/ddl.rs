//! Arena-allocated DDL parsing for ALTER TABLE statements.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{
    AddColumnStmt, AddConstraintStmt, AlterColumnStmt, AlterTableStmt, ChangeColumnStmt,
    ColumnConstraint, ColumnConstraintKind, ColumnDef, DropColumnStmt, DropConstraintStmt,
    Expression, IndexColumn, ModifyColumnStmt, ReferentialAction, RenameTableStmt, SortOrder,
    TableConstraint, TableConstraintKind,
};

use super::ArenaParser;
use crate::keywords::Keyword;
use crate::token::Token;
use crate::ParseError;

impl<'arena> ArenaParser<'arena> {
    /// Parse ALTER TABLE statement.
    pub fn parse_alter_table_statement(
        &mut self,
    ) -> Result<&'arena AlterTableStmt<'arena>, ParseError> {
        // ALTER TABLE
        self.expect_keyword(Keyword::Alter)?;
        self.expect_keyword(Keyword::Table)?;

        let table_name = self.parse_table_name()?;

        // Dispatch based on operation
        let stmt = match self.peek() {
            Token::Keyword(Keyword::Add) => {
                self.advance();
                match self.peek() {
                    Token::Keyword(Keyword::Column) => {
                        self.advance();
                        self.parse_add_column(table_name)?
                    }
                    // SQL:1999 allows adding constraints with or without CONSTRAINT keyword
                    Token::Keyword(
                        Keyword::Constraint
                        | Keyword::Check
                        | Keyword::Unique
                        | Keyword::Primary
                        | Keyword::Foreign,
                    ) => self.parse_add_constraint(table_name)?,
                    // SQL:1999 allows ADD COLUMN without the COLUMN keyword
                    Token::Identifier(_) => self.parse_add_column(table_name)?,
                    _ => {
                        return Err(ParseError {
                            message: "Expected COLUMN, constraint keyword, or column name after ADD"
                                .to_string(),
                        })
                    }
                }
            }
            Token::Keyword(Keyword::Drop) => {
                self.advance();
                match self.peek() {
                    Token::Keyword(Keyword::Column) => {
                        self.advance();
                        self.parse_drop_column(table_name)?
                    }
                    Token::Keyword(Keyword::Constraint) => {
                        self.advance();
                        self.parse_drop_constraint(table_name)?
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected COLUMN or CONSTRAINT after DROP".to_string(),
                        })
                    }
                }
            }
            Token::Keyword(Keyword::Alter) => {
                self.advance();
                self.expect_keyword(Keyword::Column)?;
                self.parse_alter_column(table_name)?
            }
            Token::Keyword(Keyword::Rename) => {
                self.advance();
                self.parse_rename_table(table_name)?
            }
            Token::Keyword(Keyword::Modify) => {
                self.advance();
                self.parse_modify_column(table_name)?
            }
            Token::Keyword(Keyword::Change) => {
                self.advance();
                self.parse_change_column(table_name)?
            }
            _ => {
                return Err(ParseError {
                    message:
                        "Expected ADD, DROP, ALTER, RENAME, MODIFY, or CHANGE after table name"
                            .to_string(),
                })
            }
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse a table name (identifier).
    fn parse_table_name(&mut self) -> Result<&'arena str, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let name = self.alloc_str(name);
                self.advance();
                Ok(name)
            }
            _ => Err(ParseError {
                message: format!("Expected table name, found {:?}", self.peek()),
            }),
        }
    }

    /// Parse a column name (identifier).
    fn parse_column_name(&mut self) -> Result<&'arena str, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let name = self.alloc_str(name);
                self.advance();
                Ok(name)
            }
            _ => Err(ParseError {
                message: format!("Expected column name, found {:?}", self.peek()),
            }),
        }
    }

    /// Parse ADD COLUMN operation.
    fn parse_add_column(
        &mut self,
        table_name: &'arena str,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let column_name = self.parse_column_name()?;
        let data_type = self.parse_data_type()?;

        // Parse optional DEFAULT clause
        let default_value: Option<&'arena Expression<'arena>> =
            if self.peek_keyword(Keyword::Default) {
                self.advance();
                let expr = self.parse_expression()?;
                Some(self.arena.alloc(expr))
            } else {
                None
            };

        // Parse column constraints
        let mut nullable = true;
        let mut constraints = BumpVec::new_in(self.arena);

        loop {
            match self.peek() {
                Token::Keyword(Keyword::Not) => {
                    self.advance();
                    self.expect_keyword(Keyword::Null)?;
                    nullable = false;
                }
                Token::Keyword(Keyword::Primary) => {
                    self.advance();
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::PrimaryKey,
                    });
                }
                Token::Keyword(Keyword::Unique) => {
                    self.advance();
                    constraints.push(ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::Unique,
                    });
                }
                Token::Keyword(Keyword::References) => {
                    self.advance();
                    let ref_table = self.parse_table_name()?;
                    self.expect_token(Token::LParen)?;
                    let ref_column = self.parse_column_name()?;
                    self.expect_token(Token::RParen)?;
                    constraints.push(ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::References {
                            table: ref_table,
                            column: ref_column,
                            on_delete: None,
                            on_update: None,
                        },
                    });
                }
                _ => break,
            }
        }

        let column_def = ColumnDef {
            name: column_name,
            data_type,
            nullable,
            constraints,
            default_value,
            comment: None,
        };

        Ok(AlterTableStmt::AddColumn(AddColumnStmt { table_name, column_def }))
    }

    /// Parse DROP COLUMN operation.
    fn parse_drop_column(
        &mut self,
        table_name: &'arena str,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let if_exists =
            self.try_consume_keyword(Keyword::If) && self.try_consume_keyword(Keyword::Exists);

        let column_name = self.parse_column_name()?;

        Ok(AlterTableStmt::DropColumn(DropColumnStmt { table_name, column_name, if_exists }))
    }

    /// Parse ALTER COLUMN operation.
    fn parse_alter_column(
        &mut self,
        table_name: &'arena str,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let column_name = self.parse_column_name()?;

        match self.peek() {
            Token::Keyword(Keyword::Set) => {
                self.advance();
                match self.peek() {
                    Token::Keyword(Keyword::Default) => {
                        self.advance();
                        let default = self.parse_expression()?;
                        Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::SetDefault {
                            table_name,
                            column_name,
                            default,
                        }))
                    }
                    Token::Keyword(Keyword::Not) => {
                        self.advance();
                        self.expect_keyword(Keyword::Null)?;
                        Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::SetNotNull {
                            table_name,
                            column_name,
                        }))
                    }
                    _ => Err(ParseError {
                        message: "Expected DEFAULT or NOT NULL after SET".to_string(),
                    }),
                }
            }
            Token::Keyword(Keyword::Drop) => {
                self.advance();
                match self.peek() {
                    Token::Keyword(Keyword::Default) => {
                        self.advance();
                        Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::DropDefault {
                            table_name,
                            column_name,
                        }))
                    }
                    Token::Keyword(Keyword::Not) => {
                        self.advance();
                        self.expect_keyword(Keyword::Null)?;
                        Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::DropNotNull {
                            table_name,
                            column_name,
                        }))
                    }
                    _ => Err(ParseError {
                        message: "Expected DEFAULT or NOT NULL after DROP".to_string(),
                    }),
                }
            }
            _ => Err(ParseError {
                message: "Expected SET or DROP after column name".to_string(),
            }),
        }
    }

    /// Parse ADD CONSTRAINT operation.
    fn parse_add_constraint(
        &mut self,
        table_name: &'arena str,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let constraint = self.parse_table_constraint()?;
        Ok(AlterTableStmt::AddConstraint(AddConstraintStmt { table_name, constraint }))
    }

    /// Parse DROP CONSTRAINT operation.
    fn parse_drop_constraint(
        &mut self,
        table_name: &'arena str,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let constraint_name = self.parse_column_name()?;
        Ok(AlterTableStmt::DropConstraint(DropConstraintStmt { table_name, constraint_name }))
    }

    /// Parse RENAME TO operation.
    fn parse_rename_table(
        &mut self,
        table_name: &'arena str,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        self.expect_keyword(Keyword::To)?;
        let new_table_name = self.parse_table_name()?;
        Ok(AlterTableStmt::RenameTable(RenameTableStmt { table_name, new_table_name }))
    }

    /// Parse MODIFY COLUMN operation.
    fn parse_modify_column(
        &mut self,
        table_name: &'arena str,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        // MODIFY [COLUMN] column_name new_definition
        if self.peek_keyword(Keyword::Column) {
            self.advance();
        }

        let column_name = self.parse_column_name()?;
        let data_type = self.parse_data_type()?;

        // Parse optional DEFAULT clause
        let default_value: Option<&'arena Expression<'arena>> =
            if self.peek_keyword(Keyword::Default) {
                self.advance();
                let expr = self.parse_expression()?;
                Some(self.arena.alloc(expr))
            } else {
                None
            };

        // Parse column constraints
        let (nullable, constraints) = self.parse_column_constraints()?;

        let new_column_def = ColumnDef {
            name: column_name,
            data_type,
            nullable,
            constraints,
            default_value,
            comment: None,
        };

        Ok(AlterTableStmt::ModifyColumn(ModifyColumnStmt {
            table_name,
            column_name,
            new_column_def,
        }))
    }

    /// Parse CHANGE COLUMN operation.
    fn parse_change_column(
        &mut self,
        table_name: &'arena str,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        // CHANGE [COLUMN] old_column_name new_column_name new_definition
        if self.peek_keyword(Keyword::Column) {
            self.advance();
        }

        let old_column_name = self.parse_column_name()?;
        let new_column_name = self.parse_column_name()?;
        let data_type = self.parse_data_type()?;

        // Parse optional DEFAULT clause
        let default_value: Option<&'arena Expression<'arena>> =
            if self.peek_keyword(Keyword::Default) {
                self.advance();
                let expr = self.parse_expression()?;
                Some(self.arena.alloc(expr))
            } else {
                None
            };

        // Parse column constraints
        let (nullable, constraints) = self.parse_column_constraints()?;

        let new_column_def = ColumnDef {
            name: new_column_name,
            data_type,
            nullable,
            constraints,
            default_value,
            comment: None,
        };

        Ok(AlterTableStmt::ChangeColumn(ChangeColumnStmt {
            table_name,
            old_column_name,
            new_column_def,
        }))
    }

    /// Parse column constraints.
    fn parse_column_constraints(
        &mut self,
    ) -> Result<(bool, BumpVec<'arena, ColumnConstraint<'arena>>), ParseError> {
        let mut nullable = true;
        let mut constraints = BumpVec::new_in(self.arena);

        loop {
            match self.peek() {
                Token::Keyword(Keyword::Not) => {
                    self.advance();
                    self.expect_keyword(Keyword::Null)?;
                    nullable = false;
                }
                Token::Keyword(Keyword::Primary) => {
                    self.advance();
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::PrimaryKey,
                    });
                }
                Token::Keyword(Keyword::Unique) => {
                    self.advance();
                    constraints.push(ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::Unique,
                    });
                }
                Token::Keyword(Keyword::References) => {
                    self.advance();
                    let ref_table = self.parse_table_name()?;
                    self.expect_token(Token::LParen)?;
                    let ref_column = self.parse_column_name()?;
                    self.expect_token(Token::RParen)?;
                    constraints.push(ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::References {
                            table: ref_table,
                            column: ref_column,
                            on_delete: None,
                            on_update: None,
                        },
                    });
                }
                _ => break,
            }
        }

        Ok((nullable, constraints))
    }

    /// Parse a table-level constraint.
    fn parse_table_constraint(&mut self) -> Result<TableConstraint<'arena>, ParseError> {
        // Optional constraint name: CONSTRAINT name
        let name = if self.try_consume_keyword(Keyword::Constraint) {
            Some(self.parse_column_name()?)
        } else {
            None
        };

        let kind = match self.peek() {
            Token::Keyword(Keyword::Primary) => {
                self.advance();
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;
                let columns = self.parse_index_column_list()?;
                self.expect_token(Token::RParen)?;
                TableConstraintKind::PrimaryKey { columns }
            }
            Token::Keyword(Keyword::Unique) => {
                self.advance();
                self.expect_token(Token::LParen)?;
                let columns = self.parse_index_column_list()?;
                self.expect_token(Token::RParen)?;
                TableConstraintKind::Unique { columns }
            }
            Token::Keyword(Keyword::Foreign) => {
                self.advance();
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;
                let columns = self.parse_column_name_list()?;
                self.expect_token(Token::RParen)?;
                self.expect_keyword(Keyword::References)?;
                let references_table = self.parse_table_name()?;
                self.expect_token(Token::LParen)?;
                let references_columns = self.parse_column_name_list()?;
                self.expect_token(Token::RParen)?;

                let (on_delete, on_update) = self.parse_referential_actions()?;

                TableConstraintKind::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                    on_delete,
                    on_update,
                }
            }
            Token::Keyword(Keyword::Check) => {
                self.advance();
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                TableConstraintKind::Check { expr: self.arena.alloc(expr) }
            }
            _ => {
                return Err(ParseError {
                    message: format!("Expected constraint type, found {:?}", self.peek()),
                })
            }
        };

        Ok(TableConstraint { name, kind })
    }

    /// Parse index column list.
    fn parse_index_column_list(
        &mut self,
    ) -> Result<BumpVec<'arena, IndexColumn<'arena>>, ParseError> {
        let mut columns = BumpVec::new_in(self.arena);

        loop {
            let column_name = self.parse_column_name()?;

            // Optional length for prefix indexes
            let length = if self.try_consume(&Token::LParen) {
                let len = match self.peek() {
                    Token::Number(n) => {
                        let len = n.parse::<u32>().ok();
                        self.advance();
                        len
                    }
                    _ => None,
                };
                self.expect_token(Token::RParen)?;
                len
            } else {
                None
            };

            // Optional sort order
            let order = if self.try_consume_keyword(Keyword::Asc) {
                Some(SortOrder::Asc)
            } else if self.try_consume_keyword(Keyword::Desc) {
                Some(SortOrder::Desc)
            } else {
                None
            };

            columns.push(IndexColumn { column_name, length, order });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse column name list.
    fn parse_column_name_list(&mut self) -> Result<BumpVec<'arena, &'arena str>, ParseError> {
        let mut columns = BumpVec::new_in(self.arena);

        loop {
            let col = self.parse_column_name()?;
            columns.push(col);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse referential actions (ON DELETE, ON UPDATE).
    fn parse_referential_actions(
        &mut self,
    ) -> Result<(Option<ReferentialAction>, Option<ReferentialAction>), ParseError> {
        let mut on_delete = None;
        let mut on_update = None;

        for _ in 0..2 {
            if self.try_consume_keyword(Keyword::On) {
                if self.try_consume_keyword(Keyword::Delete) {
                    on_delete = Some(self.parse_referential_action()?);
                } else if self.try_consume_keyword(Keyword::Update) {
                    on_update = Some(self.parse_referential_action()?);
                }
            }
        }

        Ok((on_delete, on_update))
    }

    /// Parse a single referential action.
    fn parse_referential_action(&mut self) -> Result<ReferentialAction, ParseError> {
        if self.try_consume_keyword(Keyword::Cascade) {
            Ok(ReferentialAction::Cascade)
        } else if self.try_consume_keyword(Keyword::Restrict) {
            Ok(ReferentialAction::Restrict)
        } else if self.try_consume_keyword(Keyword::Set) {
            if self.try_consume_keyword(Keyword::Null) {
                Ok(ReferentialAction::SetNull)
            } else if self.try_consume_keyword(Keyword::Default) {
                Ok(ReferentialAction::SetDefault)
            } else {
                Err(ParseError { message: "Expected NULL or DEFAULT after SET".to_string() })
            }
        } else if self.try_consume_keyword(Keyword::No) {
            self.expect_keyword(Keyword::Action)?;
            Ok(ReferentialAction::NoAction)
        } else {
            Err(ParseError {
                message: "Expected CASCADE, RESTRICT, SET NULL, SET DEFAULT, or NO ACTION"
                    .to_string(),
            })
        }
    }
}
