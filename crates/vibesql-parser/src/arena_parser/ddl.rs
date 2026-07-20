//! Arena-allocated DDL statement parsing.
//!
//! This module provides parsing for DDL statements including:
//! - Transaction statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
//! - CREATE/DROP TABLE, INDEX, VIEW
//! - ALTER TABLE (including MySQL MODIFY/CHANGE COLUMN)
//! - ANALYZE

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{
    AddColumnStmt, AddConstraintStmt, AlterColumnStmt, AlterTableStmt, AnalyzeStmt, BeginStmt,
    ChangeColumnStmt, ColumnConstraint, ColumnConstraintKind, ColumnDef, CommitStmt,
    CreateIndexStmt, CreateViewStmt, DropColumnStmt, DropConstraintStmt, DropIndexStmt,
    DropTableStmt, DropViewStmt, DurabilityHint, Expression, IndexColumn, IndexType,
    ModifyColumnStmt, OrderDirection, PragmaStmt, PragmaValue, ReferentialAction,
    ReleaseSavepointStmt, RenameTableStmt, RollbackStmt, RollbackToSavepointStmt, SavepointStmt,
    Symbol, TableConstraint, TableConstraintKind, TruncateCascadeOption, TruncateTableStmt,
    VacuumStmt,
};

use super::ArenaParser;
use crate::{keywords::Keyword, token::Token, ParseError};

impl<'arena> ArenaParser<'arena> {
    // ========================================================================
    // Transaction Statements
    // ========================================================================

    /// Parse BEGIN [TRANSACTION] [WITH DURABILITY = <mode>] or START TRANSACTION [WITH DURABILITY =
    /// <mode>] statement.
    pub(crate) fn parse_begin_statement(&mut self) -> Result<BeginStmt, ParseError> {
        if self.peek_keyword(Keyword::Begin) {
            self.consume_keyword(Keyword::Begin)?;
        } else if self.peek_keyword(Keyword::Start) {
            self.consume_keyword(Keyword::Start)?;
        } else {
            return Err(ParseError { message: "Expected BEGIN or START".to_string() });
        }

        // Optional TRANSACTION keyword
        self.try_consume_keyword(Keyword::Transaction);

        // Parse optional WITH DURABILITY = <mode> clause
        let durability = if self.peek_keyword(Keyword::With) {
            self.consume_keyword(Keyword::With)?;
            self.consume_keyword(Keyword::Durability)?;

            // Optional = sign
            self.try_consume(&Token::Symbol('='));

            // Parse durability mode
            self.parse_durability_hint()?
        } else {
            DurabilityHint::Default
        };

        Ok(BeginStmt { durability })
    }

    /// Parse a durability hint keyword.
    fn parse_durability_hint(&mut self) -> Result<DurabilityHint, ParseError> {
        if self.try_consume_keyword(Keyword::Default) {
            Ok(DurabilityHint::Default)
        } else if self.try_consume_keyword(Keyword::Durable) {
            Ok(DurabilityHint::Durable)
        } else if self.try_consume_keyword(Keyword::Lazy) {
            Ok(DurabilityHint::Lazy)
        } else if self.try_consume_keyword(Keyword::Volatile) {
            Ok(DurabilityHint::Volatile)
        } else {
            Err(ParseError {
                message: "Expected durability mode: DEFAULT, DURABLE, LAZY, or VOLATILE"
                    .to_string(),
            })
        }
    }

    /// Parse COMMIT or END statement (END is SQLite alias for COMMIT).
    pub(crate) fn parse_commit_statement(&mut self) -> Result<CommitStmt, ParseError> {
        // Consume COMMIT or END (SQLite uses END as alias for COMMIT)
        if self.peek_keyword(Keyword::Commit) {
            self.consume_keyword(Keyword::Commit)?;
        } else if self.peek_keyword(Keyword::End) {
            self.consume_keyword(Keyword::End)?;
        } else {
            return Err(ParseError { message: "Expected COMMIT or END".to_string() });
        }
        Ok(CommitStmt)
    }

    /// Parse ROLLBACK statement.
    pub(crate) fn parse_rollback_statement(&mut self) -> Result<RollbackStmt, ParseError> {
        self.consume_keyword(Keyword::Rollback)?;
        Ok(RollbackStmt)
    }

    /// Parse ROLLBACK TO SAVEPOINT statement.
    pub(crate) fn parse_rollback_to_savepoint_statement(
        &mut self,
    ) -> Result<RollbackToSavepointStmt, ParseError> {
        self.consume_keyword(Keyword::Rollback)?;
        self.consume_keyword(Keyword::To)?;
        // SAVEPOINT keyword is optional in SQLite: `ROLLBACK TO <name>`.
        self.try_consume_keyword(Keyword::Savepoint);
        let name = self.parse_arena_identifier()?;
        Ok(RollbackToSavepointStmt { name })
    }

    /// Parse SAVEPOINT statement.
    pub(crate) fn parse_savepoint_statement(&mut self) -> Result<SavepointStmt, ParseError> {
        self.consume_keyword(Keyword::Savepoint)?;
        let name = self.parse_arena_identifier()?;
        Ok(SavepointStmt { name })
    }

    /// Parse RELEASE SAVEPOINT statement.
    pub(crate) fn parse_release_savepoint_statement(
        &mut self,
    ) -> Result<ReleaseSavepointStmt, ParseError> {
        self.consume_keyword(Keyword::Release)?;
        // SAVEPOINT keyword is optional in SQLite: `RELEASE <name>`.
        self.try_consume_keyword(Keyword::Savepoint);
        let name = self.parse_arena_identifier()?;
        Ok(ReleaseSavepointStmt { name })
    }

    // ========================================================================
    // CREATE Statements
    // ========================================================================

    /// Parse CREATE INDEX statement.
    pub(crate) fn parse_create_index_statement(
        &mut self,
    ) -> Result<CreateIndexStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Create)?;

        // Check for UNIQUE or FULLTEXT
        let index_type = if self.try_consume_keyword(Keyword::Unique) {
            IndexType::BTree { unique: true }
        } else if self.try_consume_keyword(Keyword::Fulltext) {
            IndexType::Fulltext
        } else if self.try_consume_keyword(Keyword::Spatial) {
            IndexType::Spatial
        } else {
            IndexType::BTree { unique: false }
        };

        self.consume_keyword(Keyword::Index)?;

        // Check for IF NOT EXISTS
        let if_not_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // SQLite fallback keywords are legal index names (keyword1.test:
        // `CREATE INDEX abort ON t1(a)`). Lowercased like any unquoted
        // identifier.
        let index_name = match self.peek() {
            Token::Keyword { keyword: kw, .. } if kw.is_sqlite_fallback_keyword() => {
                let name = kw.to_string().to_lowercase();
                self.advance();
                self.intern(&name)
            }
            _ => self.parse_arena_identifier()?,
        };

        self.consume_keyword(Keyword::On)?;
        let table_name = self.parse_arena_identifier()?;

        self.expect_token(Token::LParen)?;
        let columns = self.parse_index_columns()?;
        self.expect_token(Token::RParen)?;

        Ok(CreateIndexStmt { if_not_exists, index_name, table_name, index_type, columns })
    }

    /// Parse CREATE VIEW statement.
    pub(crate) fn parse_create_view_statement(
        &mut self,
    ) -> Result<CreateViewStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Create)?;

        // Check for OR REPLACE
        let or_replace = if self.try_consume_keyword(Keyword::Or) {
            self.expect_keyword(Keyword::Replace)?;
            true
        } else {
            false
        };

        // Check for TEMP/TEMPORARY
        let temporary =
            self.try_consume_keyword(Keyword::Temp) || self.try_consume_keyword(Keyword::Temporary);

        self.consume_keyword(Keyword::View)?;

        // Check for IF NOT EXISTS
        let if_not_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let view_name = self.parse_arena_identifier()?;

        // Parse optional column list
        let columns = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.consume_keyword(Keyword::As)?;

        // Parse the query: can be SELECT or VALUES
        // Example: CREATE VIEW dual AS VALUES('x')
        let query = if self.peek_keyword(Keyword::Values) {
            self.parse_values_statement()?
        } else {
            self.parse_select_statement()?
        };

        // Check for WITH CHECK OPTION
        let with_check_option = if self.try_consume_keyword(Keyword::With) {
            self.expect_keyword(Keyword::Check)?;
            self.expect_keyword(Keyword::Option)?;
            true
        } else {
            false
        };

        Ok(CreateViewStmt {
            view_name,
            columns,
            query,
            with_check_option,
            or_replace,
            if_not_exists,
            temporary,
        })
    }

    // ========================================================================
    // DROP Statements
    // ========================================================================

    /// Parse DROP TABLE statement.
    pub(crate) fn parse_drop_table_statement(&mut self) -> Result<DropTableStmt, ParseError> {
        self.consume_keyword(Keyword::Drop)?;
        self.consume_keyword(Keyword::Table)?;

        let if_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let table_name = self.parse_arena_identifier()?;

        Ok(DropTableStmt { table_name, if_exists })
    }

    /// Parse DROP INDEX statement.
    pub(crate) fn parse_drop_index_statement(&mut self) -> Result<DropIndexStmt, ParseError> {
        self.consume_keyword(Keyword::Drop)?;
        self.consume_keyword(Keyword::Index)?;

        let if_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let index_name = self.parse_arena_identifier()?;

        Ok(DropIndexStmt { if_exists, index_name })
    }

    /// Parse DROP VIEW statement.
    pub(crate) fn parse_drop_view_statement(&mut self) -> Result<DropViewStmt, ParseError> {
        self.consume_keyword(Keyword::Drop)?;
        self.consume_keyword(Keyword::View)?;

        let if_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let view_name = self.parse_arena_identifier()?;

        let (cascade, restrict) = if self.try_consume_keyword(Keyword::Cascade) {
            (true, false)
        } else if self.try_consume_keyword(Keyword::Restrict) {
            (false, true)
        } else {
            (false, false)
        };

        Ok(DropViewStmt { view_name, if_exists, cascade, restrict })
    }

    /// Parse TRUNCATE TABLE statement.
    pub(crate) fn parse_truncate_table_statement(
        &mut self,
    ) -> Result<TruncateTableStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Truncate)?;
        self.try_consume_keyword(Keyword::Table);

        let if_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse table names (can be comma-separated)
        let mut table_names = BumpVec::new_in(self.arena);
        loop {
            table_names.push(self.parse_arena_identifier()?);
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        let cascade = if self.try_consume_keyword(Keyword::Cascade) {
            Some(TruncateCascadeOption::Cascade)
        } else if self.try_consume_keyword(Keyword::Restrict) {
            Some(TruncateCascadeOption::Restrict)
        } else {
            None
        };

        Ok(TruncateTableStmt { table_names, if_exists, cascade })
    }

    // ========================================================================
    // ALTER TABLE Statements
    // ========================================================================

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
            Token::Keyword { keyword: Keyword::Add, .. } => {
                self.advance();
                match self.peek() {
                    Token::Keyword { keyword: Keyword::Column, .. } => {
                        self.advance();
                        self.parse_add_column(table_name)?
                    }
                    // SQL:1999 allows adding constraints with or without CONSTRAINT keyword
                    Token::Keyword {
                        keyword:
                            Keyword::Constraint
                            | Keyword::Check
                            | Keyword::Unique
                            | Keyword::Primary
                            | Keyword::Foreign,
                        ..
                    } => self.parse_add_constraint(table_name)?,
                    // SQL:1999 allows ADD COLUMN without the COLUMN keyword
                    Token::Identifier(_) => self.parse_add_column(table_name)?,
                    _ => {
                        return Err(ParseError {
                            message:
                                "Expected COLUMN, constraint keyword, or column name after ADD"
                                    .to_string(),
                        })
                    }
                }
            }
            Token::Keyword { keyword: Keyword::Drop, .. } => {
                self.advance();
                match self.peek() {
                    Token::Keyword { keyword: Keyword::Column, .. } => {
                        self.advance();
                        self.parse_drop_column(table_name)?
                    }
                    Token::Keyword { keyword: Keyword::Constraint, .. } => {
                        self.advance();
                        self.parse_drop_constraint(table_name)?
                    }
                    // SQLite allows DROP COLUMN without the COLUMN keyword:
                    // `ALTER TABLE t DROP <col>` is a synonym for
                    // `ALTER TABLE t DROP COLUMN <col>`. If we see a bare column
                    // name, treat it as a column drop (mirrors the ADD case).
                    Token::Identifier(_) => self.parse_drop_column(table_name)?,
                    _ => {
                        return Err(ParseError {
                            message: "Expected COLUMN or CONSTRAINT after DROP".to_string(),
                        })
                    }
                }
            }
            Token::Keyword { keyword: Keyword::Alter, .. } => {
                self.advance();
                self.expect_keyword(Keyword::Column)?;
                self.parse_alter_column(table_name)?
            }
            Token::Keyword { keyword: Keyword::Rename, .. } => {
                self.advance();
                self.parse_rename_table(table_name)?
            }
            Token::Keyword { keyword: Keyword::Modify, .. } => {
                self.advance();
                self.parse_modify_column(table_name)?
            }
            Token::Keyword { keyword: Keyword::Change, .. } => {
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
    fn parse_table_name(&mut self) -> Result<Symbol, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(self.intern(&name))
            }
            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }

    /// Parse a column name (identifier).
    fn parse_column_name(&mut self) -> Result<Symbol, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(self.intern(&name))
            }
            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }

    /// Parse ADD COLUMN operation.
    fn parse_add_column(
        &mut self,
        table_name: Symbol,
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
                Token::Keyword { keyword: Keyword::Not, .. } => {
                    self.advance();
                    self.expect_keyword(Keyword::Null)?;
                    nullable = false;
                }
                Token::Keyword { keyword: Keyword::Primary, .. } => {
                    self.advance();
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::PrimaryKey,
                    });
                }
                Token::Keyword { keyword: Keyword::Unique, .. } => {
                    self.advance();
                    constraints
                        .push(ColumnConstraint { name: None, kind: ColumnConstraintKind::Unique });
                }
                Token::Keyword { keyword: Keyword::References, .. } => {
                    self.advance();
                    let ref_table = self.parse_table_name()?;
                    // Column specification is optional in SQLite - defaults to PK
                    let ref_column = if self.peek() == &Token::LParen {
                        self.advance(); // consume LParen
                        let col = self.parse_column_name()?;
                        self.expect_token(Token::RParen)?;
                        Some(col)
                    } else {
                        None
                    };
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
            generated_expr: None,
        };

        Ok(AlterTableStmt::AddColumn(AddColumnStmt { table_name, column_def }))
    }

    /// Parse DROP COLUMN operation.
    fn parse_drop_column(
        &mut self,
        table_name: Symbol,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let if_exists =
            self.try_consume_keyword(Keyword::If) && self.try_consume_keyword(Keyword::Exists);

        let column_name = self.parse_column_name()?;

        Ok(AlterTableStmt::DropColumn(DropColumnStmt { table_name, column_name, if_exists }))
    }

    /// Parse ALTER COLUMN operation.
    fn parse_alter_column(
        &mut self,
        table_name: Symbol,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let column_name = self.parse_column_name()?;

        match self.peek() {
            Token::Keyword { keyword: Keyword::Set, .. } => {
                self.advance();
                match self.peek() {
                    Token::Keyword { keyword: Keyword::Default, .. } => {
                        self.advance();
                        let default = self.parse_expression()?;
                        Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::SetDefault {
                            table_name,
                            column_name,
                            default,
                        }))
                    }
                    Token::Keyword { keyword: Keyword::Not, .. } => {
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
            Token::Keyword { keyword: Keyword::Drop, .. } => {
                self.advance();
                match self.peek() {
                    Token::Keyword { keyword: Keyword::Default, .. } => {
                        self.advance();
                        Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::DropDefault {
                            table_name,
                            column_name,
                        }))
                    }
                    Token::Keyword { keyword: Keyword::Not, .. } => {
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
            _ => Err(ParseError { message: "Expected SET or DROP after column name".to_string() }),
        }
    }

    /// Parse ADD CONSTRAINT operation.
    fn parse_add_constraint(
        &mut self,
        table_name: Symbol,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let constraint = self.parse_table_constraint()?;
        Ok(AlterTableStmt::AddConstraint(AddConstraintStmt { table_name, constraint }))
    }

    /// Parse DROP CONSTRAINT operation.
    fn parse_drop_constraint(
        &mut self,
        table_name: Symbol,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        let constraint_name = self.parse_column_name()?;
        Ok(AlterTableStmt::DropConstraint(DropConstraintStmt { table_name, constraint_name }))
    }

    /// Parse RENAME TO operation.
    fn parse_rename_table(
        &mut self,
        table_name: Symbol,
    ) -> Result<AlterTableStmt<'arena>, ParseError> {
        self.expect_keyword(Keyword::To)?;
        let new_table_name = self.parse_table_name()?;
        Ok(AlterTableStmt::RenameTable(RenameTableStmt { table_name, new_table_name }))
    }

    /// Parse MODIFY COLUMN operation (MySQL-style).
    fn parse_modify_column(
        &mut self,
        table_name: Symbol,
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
            generated_expr: None,
        };

        Ok(AlterTableStmt::ModifyColumn(ModifyColumnStmt {
            table_name,
            column_name,
            new_column_def,
        }))
    }

    /// Parse CHANGE COLUMN operation (MySQL-style - rename and modify).
    fn parse_change_column(
        &mut self,
        table_name: Symbol,
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
            generated_expr: None,
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
                Token::Keyword { keyword: Keyword::Not, .. } => {
                    self.advance();
                    self.expect_keyword(Keyword::Null)?;
                    nullable = false;
                }
                Token::Keyword { keyword: Keyword::Primary, .. } => {
                    self.advance();
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::PrimaryKey,
                    });
                }
                Token::Keyword { keyword: Keyword::Unique, .. } => {
                    self.advance();
                    constraints
                        .push(ColumnConstraint { name: None, kind: ColumnConstraintKind::Unique });
                }
                Token::Keyword { keyword: Keyword::References, .. } => {
                    self.advance();
                    let ref_table = self.parse_table_name()?;
                    // Column specification is optional in SQLite - defaults to PK
                    let ref_column = if self.peek() == &Token::LParen {
                        self.advance(); // consume LParen
                        let col = self.parse_column_name()?;
                        self.expect_token(Token::RParen)?;
                        Some(col)
                    } else {
                        None
                    };
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
            Token::Keyword { keyword: Keyword::Primary, .. } => {
                self.advance();
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;
                let columns = self.parse_index_column_list()?;
                self.expect_token(Token::RParen)?;
                TableConstraintKind::PrimaryKey { columns }
            }
            Token::Keyword { keyword: Keyword::Unique, .. } => {
                self.advance();
                self.expect_token(Token::LParen)?;
                let columns = self.parse_index_column_list()?;
                self.expect_token(Token::RParen)?;
                TableConstraintKind::Unique { columns }
            }
            Token::Keyword { keyword: Keyword::Foreign, .. } => {
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
            Token::Keyword { keyword: Keyword::Check, .. } => {
                self.advance();
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                TableConstraintKind::Check { expr: self.arena.alloc(expr) }
            }
            _ => return Err(ParseError { message: self.peek().syntax_error() }),
        };

        Ok(TableConstraint { name, kind })
    }

    /// Parse index column list for constraints.
    fn parse_index_column_list(
        &mut self,
    ) -> Result<BumpVec<'arena, IndexColumn<'arena>>, ParseError> {
        let mut columns = BumpVec::new_in(self.arena);

        loop {
            let column_name = self.parse_column_name()?;

            // Optional prefix length for prefix indexes
            let prefix_length = if self.try_consume(&Token::LParen) {
                let len = match self.peek() {
                    Token::Number(n) => {
                        let len = n.parse::<u64>().ok();
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
            let direction = if self.try_consume_keyword(Keyword::Asc) {
                OrderDirection::Asc
            } else if self.try_consume_keyword(Keyword::Desc) {
                OrderDirection::Desc
            } else {
                OrderDirection::Asc
            };

            columns.push(IndexColumn::Column { column_name, direction, prefix_length, collation: None });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse column name list.
    fn parse_column_name_list(&mut self) -> Result<BumpVec<'arena, Symbol>, ParseError> {
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

    /// Parse the ON DELETE/UPDATE actions and MATCH clauses of a
    /// foreign-key-clause.
    ///
    /// `ON DELETE/UPDATE action` and `MATCH name` clauses may appear in any
    /// order and interleaved. MATCH clauses are parsed but ignored: every
    /// SQLite foreign key behaves as if MATCH SIMPLE were specified (see
    /// e_fkey-62). Rejecting MATCH as a syntax error made every schema that
    /// spells one out fail to create.
    fn parse_referential_actions(
        &mut self,
    ) -> Result<(Option<ReferentialAction>, Option<ReferentialAction>), ParseError> {
        let mut on_delete = None;
        let mut on_update = None;

        loop {
            if self.try_consume_keyword(Keyword::On) {
                if self.try_consume_keyword(Keyword::Delete) {
                    on_delete = Some(self.parse_referential_action()?);
                } else if self.try_consume_keyword(Keyword::Update) {
                    on_update = Some(self.parse_referential_action()?);
                }
            } else if self.try_consume_keyword(Keyword::Match) {
                // MATCH <name> (SIMPLE/FULL/PARTIAL/NONE or any identifier-like
                // token) — parsed and ignored. FULL and NONE tokenize as
                // keywords while SIMPLE and PARTIAL tokenize as identifiers, so
                // accept both token classes.
                match self.peek() {
                    Token::Identifier(_) | Token::Keyword { .. } => {
                        self.advance();
                    }
                    _ => {
                        return Err(ParseError { message: "Expected name after MATCH".to_string() })
                    }
                }
            } else {
                break;
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

    // ========================================================================
    // ANALYZE Statement
    // ========================================================================

    /// Parse ANALYZE statement.
    pub(crate) fn parse_analyze_statement(&mut self) -> Result<AnalyzeStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Analyze)?;

        // Parse optional table name
        let table_name = if let Token::Identifier(_) = self.peek() {
            Some(self.parse_arena_identifier()?)
        } else {
            None
        };

        // Parse optional column list
        let columns = if table_name.is_some() && self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        Ok(AnalyzeStmt { table_name, columns })
    }

    // ========================================================================
    // VACUUM Statement
    // ========================================================================

    /// Parse VACUUM statement (SQLite compatibility).
    ///
    /// Syntax:
    ///   VACUUM [schema_name] [INTO 'filename']
    pub(crate) fn parse_vacuum_statement(&mut self) -> Result<VacuumStmt, ParseError> {
        self.consume_keyword(Keyword::Vacuum)?;

        // Parse optional schema name (e.g. VACUUM main)
        let schema_name = if let Token::Identifier(_) = self.peek() {
            Some(self.parse_arena_identifier()?)
        } else {
            None
        };

        // Parse optional INTO 'filename'
        let into_file = if self.try_consume_keyword(Keyword::Into) {
            match self.peek().clone() {
                Token::String(s) => {
                    self.advance();
                    Some(self.intern(&s))
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected string literal after VACUUM INTO".to_string(),
                    })
                }
            }
        } else {
            None
        };

        Ok(VacuumStmt { schema_name, into_file })
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    /// Parse index column specifications.
    fn parse_index_columns(&mut self) -> Result<BumpVec<'arena, IndexColumn<'arena>>, ParseError> {
        let mut columns = BumpVec::new_in(self.arena);
        loop {
            let column_name = self.parse_arena_identifier()?;

            // Parse optional prefix length (e.g., name(10))
            let prefix_length = if self.try_consume(&Token::LParen) {
                let len = match self.peek() {
                    Token::Number(n) => n
                        .parse::<u64>()
                        .map_err(|_| ParseError { message: "Invalid prefix length".to_string() })?,
                    _ => {
                        return Err(ParseError {
                            message: "Expected number for prefix length".to_string(),
                        })
                    }
                };
                self.advance();
                self.expect_token(Token::RParen)?;
                Some(len)
            } else {
                None
            };

            // Parse optional direction
            let direction = if self.try_consume_keyword(Keyword::Desc) {
                OrderDirection::Desc
            } else {
                self.try_consume_keyword(Keyword::Asc);
                OrderDirection::Asc
            };

            columns.push(IndexColumn::Column { column_name, direction, prefix_length, collation: None });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }
        Ok(columns)
    }

    // ========================================================================
    // PRAGMA Statement
    // ========================================================================

    /// Parse PRAGMA statement (SQLite compatibility).
    ///
    /// Syntax:
    /// - PRAGMA pragma_name;
    /// - PRAGMA pragma_name = value;
    /// - PRAGMA pragma_name(value);
    /// - PRAGMA database.pragma_name;
    pub(crate) fn parse_pragma_statement(&mut self) -> Result<PragmaStmt, ParseError> {
        self.consume_keyword(Keyword::Pragma)?;

        // Parse first identifier (could be database or pragma name)
        let first_ident = self.parse_arena_identifier()?;

        // Check for dot (database.pragma_name syntax)
        let (database, name) = if self.try_consume(&Token::Symbol('.')) {
            let pragma_name = self.parse_arena_identifier()?;
            (Some(first_ident), pragma_name)
        } else {
            (None, first_ident)
        };

        // Check for optional value assignment or function-style argument
        let value = if self.try_consume(&Token::Symbol('=')) {
            Some(self.parse_pragma_value()?)
        } else if self.try_consume(&Token::LParen) {
            let val = self.parse_pragma_value()?;
            self.expect_token(Token::RParen)?;
            Some(val)
        } else {
            None
        };

        Ok(PragmaStmt { database, name, value })
    }

    /// Parse a PRAGMA value.
    fn parse_pragma_value(&mut self) -> Result<PragmaValue, ParseError> {
        match self.peek().clone() {
            Token::Identifier(ident) => {
                self.advance();
                let sym = self.interner.intern(&ident);
                Ok(PragmaValue::Identifier(sym))
            }
            Token::DelimitedIdentifier(ident) => {
                // Accept double-quoted/bracketed/backtick identifiers as
                // PRAGMA arguments (e.g. `PRAGMA table_info("name with space")`).
                self.advance();
                let sym = self.interner.intern(&ident);
                Ok(PragmaValue::Identifier(sym))
            }
            Token::Keyword { keyword: kw, .. } => {
                self.advance();
                let sym = self.interner.intern(&kw.to_string());
                Ok(PragmaValue::Identifier(sym))
            }
            Token::String(s) => {
                self.advance();
                let sym = self.interner.intern(&s);
                Ok(PragmaValue::String(sym))
            }
            Token::Number(n) => {
                self.advance();
                let sym = self.interner.intern(&n);
                Ok(PragmaValue::Number(sym))
            }
            Token::Symbol('-') => {
                self.advance(); // consume '-'
                match self.peek().clone() {
                    Token::Number(n) => {
                        self.advance();
                        let sym = self.interner.intern(&format!("-{}", n));
                        Ok(PragmaValue::SignedNumber(sym))
                    }
                    _ => Err(ParseError {
                        message: "Expected number after minus sign in PRAGMA value".to_string(),
                    }),
                }
            }
            Token::Symbol('+') => {
                self.advance(); // consume '+'
                match self.peek().clone() {
                    Token::Number(n) => {
                        self.advance();
                        let sym = self.interner.intern(&n);
                        Ok(PragmaValue::Number(sym))
                    }
                    _ => Err(ParseError {
                        message: "Expected number after plus sign in PRAGMA value".to_string(),
                    }),
                }
            }
            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }
}
