//! Cursor declaration parsing (SQL:1999 Feature E121)

use vibesql_ast::{
    CloseCursorStmt, CursorUpdatability, DeclareCursorStmt, FetchOrientation, FetchStmt,
    OpenCursorStmt,
};

use crate::{
    keywords::Keyword,
    parser::{ParseError, Parser},
    token::Token,
};

impl Parser {
    /// Parse DECLARE CURSOR statement
    ///
    /// Syntax:
    /// ```sql
    /// DECLARE cursor_name [INSENSITIVE] [SCROLL] CURSOR
    ///   [WITH HOLD | WITHOUT HOLD]
    ///   FOR query_expression
    ///   [FOR {READ ONLY | UPDATE [OF column_list]}]
    /// ```
    pub(super) fn parse_declare_cursor_statement(
        &mut self,
    ) -> Result<DeclareCursorStmt, ParseError> {
        // DECLARE keyword
        self.expect_keyword(Keyword::Declare)?;

        // Cursor name
        let cursor_name = self.parse_identifier()?;

        // Optional INSENSITIVE
        let insensitive = self.try_consume_keyword(Keyword::Insensitive);

        // Optional SCROLL
        let scroll = self.try_consume_keyword(Keyword::Scroll);

        // Required CURSOR keyword
        self.expect_keyword(Keyword::Cursor)?;

        // Optional WITH HOLD | WITHOUT HOLD
        let hold = if self.try_consume_keyword(Keyword::With) {
            self.expect_keyword(Keyword::Hold)?;
            Some(true)
        } else if self.try_consume_keyword(Keyword::Without) {
            self.expect_keyword(Keyword::Hold)?;
            Some(false)
        } else {
            None
        };

        // Required FOR keyword
        self.expect_keyword(Keyword::For)?;

        // Query expression (SELECT statement)
        // Use embedded variant since FOR READ ONLY/UPDATE may follow
        let query = Box::new(self.parse_embedded_select_statement()?);

        // Optional FOR {READ ONLY | UPDATE [OF column_list]}
        let updatability = if self.try_consume_keyword(Keyword::For) {
            if self.try_consume_keyword(Keyword::Read) {
                self.expect_keyword(Keyword::Only)?;
                CursorUpdatability::ReadOnly
            } else if self.try_consume_keyword(Keyword::Update) {
                // Check for OF column_list
                let columns = if self.try_consume_keyword(Keyword::Of) {
                    let mut cols = Vec::new();
                    cols.push(self.parse_identifier()?);
                    while self.try_consume(&Token::Comma) {
                        cols.push(self.parse_identifier()?);
                    }
                    Some(cols)
                } else {
                    None
                };
                CursorUpdatability::Update { columns }
            } else {
                return Err(ParseError {
                    message: "Expected READ ONLY or UPDATE after FOR".to_string(),
                });
            }
        } else {
            CursorUpdatability::Unspecified
        };

        Ok(DeclareCursorStmt { cursor_name, insensitive, scroll, hold, query, updatability })
    }

    /// Parse OPEN CURSOR statement
    ///
    /// Syntax:
    /// ```sql
    /// OPEN cursor_name
    /// ```
    pub(super) fn parse_open_cursor_statement(&mut self) -> Result<OpenCursorStmt, ParseError> {
        // OPEN keyword
        self.expect_keyword(Keyword::Open)?;

        // Cursor name
        let cursor_name = self.parse_identifier()?;

        Ok(OpenCursorStmt { cursor_name })
    }

    /// Parse FETCH statement
    ///
    /// Syntax:
    /// ```sql
    /// FETCH [ [ NEXT | PRIOR | FIRST | LAST | ABSOLUTE n | RELATIVE n ] FROM ] cursor_name
    ///   [ INTO variable [, ...] ]
    /// ```
    pub(super) fn parse_fetch_statement(&mut self) -> Result<FetchStmt, ParseError> {
        // FETCH keyword
        self.expect_keyword(Keyword::Fetch)?;

        // Parse orientation (optional)
        let orientation = if self.try_consume_keyword(Keyword::Next) {
            FetchOrientation::Next
        } else if self.try_consume_keyword(Keyword::Prior) {
            FetchOrientation::Prior
        } else if self.try_consume_keyword(Keyword::First) {
            FetchOrientation::First
        } else if self.try_consume_keyword(Keyword::Last) {
            FetchOrientation::Last
        } else if self.try_consume_keyword(Keyword::Absolute) {
            let n = self.parse_integer_literal()?;
            FetchOrientation::Absolute(n)
        } else if self.try_consume_keyword(Keyword::Relative) {
            let n = self.parse_integer_literal()?;
            FetchOrientation::Relative(n)
        } else {
            FetchOrientation::Next // default
        };

        // Optional FROM keyword
        self.try_consume_keyword(Keyword::From);

        // Cursor name
        let cursor_name = self.parse_identifier()?;

        // Optional INTO clause
        let into_variables = if self.try_consume_keyword(Keyword::Into) {
            let mut vars = Vec::new();
            vars.push(self.parse_identifier()?);
            while self.try_consume(&Token::Comma) {
                vars.push(self.parse_identifier()?);
            }
            Some(vars)
        } else {
            None
        };

        Ok(FetchStmt { cursor_name, orientation, into_variables })
    }

    /// Parse CLOSE CURSOR statement
    ///
    /// Syntax:
    /// ```sql
    /// CLOSE cursor_name
    /// ```
    pub(super) fn parse_close_cursor_statement(&mut self) -> Result<CloseCursorStmt, ParseError> {
        // CLOSE keyword
        self.expect_keyword(Keyword::Close)?;

        // Cursor name
        let cursor_name = self.parse_identifier()?;

        Ok(CloseCursorStmt { cursor_name })
    }
}
