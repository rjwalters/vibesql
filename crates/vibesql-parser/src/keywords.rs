use std::fmt;

/// SQL Keywords supported by the parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Select,
    Distinct,
    From,
    Where,
    Insert,
    Into,
    Replace,
    Ignore,
    Update,
    Delete,
    Duplicate,
    Create,
    Table,
    Truncate,
    Drop,
    Alter,
    And,
    Or,
    Not,
    Null,
    True,
    False,
    Unknown,
    As,
    Join,
    Left,
    Right,
    Inner,
    Outer,
    Cross,
    Full,
    Natural,
    On,
    Group,
    Groups,
    By,
    // OLAP extensions for GROUP BY
    Rollup,
    Cube,
    Sets,
    Grouping,
    GroupingId,
    Having,
    Order,
    Asc,
    Desc,
    Nulls,
    Limit,
    Offset,
    Set,
    Values,
    In,
    Between,
    Asymmetric,
    Symmetric,
    Like,
    Glob,
    Escape,
    Exists,
    If,
    All,
    Any,
    Some,
    Union,
    Intersect,
    Except,
    With,
    Recursive,
    // CTE materialization hints (SQL:1999)
    Materialized,
    Date,
    Time,
    Timestamp,
    Interval,
    Cast,
    // CASE expression keywords
    Case,
    When,
    Then,
    Else,
    End,
    // Window function keywords
    Over,
    Partition,
    Rows,
    Range,
    Preceding,
    Following,
    Unbounded,
    Current,
    Filter,
    Exclude,
    Others,
    Ties,
    Window,
    // Note: Window function names (ROW_NUMBER, RANK, etc.) are identifiers, not keywords
    // Constraint keywords
    Primary,
    Foreign,
    Key,
    Unique,
    Check,
    References,
    // IS NULL keywords
    Is,
    // SQLite ISNULL/NOTNULL postfix operators
    Isnull,
    Notnull,
    // Transaction keywords
    Begin,
    Commit,
    Rollback,
    // SCHEMA keywords
    Cascade,
    Restrict,
    Schema,
    Start,
    Transaction,
    // ALTER TABLE keywords
    Add,
    Column,
    Constraint,
    Default,
    Rename,
    Modify,
    Change,
    /// RETURNING clause on DML statements (SQLite 3.35.0+)
    Returning,
    // SAVEPOINT keywords
    Savepoint,
    Release,
    To,
    // Role management keywords
    Role,
    // TRIM function keywords
    Both,
    Leading,
    Trailing,
    // MySQL arithmetic operators
    Div,
    // Data type keywords
    Varying,
    Characters,
    Octets,
    Using,
    // SUBSTRING function keywords
    For,
    // Current date/time function keywords
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    // GRANT keywords
    Grant,
    Privileges,
    Usage,
    Option,
    // REVOKE keywords
    Revoke,
    Granted,
    // Advanced privilege keywords
    Execute,
    Trigger,
    Under,
    // Advanced SQL object keywords
    Domain,
    Sequence,
    Type,
    Collation,
    Character,
    Translation,
    View,
    Index,
    Analyze,
    Reindex,
    Vacuum,
    Explain,
    Assertion,
    Specific,
    // Trigger-specific keywords
    Before,
    After,
    Instead,
    Of,
    Each,
    Row,
    Statement,
    Enable,
    Disable,
    // SEQUENCE specific keywords
    Increment,
    Minvalue,
    Maxvalue,
    Cycle,
    No,
    Restart,
    Next,
    // Referential action keywords
    Action,
    // Constraint deferral keywords (SQL:1999)
    Deferrable,
    Deferred,
    Immediate,
    Initially,
    // Session configuration keywords (SQL:1999)
    Catalog,
    Names,
    Zone,
    Local,
    Session,
    Global,
    // Interval unit keywords
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Microsecond,
    // Callable object keywords (functions, procedures, methods)
    Function,
    Procedure,
    Call,
    Routine,
    Method,
    Constructor,
    Static,
    Instance,
    // Procedure/Function parameter modes
    Out,
    InOut,
    // Procedure/Function keywords
    Returns,
    While,
    Do,
    Loop,
    Repeat,
    Until,
    Return,
    Leave,
    Iterate,
    // Procedure/Function characteristics (Phase 6)
    Deterministic,
    Language,
    Sql,
    Security,
    Definer,
    Invoker,
    // Internationalization keywords (SQL:1999)
    Get,
    Pad,
    Space,
    Collate,
    // Generated column keywords (SQLite)
    Generated,
    Always,
    Stored,
    Virtual,
    // CURSOR keywords
    Declare,
    Cursor,
    Insensitive,
    // Prepared statement keywords
    Prepare,
    Deallocate,
    Scroll,
    Hold,
    Without,
    Read,
    Only,
    Oids,
    Rowid,
    Open,
    Fetch,
    Close,
    Prior,
    First,
    Last,
    Absolute,
    Relative,
    Serializable,
    Isolation,
    Level,
    Write,
    Comment,
    // MySQL table option keywords
    KeyBlockSize,
    Connection,
    InsertMethod,
    RowFormat,
    DelayKeyWrite,
    TableChecksum,
    Checksum,
    StatsSamplePages,
    Password,
    AvgRowLength,
    MinRows,
    MaxRows,
    SecondaryEngine,
    // MySQL table option values
    Dynamic,
    Fixed,
    Compressed,
    Redundant,
    Compact,
    // Full-text search keywords
    Fulltext,
    Match,
    Against,
    Boolean,
    Expansion,
    Mode,
    Query,
    Plan,
    // Spatial index keywords
    Spatial,
    // Vector index keywords (IVFFlat, HNSW)
    Ivfflat,
    Hnsw,
    Lists,
    Probes,
    EfConstruction,
    EfSearch,
    M,
    // Database introspection keywords
    Show,
    Describe,
    Databases,
    Tables,
    Columns,
    Fields,
    Indexes,
    Keys,
    // Auto-increment keywords (MySQL and SQLite)
    AutoIncrement,
    // Temporary object keywords (SQLite)
    Temp,
    Temporary,
    // VibeSQL storage format keywords
    Storage,
    Columnar,
    // Transaction durability hint keywords
    Durability,
    Lazy,
    Durable,
    Volatile,
    // SQLite compatibility keywords
    Pragma,
    // SQLite conflict resolution keywords
    Abort,
    Fail,
    Conflict,
    Nothing,
}

impl Keyword {
    /// Returns true if this keyword can be used as an unquoted column or table name.
    /// These are "unreserved" keywords that are only treated as keywords in specific contexts.
    ///
    /// This follows SQLite's behavior where temporal type keywords (TIMESTAMP, DATE, TIME,
    /// INTERVAL) and interval unit keywords (YEAR, MONTH, DAY, etc.) can be used as identifiers
    /// without quoting.
    ///
    /// TYPE and SQL are also unreserved to match SQLite compatibility (neither is in SQLite's
    /// 147-keyword reserved list). They work safely because:
    /// - TYPE: Parser uses peek_next_keyword() for CREATE TYPE dispatch
    /// - SQL: Only consumed after explicit LANGUAGE keyword in routines
    pub fn can_be_identifier(&self) -> bool {
        matches!(
            self,
            // Temporal type keywords
            Keyword::Timestamp | Keyword::Date | Keyword::Time | Keyword::Interval |
            // Interval unit keywords (used in EXTRACT, DATE_ADD, etc. but also valid as identifiers)
            Keyword::Year | Keyword::Quarter | Keyword::Month | Keyword::Week |
            Keyword::Day | Keyword::Hour | Keyword::Minute | Keyword::Second |
            Keyword::Microsecond |
            // SQLite compatibility: TYPE and SQL are not reserved in SQLite
            // (allows unquoted access to sqlite_master.type and sqlite_master.sql columns)
            // ROWID is the pseudo-column for implicit row IDs in SQLite tables
            Keyword::Type | Keyword::Sql | Keyword::Rowid |
            // Vector index parameter: M is only contextual in HNSW WITH clause
            // SQLite TCL tests commonly use 'm' as a column name
            Keyword::M |
            // NULLS is only contextual in ORDER BY (NULLS FIRST/LAST)
            // Can be used as identifier for table/column/CTE names
            Keyword::Nulls |
            // SQLite schema keywords: TEMP/TEMPORARY are valid schema names
            // Allows three-part qualified names like temp.t1.column
            Keyword::Temp | Keyword::Temporary |
            // Procedure parameter direction keywords: OUT/INOUT are only meaningful
            // in stored procedure contexts, safe to use as identifiers elsewhere
            // SQLite TCL tests use 'out' as a CTE name
            Keyword::Out | Keyword::InOut |
            // Join keywords: SQLite allows these as identifiers (table/column/function names)
            // when not in a join context. SQLite TCL tests (func8.test) use these extensively.
            Keyword::Cross | Keyword::Full | Keyword::Inner | Keyword::Left |
            Keyword::Natural | Keyword::Outer | Keyword::Right |
            // Window function keywords: These are contextual in window frame specifications
            // but can be used as identifiers elsewhere. SQLite TCL tests (window1.test)
            // create tables with these as column names.
            Keyword::Current | Keyword::Exclude | Keyword::Filter | Keyword::Following |
            Keyword::Groups | Keyword::No | Keyword::Others | Keyword::Over |
            Keyword::Partition | Keyword::Preceding | Keyword::Range | Keyword::Ties |
            Keyword::Unbounded | Keyword::Window | Keyword::First | Keyword::Last |
            // RETURNING is a "fallback" keyword in SQLite (3.35.0+): it is only
            // meaningful at the end of a DML statement and remains usable as an
            // ordinary identifier elsewhere.
            Keyword::Returning
        )
    }

    /// Returns true if this keyword may be used as an unquoted column reference
    /// in **expression / primary position** (e.g. `SELECT release FROM t`,
    /// `WHERE asc = 1`).
    ///
    /// SQLite reserves only a small set of words and accepts the rest as
    /// identifiers when they appear where a column reference is expected. This
    /// predicate models that behavior with a *denylist*: it accepts any keyword
    /// EXCEPT the ones that must stay reserved as the start of an operand,
    /// because allowing them there would create grammar ambiguity or mask
    /// genuine syntax errors.
    ///
    /// The denylist intentionally covers three categories of keyword:
    ///
    /// 1. **Operators** consumed by the expression precedence cascade in
    ///    *operator* position (`AND`, `OR`, `NOT`, `IN`, `BETWEEN`,
    ///    `ESCAPE`, `IS`, `ISNULL`, `NOTNULL`, `COLLATE`, `DIV`,
    ///    `ALL`/`ANY`/`SOME`, `AS`). These must never be reinterpreted as a
    ///    column when an operand is expected. Note that `LIKE`/`GLOB` are
    ///    *not* denied: at operand start they can only be a column reference
    ///    (or a `like(...)`/`glob(...)` function call, which the function-call
    ///    parser claims first); their operator role only arises in operator
    ///    position, which this predicate never governs (keyword1.test).
    /// 2. **Statement / clause structure** keywords that terminate or introduce
    ///    a clause and which the expression caller relies on to STOP parsing
    ///    (`SELECT`, `FROM`, `WHERE`, `GROUP`, `HAVING`, `ORDER`,
    ///    `LIMIT`, `WINDOW`, `UNION`, `INTERSECT`, `EXCEPT`, `INTO`,
    ///    `VALUES`, `SET`, `ON`, `USING`, `JOIN` and its modifiers, the DML/DDL
    ///    verbs, etc.). Words that only structure a clause *after* another
    ///    keyword has been consumed (`BY`, `OFFSET`, `WITH`, `RECURSIVE`,
    ///    `REPLACE`, `PRAGMA`) are NOT denied: at operand start they are
    ///    unambiguous column references, matching SQLite's fallback rules
    ///    (`SELECT by FROM t ORDER BY by`, keyword1.test).
    /// 3. **Special primary forms / literals** that have dedicated parsing
    ///    (`CASE`/`WHEN`/`THEN`/`ELSE`, `CAST`, `EXISTS`, `NULL`, `TRUE`,
    ///    `FALSE`, `UNKNOWN`, the `CURRENT_*` constants, `DISTINCT`). `END` is
    ///    NOT denied: it is only meaningful *after* a complete CASE body, where
    ///    the CASE parser consumes it at clause level before expression
    ///    parsing resumes; at operand start SQLite treats it as a column
    ///    (`ORDER BY end`, keyword1.test).
    ///
    /// Everything else — including otherwise-reserved column-name words like
    /// `RELEASE`, `SAVEPOINT`, `KEY`, `ASC`, `DESC`, `BEGIN`, `COMMIT`,
    /// `ROLLBACK`, `MATCH`, `COLUMN`, `INDEX`, ... — is accepted as a column
    /// reference, matching SQLite (see table.test table-7.3).
    pub fn can_be_identifier_in_expression(&self) -> bool {
        // Words that already round-trip as identifiers in any position are a
        // superset-safe shortcut.
        if self.can_be_identifier() {
            return true;
        }

        !matches!(
            self,
            // --- Category 1: operators (operator-position keywords) ---
            Keyword::And
                | Keyword::Or
                | Keyword::Not
                | Keyword::In
                | Keyword::Between
                | Keyword::Asymmetric
                | Keyword::Symmetric
                | Keyword::Escape
                | Keyword::Is
                | Keyword::Isnull
                | Keyword::Notnull
                | Keyword::Collate
                | Keyword::Div
                | Keyword::All
                | Keyword::Any
                | Keyword::Some
                | Keyword::As
                // --- Category 2: statement / clause structure ---
                | Keyword::Select
                | Keyword::From
                | Keyword::Where
                | Keyword::Group
                | Keyword::Having
                | Keyword::Order
                | Keyword::Limit
                | Keyword::Into
                | Keyword::Values
                | Keyword::Set
                | Keyword::On
                | Keyword::Using
                | Keyword::Join
                | Keyword::Union
                | Keyword::Intersect
                | Keyword::Except
                | Keyword::Insert
                | Keyword::Update
                | Keyword::Delete
                | Keyword::Create
                | Keyword::Drop
                | Keyword::Alter
                | Keyword::Truncate
                // --- Category 3: special primary forms / literals ---
                | Keyword::Case
                | Keyword::When
                | Keyword::Then
                | Keyword::Else
                | Keyword::Cast
                | Keyword::Exists
                | Keyword::Null
                | Keyword::True
                | Keyword::False
                | Keyword::Unknown
                | Keyword::Distinct
                | Keyword::CurrentDate
                | Keyword::CurrentTime
                | Keyword::CurrentTimestamp
        )
    }

    /// Returns true if this keyword may be used as an unquoted *table name*
    /// (e.g. after `FROM`, in `INSERT INTO`, `DROP TABLE`).
    ///
    /// This is the expression-position set plus the special primary forms
    /// `CAST` and `CURRENT_DATE`/`CURRENT_TIME`/`CURRENT_TIMESTAMP`: in
    /// expression position those words must keep their dedicated parsing
    /// (`CAST(x AS t)`, the datetime literals), but in table-name position no
    /// such form can occur, so SQLite's fallback rules accept them as plain
    /// names (`SELECT * FROM cast`, keyword1.test).
    pub fn can_be_identifier_in_table_position(&self) -> bool {
        self.can_be_identifier_in_expression()
            || matches!(
                self,
                Keyword::Cast
                    | Keyword::CurrentDate
                    | Keyword::CurrentTime
                    | Keyword::CurrentTimestamp
            )
    }

    /// Returns true if this keyword is in SQLite's `%fallback ID` set — the
    /// keywords that SQLite's parser demotes to plain identifiers whenever the
    /// keyword reading does not fit the grammar (see `parse.y` in the SQLite
    /// sources, and keyword1.test which exercises them as table, column, type,
    /// and index names).
    ///
    /// VibeSQL uses this predicate for positions where SQLite accepts any
    /// fallback keyword as a name but truly-reserved words (`PRIMARY`, `NOT`,
    /// `CROSS`, `SELECT`, ...) must stay rejected:
    ///
    /// - column *type* names (`CREATE TABLE abort(abort abort)` is legal;
    ///   `CREATE TABLE t(x primary)` is not)
    /// - index names in `CREATE INDEX` / `DROP INDEX`
    /// - index names after `INDEXED BY`
    ///
    /// Two members of SQLite's fallback set are deliberately omitted:
    /// `GENERATED` and `ALWAYS`. VibeSQL parses `ADD COLUMN x GENERATED ALWAYS
    /// AS (...)` by consuming `GENERATED` before the type, so demoting it here
    /// would swallow generated-column syntax in ALTER TABLE.
    pub fn is_sqlite_fallback_keyword(&self) -> bool {
        matches!(
            self,
            Keyword::Abort
                | Keyword::Action
                | Keyword::After
                | Keyword::Analyze
                | Keyword::Asc
                | Keyword::Before
                | Keyword::Begin
                | Keyword::By
                | Keyword::Cascade
                | Keyword::Cast
                | Keyword::Column
                | Keyword::Conflict
                | Keyword::Current
                | Keyword::CurrentDate
                | Keyword::CurrentTime
                | Keyword::CurrentTimestamp
                | Keyword::Deferred
                | Keyword::Desc
                | Keyword::Do
                | Keyword::Each
                | Keyword::End
                | Keyword::Exclude
                | Keyword::Explain
                | Keyword::Fail
                | Keyword::First
                | Keyword::Following
                | Keyword::For
                | Keyword::Glob
                | Keyword::Groups
                | Keyword::If
                | Keyword::Ignore
                | Keyword::Immediate
                | Keyword::Initially
                | Keyword::Instead
                | Keyword::Key
                | Keyword::Last
                | Keyword::Like
                | Keyword::Match
                | Keyword::Materialized
                | Keyword::No
                | Keyword::Nulls
                | Keyword::Of
                | Keyword::Offset
                | Keyword::Others
                | Keyword::Partition
                | Keyword::Plan
                | Keyword::Pragma
                | Keyword::Preceding
                | Keyword::Query
                | Keyword::Range
                | Keyword::Recursive
                | Keyword::Reindex
                | Keyword::Release
                | Keyword::Rename
                | Keyword::Replace
                | Keyword::Restrict
                | Keyword::Rollback
                | Keyword::Row
                | Keyword::Rows
                | Keyword::Savepoint
                | Keyword::Temp
                | Keyword::Temporary
                | Keyword::Ties
                | Keyword::Trigger
                | Keyword::Unbounded
                | Keyword::Vacuum
                | Keyword::View
                | Keyword::Virtual
                | Keyword::With
                | Keyword::Without
        )
    }
}

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keyword_str = match self {
            Keyword::Select => "SELECT",
            Keyword::Distinct => "DISTINCT",
            Keyword::From => "FROM",
            Keyword::Where => "WHERE",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Replace => "REPLACE",
            Keyword::Ignore => "IGNORE",
            Keyword::Update => "UPDATE",
            Keyword::Delete => "DELETE",
            Keyword::Duplicate => "DUPLICATE",
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Truncate => "TRUNCATE",
            Keyword::Drop => "DROP",
            Keyword::Alter => "ALTER",
            Keyword::And => "AND",
            Keyword::Or => "OR",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Unknown => "UNKNOWN",
            Keyword::As => "AS",
            Keyword::Join => "JOIN",
            Keyword::Left => "LEFT",
            Keyword::Right => "RIGHT",
            Keyword::Inner => "INNER",
            Keyword::Outer => "OUTER",
            Keyword::Cross => "CROSS",
            Keyword::Full => "FULL",
            Keyword::Natural => "NATURAL",
            Keyword::On => "ON",
            Keyword::Group => "GROUP",
            Keyword::Groups => "GROUPS",
            Keyword::By => "BY",
            Keyword::Rollup => "ROLLUP",
            Keyword::Cube => "CUBE",
            Keyword::Sets => "SETS",
            Keyword::Grouping => "GROUPING",
            Keyword::GroupingId => "GROUPING_ID",
            Keyword::Having => "HAVING",
            Keyword::Order => "ORDER",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
            Keyword::Nulls => "NULLS",
            Keyword::Limit => "LIMIT",
            Keyword::Offset => "OFFSET",
            Keyword::Set => "SET",
            Keyword::Values => "VALUES",
            Keyword::In => "IN",
            Keyword::Between => "BETWEEN",
            Keyword::Asymmetric => "ASYMMETRIC",
            Keyword::Symmetric => "SYMMETRIC",
            Keyword::Like => "LIKE",
            Keyword::Glob => "GLOB",
            Keyword::Escape => "ESCAPE",
            Keyword::Exists => "EXISTS",
            Keyword::If => "IF",
            Keyword::All => "ALL",
            Keyword::Any => "ANY",
            Keyword::Some => "SOME",
            Keyword::Union => "UNION",
            Keyword::Intersect => "INTERSECT",
            Keyword::Except => "EXCEPT",
            Keyword::With => "WITH",
            Keyword::Recursive => "RECURSIVE",
            Keyword::Materialized => "MATERIALIZED",
            Keyword::Date => "DATE",
            Keyword::Time => "TIME",
            Keyword::Timestamp => "TIMESTAMP",
            Keyword::Interval => "INTERVAL",
            Keyword::Cast => "CAST",
            Keyword::Case => "CASE",
            Keyword::When => "WHEN",
            Keyword::Then => "THEN",
            Keyword::Else => "ELSE",
            Keyword::End => "END",
            Keyword::Over => "OVER",
            Keyword::Partition => "PARTITION",
            Keyword::Rows => "ROWS",
            Keyword::Range => "RANGE",
            Keyword::Preceding => "PRECEDING",
            Keyword::Following => "FOLLOWING",
            Keyword::Unbounded => "UNBOUNDED",
            Keyword::Current => "CURRENT",
            Keyword::Filter => "FILTER",
            Keyword::Exclude => "EXCLUDE",
            Keyword::Others => "OTHERS",
            Keyword::Ties => "TIES",
            Keyword::Window => "WINDOW",
            Keyword::Primary => "PRIMARY",
            Keyword::Foreign => "FOREIGN",
            Keyword::Key => "KEY",
            Keyword::Unique => "UNIQUE",
            Keyword::Check => "CHECK",
            Keyword::References => "REFERENCES",
            Keyword::Is => "IS",
            Keyword::Isnull => "ISNULL",
            Keyword::Notnull => "NOTNULL",
            Keyword::Begin => "BEGIN",
            Keyword::Commit => "COMMIT",
            Keyword::Rollback => "ROLLBACK",
            Keyword::Cascade => "CASCADE",
            Keyword::Restrict => "RESTRICT",
            Keyword::Schema => "SCHEMA",
            Keyword::Start => "START",
            Keyword::Transaction => "TRANSACTION",
            Keyword::Add => "ADD",
            Keyword::Column => "COLUMN",
            Keyword::Constraint => "CONSTRAINT",
            Keyword::Default => "DEFAULT",
            Keyword::Rename => "RENAME",
            Keyword::Returning => "RETURNING",
            Keyword::Modify => "MODIFY",
            Keyword::Change => "CHANGE",
            Keyword::Savepoint => "SAVEPOINT",
            Keyword::Release => "RELEASE",
            Keyword::To => "TO",
            Keyword::Role => "ROLE",
            Keyword::Both => "BOTH",
            Keyword::Leading => "LEADING",
            Keyword::Trailing => "TRAILING",
            Keyword::Div => "DIV",
            Keyword::Varying => "VARYING",
            Keyword::Characters => "CHARACTERS",
            Keyword::Octets => "OCTETS",
            Keyword::Using => "USING",
            Keyword::For => "FOR",
            Keyword::CurrentDate => "CURRENT_DATE",
            Keyword::CurrentTime => "CURRENT_TIME",
            Keyword::CurrentTimestamp => "CURRENT_TIMESTAMP",
            Keyword::Grant => "GRANT",
            Keyword::Privileges => "PRIVILEGES",
            Keyword::Usage => "USAGE",
            Keyword::Option => "OPTION",
            Keyword::Revoke => "REVOKE",
            Keyword::Granted => "GRANTED",
            Keyword::Execute => "EXECUTE",
            Keyword::Trigger => "TRIGGER",
            Keyword::Under => "UNDER",
            Keyword::Domain => "DOMAIN",
            Keyword::Sequence => "SEQUENCE",
            Keyword::Type => "TYPE",
            Keyword::Collation => "COLLATION",
            Keyword::Character => "CHARACTER",
            Keyword::Translation => "TRANSLATION",
            Keyword::View => "VIEW",
            Keyword::Index => "INDEX",
            Keyword::Analyze => "ANALYZE",
            Keyword::Reindex => "REINDEX",
            Keyword::Vacuum => "VACUUM",
            Keyword::Explain => "EXPLAIN",
            Keyword::Assertion => "ASSERTION",
            Keyword::Specific => "SPECIFIC",
            Keyword::Before => "BEFORE",
            Keyword::After => "AFTER",
            Keyword::Instead => "INSTEAD",
            Keyword::Of => "OF",
            Keyword::Each => "EACH",
            Keyword::Row => "ROW",
            Keyword::Statement => "STATEMENT",
            Keyword::Enable => "ENABLE",
            Keyword::Disable => "DISABLE",
            Keyword::Increment => "INCREMENT",
            Keyword::Minvalue => "MINVALUE",
            Keyword::Maxvalue => "MAXVALUE",
            Keyword::Cycle => "CYCLE",
            Keyword::No => "NO",
            Keyword::Restart => "RESTART",
            Keyword::Next => "NEXT",
            Keyword::Action => "ACTION",
            Keyword::Deferrable => "DEFERRABLE",
            Keyword::Deferred => "DEFERRED",
            Keyword::Immediate => "IMMEDIATE",
            Keyword::Initially => "INITIALLY",
            Keyword::Catalog => "CATALOG",
            Keyword::Names => "NAMES",
            Keyword::Zone => "ZONE",
            Keyword::Local => "LOCAL",
            Keyword::Session => "SESSION",
            Keyword::Global => "GLOBAL",
            Keyword::Year => "YEAR",
            Keyword::Quarter => "QUARTER",
            Keyword::Month => "MONTH",
            Keyword::Week => "WEEK",
            Keyword::Day => "DAY",
            Keyword::Hour => "HOUR",
            Keyword::Minute => "MINUTE",
            Keyword::Second => "SECOND",
            Keyword::Microsecond => "MICROSECOND",
            Keyword::Function => "FUNCTION",
            Keyword::Procedure => "PROCEDURE",
            Keyword::Call => "CALL",
            Keyword::Routine => "ROUTINE",
            Keyword::Method => "METHOD",
            Keyword::Constructor => "CONSTRUCTOR",
            Keyword::Static => "STATIC",
            Keyword::Instance => "INSTANCE",
            Keyword::Out => "OUT",
            Keyword::InOut => "INOUT",
            Keyword::Returns => "RETURNS",
            Keyword::While => "WHILE",
            Keyword::Do => "DO",
            Keyword::Loop => "LOOP",
            Keyword::Repeat => "REPEAT",
            Keyword::Until => "UNTIL",
            Keyword::Return => "RETURN",
            Keyword::Leave => "LEAVE",
            Keyword::Iterate => "ITERATE",
            Keyword::Deterministic => "DETERMINISTIC",
            Keyword::Language => "LANGUAGE",
            Keyword::Sql => "SQL",
            Keyword::Security => "SECURITY",
            Keyword::Definer => "DEFINER",
            Keyword::Invoker => "INVOKER",
            Keyword::Comment => "COMMENT",
            Keyword::Get => "GET",
            Keyword::Pad => "PAD",
            Keyword::Space => "SPACE",
            Keyword::Collate => "COLLATE",
            Keyword::Generated => "GENERATED",
            Keyword::Always => "ALWAYS",
            Keyword::Stored => "STORED",
            Keyword::Virtual => "VIRTUAL",
            Keyword::Declare => "DECLARE",
            Keyword::Cursor => "CURSOR",
            Keyword::Insensitive => "INSENSITIVE",
            Keyword::Prepare => "PREPARE",
            Keyword::Deallocate => "DEALLOCATE",
            Keyword::Scroll => "SCROLL",
            Keyword::Hold => "HOLD",
            Keyword::Without => "WITHOUT",
            Keyword::Read => "READ",
            Keyword::Only => "ONLY",
            Keyword::Oids => "OIDS",
            Keyword::Rowid => "ROWID",
            Keyword::Open => "OPEN",
            Keyword::Fetch => "FETCH",
            Keyword::Close => "CLOSE",
            Keyword::Prior => "PRIOR",
            Keyword::First => "FIRST",
            Keyword::Last => "LAST",
            Keyword::Absolute => "ABSOLUTE",
            Keyword::Relative => "RELATIVE",
            Keyword::Serializable => "SERIALIZABLE",
            Keyword::Isolation => "ISOLATION",
            Keyword::Level => "LEVEL",
            Keyword::Write => "WRITE",
            // MySQL table option keywords
            Keyword::KeyBlockSize => "KEY_BLOCK_SIZE",
            Keyword::Connection => "CONNECTION",
            Keyword::InsertMethod => "INSERT_METHOD",
            Keyword::RowFormat => "ROW_FORMAT",
            Keyword::DelayKeyWrite => "DELAY_KEY_WRITE",
            Keyword::TableChecksum => "TABLE_CHECKSUM",
            Keyword::Checksum => "CHECKSUM",
            Keyword::StatsSamplePages => "STATS_SAMPLE_PAGES",
            Keyword::Password => "PASSWORD",
            Keyword::AvgRowLength => "AVG_ROW_LENGTH",
            Keyword::MinRows => "MIN_ROWS",
            Keyword::MaxRows => "MAX_ROWS",
            Keyword::SecondaryEngine => "SECONDARY_ENGINE",
            // MySQL table option values
            Keyword::Dynamic => "DYNAMIC",
            Keyword::Fixed => "FIXED",
            Keyword::Compressed => "COMPRESSED",
            Keyword::Redundant => "REDUNDANT",
            Keyword::Compact => "COMPACT",
            // Full-text search keywords
            Keyword::Fulltext => "FULLTEXT",
            Keyword::Match => "MATCH",
            Keyword::Against => "AGAINST",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Expansion => "EXPANSION",
            Keyword::Mode => "MODE",
            Keyword::Query => "QUERY",
            Keyword::Plan => "PLAN",
            // Spatial index keywords
            Keyword::Spatial => "SPATIAL",
            // Vector index keywords (IVFFlat, HNSW)
            Keyword::Ivfflat => "IVFFLAT",
            Keyword::Hnsw => "HNSW",
            Keyword::Lists => "LISTS",
            Keyword::Probes => "PROBES",
            Keyword::EfConstruction => "EF_CONSTRUCTION",
            Keyword::EfSearch => "EF_SEARCH",
            Keyword::M => "M",
            // Database introspection keywords
            Keyword::Show => "SHOW",
            Keyword::Describe => "DESCRIBE",
            Keyword::Databases => "DATABASES",
            Keyword::Tables => "TABLES",
            Keyword::Columns => "COLUMNS",
            Keyword::Fields => "FIELDS",
            Keyword::Indexes => "INDEXES",
            Keyword::Keys => "KEYS",
            // Auto-increment keywords
            Keyword::AutoIncrement => "AUTO_INCREMENT",
            // Temporary object keywords
            Keyword::Temp => "TEMP",
            Keyword::Temporary => "TEMPORARY",
            // VibeSQL storage format keywords
            Keyword::Storage => "STORAGE",
            Keyword::Columnar => "COLUMNAR",
            // Transaction durability hint keywords
            Keyword::Durability => "DURABILITY",
            Keyword::Lazy => "LAZY",
            Keyword::Durable => "DURABLE",
            Keyword::Volatile => "VOLATILE",
            // SQLite compatibility keywords
            Keyword::Pragma => "PRAGMA",
            // SQLite conflict resolution keywords
            Keyword::Abort => "ABORT",
            Keyword::Fail => "FAIL",
            Keyword::Conflict => "CONFLICT",
            Keyword::Nothing => "NOTHING",
        };
        write!(f, "{}", keyword_str)
    }
}
