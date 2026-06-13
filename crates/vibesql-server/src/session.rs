use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use vibesql_executor::{
    CursorExecutor, CursorStore, FetchResult as CursorFetchResult, PreparedStatement,
    PreparedStatementCache, PreparedStatementCacheStats,
};
use vibesql_types::SqlValue;

use crate::{
    registry::SharedDatabase,
    replication::{ReplicationHandle, SqlError, SQLSTATE_INVALID_PARAMETER},
    transaction::SessionTransactionManager,
};

/// Read-consistency mode of a replicated session (#5383), selected with
/// `SET vibesql_read_consistency = '...'`. Maps onto the `MvccRaftNode`
/// read paths (Raft Phase B2, #5200).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Local, stale-allowed (the default): read whatever this node has
    /// applied, no leadership check, no network round.
    Local,
    /// Linearizable: quorum-confirmed leadership (ReadIndex). On a
    /// follower this fails with SQLSTATE 25006 and a leader hint.
    Linearizable,
    /// Bounded staleness: served locally iff this node can prove its
    /// state is no staler than `vibesql_max_staleness_ms`. A bound of 0
    /// delegates to linearizable (the "staleness 0 redirects to the
    /// leader" contract).
    BoundedStaleness,
    /// Read-your-writes: waits (up to `vibesql_ryw_wait_ms`) until this
    /// node has applied the session's last write token.
    ReadYourWrites,
}

/// Default wait bound for read-your-writes reads (overridable with
/// `SET vibesql_ryw_wait_ms = N`).
const DEFAULT_RYW_WAIT_MS: u64 = 5_000;

/// Per-session replicated-mode state: the shared consensus handle plus
/// the session's read-consistency settings and write token.
struct ReplicatedSessionState {
    handle: Arc<ReplicationHandle>,
    read_consistency: ReadConsistency,
    /// Staleness bound for [`ReadConsistency::BoundedStaleness`], in ms.
    max_staleness_ms: u64,
    /// Wait bound for [`ReadConsistency::ReadYourWrites`], in ms.
    ryw_wait_ms: u64,
    /// Dense log index of this session's last replicated write (`0` =
    /// none) — the read-your-writes token `execute_replicated` returned.
    last_write_token: vibesql_consensus::LogIndex,
    /// Open interactive transaction (#5391): `Some` between `BEGIN` and
    /// `COMMIT`/`ROLLBACK`. Holds the buffered write statements (in
    /// execution order) that are proposed as **one** `TxnEntry` at
    /// `COMMIT` — the one-entry-per-committed-transaction design.
    ///
    /// During the open transaction the buffer is *not yet* proposed, so:
    /// - write statements return an optimistic ack (`rows_affected = 0`); the authoritative row
    ///   counts are knowable only after the atomic apply at `COMMIT`;
    /// - reads are refused once the buffer is non-empty, because the buffered writes are not
    ///   applied anywhere yet and a local read would silently miss the session's own writes (full
    ///   mid-txn read fidelity is the scoped follow-on #5401).
    open_txn: Option<Vec<String>>,
}

/// Session state for a database connection
pub struct Session {
    /// Database name
    #[allow(dead_code)]
    pub database: String,
    /// User name
    #[allow(dead_code)]
    pub user: String,
    /// Shared database instance (shared across all connections to the same database)
    db: SharedDatabase,
    /// Prepared statement cache for reduced parsing overhead
    stmt_cache: Arc<PreparedStatementCache>,
    /// Named prepared statements (from PREPARE SQL syntax)
    named_statements: HashMap<String, Arc<PreparedStatement>>,
    /// Cursor storage for DECLARE/OPEN/FETCH/CLOSE operations
    cursors: CursorStore,
    /// Session-level transaction manager for READ COMMITTED isolation
    /// Tracks uncommitted changes that should only be visible to this session
    txn_manager: SessionTransactionManager,
    /// Replicated-mode state (#5383): `Some` when this session routes
    /// statements through consensus instead of the local executor.
    replication: Option<ReplicatedSessionState>,
}

/// Simplified execution result for wire protocol
#[derive(Debug)]
pub enum ExecutionResult {
    Select {
        rows: Vec<Row>,
        columns: Vec<Column>,
    },
    Insert {
        rows_affected: usize,
    },
    Update {
        rows_affected: usize,
    },
    Delete {
        rows_affected: usize,
    },
    CreateTable,
    CreateIndex,
    CreateView,
    DropTable,
    DropIndex,
    DropView,
    Analyze {
        tables_analyzed: usize,
    },
    /// Statement prepared successfully
    Prepare {
        statement_name: String,
    },
    /// Prepared statement deallocated
    Deallocate {
        statement_name: String,
    },
    /// Cursor declared successfully
    DeclareCursor {
        cursor_name: String,
    },
    /// Cursor opened successfully
    OpenCursor {
        cursor_name: String,
    },
    /// Cursor fetched rows
    Fetch {
        rows: Vec<Row>,
        columns: Vec<Column>,
    },
    /// Cursor closed successfully
    CloseCursor {
        cursor_name: String,
    },
    /// Transaction started
    Begin,
    /// Transaction committed
    Commit,
    /// Transaction rolled back
    Rollback,
    /// EXPLAIN query plan output
    Explain {
        plan_text: String,
        /// Column headers for the result
        columns: Vec<Column>,
    },
    Other {
        message: String,
    },
}

impl ExecutionResult {
    /// Get the statement type as a string for metrics
    pub fn statement_type(&self) -> &str {
        match self {
            ExecutionResult::Select { .. } => "SELECT",
            ExecutionResult::Insert { .. } => "INSERT",
            ExecutionResult::Update { .. } => "UPDATE",
            ExecutionResult::Delete { .. } => "DELETE",
            ExecutionResult::CreateTable => "CREATE_TABLE",
            ExecutionResult::CreateIndex => "CREATE_INDEX",
            ExecutionResult::CreateView => "CREATE_VIEW",
            ExecutionResult::DropTable => "DROP_TABLE",
            ExecutionResult::DropIndex => "DROP_INDEX",
            ExecutionResult::DropView => "DROP_VIEW",
            ExecutionResult::Analyze { .. } => "ANALYZE",
            ExecutionResult::Prepare { .. } => "PREPARE",
            ExecutionResult::Deallocate { .. } => "DEALLOCATE",
            ExecutionResult::DeclareCursor { .. } => "DECLARE_CURSOR",
            ExecutionResult::OpenCursor { .. } => "OPEN_CURSOR",
            ExecutionResult::Fetch { .. } => "FETCH",
            ExecutionResult::CloseCursor { .. } => "CLOSE_CURSOR",
            ExecutionResult::Begin => "BEGIN",
            ExecutionResult::Commit => "COMMIT",
            ExecutionResult::Rollback => "ROLLBACK",
            ExecutionResult::Explain { .. } => "EXPLAIN",
            ExecutionResult::Other { .. } => "OTHER",
        }
    }

    /// Get the number of rows affected
    pub fn rows_affected(&self) -> u64 {
        match self {
            ExecutionResult::Select { rows, .. } => rows.len() as u64,
            ExecutionResult::Insert { rows_affected } => *rows_affected as u64,
            ExecutionResult::Update { rows_affected } => *rows_affected as u64,
            ExecutionResult::Delete { rows_affected } => *rows_affected as u64,
            ExecutionResult::Fetch { rows, .. } => rows.len() as u64,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<vibesql_types::SqlValue>,
}

impl Session {
    /// Create a new session with a shared database
    ///
    /// This constructor is used for wire protocol connections where multiple
    /// connections to the same database should share data.
    pub fn new(database: String, user: String, db: SharedDatabase) -> Self {
        Self {
            database,
            user,
            db,
            stmt_cache: Arc::new(PreparedStatementCache::default_cache()),
            named_statements: HashMap::new(),
            cursors: CursorStore::new(),
            txn_manager: SessionTransactionManager::new(),
            replication: None,
        }
    }

    /// Create a session that routes statements through consensus (#5383):
    /// writes go through `MvccRaftNode::execute_replicated` (leader-only,
    /// `NotLeader` surfaces as SQLSTATE 25006 with a leader hint), reads
    /// run against the replicated state machine per the session's
    /// `vibesql_read_consistency` setting (local by default).
    ///
    /// The `db` handle is retained for API compatibility but replicated
    /// sessions do not execute against it — the replicated database lives
    /// inside the consensus state machine.
    pub fn new_replicated(
        database: String,
        user: String,
        db: SharedDatabase,
        handle: Arc<ReplicationHandle>,
    ) -> Self {
        let mut session = Self::new(database, user, db);
        session.replication = Some(ReplicatedSessionState {
            handle,
            read_consistency: ReadConsistency::Local,
            max_staleness_ms: 0,
            ryw_wait_ms: DEFAULT_RYW_WAIT_MS,
            last_write_token: 0,
            open_txn: None,
        });
        session
    }

    /// Whether this session routes statements through consensus.
    pub fn is_replicated(&self) -> bool {
        self.replication.is_some()
    }

    /// Create a new standalone session with its own isolated database
    ///
    /// This constructor is used for HTTP API requests and other stateless
    /// operations where data isolation between requests is acceptable.
    pub fn new_standalone(database: String, user: String) -> Self {
        let db = SharedDatabase::new(vibesql_storage::Database::new());
        Self::new(database, user, db)
    }

    /// Check if this session is currently in a transaction
    pub fn in_transaction(&self) -> bool {
        self.txn_manager.in_transaction()
    }

    /// Get the shared database handle
    pub fn shared_database(&self) -> &SharedDatabase {
        &self.db
    }

    /// Create a new session with a shared statement cache
    #[allow(dead_code)]
    pub fn with_cache(
        database: String,
        user: String,
        db: SharedDatabase,
        cache: Arc<PreparedStatementCache>,
    ) -> Self {
        Self {
            database,
            user,
            db,
            stmt_cache: cache,
            named_statements: HashMap::new(),
            cursors: CursorStore::new(),
            txn_manager: SessionTransactionManager::new(),
            replication: None,
        }
    }

    /// Prepare a SQL statement for repeated execution
    ///
    /// The statement is parsed once and cached. Subsequent calls with the same
    /// SQL will return the cached prepared statement without re-parsing.
    ///
    /// # Example
    /// ```text
    /// let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
    /// let result = session.execute_prepared(&stmt, &[SqlValue::Integer(1)]).await?;
    /// ```
    #[allow(dead_code)]
    pub fn prepare(&self, sql: &str) -> Result<Arc<PreparedStatement>> {
        self.stmt_cache.get_or_prepare(sql).map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Execute a prepared statement with parameters
    ///
    /// Binds the provided parameters to the prepared statement and executes it.
    /// This avoids the parsing overhead of the original SQL.
    #[allow(dead_code)]
    pub async fn execute_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<ExecutionResult> {
        // Bind parameters to get executable statement
        let bound_stmt = stmt.bind(params).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Execute the bound statement
        self.execute_statement(&bound_stmt).await
    }

    /// Execute a SQL query with auto-caching
    ///
    /// This method automatically caches parsed statements for performance.
    /// For repeated queries, use `prepare()` + `execute_prepared()` for best performance.
    pub async fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
        // Try to get from cache or prepare
        let prepared = self.stmt_cache.get_or_prepare(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Replicated sessions route by statement kind, with the original
        // SQL text in hand (the consensus propose path replicates SQL).
        if self.replication.is_some() {
            return self.execute_replicated(sql, prepared.statement()).await;
        }

        // For non-parameterized queries, execute directly from cached AST
        self.execute_statement(prepared.statement()).await
    }

    /// Execute a SQL query with parameters (convenience method)
    ///
    /// Combines prepare + execute_prepared in one call.
    #[allow(dead_code)]
    pub async fn execute_with_params(
        &mut self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<ExecutionResult> {
        let prepared = self.prepare(sql)?;
        self.execute_prepared(&prepared, params).await
    }

    /// Execute one statement of a **replicated** session (#5383): writes
    /// are proposed through consensus (`MvccRaftNode::execute_replicated`,
    /// leader-only, freeze-at-propose), reads run against the replicated
    /// state machine per the session's read-consistency setting, and
    /// features without a sound replicated semantic yet fail with
    /// SQLSTATE `0A000` and a follow-on pointer instead of silently
    /// executing against the (empty) local database.
    async fn execute_replicated(
        &mut self,
        sql: &str,
        statement: &vibesql_ast::Statement,
    ) -> Result<ExecutionResult> {
        use vibesql_ast::Statement;

        {
            let state = self.replication.as_ref().expect("execute_replicated without state");
            // A halted node serves nothing further (fatal apply, see
            // ConsensusError::FatalApply): every statement gets the
            // retained reason instead of a stale read or a hung write.
            if let Some(reason) = state.handle.fatal_reason() {
                return Err(state.handle.halted_error(reason).into());
            }
        }

        // Interactive transaction control (#5391): BEGIN opens a write
        // buffer, COMMIT proposes it as one TxnEntry, ROLLBACK discards
        // it. Handled before the per-statement dispatch so an open
        // transaction can intercept buffered writes and gated reads.
        match statement {
            Statement::BeginTransaction(_) => return self.replicated_begin(),
            Statement::Commit(_) => return self.replicated_commit().await,
            Statement::Rollback(_) => return self.replicated_rollback(),
            // Savepoints are out of scope (#5391); standalone sessions do
            // not support them either. Refuse clearly.
            Statement::Savepoint(_)
            | Statement::RollbackToSavepoint(_)
            | Statement::ReleaseSavepoint(_) => {
                return Err(SqlError::not_supported(
                    "savepoints in replicated transactions",
                    "#5401",
                )
                .into())
            }
            _ => {}
        }

        // Inside an open transaction, writes are buffered (proposed as
        // one entry at COMMIT) and reads are gated once the buffer is
        // non-empty. Re-borrow `state` after this returns where needed.
        if self.replication.as_ref().is_some_and(|s| s.open_txn.is_some()) {
            return self.execute_in_open_txn(sql, statement);
        }

        let state = self.replication.as_ref().expect("execute_replicated without state");

        match statement {
            // Reads: dispatch on the session's read-consistency mode.
            Statement::Select(_) => {
                let handle = Arc::clone(&state.handle);
                let rows = match state.read_consistency {
                    ReadConsistency::Local => handle.query_local(sql)?,
                    ReadConsistency::Linearizable => handle.query_linearizable(sql).await?,
                    ReadConsistency::BoundedStaleness => {
                        handle
                            .query_bounded_staleness(
                                Duration::from_millis(state.max_staleness_ms),
                                sql,
                            )
                            .await?
                    }
                    ReadConsistency::ReadYourWrites => {
                        handle
                            .query_at_least(
                                state.last_write_token,
                                sql,
                                Duration::from_millis(state.ryw_wait_ms),
                            )
                            .await?
                    }
                };
                // Placeholder column names, matching the standalone
                // SELECT path (which also does not resolve real names
                // yet — see the TODO there).
                let columns = rows
                    .first()
                    .map(|r| (0..r.len()).map(|i| Column { name: format!("col{i}") }).collect())
                    .unwrap_or_default();
                let rows = rows.into_iter().map(|values| Row { values }).collect();
                Ok(ExecutionResult::Select { rows, columns })
            }

            // Writes: one autocommit statement = one replicated entry.
            Statement::Insert(stmt) => {
                let affected = self.replicated_write(sql).await?;
                self.stmt_cache.invalidate_table(&stmt.table_name);
                Ok(ExecutionResult::Insert { rows_affected: affected })
            }
            Statement::Update(stmt) => {
                let affected = self.replicated_write(sql).await?;
                self.stmt_cache.invalidate_table(&stmt.table_name);
                Ok(ExecutionResult::Update { rows_affected: affected })
            }
            Statement::Delete(stmt) => {
                let affected = self.replicated_write(sql).await?;
                self.stmt_cache.invalidate_table(&stmt.table_name);
                Ok(ExecutionResult::Delete { rows_affected: affected })
            }

            // DDL: replicated like any write (schema changes must apply
            // on every voter in log order).
            Statement::CreateTable(_) => {
                self.replicated_write(sql).await?;
                Ok(ExecutionResult::CreateTable)
            }
            Statement::CreateIndex(_) => {
                self.replicated_write(sql).await?;
                Ok(ExecutionResult::CreateIndex)
            }
            Statement::CreateView(_) => {
                self.replicated_write(sql).await?;
                Ok(ExecutionResult::CreateView)
            }
            Statement::DropTable(stmt) => {
                self.replicated_write(sql).await?;
                self.stmt_cache.invalidate_table(&stmt.table_name);
                Ok(ExecutionResult::DropTable)
            }
            Statement::DropIndex(_) => {
                self.replicated_write(sql).await?;
                Ok(ExecutionResult::DropIndex)
            }
            Statement::DropView(_) => {
                self.replicated_write(sql).await?;
                Ok(ExecutionResult::DropView)
            }

            // Session-local settings for the replicated read modes.
            Statement::SetVariable(stmt) => self.execute_replicated_set(stmt),

            // PRAGMA stays the no-op it is in standalone mode. SESSION-CONTEXT
            // AUDIT (#5392): a write-semantics PRAGMA (`foreign_keys`,
            // `case_sensitive_like`, `defer_foreign_keys`, …) must not become
            // a meaningful-but-ignored setting here — the replicated executor
            // ignores session PRAGMAs (they are never proposed, and the state
            // machine rejects PRAGMA inside an entry). If a PRAGMA must affect
            // replicated writes, capture it into the `TxnEntry`; do not honor
            // it only in this session.
            Statement::Pragma(_) => Ok(ExecutionResult::Other { message: "PRAGMA".to_string() }),

            // Transaction control (BEGIN/COMMIT/ROLLBACK/SAVEPOINT) is
            // intercepted above, before this autocommit dispatch.

            // Named prepared-statement syntax (#5393): PREPARE captures the
            // SQL *text* (not a bound AST), EXECUTE substitutes its literal
            // parameters back into that text and re-routes through this same
            // replicated dispatch — so a PREPARE'd INSERT proposes through
            // consensus with freeze-at-propose exactly like the simple path,
            // and a PREPARE'd SELECT honors the session read-consistency mode.
            // DEALLOCATE drops the named statement. None of these execute
            // against the local (empty) database.
            Statement::Prepare(stmt) => self.replicated_prepare(stmt),
            Statement::Execute(stmt) => self.replicated_execute(stmt).await,
            Statement::Deallocate(stmt) => self.execute_deallocate(stmt),

            // Cursors, EXPLAIN, ANALYZE, and VACUUM would all run against the
            // local (empty) database. Refuse clearly rather than mislead.
            Statement::DeclareCursor(_)
            | Statement::OpenCursor(_)
            | Statement::Fetch(_)
            | Statement::CloseCursor(_) => Err(SqlError::not_supported("cursors", "#5393").into()),
            Statement::Explain(_) => Err(SqlError::not_supported("EXPLAIN", "#5393").into()),
            Statement::Analyze(_) => Err(SqlError::not_supported("ANALYZE", "#5393").into()),
            Statement::Vacuum(_) => Err(SqlError::not_supported("VACUUM", "#5393").into()),

            _ => Err(SqlError::not_supported("this statement", "#5393").into()),
        }
    }

    /// Propose one autocommit write through consensus and update the
    /// session's read-your-writes token. Returns the rows affected.
    async fn replicated_write(&mut self, sql: &str) -> Result<usize> {
        let state = self.replication.as_ref().expect("replicated_write without state");
        let handle = Arc::clone(&state.handle);
        let (index, outcome) = handle.execute_write(sql).await?;
        // The entry consumed its index whether it applied or rolled
        // back; either way this session's reads may rely on it.
        let state = self.replication.as_mut().expect("replicated_write without state");
        state.last_write_token = index;
        match outcome {
            vibesql_consensus::ApplyOutcome::Applied { rows_affected } => Ok(rows_affected),
            // A deterministic statement failure, rejected identically on
            // every replica: surface the original executor error.
            vibesql_consensus::ApplyOutcome::Rejected { reason } => {
                Err(anyhow::anyhow!("{}", reason))
            }
            // Unreachable from a fresh propose (idempotence replays only
            // overlap snapshot-install with log replay), but harmless.
            vibesql_consensus::ApplyOutcome::AlreadyApplied => Ok(0),
        }
    }

    /// Open an interactive transaction in a replicated session (#5391):
    /// installs an empty write buffer that subsequent DML/DDL append to
    /// and that `COMMIT` proposes as one `TxnEntry`. `BEGIN` inside an
    /// open transaction errors, matching the standalone behavior.
    fn replicated_begin(&mut self) -> Result<ExecutionResult> {
        let state = self.replication.as_mut().expect("replicated_begin without state");
        if state.open_txn.is_some() {
            return Err(anyhow::anyhow!("a transaction is already in progress"));
        }
        state.open_txn = Some(Vec::new());
        Ok(ExecutionResult::Begin)
    }

    /// Commit an open replicated transaction (#5391): propose the whole
    /// buffered write batch as **one** `TxnEntry` via
    /// `execute_replicated_txn` (freeze-at-propose runs once, on the
    /// leader, at COMMIT) and map the outcome:
    ///
    /// - `Applied` → the transaction committed atomically (`COMMIT`);
    /// - `Rejected` → a deterministic statement failure rolled the whole batch back identically on
    ///   every replica; the buffer is discarded and the executor error surfaced (no partial state);
    /// - `NotLeader` / `FatalApply` → surfaced as the usual SQLSTATE; the buffer is discarded so a
    ///   retry against the leader starts clean.
    ///
    /// An empty transaction (`BEGIN; COMMIT;` with no writes) consumes no
    /// log index — there is nothing to replicate.
    async fn replicated_commit(&mut self) -> Result<ExecutionResult> {
        let buffered = {
            let state = self.replication.as_mut().expect("replicated_commit without state");
            match state.open_txn.take() {
                Some(buf) => buf,
                None => return Err(anyhow::anyhow!("no transaction is in progress")),
            }
        };

        // Empty transaction: nothing to propose.
        if buffered.is_empty() {
            return Ok(ExecutionResult::Commit);
        }

        let state = self.replication.as_ref().expect("replicated_commit without state");
        let handle = Arc::clone(&state.handle);
        let statements: Vec<&str> = buffered.iter().map(String::as_str).collect();

        // Propose the batch as one entry. `NotLeader`/`FatalApply`/etc.
        // surface as a structured SqlError; the buffer is already cleared
        // above so the session is back in autocommit on any error.
        let (index, outcome) = handle.execute_txn(&statements).await?;

        let state = self.replication.as_mut().expect("replicated_commit without state");
        state.last_write_token = index;

        match outcome {
            vibesql_consensus::ApplyOutcome::Applied { .. } => Ok(ExecutionResult::Commit),
            // Deterministic failure inside the batch: rolled back
            // identically on every replica, no partial state committed.
            vibesql_consensus::ApplyOutcome::Rejected { reason } => {
                Err(anyhow::anyhow!("{}", reason))
            }
            // Not reachable from a fresh propose (see `replicated_write`).
            vibesql_consensus::ApplyOutcome::AlreadyApplied => Ok(ExecutionResult::Commit),
        }
    }

    /// PREPARE in a replicated session (#5393): store the statement's SQL
    /// *text* under its name. EXECUTE substitutes literal parameters back
    /// into that text and routes it through the normal replicated dispatch,
    /// so a PREPARE'd write proposes through consensus (freeze-at-propose)
    /// exactly like the simple-query path. Shares the `named_statements`
    /// registry with standalone mode; only the EXECUTE path differs.
    fn replicated_prepare(
        &mut self,
        prepare_stmt: &vibesql_ast::PrepareStmt,
    ) -> Result<ExecutionResult> {
        // Reuse the standalone PREPARE machinery: it parses + caches the
        // SQL and retains the original text (PreparedStatement::sql()),
        // which is exactly what the consensus propose path needs.
        self.execute_prepare(prepare_stmt)
    }

    /// EXECUTE in a replicated session (#5393): bind the statement's literal
    /// parameters into the prepared SQL *text* and re-route the result
    /// through `execute` (→ `execute_replicated`). Writes therefore propose
    /// through consensus and reads honor the session read-consistency mode,
    /// identically to issuing the substituted SQL directly.
    async fn replicated_execute(
        &mut self,
        execute_stmt: &vibesql_ast::ExecuteStmt,
    ) -> Result<ExecutionResult> {
        let name = execute_stmt.name.clone();
        let prepared = self
            .named_statements
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("Prepared statement '{}' not found", name))?
            .clone();

        // Evaluate the EXECUTE arguments to literal values, then render them
        // back into the prepared statement's SQL text by replacing the `?`
        // / `$N` placeholders in order — the same text-substitution contract
        // the extended wire protocol's Bind path already uses to route
        // parameterized writes through consensus.
        let params: Vec<SqlValue> =
            execute_stmt.params.iter().map(evaluate_expression).collect::<Result<Vec<_>>>()?;

        if params.len() != prepared.param_count() {
            return Err(anyhow::anyhow!(
                "EXECUTE for '{}' expected {} parameter(s), got {}",
                name,
                prepared.param_count(),
                params.len()
            ));
        }

        let sql = substitute_named_params(prepared.sql(), &params);
        // Box the recursive dispatch: `execute` → `execute_replicated` →
        // here → `execute` would otherwise be an infinitely sized future.
        Box::pin(self.execute(&sql)).await
    }

    /// Roll back an open replicated transaction (#5391): discard the
    /// buffered writes without proposing anything — the batch never
    /// reached consensus, so no log index is consumed.
    fn replicated_rollback(&mut self) -> Result<ExecutionResult> {
        let state = self.replication.as_mut().expect("replicated_rollback without state");
        if state.open_txn.take().is_none() {
            return Err(anyhow::anyhow!("no transaction is in progress"));
        }
        Ok(ExecutionResult::Rollback)
    }

    /// Execute one statement inside an open replicated transaction
    /// (#5391). Writes are appended to the buffer and acknowledged
    /// optimistically; reads are gated; unsupported features keep their
    /// `0A000` refusal.
    ///
    /// Mid-transaction result semantics (documented contract):
    /// - **Writes** return their command tag with `rows_affected = 0`. The buffer is not proposed
    ///   until `COMMIT`, so the authoritative row count is not yet known; clients learn the outcome
    ///   (success or a deterministic rejection) at `COMMIT`.
    /// - **Reads** are refused with `0A000` once the buffer holds any writes: those writes are
    ///   applied nowhere yet, so a local read would silently miss the session's own writes. A read
    ///   before any buffered write (it observes committed state coherently) is allowed. Full
    ///   read-your-own-writes inside the transaction is the scoped follow-on #5401.
    fn execute_in_open_txn(
        &mut self,
        sql: &str,
        statement: &vibesql_ast::Statement,
    ) -> Result<ExecutionResult> {
        use vibesql_ast::Statement;

        // Whether the buffer already holds writes (read gating).
        let buffer_has_writes = self
            .replication
            .as_ref()
            .and_then(|s| s.open_txn.as_ref())
            .is_some_and(|buf| !buf.is_empty());

        // Append `sql` to the open transaction buffer.
        let buffer = |this: &mut Self| {
            this.replication
                .as_mut()
                .and_then(|s| s.open_txn.as_mut())
                .expect("execute_in_open_txn without open transaction")
                .push(sql.to_string());
        };

        match statement {
            Statement::Select(_) => {
                if buffer_has_writes {
                    Err(SqlError::not_supported(
                        "reads of a replicated transaction's own buffered writes",
                        "#5401",
                    )
                    .into())
                } else {
                    // No buffered writes yet: serve against committed
                    // state through the normal local read path.
                    let handle = Arc::clone(
                        &self
                            .replication
                            .as_ref()
                            .expect("execute_in_open_txn without state")
                            .handle,
                    );
                    let rows = handle.query_local(sql)?;
                    let columns = rows
                        .first()
                        .map(|r| (0..r.len()).map(|i| Column { name: format!("col{i}") }).collect())
                        .unwrap_or_default();
                    let rows = rows.into_iter().map(|values| Row { values }).collect();
                    Ok(ExecutionResult::Select { rows, columns })
                }
            }

            // Writes: buffer for the COMMIT batch, ack optimistically.
            Statement::Insert(stmt) => {
                buffer(self);
                self.stmt_cache.invalidate_table(&stmt.table_name);
                Ok(ExecutionResult::Insert { rows_affected: 0 })
            }
            Statement::Update(stmt) => {
                buffer(self);
                self.stmt_cache.invalidate_table(&stmt.table_name);
                Ok(ExecutionResult::Update { rows_affected: 0 })
            }
            Statement::Delete(stmt) => {
                buffer(self);
                self.stmt_cache.invalidate_table(&stmt.table_name);
                Ok(ExecutionResult::Delete { rows_affected: 0 })
            }
            Statement::CreateTable(_) => {
                buffer(self);
                Ok(ExecutionResult::CreateTable)
            }
            Statement::CreateIndex(_) => {
                buffer(self);
                Ok(ExecutionResult::CreateIndex)
            }
            Statement::CreateView(_) => {
                buffer(self);
                Ok(ExecutionResult::CreateView)
            }
            Statement::DropTable(stmt) => {
                buffer(self);
                self.stmt_cache.invalidate_table(&stmt.table_name);
                Ok(ExecutionResult::DropTable)
            }
            Statement::DropIndex(_) => {
                buffer(self);
                Ok(ExecutionResult::DropIndex)
            }
            Statement::DropView(_) => {
                buffer(self);
                Ok(ExecutionResult::DropView)
            }

            // SET adjusts session-local read settings; harmless inside a
            // transaction and not part of the replicated batch.
            Statement::SetVariable(stmt) => self.execute_replicated_set(stmt),

            // PRAGMA stays the no-op it is in standalone mode.
            Statement::Pragma(_) => Ok(ExecutionResult::Other { message: "PRAGMA".to_string() }),

            // PREPARE/DEALLOCATE are session-local and harmless inside a
            // transaction. EXECUTE of a prepared *write* inside a
            // transaction is refused: `execute_in_open_txn` is synchronous
            // (it cannot re-enter the async replicated dispatch to buffer the
            // substituted SQL), and silently dropping it would be unsound.
            // Use the substituted SQL directly inside the transaction, or
            // run the prepared statement outside one (full prepared-EXECUTE
            // inside replicated transactions is the scoped follow-on #5401).
            Statement::Prepare(stmt) => self.replicated_prepare(stmt),
            Statement::Deallocate(stmt) => self.execute_deallocate(stmt),
            Statement::Execute(_) => Err(SqlError::not_supported(
                "EXECUTE of a prepared statement inside a replicated transaction",
                "#5401",
            )
            .into()),
            Statement::DeclareCursor(_)
            | Statement::OpenCursor(_)
            | Statement::Fetch(_)
            | Statement::CloseCursor(_) => Err(SqlError::not_supported("cursors", "#5393").into()),
            Statement::Explain(_) => Err(SqlError::not_supported("EXPLAIN", "#5393").into()),
            Statement::Analyze(_) => Err(SqlError::not_supported("ANALYZE", "#5393").into()),
            Statement::Vacuum(_) => Err(SqlError::not_supported("VACUUM", "#5393").into()),

            _ => Err(SqlError::not_supported("this statement", "#5393").into()),
        }
    }

    /// Handle `SET vibesql_*` in a replicated session. Unknown variables
    /// keep the standalone behavior (accepted as a no-op).
    fn execute_replicated_set(
        &mut self,
        stmt: &vibesql_ast::SetVariableStmt,
    ) -> Result<ExecutionResult> {
        let state = self.replication.as_mut().expect("replicated SET without state");
        let name = stmt.variable.to_ascii_lowercase();
        match name.as_str() {
            "vibesql_read_consistency" => {
                let value = set_value_string(&stmt.value)?;
                state.read_consistency = match value.to_ascii_lowercase().as_str() {
                    "local" => ReadConsistency::Local,
                    "linearizable" => ReadConsistency::Linearizable,
                    "bounded_staleness" => ReadConsistency::BoundedStaleness,
                    "read_your_writes" => ReadConsistency::ReadYourWrites,
                    other => {
                        return Err(SqlError::new(
                            SQLSTATE_INVALID_PARAMETER,
                            format!(
                                "invalid value for vibesql_read_consistency: '{other}' (expected \
                                 'local', 'linearizable', 'bounded_staleness', or \
                                 'read_your_writes')"
                            ),
                        )
                        .into())
                    }
                };
                Ok(ExecutionResult::Other { message: "SET".to_string() })
            }
            "vibesql_max_staleness_ms" => {
                state.max_staleness_ms = set_value_ms(&stmt.value, "vibesql_max_staleness_ms")?;
                Ok(ExecutionResult::Other { message: "SET".to_string() })
            }
            "vibesql_ryw_wait_ms" => {
                state.ryw_wait_ms = set_value_ms(&stmt.value, "vibesql_ryw_wait_ms")?;
                Ok(ExecutionResult::Other { message: "SET".to_string() })
            }
            // Anything else: same lenient no-op as the standalone
            // catch-all (PG clients SET all sorts of things at startup).
            //
            // SESSION-CONTEXT AUDIT (#5392): this no-op is only sound for
            // settings that do not change *write* semantics. The replicated
            // executor runs against a bare state-machine `Database` at fixed
            // defaults (see `vibesql-consensus::state_machine` module docs),
            // so a write-semantics-affecting SET accepted-and-ignored here
            // (e.g. `SET foreign_keys`, `SET sql_mode`, a session var a
            // function reads) would make this session *believe* a setting is
            // in effect while replicated writes ignore it. Do NOT add such a
            // setting to this no-op: either reject it, or capture it into the
            // proposed `TxnEntry` and materialize it deterministically. The
            // settings handled above are read-routing only and never enter a
            // replicated entry.
            _ => Ok(ExecutionResult::Other { message: "SET".to_string() }),
        }
    }

    /// Execute a parsed statement
    ///
    /// This method uses read locks for SELECT queries to enable concurrent execution,
    /// and write locks for mutations (INSERT/UPDATE/DELETE/DDL) to ensure data integrity.
    async fn execute_statement(
        &mut self,
        statement: &vibesql_ast::Statement,
    ) -> Result<ExecutionResult> {
        use vibesql_ast::Statement;

        // Replicated sessions never reach this path through `execute`;
        // the only way in is the named prepared-statement machinery
        // (`execute_prepared`), which executes from a bound AST without
        // the SQL text the consensus propose path needs.
        if self.replication.is_some() {
            return Err(SqlError::not_supported(
                "prepared-statement execution against the replicated database",
                "#5393",
            )
            .into());
        }

        match statement {
            // Read-only operations use read lock for concurrent execution
            Statement::Select(select_stmt) => {
                let db = self.db.read().await;
                let executor = vibesql_executor::SelectExecutor::new(&db);
                let rows = executor.execute(select_stmt)?;

                // Convert to our result format
                let result_rows: Vec<Row> =
                    rows.iter().map(|r| Row { values: r.values.to_vec() }).collect();

                // TODO: Get actual column names from select statement
                let columns = if !rows.is_empty() {
                    (0..rows[0].values.len())
                        .map(|i| Column { name: format!("col{}", i) })
                        .collect()
                } else {
                    Vec::new()
                };

                Ok(ExecutionResult::Select { rows: result_rows, columns })
            }

            // Write operations require exclusive write lock
            Statement::Insert(insert_stmt) => {
                let mut db = self.db.write().await;
                let affected = vibesql_executor::InsertExecutor::execute(&mut db, insert_stmt)?;
                // Track changes count for changes() and total_changes() functions
                db.set_last_changes_count(affected);
                db.increment_total_changes_count(affected);
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&insert_stmt.table_name);
                Ok(ExecutionResult::Insert { rows_affected: affected })
            }

            Statement::Update(update_stmt) => {
                let mut db = self.db.write().await;
                let affected = vibesql_executor::UpdateExecutor::execute(update_stmt, &mut db)?;
                // Track changes count for changes() and total_changes() functions
                db.set_last_changes_count(affected);
                db.increment_total_changes_count(affected);
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&update_stmt.table_name);
                Ok(ExecutionResult::Update { rows_affected: affected })
            }

            Statement::Delete(delete_stmt) => {
                let mut db = self.db.write().await;
                let affected = vibesql_executor::DeleteExecutor::execute(delete_stmt, &mut db)?;
                // Track changes count for changes() and total_changes() functions
                db.set_last_changes_count(affected);
                db.increment_total_changes_count(affected);
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&delete_stmt.table_name);
                Ok(ExecutionResult::Delete { rows_affected: affected })
            }

            // DDL operations require exclusive write lock
            Statement::CreateTable(create_stmt) => {
                let mut db = self.db.write().await;
                vibesql_executor::CreateTableExecutor::execute(create_stmt, &mut db)?;
                Ok(ExecutionResult::CreateTable)
            }

            Statement::CreateIndex(index_stmt) => {
                let mut db = self.db.write().await;
                vibesql_executor::CreateIndexExecutor::execute(index_stmt, &mut db)?;
                Ok(ExecutionResult::CreateIndex)
            }

            Statement::CreateView(view_stmt) => {
                let mut db = self.db.write().await;
                vibesql_executor::advanced_objects::execute_create_view(view_stmt, &mut db)?;
                Ok(ExecutionResult::CreateView)
            }

            Statement::DropTable(drop_stmt) => {
                let mut db = self.db.write().await;
                vibesql_executor::DropTableExecutor::execute(drop_stmt, &mut db)?;
                // Invalidate cache for dropped table
                self.stmt_cache.invalidate_table(&drop_stmt.table_name);
                Ok(ExecutionResult::DropTable)
            }

            Statement::DropIndex(drop_stmt) => {
                let mut db = self.db.write().await;
                vibesql_executor::DropIndexExecutor::execute(drop_stmt, &mut db)?;
                Ok(ExecutionResult::DropIndex)
            }

            Statement::DropView(drop_stmt) => {
                let mut db = self.db.write().await;
                vibesql_executor::advanced_objects::execute_drop_view(drop_stmt, &mut db)?;
                Ok(ExecutionResult::DropView)
            }

            // ANALYZE updates statistics, requires write lock
            Statement::Analyze(analyze_stmt) => {
                let mut db = self.db.write().await;
                let message = vibesql_executor::AnalyzeExecutor::execute(analyze_stmt, &mut db)?;
                // Extract table count from message - the executor returns a message like
                // "ANALYZE completed - N table(s) analyzed"
                let tables_analyzed =
                    if analyze_stmt.table_name.is_some() { 1 } else { db.list_tables().len() };
                let _ = message; // Message is informational, we track count
                Ok(ExecutionResult::Analyze { tables_analyzed })
            }

            // VACUUM maps to MVCC garbage collection (no-op when MVCC is
            // disabled), requires write lock
            Statement::Vacuum(vacuum_stmt) => {
                if vacuum_stmt.into_file.is_some() {
                    return Err(anyhow::anyhow!("VACUUM INTO is not supported in VibeSQL"));
                }
                let mut db = self.db.write().await;
                let _reclaimed = db.vacuum_mvcc().map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ExecutionResult::Other { message: "VACUUM".to_string() })
            }

            // Session-local operations (no db lock needed)
            Statement::Prepare(prepare_stmt) => self.execute_prepare(prepare_stmt),

            Statement::Execute(execute_stmt) => self.execute_execute(execute_stmt).await,

            Statement::Deallocate(deallocate_stmt) => self.execute_deallocate(deallocate_stmt),

            Statement::DeclareCursor(declare_stmt) => self.execute_declare_cursor(declare_stmt),

            Statement::OpenCursor(open_stmt) => {
                // OpenCursor needs read access to execute the cursor's query
                let db = self.db.read().await;
                CursorExecutor::open(&mut self.cursors, open_stmt, &db)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ExecutionResult::OpenCursor { cursor_name: open_stmt.cursor_name.clone() })
            }

            Statement::Fetch(fetch_stmt) => self.execute_fetch(fetch_stmt),

            Statement::CloseCursor(close_stmt) => self.execute_close_cursor(close_stmt),

            // Transaction control
            Statement::BeginTransaction(_) => self.begin_transaction().await,

            Statement::Commit(_) => self.commit().await,

            Statement::Rollback(_) => self.rollback().await,

            Statement::RollbackToSavepoint(_savepoint_stmt) => {
                // TODO: Implement savepoints in SessionTransactionManager
                Ok(ExecutionResult::Other { message: "ROLLBACK TO SAVEPOINT".to_string() })
            }

            Statement::Savepoint(_savepoint_stmt) => {
                // TODO: Implement savepoints in SessionTransactionManager
                Ok(ExecutionResult::Other { message: "SAVEPOINT".to_string() })
            }

            Statement::ReleaseSavepoint(_release_stmt) => {
                // TODO: Implement savepoints in SessionTransactionManager
                Ok(ExecutionResult::Other { message: "RELEASE SAVEPOINT".to_string() })
            }

            // PRAGMA statements are SQLite-specific configuration commands
            // We parse them for compatibility but treat them as no-ops
            Statement::Pragma(_pragma_stmt) => {
                Ok(ExecutionResult::Other { message: "PRAGMA".to_string() })
            }

            // EXPLAIN and EXPLAIN QUERY PLAN
            Statement::Explain(explain_stmt) => {
                let db = self.db.read().await;
                let result = vibesql_executor::ExplainExecutor::execute(explain_stmt, &db)?;

                // Format based on the statement options
                let plan_text = if explain_stmt.query_plan {
                    // SQLite-style EXPLAIN QUERY PLAN output
                    result.to_sqlite_eqp()
                } else {
                    match explain_stmt.format {
                        vibesql_ast::ExplainFormat::Json => result.to_json(),
                        vibesql_ast::ExplainFormat::Text => result.to_text(),
                    }
                };

                Ok(ExecutionResult::Explain {
                    plan_text,
                    columns: vec![Column { name: "QUERY PLAN".to_string() }],
                })
            }

            _ => {
                // For now, return a generic success for other statements
                Ok(ExecutionResult::Other { message: "Command completed successfully".to_string() })
            }
        }
    }

    /// Execute PREPARE statement - registers a named prepared statement
    fn execute_prepare(
        &mut self,
        prepare_stmt: &vibesql_ast::PrepareStmt,
    ) -> Result<ExecutionResult> {
        use vibesql_ast::PreparedStatementBody;

        let name = prepare_stmt.name.clone();

        // Get the SQL string to prepare
        let sql = match &prepare_stmt.statement {
            PreparedStatementBody::SqlString(s) => s.clone(),
            PreparedStatementBody::ParsedStatement(_stmt) => {
                // For parsed statements, we need to re-serialize to SQL for caching
                // For now, we'll create a prepared statement directly from the AST
                // This is a simplified approach - a full implementation would
                // need to serialize the AST back to SQL or work with AST directly
                return Err(anyhow::anyhow!(
                    "PREPARE ... AS syntax not yet supported. Use PREPARE ... FROM 'sql_string' instead"
                ));
            }
        };

        // Create the prepared statement
        let prepared = self
            .stmt_cache
            .get_or_prepare(&sql)
            .map_err(|e| anyhow::anyhow!("Failed to prepare statement: {}", e))?;

        // Store in named statements registry
        self.named_statements.insert(name.clone(), prepared);

        Ok(ExecutionResult::Prepare { statement_name: name })
    }

    /// Execute EXECUTE statement - runs a named prepared statement
    fn execute_execute(
        &mut self,
        execute_stmt: &vibesql_ast::ExecuteStmt,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecutionResult>> + Send + '_>>
    {
        let name = execute_stmt.name.clone();
        let param_exprs = execute_stmt.params.clone();

        Box::pin(async move {
            // Look up the named statement
            let prepared = self
                .named_statements
                .get(&name)
                .ok_or_else(|| anyhow::anyhow!("Prepared statement '{}' not found", name))?
                .clone();

            // Evaluate parameter expressions to get values
            let params: Vec<SqlValue> =
                param_exprs.iter().map(evaluate_expression).collect::<Result<Vec<_>>>()?;

            // Execute the prepared statement with parameters
            self.execute_prepared(&prepared, &params).await
        })
    }

    /// Execute DEALLOCATE statement - removes a named prepared statement
    fn execute_deallocate(
        &mut self,
        deallocate_stmt: &vibesql_ast::DeallocateStmt,
    ) -> Result<ExecutionResult> {
        use vibesql_ast::DeallocateTarget;

        match &deallocate_stmt.target {
            DeallocateTarget::Name(name) => {
                if self.named_statements.remove(name).is_none() {
                    return Err(anyhow::anyhow!("Prepared statement '{}' not found", name));
                }
                Ok(ExecutionResult::Deallocate { statement_name: name.clone() })
            }
            DeallocateTarget::All => {
                let count = self.named_statements.len();
                self.named_statements.clear();
                Ok(ExecutionResult::Other {
                    message: format!("Deallocated {} prepared statement(s)", count),
                })
            }
        }
    }

    /// Get prepared statement cache statistics
    #[allow(dead_code)]
    pub fn cache_stats(&self) -> PreparedStatementCacheStats {
        self.stmt_cache.stats()
    }

    /// Clear the prepared statement cache
    #[allow(dead_code)]
    pub fn clear_cache(&self) {
        self.stmt_cache.clear();
    }

    /// Begin a transaction
    ///
    /// Starts a new transaction for this session. While in a transaction:
    /// - Changes are tracked for potential rollback
    /// - The storage layer manages transaction state
    pub async fn begin_transaction(&mut self) -> Result<ExecutionResult> {
        // Track session-level transaction state
        self.txn_manager.begin().map_err(|e| anyhow::anyhow!("{}", e))?;

        // Also start transaction in the shared database storage layer
        let mut db = self.db.write().await;
        db.begin_transaction()
            .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

        Ok(ExecutionResult::Begin)
    }

    /// Commit the current transaction
    ///
    /// Commits all changes made during this transaction.
    /// After commit, changes are permanent and visible to all sessions.
    pub async fn commit(&mut self) -> Result<ExecutionResult> {
        // Commit session-level transaction state (clears buffered change tracking)
        let _changes = self.txn_manager.commit().map_err(|e| anyhow::anyhow!("{}", e))?;

        // Commit in the shared database storage layer
        let mut db = self.db.write().await;
        db.commit_transaction()
            .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;

        Ok(ExecutionResult::Commit)
    }

    /// Rollback the current transaction
    ///
    /// Discards all changes made during this transaction.
    /// The database state is restored to what it was before BEGIN.
    pub async fn rollback(&mut self) -> Result<ExecutionResult> {
        // Rollback session-level transaction state
        self.txn_manager.rollback().map_err(|e| anyhow::anyhow!("{}", e))?;

        // Rollback in the shared database storage layer
        let mut db = self.db.write().await;
        db.rollback_transaction()
            .map_err(|e| anyhow::anyhow!("Failed to rollback transaction: {}", e))?;

        Ok(ExecutionResult::Rollback)
    }

    /// Execute DECLARE CURSOR statement
    fn execute_declare_cursor(
        &mut self,
        stmt: &vibesql_ast::DeclareCursorStmt,
    ) -> Result<ExecutionResult> {
        CursorExecutor::declare(&mut self.cursors, stmt).map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(ExecutionResult::DeclareCursor { cursor_name: stmt.cursor_name.clone() })
    }

    /// Execute FETCH statement
    fn execute_fetch(&mut self, stmt: &vibesql_ast::FetchStmt) -> Result<ExecutionResult> {
        let fetch_result: CursorFetchResult =
            CursorExecutor::fetch(&mut self.cursors, stmt).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Convert cursor rows to session rows
        let rows: Vec<Row> =
            fetch_result.rows.iter().map(|r| Row { values: r.values.to_vec() }).collect();
        let columns: Vec<Column> =
            fetch_result.columns.iter().map(|name| Column { name: name.clone() }).collect();

        Ok(ExecutionResult::Fetch { rows, columns })
    }

    /// Execute CLOSE CURSOR statement
    fn execute_close_cursor(
        &mut self,
        stmt: &vibesql_ast::CloseCursorStmt,
    ) -> Result<ExecutionResult> {
        CursorExecutor::close(&mut self.cursors, stmt).map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(ExecutionResult::CloseCursor { cursor_name: stmt.cursor_name.clone() })
    }
}

/// Extract a string value from a `SET` statement's value expression.
fn set_value_string(expr: &vibesql_ast::Expression) -> Result<String> {
    match evaluate_expression(expr)? {
        SqlValue::Varchar(s) | SqlValue::Character(s) => Ok(s.to_string()),
        other => Err(SqlError::new(
            SQLSTATE_INVALID_PARAMETER,
            format!("expected a string value, got {}", other.type_name()),
        )
        .into()),
    }
}

/// Extract a non-negative millisecond value from a `SET` statement's
/// value expression.
fn set_value_ms(expr: &vibesql_ast::Expression, variable: &str) -> Result<u64> {
    let value = evaluate_expression(expr)?;
    let ms = match value {
        SqlValue::Integer(n) | SqlValue::Bigint(n) => n,
        SqlValue::Smallint(n) => i64::from(n),
        // Already a non-negative `u64`; no range check needed.
        SqlValue::Unsigned(n) => return Ok(n),
        other => {
            return Err(SqlError::new(
                SQLSTATE_INVALID_PARAMETER,
                format!("{variable} expects a non-negative integer, got {}", other.type_name()),
            )
            .into())
        }
    };
    u64::try_from(ms).map_err(|_| {
        anyhow::Error::from(SqlError::new(
            SQLSTATE_INVALID_PARAMETER,
            format!("{variable} must be non-negative"),
        ))
    })
}

/// Substitute a prepared statement's positional (`?`) and numbered (`$N`)
/// placeholders with rendered SQL literals, for routing a replicated
/// `EXECUTE` through the SQL-text propose path (#5393).
///
/// `?` placeholders are consumed left to right; `$N` placeholders refer to
/// `params[N - 1]`. String/quote-bearing literals are rendered safely via
/// [`vibesql_ast::ToSql`] (the same escaping the wire `Bind` path uses), so
/// values containing quotes cannot break out of the statement. Placeholders
/// inside string literals are not substituted.
fn substitute_named_params(sql: &str, params: &[SqlValue]) -> String {
    use vibesql_ast::pretty_print::ToSql;

    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut positional = 0usize;
    // Track whether we are inside a single- or double-quoted string literal
    // so placeholders in literal text are left untouched.
    let mut in_single = false;
    let mut in_double = false;

    while let Some(c) = chars.next() {
        match c {
            '\'' if !in_double => {
                in_single = !in_single;
                out.push(c);
            }
            '"' if !in_single => {
                in_double = !in_double;
                out.push(c);
            }
            '?' if !in_single && !in_double => {
                match params.get(positional) {
                    Some(v) => out.push_str(&v.to_sql()),
                    None => out.push('?'),
                }
                positional += 1;
            }
            '$' if !in_single && !in_double && chars.peek().is_some_and(char::is_ascii_digit) => {
                let mut digits = String::new();
                while chars.peek().is_some_and(char::is_ascii_digit) {
                    digits.push(chars.next().unwrap());
                }
                let idx: usize = digits.parse().unwrap_or(0);
                match idx.checked_sub(1).and_then(|i| params.get(i)) {
                    Some(v) => out.push_str(&v.to_sql()),
                    None => {
                        out.push('$');
                        out.push_str(&digits);
                    }
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// Evaluate an expression to a SqlValue (for EXECUTE parameters)
fn evaluate_expression(expr: &vibesql_ast::Expression) -> Result<SqlValue> {
    use vibesql_ast::Expression;

    match expr {
        // Expression::Literal wraps SqlValue directly
        Expression::Literal(val) => Ok(val.clone()),
        Expression::UnaryOp { op, expr: operand } => {
            // Handle negative numbers
            if let vibesql_ast::UnaryOperator::Minus = op {
                let val = evaluate_expression(operand)?;
                match val {
                    SqlValue::Integer(n) => Ok(SqlValue::Integer(-n)),
                    SqlValue::Bigint(n) => Ok(SqlValue::Bigint(-n)),
                    SqlValue::Float(n) => Ok(SqlValue::Float(-n)),
                    SqlValue::Double(n) => Ok(SqlValue::Double(-n)),
                    SqlValue::Numeric(n) => Ok(SqlValue::Numeric(-n)),
                    _ => Err(anyhow::anyhow!("Cannot negate non-numeric value")),
                }
            } else {
                Err(anyhow::anyhow!("Unsupported unary operator in EXECUTE parameter"))
            }
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported expression type in EXECUTE parameters. Only literals are currently supported."
        )),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_storage::Database;

    use super::*;

    fn create_shared_db() -> SharedDatabase {
        SharedDatabase::new(Database::new())
    }

    #[test]
    fn test_session_creation() {
        let db = create_shared_db();
        let session = Session::new("testdb".to_string(), "testuser".to_string(), db);
        assert_eq!(session.database, "testdb");
        assert_eq!(session.user, "testuser");
        assert!(!session.in_transaction());
    }

    #[tokio::test]
    async fn test_transaction_state() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Not in transaction initially
        assert!(!session.in_transaction());

        // Begin transaction
        assert!(session.begin_transaction().await.is_ok());
        assert!(session.in_transaction());

        // Can't begin twice
        assert!(session.begin_transaction().await.is_err());

        // Commit
        assert!(session.commit().await.is_ok());
        assert!(!session.in_transaction());

        // Can't commit when not in transaction
        assert!(session.commit().await.is_err());
    }

    #[tokio::test]
    async fn test_prepare_and_execute() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table first
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();

        // Prepare a statement (note: parser doesn't support ?, so use literal value)
        let stmt = session.prepare("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(stmt.param_count(), 0);

        // Execute the prepared statement
        let result = session.execute_prepared(&stmt, &[]).await;
        assert!(result.is_ok());

        // Verify we get a Select result
        match result.unwrap() {
            ExecutionResult::Select { .. } => (),
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_cache_hit() {
        let db = create_shared_db();
        let session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // First prepare - cache miss
        let _stmt1 = session.prepare("SELECT 1").unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);

        // Second prepare - cache hit
        let _stmt2 = session.prepare("SELECT 1").unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
    }

    #[tokio::test]
    async fn test_auto_caching_in_execute() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // First execute - cache miss
        session.execute("SELECT 1").await.unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.misses, 1);

        // Second execute - cache hit
        session.execute("SELECT 1").await.unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.hits, 1);
    }

    #[tokio::test]
    async fn test_analyze_single_table() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table and insert data
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();
        session.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
        session.execute("INSERT INTO users VALUES (2, 'Bob')").await.unwrap();

        // Analyze the table
        let result = session.execute("ANALYZE users").await.unwrap();

        // Verify we get an Analyze result
        match result {
            ExecutionResult::Analyze { tables_analyzed } => {
                assert_eq!(tables_analyzed, 1);
            }
            other => panic!("Expected Analyze result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_analyze_all_tables() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create multiple tables
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();
        session.execute("CREATE TABLE products (id INT, price INT)").await.unwrap();
        session.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
        session.execute("INSERT INTO products VALUES (1, 100)").await.unwrap();

        // Analyze all tables (no table name)
        let result = session.execute("ANALYZE").await.unwrap();

        // Verify we get an Analyze result with 2 tables
        match result {
            ExecutionResult::Analyze { tables_analyzed } => {
                assert_eq!(tables_analyzed, 2);
            }
            other => panic!("Expected Analyze result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_analyze_with_columns() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100), age INT)").await.unwrap();
        session.execute("INSERT INTO users VALUES (1, 'Alice', 30)").await.unwrap();

        // Analyze specific columns
        let result = session.execute("ANALYZE users (id, name)").await.unwrap();

        // Verify we get an Analyze result
        match result {
            ExecutionResult::Analyze { tables_analyzed } => {
                assert_eq!(tables_analyzed, 1);
            }
            other => panic!("Expected Analyze result, got {:?}", other),
        }
    }

    #[test]
    fn test_analyze_statement_type() {
        let result = ExecutionResult::Analyze { tables_analyzed: 1 };
        assert_eq!(result.statement_type(), "ANALYZE");
    }

    #[tokio::test]
    async fn test_shared_database_across_sessions() {
        // Create a shared database
        let db = create_shared_db();

        // Create two sessions using the same shared database
        let mut session1 = Session::new("testdb".to_string(), "user1".to_string(), db.clone());
        let mut session2 = Session::new("testdb".to_string(), "user2".to_string(), db.clone());

        // Create a table through session 1
        session1.execute("CREATE TABLE shared_test (id INT, value VARCHAR(100))").await.unwrap();

        // Insert data through session 1
        session1.execute("INSERT INTO shared_test VALUES (1, 'from session 1')").await.unwrap();

        // Should be visible through session 2
        let result = session2.execute("SELECT * FROM shared_test").await.unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Select result"),
        }

        // Insert through session 2
        session2.execute("INSERT INTO shared_test VALUES (2, 'from session 2')").await.unwrap();

        // Should be visible through session 1
        let result = session1.execute("SELECT * FROM shared_test").await.unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Select result"),
        }
    }

    // =========================================================================
    // EXPLAIN QUERY PLAN Tests
    // =========================================================================

    #[tokio::test]
    async fn test_explain_query_plan_basic() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();

        // Test EXPLAIN QUERY PLAN
        let result = session.execute("EXPLAIN QUERY PLAN SELECT * FROM users").await.unwrap();

        match result {
            ExecutionResult::Explain { plan_text, columns } => {
                // Should have the QUERY PLAN column header
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "QUERY PLAN");

                // Should contain expected plan output
                assert!(
                    plan_text.contains("SCAN") || plan_text.contains("Scan"),
                    "Expected scan operation in plan: {}",
                    plan_text
                );
            }
            other => panic!("Expected Explain result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_explain_text_format() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();

        // Test EXPLAIN (default text format)
        let result = session.execute("EXPLAIN SELECT * FROM users").await.unwrap();

        match result {
            ExecutionResult::Explain { plan_text, .. } => {
                // PostgreSQL-style text output
                assert!(plan_text.contains("Select"), "Expected Select in plan: {}", plan_text);
            }
            other => panic!("Expected Explain result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_explain_json_format() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();

        // Test EXPLAIN FORMAT JSON
        let result = session.execute("EXPLAIN FORMAT JSON SELECT * FROM users").await.unwrap();

        match result {
            ExecutionResult::Explain { plan_text, .. } => {
                // Should be valid JSON structure
                assert!(plan_text.starts_with('{'), "JSON should start with {{: {}", plan_text);
                assert!(plan_text.ends_with('}'), "JSON should end with }}: {}", plan_text);
                assert!(
                    plan_text.contains("\"operation\""),
                    "JSON should have operation field: {}",
                    plan_text
                );
            }
            other => panic!("Expected Explain result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_explain_with_index() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table with an index
        session.execute("CREATE TABLE users (id INT, email VARCHAR(100))").await.unwrap();
        session.execute("CREATE INDEX idx_email ON users(email)").await.unwrap();

        // Test EXPLAIN with index usage
        let result = session
            .execute("EXPLAIN SELECT * FROM users WHERE email = 'test@example.com'")
            .await
            .unwrap();

        match result {
            ExecutionResult::Explain { plan_text, .. } => {
                // Should show some scan operation (index or seq scan depending on optimizer)
                assert!(
                    plan_text.contains("Scan") || plan_text.contains("Select"),
                    "Expected scan or select in plan: {}",
                    plan_text
                );
            }
            other => panic!("Expected Explain result, got {:?}", other),
        }
    }

    #[test]
    fn test_substitute_named_params_positional_and_numbered() {
        // Positional `?` consumed in order.
        assert_eq!(
            substitute_named_params(
                "INSERT INTO t VALUES (?, ?)",
                &[SqlValue::Integer(1), SqlValue::Varchar("Alice".into())]
            ),
            "INSERT INTO t VALUES (1, 'Alice')"
        );
        // Numbered `$N` indexes into params; reuse is allowed.
        assert_eq!(
            substitute_named_params(
                "SELECT $1, $2, $1",
                &[SqlValue::Integer(7), SqlValue::Integer(8)]
            ),
            "SELECT 7, 8, 7"
        );
    }

    #[test]
    fn test_substitute_named_params_escapes_quotes() {
        // A value containing a single quote is escaped (no statement
        // break-out): `O'Brien` -> `'O''Brien'`.
        assert_eq!(
            substitute_named_params(
                "INSERT INTO t VALUES (?)",
                &[SqlValue::Varchar("O'Brien".into())]
            ),
            "INSERT INTO t VALUES ('O''Brien')"
        );
    }

    #[test]
    fn test_substitute_named_params_ignores_placeholders_in_string_literals() {
        // A `?` / `$1` already inside a quoted literal is left untouched.
        assert_eq!(
            substitute_named_params("SELECT '? and $1', ?", &[SqlValue::Integer(5)]),
            "SELECT '? and $1', 5"
        );
    }

    #[test]
    fn test_explain_statement_type() {
        let result = ExecutionResult::Explain {
            plan_text: "QUERY PLAN\n`--SCAN users".to_string(),
            columns: vec![Column { name: "QUERY PLAN".to_string() }],
        };
        assert_eq!(result.statement_type(), "EXPLAIN");
    }
}
