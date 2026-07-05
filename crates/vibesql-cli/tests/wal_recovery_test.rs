// ============================================================================
// WAL Recovery Integration Tests — issue #5698
// ============================================================================
//
// Phase 1 wired VibeSQL's WAL + crash-recovery engine into the CLI behind the
// `[database] wal = true` config flag. Phase 2 (this file's current state) makes
// committed *row data* (DML) durable across an unclean shutdown by replaying
// Insert/Update/Delete from the WAL, and flips `wal` on by default.
//
// These tests assert:
//
//   * DDL (table schemas) survives an unclean shutdown via WAL replay.
//   * DML (row data) survives an unclean shutdown via WAL replay — both inserts
//     and the post-recovery state of updates/deletes.
//   * Uncommitted rows (BEGIN ... INSERT, no COMMIT before the crash) are NOT
//     replayed.
//   * End-to-end: a real `vibesql` subprocess survives a SIGKILL with both its
//     table schema and its committed rows intact on reopen.

use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
};

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{
    wal::{PersistenceConfig, PersistenceEngine, RecoveryManager},
    Database, Row,
};
use vibesql_types::{DataType, SqlValue};

fn vibesql_binary() -> &'static str {
    env!("CARGO_BIN_EXE_vibesql")
}

/// Build a minimal single-column table schema for tests.
fn simple_schema(name: &str) -> TableSchema {
    TableSchema::new(
        name.to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, true)],
    )
}

/// Derive the WAL sibling paths the CLI uses for a given database path.
fn wal_paths(db_path: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let wal = db_path.with_extension("wal");
    let stem = db_path.file_stem().unwrap().to_string_lossy().to_string();
    let dir = db_path.parent().unwrap().join(format!("{stem}-checkpoints"));
    (wal, dir)
}

// ----------------------------------------------------------------------------
// In-process recovery tests (exercise the WAL replay path directly)
// ----------------------------------------------------------------------------

/// DDL is durable across a crash via WAL replay (no checkpoint written).
///
/// We emit a `CreateTable` op to the WAL, force it to disk with
/// `sync_persistence`, then *forget* to write a checkpoint (simulating a crash
/// before the next `\save`). Recovery must reconstruct the table by replaying
/// the WAL alone.
#[test]
fn test_ddl_survives_crash_via_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ddl_replay.vbsql");
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);

    // --- Session 1: write DDL to the WAL, flush, then "crash" (no checkpoint).
    {
        let mut db = Database::new();
        let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        db.create_table(simple_schema("survivors")).unwrap();

        // Force the WAL entry to disk. After this, the CreateTable op is durable
        // in the WAL file even though we never created a checkpoint.
        db.sync_persistence().unwrap();
        // `db` drops here (clean engine shutdown) — but the data is already on
        // disk in the WAL, which is what recovery will read.
    }

    // No checkpoint should have been produced by this flow.
    assert!(
        !checkpoint_dir.exists() || fs::read_dir(&checkpoint_dir).unwrap().next().is_none(),
        "no checkpoint should exist; recovery must rely on WAL replay"
    );

    // --- Session 2: recover purely from the WAL and assert the table is back.
    let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
    let (recovered, stats) = manager.recover().unwrap();

    assert!(
        recovered.list_tables().iter().any(|t| t.to_lowercase().contains("survivors")),
        "DDL (table schema) must survive a crash via WAL replay; tables = {:?}",
        recovered.list_tables()
    );
    assert!(stats.tables_created >= 1, "recovery stats should record the replayed CreateTable");
}

/// DML (row data) survives a crash via WAL replay (Phase 2, #5698).
///
/// `RecoveryManager::apply_op` now applies Insert/Update/Delete during replay,
/// routed by the inline `table_name` carried in WAL format v2 DML ops. We emit a
/// CreateTable + a couple of Inserts to the WAL, flush, then "crash" with no
/// checkpoint. Recovery must restore both the schema AND the rows.
#[test]
fn test_dml_survives_crash_via_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("dml_replay.vbsql");
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);

    // --- Session 1: create a table and insert rows, flush, then "crash".
    {
        let mut db = Database::new();
        let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        db.create_table(simple_schema("rows_survive")).unwrap();
        db.insert_row("rows_survive", Row::from_vec(vec![SqlValue::Integer(1)])).unwrap();
        db.insert_row("rows_survive", Row::from_vec(vec![SqlValue::Integer(2)])).unwrap();

        db.sync_persistence().unwrap();
    }

    // No checkpoint was written: recovery relies on WAL replay alone.
    assert!(
        !checkpoint_dir.exists() || fs::read_dir(&checkpoint_dir).unwrap().next().is_none(),
        "no checkpoint should exist; DML must be recovered from the WAL"
    );

    // --- Session 2: recover from the WAL.
    let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
    let (recovered, stats) = manager.recover().unwrap();

    let table_name = recovered
        .list_tables()
        .into_iter()
        .find(|t| t.to_lowercase().contains("rows_survive"))
        .expect("table schema should be recovered via WAL replay");

    assert_eq!(stats.inserts_applied, 2, "both inserts should be replayed");

    let table = recovered.get_table(&table_name).expect("table exists after recovery");
    let rows: Vec<_> = table.scan_live().map(|(_, r)| r.clone()).collect();
    assert_eq!(rows.len(), 2, "both committed rows must survive a crash via WAL replay");
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
}

/// Uncommitted rows (an open transaction at crash time) are NOT replayed.
///
/// A `TxnBegin` followed by an insert, with no `TxnCommit` before the crash,
/// leaves the insert buffered in the recovery `TransactionTracker` and it must
/// be discarded.
#[test]
fn test_uncommitted_dml_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("uncommitted.vbsql");
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);

    // --- Session 1: one committed (auto-commit) insert, then an OPEN
    // transaction with an insert that never commits before the "crash".
    {
        let mut db = Database::new();
        let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        db.create_table(simple_schema("txn")).unwrap();
        db.insert_row("txn", Row::from_vec(vec![SqlValue::Integer(1)])).unwrap();

        db.begin_transaction().unwrap();
        db.insert_row("txn", Row::from_vec(vec![SqlValue::Integer(2)])).unwrap();
        // No commit_transaction(): simulate a crash mid-transaction.

        db.sync_persistence().unwrap();
    }

    // --- Session 2: recover and assert only the committed row is present.
    let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
    let (recovered, _stats) = manager.recover().unwrap();

    let table_name = recovered
        .list_tables()
        .into_iter()
        .find(|t| t.to_lowercase().contains("txn"))
        .expect("table schema should be recovered");

    let table = recovered.get_table(&table_name).unwrap();
    let rows: Vec<_> = table.scan_live().map(|(_, r)| r.clone()).collect();
    assert_eq!(rows.len(), 1, "only the committed row may survive; uncommitted row discarded");
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
}

// ----------------------------------------------------------------------------
// End-to-end subprocess SIGKILL test (exercises the real CLI wiring)
// ----------------------------------------------------------------------------

/// Spawn a real `vibesql` subprocess with WAL enabled via a temp
/// `$HOME/.vibesqlrc`, create a table, SIGKILL the process, then reopen and
/// confirm the table schema survived.
///
/// The CLI checkpoints synchronously after each modification statement (the
/// WAL-active save path), so by the time we hard-kill the process the DDL is
/// already durable on disk in the checkpoint + WAL sibling files. Reopening
/// drives the real `RecoveryManager::recover()` path.
#[cfg(unix)]
#[test]
fn test_subprocess_ddl_survives_sigkill_with_wal() {
    use std::{io::Write, thread, time::Duration};

    let home = tempfile::tempdir().unwrap();
    // Opt into WAL via the real config path (~/.vibesqlrc, resolved from $HOME).
    fs::write(home.path().join(".vibesqlrc"), "[database]\nwal = true\n").unwrap();

    let db_path = home.path().join("crash.vbsql");
    let db_str = db_path.to_string_lossy().to_string();

    // --- Session 1: stdin session. We write the DDL and close stdin so the CLI
    // processes it (script mode reads stdin to EOF), which checkpoints the DDL
    // durably. We then SIGKILL the (now-idle, post-checkpoint) process to prove
    // no *clean* exit path is required for the schema to be durable.
    let mut child: Child = Command::new(vibesql_binary())
        .arg("--database")
        .arg(&db_str)
        .env("HOME", home.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn vibesql");

    {
        // Take and drop stdin to send EOF after writing the statement.
        let mut stdin = child.stdin.take().expect("child stdin");
        writeln!(stdin, "CREATE TABLE kill_survivor (id INTEGER);").unwrap();
        stdin.flush().unwrap();
    }

    // Wait until the WAL-active save path has actually checkpointed the DDL,
    // then hard-kill the process. We poll for a checkpoint *file* (not just the
    // checkpoint directory, which `WalState::open` creates eagerly before any
    // statement runs) and add a short settle so the write is fully flushed
    // before SIGKILL. Polling (rather than a fixed sleep) makes the test robust
    // to scheduling jitter under a loaded, parallel test runner.
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);
    let has_checkpoint_file = |dir: &Path| {
        fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(Result::ok).any(|e| e.path().extension().is_some_and(|x| x == "vchk"))
            })
            .unwrap_or(false)
    };
    let mut waited = Duration::ZERO;
    let step = Duration::from_millis(50);
    while !(checkpoint_dir.exists() && has_checkpoint_file(&checkpoint_dir))
        && waited < Duration::from_secs(10)
    {
        thread::sleep(step);
        waited += step;
    }
    // Settle: ensure the checkpoint write + WAL truncate finished on disk.
    thread::sleep(Duration::from_millis(200));

    // Hard kill — SIGKILL, no graceful shutdown / exit-time save.
    let _ = child.kill();
    let _ = child.wait();

    // The WAL/checkpoint sibling files must exist after an opt-in WAL session.
    assert!(
        wal_path.exists() || checkpoint_dir.exists(),
        "WAL sibling files should be created when wal = true (wal={:?}, ckpt={:?})",
        wal_path,
        checkpoint_dir
    );

    // --- Session 2: reopen with WAL still enabled and confirm the table exists.
    let output = Command::new(vibesql_binary())
        .arg("--database")
        .arg(&db_str)
        .arg("-c")
        .arg("SHOW TABLES")
        .env("HOME", home.path())
        .output()
        .expect("failed to reopen vibesql");

    let combined = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(
        combined.to_uppercase().contains("KILL_SURVIVOR"),
        "table schema must survive SIGKILL with wal = true; got output:\n{combined}"
    );
}

/// End-to-end Phase 2 crash recovery: a real `vibesql` subprocess writes
/// committed rows, gets SIGKILLed, and on reopen the rows are still present.
///
/// This is the primary Phase 2 acceptance gate: committed *row data* — not just
/// schema — survives an unclean shutdown. WAL is on by default now, so we do not
/// even need a `~/.vibesqlrc`; we just point at a `.vbsql` file.
#[cfg(unix)]
#[test]
fn test_subprocess_committed_rows_survive_sigkill() {
    use std::{io::Write, thread, time::Duration};

    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("rows_crash.vbsql");
    let db_str = db_path.to_string_lossy().to_string();
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);

    // --- Session 1: create a table and insert rows over stdin (each auto-commit
    // modification statement drives the WAL-active save path), then SIGKILL.
    let mut child: Child = Command::new(vibesql_binary())
        .arg("--database")
        .arg(&db_str)
        .env("HOME", home.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn vibesql");

    {
        let mut stdin = child.stdin.take().expect("child stdin");
        writeln!(stdin, "CREATE TABLE survivors (id INTEGER, label VARCHAR(50));").unwrap();
        writeln!(stdin, "INSERT INTO survivors VALUES (1, 'alpha');").unwrap();
        writeln!(stdin, "INSERT INTO survivors VALUES (2, 'beta');").unwrap();
        writeln!(stdin, "INSERT INTO survivors VALUES (3, 'gamma');").unwrap();
        stdin.flush().unwrap();
        // Drop stdin (EOF) so script mode processes the statements and runs the
        // per-statement WAL-active save for each.
    }

    // Wait until the WAL-active save path has produced sibling files on disk.
    let mut waited = Duration::ZERO;
    let step = Duration::from_millis(50);
    while !(wal_path.exists() || checkpoint_dir.exists()) && waited < Duration::from_secs(10) {
        thread::sleep(step);
        waited += step;
    }
    // Give the script a moment to finish applying all three inserts.
    thread::sleep(Duration::from_millis(300));

    let _ = child.kill();
    let _ = child.wait();

    // --- Session 2: reopen and confirm all three committed rows are present.
    let output = Command::new(vibesql_binary())
        .arg("--database")
        .arg(&db_str)
        .arg("-c")
        .arg("SELECT id, label FROM survivors ORDER BY id")
        .env("HOME", home.path())
        .output()
        .expect("failed to reopen vibesql");

    let combined = String::from_utf8_lossy(&output.stdout).to_string();
    for label in ["alpha", "beta", "gamma"] {
        assert!(
            combined.contains(label),
            "committed row '{label}' must survive SIGKILL with wal on by default; \
             got output:\n{combined}"
        );
    }
}

/// Run a one-shot `vibesql <db> < script` invocation to completion (clean exit).
#[cfg(unix)]
fn run_script(binary: &str, db: &Path, home: &Path, script: &str) -> String {
    use std::io::Write;

    let mut child = Command::new(binary)
        .arg(db)
        .env("HOME", home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn vibesql");
    {
        let mut stdin = child.stdin.take().expect("child stdin");
        stdin.write_all(script.as_bytes()).unwrap();
        stdin.flush().unwrap();
        // Drop stdin (EOF) → script mode runs to completion and exits cleanly,
        // writing a checkpoint + truncating the WAL on the way out.
    }
    let out = child.wait_with_output().expect("vibesql did not exit");
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// End-to-end regression for #5766: committed DML written by one CLI *process*
/// must survive when a *subsequent* process re-opens the same file-backed DB
/// under the default (WAL-on) config.
///
/// This is the exact shape of the bug: separate `vibesql <db> < stmt`
/// invocations (which is how the TCL shim drives `execsql`). Before the fix the
/// LSN counter reset to 1 each open, so the post-DELETE checkpoint carried a
/// lower LSN than the pre-DELETE one and recovery resurrected the pre-delete
/// state — the final `SELECT count(*)` returned 3 instead of 2.
#[cfg(unix)]
#[test]
fn test_committed_delete_survives_separate_process_reopen() {
    let home = tempfile::tempdir().unwrap();
    // Default config (WAL on). No ~/.vibesqlrc needed.
    let db_path = home.path().join("crossproc.vbsql");
    let bin = vibesql_binary();

    // Process 1: create + insert 3 rows inside an explicit transaction.
    run_script(
        bin,
        &db_path,
        home.path(),
        "CREATE TABLE t(x);\nBEGIN;\nINSERT INTO t VALUES(1);\n\
         INSERT INTO t VALUES(2);\nINSERT INTO t VALUES(3);\nCOMMIT;\n",
    );

    // Process 2: delete one committed row.
    run_script(bin, &db_path, home.path(), "DELETE FROM t WHERE x=1;\n");

    // Process 3: the deletion must be visible — count is 2, not 3.
    let out = run_script(bin, &db_path, home.path(), "SELECT count(*) AS n FROM t;\n");
    assert!(
        out.contains('2') && !out.contains('3'),
        "committed DELETE must persist across separate process re-opens with WAL on \
         by default (expected count 2); got output:\n{out}"
    );
}

/// Companion to the DELETE case: two bare auto-commit INSERTs issued by two
/// separate processes must *accumulate* (final count 2), and a third process's
/// UPDATE must also persist. Exercises the auto-commit path and UPDATE/INSERT
/// (not just DELETE) across re-opens.
#[cfg(unix)]
#[test]
fn test_autocommit_writes_accumulate_across_separate_processes() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("accumulate.vbsql");
    let bin = vibesql_binary();

    run_script(bin, &db_path, home.path(), "CREATE TABLE t(x, y);\n");
    run_script(bin, &db_path, home.path(), "INSERT INTO t VALUES(1, 10);\n");
    run_script(bin, &db_path, home.path(), "INSERT INTO t VALUES(2, 20);\n");

    let count = run_script(bin, &db_path, home.path(), "SELECT count(*) AS n FROM t;\n");
    assert!(
        count.contains('2'),
        "auto-commit INSERTs from separate processes must accumulate (expected 2); got:\n{count}"
    );

    // An UPDATE from yet another process must persist too.
    run_script(bin, &db_path, home.path(), "UPDATE t SET y=99 WHERE x=1;\n");
    let updated = run_script(bin, &db_path, home.path(), "SELECT y FROM t WHERE x=1;\n");
    assert!(
        updated.contains("99"),
        "committed UPDATE must persist across a process re-open; got:\n{updated}"
    );
}

/// End-to-end regression for issue #5835: INTEGER PRIMARY KEY ⇄ rowid
/// aliasing must survive a cross-process reopen. Before the fix,
/// `rowid_alias_column` was never rebuilt on load, so `WHERE rowid=5`
/// returned zero rows and `DELETE ... WHERE rowid=N` removed the WRONG row
/// (intpkey-2.6) after a restart.
#[cfg(unix)]
#[test]
fn test_rowid_alias_survives_separate_process_reopen() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("rowid_alias.vbsql");
    let bin = vibesql_binary();

    run_script(
        bin,
        &db_path,
        home.path(),
        "CREATE TABLE p(a INTEGER PRIMARY KEY, b);\n\
         INSERT INTO p VALUES(5,'five'),(7,'seven'),(9,'nine');\n",
    );

    // Process 2: rowid lookups must resolve through the IPK alias.
    let out = run_script(bin, &db_path, home.path(), "SELECT b FROM p WHERE rowid=5;\n");
    assert!(
        out.contains("five"),
        "WHERE rowid=5 must find the a=5 row after a process reopen; got:\n{out}"
    );

    // Process 3: DELETE by rowid must remove exactly the a=7 row.
    run_script(bin, &db_path, home.path(), "DELETE FROM p WHERE rowid=7;\n");
    let remaining = run_script(bin, &db_path, home.path(), "SELECT b FROM p ORDER BY a;\n");
    assert!(
        remaining.contains("five") && remaining.contains("nine") && !remaining.contains("seven"),
        "DELETE WHERE rowid=7 must remove the a=7 row (and only it); got:\n{remaining}"
    );
}

/// End-to-end regression for issues #5835 / #5871: a plain REPLACE INTO must
/// be durable across a cross-process reopen. Before the fix the REPLACE's
/// conflict-delete emitted no WAL op (and REPLACE never triggered the
/// exit-time checkpoint), so the next process resurrected the old row next to
/// the new one — two rows with the same INTEGER PRIMARY KEY.
#[cfg(unix)]
#[test]
fn test_replace_into_durable_across_separate_process_reopen() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("replace_durable.vbsql");
    let bin = vibesql_binary();

    run_script(
        bin,
        &db_path,
        home.path(),
        "CREATE TABLE t1(aa INTEGER PRIMARY KEY, bb INT);\n\
         INSERT INTO t1 VALUES(11,22);\nCREATE UNIQUE INDEX t1bb ON t1(bb);\n",
    );

    // Process 2: REPLACE the conflicting row (delete (11,22), insert (11,33)).
    run_script(bin, &db_path, home.path(), "REPLACE INTO t1 VALUES(11,33);\n");

    // Process 3: exactly one row, with the replaced value.
    let count = run_script(bin, &db_path, home.path(), "SELECT count(*) AS n FROM t1;\n");
    assert!(
        count.contains('1') && !count.contains('2'),
        "REPLACE must leave exactly one row after a process reopen; got:\n{count}"
    );
    let val = run_script(bin, &db_path, home.path(), "SELECT bb FROM t1 WHERE aa=11;\n");
    assert!(
        val.contains("33") && !val.contains("22"),
        "the surviving row must be the REPLACEd one (bb=33); got:\n{val}"
    );
}
