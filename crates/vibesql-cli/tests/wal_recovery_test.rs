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
//   * DML (row data) survives an unclean shutdown via WAL replay — both inserts and the
//     post-recovery state of updates/deletes.
//   * Uncommitted rows (BEGIN ... INSERT, no COMMIT before the crash) are NOT replayed.
//   * End-to-end: a real `vibesql` subprocess survives a SIGKILL with both its table schema and its
//     committed rows intact on reopen.

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

/// Like `run_script`, but returns the full process `Output` (status + stderr)
/// for sessions that are expected to fail.
#[cfg(unix)]
fn run_script_output(binary: &str, db: &Path, home: &Path, script: &str) -> std::process::Output {
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
    }
    child.wait_with_output().expect("vibesql did not exit")
}

/// End-to-end regression for issue #5883: PK, FK (with ON DELETE CASCADE),
/// CHECK, and rowid-alias semantics must survive crash recovery when the
/// CREATE TABLE lives only in the WAL — i.e. it was logged AFTER the last
/// checkpoint. Checkpoint-based recovery was fixed by #5878; this covers the
/// log-replay path (`WalOp::CreateTable`).
///
/// The crash is injected deterministically: after a healthy session seeds
/// the checkpoint archive, the checkpoint directory is made read-only so
/// every subsequent checkpoint attempt fails while the WAL keeps absorbing
/// the ops (the CLI flushes the WAL before attempting the checkpoint and
/// never truncates it on failure — issue #5832). The resulting on-disk state
/// is byte-for-byte what a SIGKILL between WAL append and checkpoint leaves
/// behind, without the scheduling races of an actual kill.
#[cfg(unix)]
#[test]
fn test_constraints_survive_crash_replay_of_create_table() {
    use std::os::unix::fs::PermissionsExt;

    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("constraints_crash.vbsql");
    let bin = vibesql_binary();
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);

    // --- Session 1 (healthy): seed the checkpoint archive so we can chmod it.
    run_script(bin, &db_path, home.path(), "CREATE TABLE seed(a INTEGER);\n");
    assert!(checkpoint_dir.is_dir(), "checkpoint dir should exist after session 1");

    // Inject the "crash": checkpoint dir read-only.
    let orig_perms = fs::metadata(&checkpoint_dir).unwrap().permissions();
    fs::set_permissions(&checkpoint_dir, fs::Permissions::from_mode(0o555)).unwrap();

    // Skip when running as root (root bypasses permission checks).
    if fs::File::create(checkpoint_dir.join(".probe")).is_ok() {
        let _ = fs::remove_file(checkpoint_dir.join(".probe"));
        fs::set_permissions(&checkpoint_dir, orig_perms).unwrap();
        eprintln!("skipping: running as root, cannot inject a permission failure");
        return;
    }

    // --- Session 2: DDL with PK + FK + CHECK, plus committed rows. Every
    // checkpoint attempt fails, so all of this lands ONLY in the WAL. The
    // process exits non-zero (fail-closed persistence policy) — that is the
    // simulated crash.
    let output = run_script_output(
        bin,
        &db_path,
        home.path(),
        "CREATE TABLE crash_parent(px INTEGER PRIMARY KEY);\n\
         CREATE TABLE crash_child(id INTEGER PRIMARY KEY, \
         py INTEGER REFERENCES crash_parent(px) ON DELETE CASCADE, \
         amount INTEGER CHECK(amount > 0));\n\
         INSERT INTO crash_parent VALUES(1);\n\
         INSERT INTO crash_child VALUES(10, 1, 5);\n",
    );
    assert!(
        !output.status.success(),
        "session 2 must exit non-zero on checkpoint failure (WAL-only state)"
    );
    assert!(wal_path.exists(), "the WAL must hold the un-checkpointed CreateTable ops");

    // Clear the injected failure: from here on, reopening replays the
    // CreateTable + Insert ops from the WAL (RecoveryManager log replay).
    fs::set_permissions(&checkpoint_dir, orig_perms).unwrap();

    // --- Reopen A: FK metadata and rowid-alias reads on the crash-replayed
    // state. Before the fix, foreign_key_list was empty and the schema had
    // no PK.
    let out = run_script(
        bin,
        &db_path,
        home.path(),
        "PRAGMA foreign_key_list(crash_child);\n\
         SELECT amount FROM crash_child WHERE rowid = 10;\n",
    );
    assert!(
        out.contains("crash_parent") && out.contains("CASCADE"),
        "PRAGMA foreign_key_list must show the FK (with CASCADE) after crash replay; got:\n{out}"
    );
    assert!(
        out.contains('5'),
        "rowid must alias the INTEGER PRIMARY KEY after crash replay; got:\n{out}"
    );

    // --- Reopen B: an FK-violating insert must FAIL and change nothing.
    let output = run_script_output(
        bin,
        &db_path,
        home.path(),
        "PRAGMA foreign_keys=ON;\nINSERT INTO crash_child VALUES(11, 99, 5);\n",
    );
    assert!(
        !output.status.success(),
        "FK-violating insert must fail after crash replay; stdout: {} stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // --- Reopen C: a CHECK-violating insert must FAIL.
    let output = run_script_output(
        bin,
        &db_path,
        home.path(),
        "INSERT INTO crash_child VALUES(12, 1, -5);\n",
    );
    assert!(
        !output.status.success(),
        "CHECK-violating insert must fail after crash replay; stdout: {} stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // --- Reopen D: a duplicate-PK insert must FAIL.
    let output = run_script_output(
        bin,
        &db_path,
        home.path(),
        "INSERT INTO crash_child VALUES(10, 1, 7);\n",
    );
    assert!(
        !output.status.success(),
        "duplicate-PK insert must fail after crash replay; stdout: {} stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // None of the violating inserts may have landed.
    let out = run_script(bin, &db_path, home.path(), "SELECT count(*) AS n FROM crash_child;\n");
    assert!(out.contains('1'), "exactly the one committed row must remain; got:\n{out}");

    // --- Reopen E+F: ON DELETE CASCADE must actually fire.
    run_script(
        bin,
        &db_path,
        home.path(),
        "PRAGMA foreign_keys=ON;\nDELETE FROM crash_parent WHERE px = 1;\n",
    );
    let out = run_script(bin, &db_path, home.path(), "SELECT count(*) AS n FROM crash_child;\n");
    assert!(
        out.contains('0'),
        "ON DELETE CASCADE must remove the child row after crash replay; got:\n{out}"
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

// ----------------------------------------------------------------------------
// CREATE INDEX / DROP INDEX crash replay (issue #6741)
// ----------------------------------------------------------------------------

/// Make the checkpoint directory read-only so every checkpoint attempt fails
/// while the WAL keeps absorbing the ops — byte-for-byte the on-disk state a
/// crash between WAL append and checkpoint leaves behind (same injection as
/// `test_constraints_survive_crash_replay_of_create_table`). Returns the
/// original permissions to restore, or `None` when running as root (root
/// bypasses permission checks, so the failure cannot be injected).
#[cfg(unix)]
fn inject_checkpoint_failure(checkpoint_dir: &Path) -> Option<fs::Permissions> {
    use std::os::unix::fs::PermissionsExt;

    let orig_perms = fs::metadata(checkpoint_dir).unwrap().permissions();
    fs::set_permissions(checkpoint_dir, fs::Permissions::from_mode(0o555)).unwrap();
    if fs::File::create(checkpoint_dir.join(".probe")).is_ok() {
        let _ = fs::remove_file(checkpoint_dir.join(".probe"));
        fs::set_permissions(checkpoint_dir, orig_perms).unwrap();
        eprintln!("skipping: running as root, cannot inject a permission failure");
        return None;
    }
    Some(orig_perms)
}

/// Run a one-shot `-c` query in raw output mode (clean exit) and return stdout.
#[cfg(unix)]
fn query_raw(binary: &str, db: &Path, home: &Path, sql: &str) -> String {
    let out = Command::new(binary)
        .arg(db)
        .args(["--format", "raw", "-c", sql])
        .env("HOME", home)
        .output()
        .expect("failed to run vibesql");
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// End-to-end regression for issue #6741: an index created AFTER the last
/// checkpoint and only logged to the WAL must be recreated by crash
/// recovery — with its exact `sqlite_master.sql` text — instead of being
/// silently lost. Covers a plain index with per-key-part collation and
/// direction, an expression index, a partial UNIQUE index, an index on a
/// table that was itself created within the same unreplayed WAL segment, and
/// an index created and dropped within that segment (net: absent).
#[cfg(unix)]
#[test]
fn test_create_index_survives_crash_replay() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("index_crash.vbsql");
    let bin = vibesql_binary();
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);

    // --- Session 1 (healthy): table + rows, checkpointed.
    run_script(
        bin,
        &db_path,
        home.path(),
        "CREATE TABLE t(a INTEGER, b TEXT, c INTEGER);\n\
         INSERT INTO t VALUES(1, 'x', 10);\n\
         INSERT INTO t VALUES(2, 'Y', 20);\n\
         INSERT INTO t VALUES(3, 'z', 30);\n",
    );
    assert!(checkpoint_dir.is_dir(), "checkpoint dir should exist after session 1");

    let Some(orig_perms) = inject_checkpoint_failure(&checkpoint_dir) else { return };

    // --- Session 2: every index DDL below lands ONLY in the WAL.
    let plain_sql = "CREATE INDEX t_b ON t(b COLLATE NOCASE DESC)";
    let expr_sql = "CREATE INDEX t_expr ON t(lower(b), a + c)";
    let partial_sql = "CREATE UNIQUE INDEX t_part ON t(c) WHERE a > 1";
    let fresh_table_sql = "CREATE INDEX u_x ON u(x)";
    let output = run_script_output(
        bin,
        &db_path,
        home.path(),
        &format!(
            "{plain_sql};\n{expr_sql};\n{partial_sql};\n\
             CREATE TABLE u(x INTEGER);\nINSERT INTO u VALUES(5);\n{fresh_table_sql};\n\
             CREATE INDEX t_gone ON t(a);\nDROP INDEX t_gone;\n"
        ),
    );
    assert!(
        !output.status.success(),
        "session 2 must exit non-zero on checkpoint failure (WAL-only state)"
    );
    assert!(wal_path.exists(), "the WAL must hold the un-checkpointed CreateIndex ops");

    fs::set_permissions(&checkpoint_dir, orig_perms).unwrap();

    // --- Reopen: recovery replays the CreateIndex/DropIndex ops.
    let out = query_raw(
        bin,
        &db_path,
        home.path(),
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' ORDER BY name",
    );
    for sql in [plain_sql, expr_sql, partial_sql, fresh_table_sql] {
        assert!(
            out.contains(sql),
            "sqlite_master.sql must hold the verbatim CREATE INDEX text `{sql}` after crash \
             replay; got:\n{out}"
        );
    }
    assert!(
        !out.contains("t_gone"),
        "an index created then dropped in the same WAL segment must stay dropped; got:\n{out}"
    );

    // The recovered indexes are functional, not just catalog entries: the
    // expression index answers lookups, and the partial UNIQUE index enforces
    // uniqueness only over rows matching its predicate.
    let out = query_raw(bin, &db_path, home.path(), "SELECT a FROM t WHERE lower(b) = 'y'");
    assert!(out.contains('2'), "expression-index lookup must find a=2; got:\n{out}");

    let dup = run_script_output(bin, &db_path, home.path(), "INSERT INTO t VALUES(4, 'w', 20);\n");
    assert!(
        !dup.status.success(),
        "partial UNIQUE index must reject c=20 for a row matching `a > 1`; stdout: {} stderr: {}",
        String::from_utf8_lossy(&dup.stdout),
        String::from_utf8_lossy(&dup.stderr)
    );
    let ok = run_script_output(bin, &db_path, home.path(), "INSERT INTO t VALUES(0, 'q', 20);\n");
    assert!(
        ok.status.success(),
        "a row outside the partial predicate must not conflict; stdout: {} stderr: {}",
        String::from_utf8_lossy(&ok.stdout),
        String::from_utf8_lossy(&ok.stderr)
    );

    // The replayed indexes are captured by the next checkpoint and survive a
    // further clean reopen.
    let out = query_raw(
        bin,
        &db_path,
        home.path(),
        "SELECT sql FROM sqlite_master WHERE type = 'index' ORDER BY name",
    );
    for sql in [plain_sql, expr_sql, partial_sql, fresh_table_sql] {
        assert!(
            out.contains(sql),
            "`{sql}` must survive the post-recovery checkpoint; got:\n{out}"
        );
    }
}

/// End-to-end regression for issue #6741: an index dropped AFTER the last
/// checkpoint and only logged to the WAL must stay dropped after crash
/// recovery instead of being silently resurrected from the checkpoint.
#[cfg(unix)]
#[test]
fn test_drop_index_survives_crash_replay() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("drop_index_crash.vbsql");
    let bin = vibesql_binary();
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);

    // --- Session 1 (healthy): the index is captured by a checkpoint.
    run_script(
        bin,
        &db_path,
        home.path(),
        "CREATE TABLE t(a INTEGER, b TEXT);\nINSERT INTO t VALUES(1, 'x');\n\
         CREATE INDEX t_a ON t(a);\n",
    );
    let out = query_raw(bin, &db_path, home.path(), "SELECT name FROM sqlite_master");
    assert!(out.contains("t_a"), "precondition: index checkpointed; got:\n{out}");

    let Some(orig_perms) = inject_checkpoint_failure(&checkpoint_dir) else { return };

    // --- Session 2: the DROP INDEX lands ONLY in the WAL.
    let output = run_script_output(bin, &db_path, home.path(), "DROP INDEX t_a;\n");
    assert!(
        !output.status.success(),
        "session 2 must exit non-zero on checkpoint failure (WAL-only state)"
    );
    assert!(wal_path.exists(), "the WAL must hold the un-checkpointed DropIndex op");

    fs::set_permissions(&checkpoint_dir, orig_perms).unwrap();

    // --- Reopen: the index must be gone from sqlite_master ...
    let out = query_raw(bin, &db_path, home.path(), "SELECT name FROM sqlite_master");
    assert!(
        !out.contains("t_a"),
        "a dropped index must not be resurrected by crash recovery; got:\n{out}"
    );
    // ... and from the storage index manager too: re-creating it succeeds.
    let recreate = run_script_output(bin, &db_path, home.path(), "CREATE INDEX t_a ON t(a);\n");
    assert!(
        recreate.status.success(),
        "re-creating the dropped index must succeed; stdout: {} stderr: {}",
        String::from_utf8_lossy(&recreate.stdout),
        String::from_utf8_lossy(&recreate.stderr)
    );
}
