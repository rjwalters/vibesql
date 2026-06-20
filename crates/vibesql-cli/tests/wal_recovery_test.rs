// ============================================================================
// WAL Recovery Integration Tests — Phase 1 of issue #5698
// ============================================================================
//
// Phase 1 wires VibeSQL's WAL + crash-recovery engine into the CLI behind the
// opt-in `[database] wal = true` config flag. These tests assert what Phase 1
// actually delivers:
//
//   * DDL (table schemas) survives an unclean shutdown and is recovered by
//     replaying the WAL (no checkpoint required).
//   * DML (row data) replay from the WAL is a KNOWN GAP (Phase 2): rows written
//     to the WAL after the last checkpoint are NOT yet replayed on recovery.
//     `test_dml_replay_is_a_known_gap_phase2` documents this explicitly so a
//     future Phase 2 change will flip it (and can be promoted to the full
//     crash-recovery assertion).
//   * End-to-end: a real `vibesql` subprocess with `wal = true` survives a
//     SIGKILL with its table schema intact on reopen.

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

/// DML replay from the WAL is a KNOWN GAP in Phase 1.
///
/// `RecoveryManager::apply_op` currently only replays DDL; Insert/Update/Delete
/// are stubbed (counted but not applied), because the WAL stores a hashed
/// `table_id` with no `table_id -> table_name` resolution. This test pins that
/// behavior: after a crash, the table schema is recovered but the inserted row
/// is NOT. Phase 2 (#5698) will make DML durable and should flip this test into
/// the full crash-recovery assertion.
#[test]
fn test_dml_replay_is_a_known_gap_phase2() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("dml_gap.vbsql");
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);

    // --- Session 1: create a table and insert a row, flush, then "crash".
    {
        let mut db = Database::new();
        let engine = PersistenceEngine::new(&wal_path, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        db.create_table(simple_schema("gappy")).unwrap();
        db.insert_row("gappy", Row::from_vec(vec![SqlValue::Integer(1)])).unwrap();

        db.sync_persistence().unwrap();
    }

    // --- Session 2: recover from the WAL.
    let manager = RecoveryManager::new(&checkpoint_dir).with_wal(&wal_path);
    let (recovered, _stats) = manager.recover().unwrap();

    // DDL recovered:
    let table_name = recovered
        .list_tables()
        .into_iter()
        .find(|t| t.to_lowercase().contains("gappy"))
        .expect("table schema should be recovered via WAL replay");

    // DML NOT recovered (Phase 1 stub). If this assertion ever fails, Phase 2
    // landed: promote this test to assert the row IS present and remove the
    // "known gap" framing.
    let row_count = recovered.get_table(&table_name).map(|t| t.row_count()).unwrap_or(0);
    assert_eq!(
        row_count, 0,
        "PHASE 1 KNOWN GAP: WAL DML replay is stubbed, so the inserted row must \
         NOT be recovered yet. If this fails, Phase 2 (#5698) is done — update \
         this test to assert the row survives."
    );
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

    // Wait until the WAL-active save path has written the checkpoint + WAL
    // sibling files to disk, then hard-kill the process. Polling (rather than a
    // fixed sleep) makes the test robust to scheduling jitter under a loaded
    // test runner.
    let (wal_path, checkpoint_dir) = wal_paths(&db_path);
    let mut waited = Duration::ZERO;
    let step = Duration::from_millis(50);
    while !(wal_path.exists() || checkpoint_dir.exists()) && waited < Duration::from_secs(10) {
        thread::sleep(step);
        waited += step;
    }

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
