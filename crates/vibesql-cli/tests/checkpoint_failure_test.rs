// Integration tests for issue #5832:
//
//   P0 fix(storage): NUMBER-typed column silently destroys the entire database
//   on clean exit.
//
// Two guarantees are asserted end-to-end against a real `vibesql` subprocess:
//
//   1. The original reproducer round-trips: `NUMBER` (any spelling — bare, `NUMBER(5)`,
//      `NUMBER(5,10)`) is accepted like SQLite accepts any type name, the column is usable, and —
//      critically — *unrelated tables in the same database survive* a clean exit + reopen.
//
//   2. THE INVARIANT: a checkpoint-write failure at exit must NEVER truncate the WAL or exit 0. It
//      must print a loud ERROR on stderr, leave the WAL intact (committed changes recoverable on
//      next open), and exit non-zero. (Cross-link #5807 / PR #5850's fail-closed recovery policy.)

use std::{fs, path::Path, process::Command};

use tempfile::TempDir;

fn vibesql_binary() -> &'static str {
    env!("CARGO_BIN_EXE_vibesql")
}

/// Create a temp `$HOME` with no `.vibesqlrc`, so subprocesses run with the
/// shipping defaults (WAL on) and are unaffected by the developer's real
/// config.
fn default_home() -> TempDir {
    tempfile::tempdir().expect("create temp home")
}

/// Run `vibesql <db> -c <sql>` with WAL-default settings.
fn run_cli(home: &TempDir, db: &Path, sql: &str) -> std::process::Output {
    Command::new(vibesql_binary())
        .args([db.to_str().unwrap(), "-c", sql])
        .env("HOME", home.path())
        .output()
        .expect("failed to execute vibesql")
}

fn stdout_of(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr_of(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

// ----------------------------------------------------------------------------
// Part 1: the issue reproducer round-trips
// ----------------------------------------------------------------------------

/// The exact reproducer from issue #5832: a NUMBER-typed column must not
/// destroy the database on clean exit. Both tables survive reopen, and the
/// unrelated table's data is intact.
#[test]
fn test_number_typed_column_does_not_destroy_database() {
    let home = default_home();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db.vbsql");

    let output = run_cli(
        &home,
        &db,
        "CREATE TABLE zz(b number); CREATE TABLE ok(a int); INSERT INTO ok VALUES(1);",
    );
    assert!(output.status.success(), "session 1 should succeed; stderr: {}", stderr_of(&output));
    assert!(
        stderr_of(&output).is_empty(),
        "session 1 must not warn/error; stderr: {}",
        stderr_of(&output)
    );

    // Reopen: BOTH tables must exist and the data must be intact.
    let output = run_cli(&home, &db, "SELECT name FROM sqlite_master;");
    assert!(output.status.success(), "reopen failed; stderr: {}", stderr_of(&output));
    let names = stdout_of(&output);
    assert!(names.contains("zz"), "table zz missing after reopen: {names}");
    assert!(names.contains("ok"), "table ok missing after reopen: {names}");

    let output = run_cli(&home, &db, "SELECT a FROM ok;");
    assert!(output.status.success());
    assert!(stdout_of(&output).contains('1'), "row in ok lost: {}", stdout_of(&output));
}

/// The NUMBER column itself must be usable (SQLite treats any type name via
/// affinity; NUMBER gets NUMERIC affinity), and values must survive reopen.
#[test]
fn test_number_column_is_usable_and_round_trips() {
    let home = default_home();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("num.vbsql");

    let output = run_cli(&home, &db, "CREATE TABLE zz(b number); INSERT INTO zz VALUES(3.14);");
    assert!(output.status.success(), "stderr: {}", stderr_of(&output));

    let output = run_cli(&home, &db, "SELECT b FROM zz;");
    assert!(output.status.success(), "stderr: {}", stderr_of(&output));
    assert!(
        stdout_of(&output).contains("3.14"),
        "NUMBER value lost across reopen: {}",
        stdout_of(&output)
    );
}

/// All spellings from the issue: `number`, `number(5)`, `number(5,10)` — none
/// may wipe the database (`decimal(5,10)` was always fine).
#[test]
fn test_number_precision_variants_round_trip() {
    let home = default_home();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("variants.vbsql");

    let output = run_cli(
        &home,
        &db,
        "CREATE TABLE t1(b number(5)); CREATE TABLE t2(c number(5,10)); \
         CREATE TABLE t3(d decimal(5,10)); CREATE TABLE ok(a int); INSERT INTO ok VALUES(1);",
    );
    assert!(output.status.success(), "stderr: {}", stderr_of(&output));

    let output = run_cli(&home, &db, "SELECT name FROM sqlite_master; SELECT a FROM ok;");
    assert!(output.status.success(), "stderr: {}", stderr_of(&output));
    let out = stdout_of(&output);
    for table in ["t1", "t2", "t3", "ok"] {
        assert!(out.contains(table), "table {table} missing after reopen: {out}");
    }
    assert!(out.contains('1'), "row in ok lost after reopen: {out}");
}

// ----------------------------------------------------------------------------
// Part 2: THE INVARIANT — checkpoint failure is loud, non-zero, WAL-preserving
// ----------------------------------------------------------------------------

/// Force the checkpoint write to fail (read-only checkpoint directory) and
/// assert the fail-safe invariant of issue #5832:
///
///   * exit code is non-zero (never a silent exit 0),
///   * a loud ERROR is printed on stderr,
///   * the WAL is NOT truncated — after clearing the failure, the committed change is recovered on
///     the next open.
#[cfg(unix)]
#[test]
fn test_checkpoint_write_failure_is_loud_nonzero_and_preserves_wal() {
    use std::os::unix::fs::PermissionsExt;

    let home = default_home();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("failsafe.vbsql");
    let checkpoint_dir = dir.path().join("failsafe-checkpoints");
    let wal_path = dir.path().join("failsafe.wal");

    // Session 1: healthy — creates the table and the checkpoint archive.
    let output = run_cli(&home, &db, "CREATE TABLE t(a int); INSERT INTO t VALUES(1);");
    assert!(output.status.success(), "setup failed; stderr: {}", stderr_of(&output));
    assert!(checkpoint_dir.is_dir(), "checkpoint dir should exist after session 1");

    // Inject the failure: checkpoint directory becomes unwritable.
    let orig_perms = fs::metadata(&checkpoint_dir).unwrap().permissions();
    fs::set_permissions(&checkpoint_dir, fs::Permissions::from_mode(0o555)).unwrap();

    // Skip when running as root (root bypasses permission checks).
    if fs::File::create(checkpoint_dir.join(".probe")).is_ok() {
        let _ = fs::remove_file(checkpoint_dir.join(".probe"));
        fs::set_permissions(&checkpoint_dir, orig_perms).unwrap();
        eprintln!("skipping: running as root, cannot inject a permission failure");
        return;
    }

    let wal_size_before = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);

    // Session 2: the INSERT commits to the WAL, but the checkpoint at exit
    // fails. This must be LOUD and the process must exit non-zero.
    let output = run_cli(&home, &db, "INSERT INTO t VALUES(2);");
    assert!(
        !output.status.success(),
        "checkpoint failure must exit non-zero (issue #5832); stdout: {} stderr: {}",
        stdout_of(&output),
        stderr_of(&output)
    );
    let stderr = stderr_of(&output);
    assert!(
        stderr.contains("ERROR"),
        "checkpoint failure must print a loud ERROR on stderr, got: {stderr}"
    );
    assert!(
        stderr.contains("left intact"),
        "stderr must state the WAL was left intact, got: {stderr}"
    );

    // The WAL must NOT have been truncated: the committed INSERT was appended,
    // so it can only have grown.
    let wal_size_after = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        wal_size_after >= wal_size_before,
        "WAL was truncated after a failed checkpoint ({wal_size_before} -> {wal_size_after})"
    );

    // Clear the failure: BOTH rows must be present — the committed INSERT is
    // recovered from the intact WAL.
    fs::set_permissions(&checkpoint_dir, orig_perms).unwrap();
    let output = run_cli(&home, &db, "SELECT a FROM t ORDER BY a;");
    assert!(output.status.success(), "reopen failed; stderr: {}", stderr_of(&output));
    let out = stdout_of(&output);
    assert!(out.contains('1'), "row 1 lost after failed checkpoint: {out}");
    assert!(out.contains('2'), "committed row 2 lost after failed checkpoint: {out}");
}
