// Integration tests for issue #6023:
//
//   WAL checkpoint archive grows unboundedly: cleanup_old_checkpoints is never
//   called.
//
// Every `\save` / clean exit writes a fresh `checkpoint_*.vchk`. Before the
// fix, nothing pruned the archive, so a long-lived workflow (e.g. the TCL test
// suite) accumulated thousands of checkpoint files. These tests drive a real
// `vibesql` subprocess through many clean-exit save cycles and assert the
// checkpoint directory stabilizes at the configured retention count.
//
// Invariants asserted end-to-end:
//
//   1. With `keep_checkpoints = N`, repeated saves leave at most `N` checkpoint
//      files (cleanup runs AFTER the new checkpoint is written, so the newest
//      one is always among the survivors).
//   2. Recovery is unaffected: after many prune cycles, the latest data is
//      still recovered on reopen (pruning never removes the newest checkpoint).
//   3. The `keep_checkpoints = 0` edge case is clamped to "keep at least 1" so
//      recovery can never be left with an empty archive (fail-closed, #5807).

use std::{fs, path::Path, process::Command};

use tempfile::TempDir;

fn vibesql_binary() -> &'static str {
    env!("CARGO_BIN_EXE_vibesql")
}

/// Create a temp `$HOME` containing a `.vibesqlrc` with the given
/// `keep_checkpoints` value (WAL stays on by default). Returns the temp dir
/// that must be kept alive for the duration of the test.
fn home_with_keep_checkpoints(keep: usize) -> TempDir {
    let home = tempfile::tempdir().expect("create temp home");
    let rc = format!("[database]\nwal = true\nkeep_checkpoints = {keep}\n");
    fs::write(home.path().join(".vibesqlrc"), rc).expect("write .vibesqlrc");
    home
}

/// Run `vibesql <db> -c <sql>` as a full open + save + clean-exit cycle.
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

/// Count `checkpoint_*.vchk` files in the archive directory.
fn count_checkpoints(checkpoint_dir: &Path) -> usize {
    fs::read_dir(checkpoint_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "vchk"))
                .count()
        })
        .unwrap_or(0)
}

/// Core repro: repeated clean-exit saves must NOT accumulate checkpoints
/// without bound. With `keep_checkpoints = 2`, the archive must never exceed 2
/// `.vchk` files no matter how many save cycles run.
#[test]
fn test_repeated_saves_prune_to_keep_checkpoints() {
    let keep = 2;
    let home = home_with_keep_checkpoints(keep);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("retain.vbsql");
    let checkpoint_dir = dir.path().join("retain-checkpoints");

    // Session 1: create the table and the checkpoint archive.
    let output = run_cli(&home, &db, "CREATE TABLE t(a int); INSERT INTO t VALUES(0);");
    assert!(output.status.success(), "setup failed; stderr: {}", stderr_of(&output));

    // Many more clean-exit save cycles. Each writes a fresh checkpoint then
    // prunes. The count must stabilize at `keep`, never grow unboundedly.
    for i in 1..=10 {
        let output = run_cli(&home, &db, &format!("INSERT INTO t VALUES({i});"));
        assert!(output.status.success(), "session {i} failed; stderr: {}", stderr_of(&output));

        let n = count_checkpoints(&checkpoint_dir);
        assert!(
            n <= keep,
            "after save cycle {i}, checkpoint count {n} exceeds keep_checkpoints={keep} \
             (archive is growing unboundedly — issue #6023)"
        );
    }

    // Final state: exactly `keep` checkpoints retained (not fewer, not more).
    let final_count = count_checkpoints(&checkpoint_dir);
    assert_eq!(
        final_count, keep,
        "expected exactly {keep} checkpoints retained, found {final_count}"
    );

    // Recovery is unaffected: the latest data survives (pruning never removes
    // the newest checkpoint).
    let output = run_cli(&home, &db, "SELECT count(*) FROM t;");
    assert!(output.status.success(), "reopen failed; stderr: {}", stderr_of(&output));
    // 11 rows: the initial 0 plus 10 inserts.
    assert!(
        stdout_of(&output).contains("11"),
        "row count wrong after pruning cycles: {}",
        stdout_of(&output)
    );
}

/// Edge case: `keep_checkpoints = 0` must be clamped to "keep at least 1" so
/// recovery is never left with an empty archive (fail-closed recovery, #5807).
#[test]
fn test_keep_checkpoints_zero_is_clamped_to_one() {
    let home = home_with_keep_checkpoints(0);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("clamp.vbsql");
    let checkpoint_dir = dir.path().join("clamp-checkpoints");

    let output = run_cli(&home, &db, "CREATE TABLE t(a int); INSERT INTO t VALUES(1);");
    assert!(output.status.success(), "setup failed; stderr: {}", stderr_of(&output));

    for i in 2..=5 {
        let output = run_cli(&home, &db, &format!("INSERT INTO t VALUES({i});"));
        assert!(output.status.success(), "session {i} failed; stderr: {}", stderr_of(&output));
    }

    // Clamped to 1 — never 0. At least the newest checkpoint always survives.
    let n = count_checkpoints(&checkpoint_dir);
    assert_eq!(
        n, 1,
        "keep_checkpoints=0 must clamp to 1 (never prune the newest checkpoint), found {n}"
    );

    // And the data is still recoverable from that single surviving checkpoint.
    let output = run_cli(&home, &db, "SELECT count(*) FROM t;");
    assert!(output.status.success(), "reopen failed; stderr: {}", stderr_of(&output));
    assert!(
        stdout_of(&output).contains('5'),
        "data lost after clamped pruning: {}",
        stdout_of(&output)
    );
}

/// A larger retention value keeps more history — confirms the knob actually
/// varies retention rather than being hard-coded.
#[test]
fn test_larger_keep_checkpoints_retains_more() {
    let keep = 4;
    let home = home_with_keep_checkpoints(keep);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("more.vbsql");
    let checkpoint_dir = dir.path().join("more-checkpoints");

    let output = run_cli(&home, &db, "CREATE TABLE t(a int); INSERT INTO t VALUES(0);");
    assert!(output.status.success(), "setup failed; stderr: {}", stderr_of(&output));

    for i in 1..=8 {
        let output = run_cli(&home, &db, &format!("INSERT INTO t VALUES({i});"));
        assert!(output.status.success(), "session {i} failed; stderr: {}", stderr_of(&output));
        assert!(
            count_checkpoints(&checkpoint_dir) <= keep,
            "count exceeded keep_checkpoints={keep} at cycle {i}"
        );
    }

    assert_eq!(
        count_checkpoints(&checkpoint_dir),
        keep,
        "expected {keep} checkpoints retained with a larger retention value"
    );
}
