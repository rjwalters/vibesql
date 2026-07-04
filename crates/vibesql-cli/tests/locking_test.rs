// ============================================================================
// Inter-Process Locking Integration Tests — issue #5808
// ============================================================================
//
// VibeSQL holds the whole database in memory and checkpoints its own image,
// so concurrent CLI writer sessions cannot be merged: they race on WAL
// appends, checkpoint IDs, and WAL truncation (last writer clobbers). The
// fix is a whole-session EXCLUSIVE advisory lock (`std::fs::File::try_lock`,
// flock on Unix) on a dedicated `<stem>.lock` sibling, taken by every
// file-backed open — WAL-active and snapshot-only alike.
//
// These tests spawn real `vibesql` subprocesses (CARGO_BIN_EXE_vibesql) and
// use MARKER-BASED synchronization, never fixed sleeps for correctness:
//
//   * WAL sibling files (`<stem>.wal` / `<stem>-checkpoints/`) are created strictly AFTER lock
//     acquisition during open, so their appearance proves the holder owns the lock.
//   * For the snapshot-only (`wal = false`) path — which creates no sibling files — a zero-timeout
//     probe subprocess is retried until it observes `database is locked`, which is itself the proof
//     the holder owns the lock.
//
// All poll loops use generous (60 s) deadlines so a loaded parallel test
// runner cannot produce false failures.

#![cfg(unix)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

fn vibesql_binary() -> &'static str {
    env!("CARGO_BIN_EXE_vibesql")
}

/// Generous deadline for all poll loops (loaded machines, parallel tests).
const POLL_DEADLINE: Duration = Duration::from_secs(60);
const POLL_STEP: Duration = Duration::from_millis(25);

/// Write a `~/.vibesqlrc` into `home` with the given `[database]` body.
fn write_config(home: &Path, database_section: &str) {
    fs::write(home.join(".vibesqlrc"), format!("[database]\n{database_section}\n")).unwrap();
}

/// Derive the WAL sibling paths the CLI uses (mirrors `WalPaths::derive`).
fn wal_siblings(db_path: &Path) -> (PathBuf, PathBuf) {
    let wal = db_path.with_extension("wal");
    let stem = db_path.file_stem().unwrap().to_string_lossy().to_string();
    let dir = db_path.parent().unwrap().join(format!("{stem}-checkpoints"));
    (wal, dir)
}

/// Spawn a `vibesql` session that OPENS the database and then HOLDS it open,
/// blocked reading stdin (script mode reads stdin to EOF; the executor — and
/// with it the exclusive lock — is created before any stdin is read).
///
/// Returns the child; drop/close its stdin to let it run to a clean exit.
fn spawn_holder(db_path: &Path, home: &Path) -> Child {
    Command::new(vibesql_binary())
        .arg("--database")
        .arg(db_path.as_os_str())
        .env("HOME", home)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn holder vibesql")
}

/// Marker-based wait: block until the holder has acquired the lock, proven by
/// the WAL sibling files, which the CLI creates strictly AFTER acquisition.
fn wait_until_holder_owns_lock_wal(db_path: &Path) {
    let (wal_path, ckpt_dir) = wal_siblings(db_path);
    let deadline = Instant::now() + POLL_DEADLINE;
    while !(wal_path.exists() || ckpt_dir.exists()) {
        assert!(
            Instant::now() < deadline,
            "holder never created WAL siblings (never finished opening?)"
        );
        thread::sleep(POLL_STEP);
    }
}

/// Run a short-lived `vibesql <db> -c <sql>` session and return
/// `(success, stdout, stderr)`.
fn run_command_session(db_path: &Path, home: &Path, sql: &str) -> (bool, String, String) {
    let output = Command::new(vibesql_binary())
        .arg("--database")
        .arg(db_path.as_os_str())
        .arg("-c")
        .arg(sql)
        .env("HOME", home)
        .output()
        .expect("failed to run vibesql -c session");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// Marker-based wait for the snapshot-only path (no WAL sibling files):
/// retry a ZERO-timeout probe session until it reports `database is locked` —
/// the observation itself proves the holder owns the lock.
fn wait_until_holder_owns_lock_probe(db_path: &Path, probe_home: &Path) {
    let deadline = Instant::now() + POLL_DEADLINE;
    loop {
        let (ok, _out, err) = run_command_session(db_path, probe_home, "SELECT 1;");
        if !ok && err.contains("database is locked") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "probe never observed the lock held (holder never acquired?); last: ok={ok} err={err}"
        );
        thread::sleep(POLL_STEP);
    }
}

// ----------------------------------------------------------------------------
// (a) Second writer blocked, then times out with `database is locked`
// ----------------------------------------------------------------------------

/// While one session holds a file-backed database open, a second
/// `vibesql <db> -c "INSERT ..."` must exit non-zero after its busy timeout
/// with stderr containing the exact SQLite wording `database is locked`.
/// After the holder exits cleanly, the same command must succeed.
#[test]
fn test_second_writer_times_out_with_database_is_locked() {
    let home_a = tempfile::tempdir().unwrap();
    let home_b = tempfile::tempdir().unwrap();
    // Short busy timeout for B so the negative case is fast.
    write_config(home_b.path(), "lock_timeout_ms = 300");

    let db_path = home_a.path().join("contended.vbsql");

    let mut holder = spawn_holder(&db_path, home_a.path());
    wait_until_holder_owns_lock_wal(&db_path);

    // Second writer: must wait out its 300 ms busy timeout, then fail loudly.
    let started = Instant::now();
    let (ok, _out, err) = run_command_session(
        &db_path,
        home_b.path(),
        "CREATE TABLE intruder (id INTEGER); INSERT INTO intruder VALUES (1);",
    );
    assert!(!ok, "second writer must exit non-zero while the database is held");
    assert!(
        err.contains("database is locked"),
        "stderr must contain the exact SQLite wording `database is locked`; got:\n{err}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(300),
        "second writer must wait out the busy timeout before failing"
    );

    // Release: close the holder's stdin; it runs its (empty) script and exits
    // cleanly, dropping the lock.
    drop(holder.stdin.take());
    let status = holder.wait().expect("holder wait");
    assert!(status.success(), "holder must exit cleanly");

    // The lock is released on clean exit: the same command now succeeds.
    let (ok, _out, err) = run_command_session(
        &db_path,
        home_b.path(),
        "CREATE TABLE intruder (id INTEGER); INSERT INTO intruder VALUES (1);",
    );
    assert!(ok, "writer must succeed after the holder released the lock; stderr:\n{err}");
}

// ----------------------------------------------------------------------------
// (a2) Brief overlap shorter than the busy timeout succeeds (waits, acquires)
// ----------------------------------------------------------------------------

/// A second session started during an overlap shorter than its busy timeout
/// must NOT fail: it waits in the retry loop and acquires once the holder
/// releases.
#[test]
fn test_waiter_within_busy_timeout_succeeds_after_release() {
    let home_a = tempfile::tempdir().unwrap();
    let home_b = tempfile::tempdir().unwrap();
    // Generous busy timeout: B must outlast the overlap comfortably.
    write_config(home_b.path(), "lock_timeout_ms = 60000");

    let db_path = home_a.path().join("overlap.vbsql");

    let mut holder = spawn_holder(&db_path, home_a.path());
    wait_until_holder_owns_lock_wal(&db_path);

    // Spawn B while A verifiably holds the lock. B enters the retry loop.
    let mut waiter = Command::new(vibesql_binary())
        .arg("--database")
        .arg(db_path.as_os_str())
        .arg("-c")
        .arg("CREATE TABLE waited (id INTEGER); INSERT INTO waited VALUES (42);")
        .env("HOME", home_b.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn waiter");

    // B must still be alive while A holds the lock (it is waiting, not dead).
    // Bounded observation window — this can only under-observe on a slow
    // machine, never produce a false failure.
    let observe_until = Instant::now() + Duration::from_millis(500);
    while Instant::now() < observe_until {
        assert!(
            waiter.try_wait().expect("try_wait").is_none(),
            "waiter must block while the holder owns the lock, not exit"
        );
        thread::sleep(POLL_STEP);
    }

    // Release the holder; the waiter must then acquire and succeed.
    drop(holder.stdin.take());
    assert!(holder.wait().expect("holder wait").success());

    let out = waiter.wait_with_output().expect("waiter wait");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "waiter within its busy timeout must succeed after release; stderr:\n{err}"
    );

    // And its write is durably visible to a fresh session.
    let (ok, stdout, _err) = run_command_session(&db_path, home_b.path(), "SELECT id FROM waited;");
    assert!(ok);
    assert!(stdout.contains("42"), "waiter's committed row must be visible; got:\n{stdout}");
}

// ----------------------------------------------------------------------------
// (b) SIGKILL of the holder: kernel releases the lock, next open is immediate
// ----------------------------------------------------------------------------

/// After SIGKILL of the lock holder, a subsequent open must succeed
/// IMMEDIATELY (zero busy timeout) — kernel advisory locks die with the
/// process; there is no stale-lock recovery step.
#[test]
fn test_sigkill_holder_releases_lock_immediately() {
    let home_a = tempfile::tempdir().unwrap();
    let home_b = tempfile::tempdir().unwrap();
    // Zero timeout: the reopen below must succeed on the FIRST attempt.
    write_config(home_b.path(), "lock_timeout_ms = 0");

    let db_path = home_a.path().join("killed.vbsql");

    let mut holder = spawn_holder(&db_path, home_a.path());
    wait_until_holder_owns_lock_wal(&db_path);

    // Hard kill — no graceful shutdown, no drop handlers, no unlock call.
    holder.kill().expect("kill holder");
    holder.wait().expect("reap holder");

    // The very next open (zero busy timeout!) must acquire without retrying.
    let (ok, _out, err) = run_command_session(
        &db_path,
        home_b.path(),
        "CREATE TABLE after_kill (id INTEGER); INSERT INTO after_kill VALUES (7);",
    );
    assert!(ok, "open after SIGKILL of the holder must succeed immediately; stderr:\n{err}");
}

// ----------------------------------------------------------------------------
// (c) Stale checkpoint_*.tmp stubs and <stem>.wal.tmp cleaned on next open
// ----------------------------------------------------------------------------

/// Pre-seed the 32-byte `checkpoint_*.tmp` stubs (exactly what interrupted
/// checkpoint writers leave — one bare `CheckpointHeader`, zero data) plus a
/// `<stem>.wal.tmp` truncate scratch file, then open the database: both must
/// be removed, completed `.vchk` checkpoints must be untouched, and the data
/// must still be there.
#[test]
fn test_stale_tmp_stubs_cleaned_on_next_open() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("stubs.vbsql");
    let (wal_path, ckpt_dir) = wal_siblings(&db_path);

    // Session 1: create real durable state (checkpoint archive + WAL).
    let (ok, _out, err) = run_command_session(
        &db_path,
        home.path(),
        "CREATE TABLE keep (id INTEGER); INSERT INTO keep VALUES (1);",
    );
    assert!(ok, "seed session failed: {err}");
    assert!(ckpt_dir.exists(), "seed session must create the checkpoint archive");

    // Record the completed checkpoints that must survive cleanup untouched.
    let vchk_before: Vec<PathBuf> = fs::read_dir(&ckpt_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "vchk"))
        .collect();
    assert!(!vchk_before.is_empty(), "seed session must have written a .vchk checkpoint");

    // Pre-seed the stale temp files an interrupted writer would leave behind.
    let stub_a = ckpt_dir.join("checkpoint_9998.tmp");
    let stub_b = ckpt_dir.join("checkpoint_9999.tmp");
    let wal_tmp = wal_path.with_extension("wal.tmp");
    fs::write(&stub_a, [0u8; 32]).unwrap();
    fs::write(&stub_b, [0u8; 32]).unwrap();
    fs::write(&wal_tmp, b"orphaned truncate scratch").unwrap();

    // Session 2: opening the database must clean the stubs (under the
    // exclusive lock, before recovery) and still see the committed data.
    let (ok, stdout, err) = run_command_session(&db_path, home.path(), "SELECT id FROM keep;");
    assert!(ok, "reopen over stale stubs failed: {err}");
    assert!(stdout.contains("1"), "committed row must survive; got:\n{stdout}");

    assert!(!stub_a.exists(), "stale checkpoint_*.tmp stub must be removed on open");
    assert!(!stub_b.exists(), "stale checkpoint_*.tmp stub must be removed on open");
    assert!(!wal_tmp.exists(), "stale <stem>.wal.tmp must be removed on open");
    for vchk in &vchk_before {
        assert!(vchk.exists(), "completed checkpoint {} must be untouched", vchk.display());
    }

    // The lock sibling exists and is left in place (never renamed/deleted).
    assert!(db_path.with_extension("lock").exists(), ".lock sibling must be left in place");
}

// ----------------------------------------------------------------------------
// Snapshot-only mode (wal = false) takes the same lock
// ----------------------------------------------------------------------------

/// The snapshot-only path has the same last-writer-wins clobber, so it must
/// be protected by the same whole-session exclusive lock.
#[test]
fn test_snapshot_only_mode_is_locked_too() {
    let home_a = tempfile::tempdir().unwrap();
    let home_b = tempfile::tempdir().unwrap();
    write_config(home_a.path(), "wal = false");
    write_config(home_b.path(), "wal = false\nlock_timeout_ms = 0");

    let db_path = home_a.path().join("snapshot.vbsql");

    let mut holder = spawn_holder(&db_path, home_a.path());
    // No WAL siblings in snapshot-only mode: the zero-timeout probe observing
    // `database is locked` IS the marker that the holder owns the lock.
    wait_until_holder_owns_lock_probe(&db_path, home_b.path());

    // Clean release: a subsequent session succeeds.
    drop(holder.stdin.take());
    assert!(holder.wait().expect("holder wait").success());

    let (ok, _out, err) = run_command_session(&db_path, home_b.path(), "SELECT 1;");
    assert!(ok, "snapshot-only session must succeed after release; stderr:\n{err}");
}

// ----------------------------------------------------------------------------
// :memory: and no-database sessions take no lock and create no .lock file
// ----------------------------------------------------------------------------

#[test]
fn test_memory_and_no_database_sessions_create_no_lock_file() {
    let home = tempfile::tempdir().unwrap();
    let workdir = tempfile::tempdir().unwrap();

    let no_lock_files = |dir: &Path| {
        fs::read_dir(dir)
            .unwrap()
            .flatten()
            .all(|e| e.path().extension().is_none_or(|x| x != "lock"))
    };

    // :memory: session.
    let status = Command::new(vibesql_binary())
        .arg("--database")
        .arg(":memory:")
        .arg("-c")
        .arg("SELECT 1;")
        .env("HOME", home.path())
        .current_dir(workdir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run :memory: session");
    assert!(status.success());

    // No-database session.
    let status = Command::new(vibesql_binary())
        .arg("-c")
        .arg("SELECT 1;")
        .env("HOME", home.path())
        .current_dir(workdir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run no-database session");
    assert!(status.success());

    assert!(no_lock_files(workdir.path()), "no .lock file may be created in the working dir");
    assert!(no_lock_files(home.path()), "no .lock file may be created in $HOME");
}

// ----------------------------------------------------------------------------
// (d) TCL-shim flow: serial short-lived -c subprocesses keep working
// ----------------------------------------------------------------------------

/// The TCL runner touches its shared results database only through SERIAL,
/// short-lived `vibesql <db> -c "..."` subprocesses. Each acquires and
/// releases the exclusive lock in turn; none may ever see `database is
/// locked`. (Uses the default 5000 ms busy timeout, like the real shim.)
#[test]
fn test_serial_short_lived_sessions_never_contend() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("results.vbsql");

    let (ok, _out, err) = run_command_session(
        &db_path,
        home.path(),
        "CREATE TABLE log (id INTEGER PRIMARY KEY, note VARCHAR(20));",
    );
    assert!(ok, "create failed: {err}");

    for i in 1..=5 {
        let (ok, _out, err) = run_command_session(
            &db_path,
            home.path(),
            &format!("INSERT INTO log VALUES ({i}, 'run {i}');"),
        );
        assert!(ok, "serial session {i} failed: {err}");
        assert!(
            !err.contains("database is locked"),
            "serial sessions must never contend; session {i} stderr:\n{err}"
        );
    }

    let (ok, stdout, err) = run_command_session(&db_path, home.path(), "SELECT COUNT(*) FROM log;");
    assert!(ok, "count failed: {err}");
    assert!(stdout.contains("5"), "all 5 serial writes must be durable; got:\n{stdout}");
}

// ----------------------------------------------------------------------------
// The lock file itself is never locked-out by leftovers: repeated sessions
// reuse the same `<stem>.lock` (left in place on exit by design).
// ----------------------------------------------------------------------------

#[test]
fn test_lock_file_left_in_place_and_reused() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("reuse.vbsql");
    let lock_path = db_path.with_extension("lock");

    let (ok, _out, _err) =
        run_command_session(&db_path, home.path(), "CREATE TABLE r (id INTEGER);");
    assert!(ok);
    assert!(lock_path.exists(), ".lock must exist after a file-backed session");

    // Second session with the leftover .lock present: must open fine.
    let (ok, _out, err) = run_command_session(&db_path, home.path(), "SELECT 1;");
    assert!(ok, "leftover .lock must not block a fresh session: {err}");
    assert!(lock_path.exists(), ".lock is left in place (never renamed or deleted)");
}
