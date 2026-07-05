// ============================================================================
// Recovery failure-surfacing tests — issue #5807
// ============================================================================
//
// The CLI must NEVER silently present an empty (or stale) database when the
// on-disk state cannot be read. These subprocess tests pin the user-visible
// contract (exit codes + stderr), which is the level at which the original
// incident manifested: a forward-format-version checkpoint opened as
// `SELECT name FROM sqlite_master` → 0 rows, exit 0, empty stderr.
//
//   * Checkpoint written by a newer binary (embedded format vN+1, valid CRC) → hard error naming
//     the version mismatch; no fallback to an older checkpoint (stale-state resurrection is also
//     data loss); no new checkpoint written (the emptiness must never be checkpointed as "real"
//     state).
//   * Corrupt newest checkpoint with a valid older one → hard error by default;
//     `--recover-fallback` opts into older-checkpoint recovery with a loud stderr report of what
//     was skipped.
//   * Legacy snapshot-only `.vbsql` (no checkpoint archive, no WAL) → opens WITH its data under the
//     WAL default instead of being silently ignored.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{
    wal::{CheckpointHeader, CheckpointWriter},
    Database, Row,
};
use vibesql_types::{DataType, SqlValue};

fn vibesql_binary() -> &'static str {
    env!("CARGO_BIN_EXE_vibesql")
}

/// Run `vibesql <db> [extra args] -c <sql>` with an isolated `$HOME` (no
/// user `~/.vibesqlrc`; WAL stays at its on-by-default setting).
fn run_cli(home: &Path, db: &Path, extra_args: &[&str], sql: &str) -> Output {
    Command::new(vibesql_binary())
        .arg(db.to_string_lossy().to_string())
        .args(extra_args)
        .arg("-c")
        .arg(sql)
        .env("HOME", home)
        .output()
        .expect("failed to run vibesql")
}

fn simple_schema(name: &str) -> TableSchema {
    TableSchema::new(
        name.to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, true)],
    )
}

/// Write a real checkpoint containing a single one-row table `table_name`
/// into `<db stem>-checkpoints/`, at the given LSN. Returns the file path.
fn write_checkpoint(db_path: &Path, lsn: u64, table_name: &str) -> PathBuf {
    let stem = db_path.file_stem().unwrap().to_string_lossy().to_string();
    let dir = db_path.parent().unwrap().join(format!("{stem}-checkpoints"));

    let mut db = Database::new();
    db.create_table(simple_schema(table_name)).unwrap();
    db.insert_row(table_name, Row::from_vec(vec![SqlValue::Integer(1)])).unwrap();

    let data = db.to_uncompressed_bytes().unwrap();
    let mut writer = CheckpointWriter::new(&dir).unwrap();
    writer.create_checkpoint(lsn, &data, 1).unwrap().path
}

/// Patch a checkpoint's *embedded* VBSQL snapshot version byte (file offset
/// 37, after the 32-byte envelope header + 5-byte "VBSQL" magic) to 255 and
/// re-stamp the envelope CRC (bytes 28..32; for v2 envelopes it covers the
/// 28-byte header prefix + payload) so the envelope is fully valid — exactly
/// what a checkpoint written by a newer VibeSQL binary looks like to this one.
fn bump_embedded_format_version(path: &Path) {
    let mut bytes = fs::read(path).unwrap();
    assert_eq!(&bytes[..4], b"VCHK", "not a checkpoint envelope");
    assert_eq!(&bytes[32..37], b"VBSQL", "payload is not a binary snapshot");
    bytes[37] = 255;
    let header = CheckpointHeader::read(&mut &bytes[..]).unwrap();
    let crc = header.expected_checksum(&bytes[32..]);
    bytes[28..32].copy_from_slice(&crc.to_le_bytes());
    fs::write(path, bytes).unwrap();
}

/// Corrupt a checkpoint's payload without fixing the envelope CRC.
fn corrupt_payload(path: &Path) {
    let mut bytes = fs::read(path).unwrap();
    assert!(bytes.len() > 40);
    bytes[40] ^= 0xFF;
    fs::write(path, bytes).unwrap();
}

fn count_checkpoint_files(db_path: &Path) -> usize {
    let stem = db_path.file_stem().unwrap().to_string_lossy().to_string();
    let dir = db_path.parent().unwrap().join(format!("{stem}-checkpoints"));
    match fs::read_dir(dir) {
        Ok(entries) => entries
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "vchk"))
            .count(),
        Err(_) => 0,
    }
}

/// The original incident: every checkpoint claims a newer format version.
/// Open must fail hard with a clear message — never `sqlite_master` → 0 rows,
/// exit 0 — and must not checkpoint the emptiness as new "real" state.
#[test]
fn test_forward_version_checkpoint_is_hard_open_error() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("future.vbsql");

    let checkpoint = write_checkpoint(&db_path, 1, "precious_data");
    bump_embedded_format_version(&checkpoint);

    let out = run_cli(home.path(), &db_path, &[], "SELECT name FROM sqlite_master");
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        !out.status.success(),
        "open must fail; stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        stderr.contains("newer version of VibeSQL"),
        "stderr must explain the version mismatch; was: {stderr}"
    );
    assert_eq!(
        count_checkpoint_files(&db_path),
        1,
        "no new checkpoint may be written after a failed open"
    );
}

/// Newest checkpoint forward-version with an older *valid* checkpoint
/// present: still a hard error — silently resurrecting the older (stale)
/// state is also data loss. `--recover-fallback` must NOT override this.
#[test]
fn test_forward_version_newest_never_falls_back_to_older() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("mixed.vbsql");

    write_checkpoint(&db_path, 1, "older_state");
    let newest = write_checkpoint(&db_path, 2, "newest_state");
    bump_embedded_format_version(&newest);

    for extra in [&[][..], &["--recover-fallback"][..]] {
        let out = run_cli(home.path(), &db_path, extra, "SELECT name FROM sqlite_master");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !out.status.success(),
            "must not fall back past a forward-version checkpoint (args: {extra:?}); \
             stdout: {}",
            String::from_utf8_lossy(&out.stdout)
        );
        assert!(
            stderr.contains("newer version of VibeSQL"),
            "stderr must explain the version mismatch (args: {extra:?}); was: {stderr}"
        );
    }
}

/// Corrupt newest checkpoint, valid older one: refuse to open by default
/// (naming the file and the opt-in); with `--recover-fallback`, open the
/// older state and report the skipped checkpoint loudly on stderr.
#[test]
fn test_corrupt_newest_checkpoint_default_error_fallback_optin_loud() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("corrupt.vbsql");

    write_checkpoint(&db_path, 1, "older_state");
    let newest = write_checkpoint(&db_path, 2, "newest_state");
    corrupt_payload(&newest);

    // Default: hard error naming the unreadable checkpoint and the opt-in.
    let out = run_cli(home.path(), &db_path, &[], "SELECT name FROM sqlite_master");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "corrupt newest checkpoint must refuse to open by default");
    assert!(
        stderr.contains(&newest.display().to_string()),
        "stderr must name the unreadable checkpoint; was: {stderr}"
    );
    assert!(
        stderr.contains("recover-fallback"),
        "stderr must mention the --recover-fallback opt-in; was: {stderr}"
    );

    // Opt-in: opens the older state, loudly.
    let out =
        run_cli(home.path(), &db_path, &["--recover-fallback"], "SELECT name FROM sqlite_master");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "opt-in fallback must open; stderr: {stderr}");
    assert!(stdout.contains("older_state"), "older state must be visible; stdout: {stdout}");
    assert!(!stdout.contains("newest_state"));
    assert!(
        stderr.contains("WARNING") && stderr.contains(&newest.display().to_string()),
        "fallback must be loud and name the skipped checkpoint; was: {stderr}"
    );
}

/// Legacy snapshot-only `.vbsql` (written before WAL mode: no checkpoint
/// archive, no `.wal` sibling) must open WITH its data under the WAL default —
/// and the data must survive the session's checkpoint so subsequent opens
/// (which then recover from the checkpoint archive) still see it.
#[test]
fn test_legacy_snapshot_only_file_opens_with_data() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("legacy.vbsql");

    let mut db = Database::new();
    db.create_table(simple_schema("legacy_table")).unwrap();
    db.insert_row("legacy_table", Row::from_vec(vec![SqlValue::Integer(42)])).unwrap();
    db.save(&db_path).unwrap();
    assert_eq!(count_checkpoint_files(&db_path), 0, "precondition: snapshot-only");

    let out = run_cli(home.path(), &db_path, &[], "SELECT id FROM legacy_table");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "legacy snapshot must open; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("42"), "legacy snapshot data must be visible; stdout: {stdout}");

    // Reopen: whatever the first session checkpointed must still contain the
    // legacy data (the first WAL session must not bury it under an empty
    // checkpoint).
    let out = run_cli(home.path(), &db_path, &[], "SELECT id FROM legacy_table");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(stdout.contains("42"), "legacy data must survive reopen; stdout: {stdout}");
}

/// Issue #5855 gap 2: flipping ANY byte of the newest checkpoint — header
/// fields (LSN at 8..16, timestamp at 16..24, num_tables at 24..28, checksum
/// at 28..32) or body (catalog region, data region, tail) — must refuse to
/// open by default. Before the v2 envelope checksum, header-field flips
/// passed verification: a flipped LSN silently changed checkpoint selection
/// and WAL replay with exit 0.
#[test]
fn test_checkpoint_byte_flip_sweep_refuses_to_open() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("flip.vbsql");

    let checkpoint = write_checkpoint(&db_path, 5, "precious_data");
    let original = fs::read(&checkpoint).unwrap();
    assert!(original.len() > 64, "need a body to corrupt; got {} bytes", original.len());

    // Sanity: the pristine checkpoint opens with its data.
    let out = run_cli(home.path(), &db_path, &[], "SELECT id FROM precious_data");
    assert!(
        out.status.success() && String::from_utf8_lossy(&out.stdout).contains('1'),
        "pristine checkpoint must open; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Representative offsets: every header field + body start / middle / tail.
    let offsets = [4usize, 8, 12, 16, 20, 24, 28, 32, 40, original.len() / 2, original.len() - 1];
    for offset in offsets {
        let mut corrupted = original.clone();
        corrupted[offset] ^= 0xFF;
        fs::write(&checkpoint, &corrupted).unwrap();

        let out = run_cli(home.path(), &db_path, &[], "SELECT id FROM precious_data");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            !out.status.success(),
            "byte flip at offset {offset} must refuse to open; stdout: {stdout}"
        );
    }

    // Restore: still opens (the sweep failures came from the flips alone).
    fs::write(&checkpoint, &original).unwrap();
    let out = run_cli(home.path(), &db_path, &[], "SELECT id FROM precious_data");
    assert!(out.status.success(), "restored checkpoint must open again");
}

/// Issue #5855 gap 2 (stale-selection variant): flip the newest checkpoint's
/// header LSN so a STALE checkpoint sorts newest. The corrupt file itself is
/// never the one loaded, so only the creation-order guard stands between the
/// user and a silent stale open. Default: hard error naming the suspect
/// file; `--recover-fallback`: opens the older state loudly.
#[test]
fn test_header_lsn_flip_stale_selection_default_error_fallback_loud() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("lsnflip.vbsql");

    write_checkpoint(&db_path, 10, "older_state");
    let newest = write_checkpoint(&db_path, 20, "newest_state");

    // Zero the newest checkpoint's header LSN (bytes 8..16) so it sorts as
    // the oldest; everything else, including the stored checksum, is intact.
    let mut bytes = fs::read(&newest).unwrap();
    bytes[8..16].copy_from_slice(&0u64.to_le_bytes());
    fs::write(&newest, &bytes).unwrap();

    // Default: refuse to open, naming the suspect checkpoint.
    let out = run_cli(home.path(), &db_path, &[], "SELECT name FROM sqlite_master");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "LSN-flipped newest checkpoint must refuse to open by default; stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        stderr.contains(&newest.display().to_string()),
        "stderr must name the suspect checkpoint; was: {stderr}"
    );
    assert!(stderr.contains("recover-fallback"), "must mention the opt-in; was: {stderr}");

    // Opt-in: opens the older state, loudly naming the suspect file.
    let out =
        run_cli(home.path(), &db_path, &["--recover-fallback"], "SELECT name FROM sqlite_master");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "opt-in fallback must open; stderr: {stderr}");
    assert!(stdout.contains("older_state"), "older state must be visible; stdout: {stdout}");
    assert!(
        stderr.contains("WARNING") && stderr.contains(&newest.display().to_string()),
        "fallback must be loud and name the suspect checkpoint; was: {stderr}"
    );
}

/// Issue #5855 gap 1 (regression): SQLite-style user-defined column type
/// names (`banana`, quoted names, parameterized forms, fallback keywords)
/// must round-trip through save + reopen. The reload used to error on the
/// unknown name and the open path swallowed it, presenting an empty database
/// with exit 0.
#[test]
fn test_userdefined_column_types_roundtrip_on_reopen() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("udt.vbsql");

    let out = run_cli(
        home.path(),
        &db_path,
        &[],
        "CREATE TABLE t(a banana, b \"my weird type\", c fruit(8), d nvarchar(70), \
         e counter(+5,-6), f attach); \
         INSERT INTO t VALUES (1,'x',2.5,'y',9,'z');",
    );
    assert!(
        out.status.success(),
        "create must succeed; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Reopen in a fresh process: the table (and its rows) must still exist.
    let out = run_cli(home.path(), &db_path, &[], "SELECT a, f FROM t");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "reopen must not error; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains('1') && stdout.contains('z'),
        "row data must survive reopen; stdout: {stdout}"
    );

    // The verbatim CREATE source must survive too.
    let out = run_cli(home.path(), &db_path, &[], "SELECT sql FROM sqlite_master WHERE name='t'");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(
        stdout.contains("banana") && stdout.contains("my weird type"),
        "original type names must survive reopen; stdout: {stdout}"
    );
}

/// A fresh, nonexistent database file still opens as an empty database — the
/// hard-error paths must not break the legitimate empty-start case.
#[test]
fn test_fresh_database_still_opens_empty() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("brand_new.vbsql");

    let out = run_cli(home.path(), &db_path, &[], "CREATE TABLE t(id INTEGER); SELECT 1;");
    assert!(
        out.status.success(),
        "fresh database must open; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
