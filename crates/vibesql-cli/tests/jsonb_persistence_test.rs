// ============================================================================
// JSONB BLOB durability integration tests — issue #6033 (Stage 3)
// ============================================================================
//
// Since Stage 1 (#6035), `jsonb()` / `jsonb_*()` emit real `SqlValue::Blob`
// output rather than JSON text. Stage 3 verifies that a JSONB BLOB column
// survives the full file-backed durability path — WAL write + checkpoint
// (`\save`, i.e. a clean exit that writes a checkpoint and truncates the WAL) +
// reopen — byte-for-byte identical, and that `json_each` / `json_tree` over the
// re-loaded blob still decode correctly (mirrors json102-1000/1000b against real
// file storage rather than the in-memory TCL harness).
//
// The shared value codec (`persistence/binary/value.rs`) is used by both the WAL
// entry codec and the btree/checkpoint page format, so these tests exercise the
// one path that both durability mechanisms share. See the curator re-scope on
// issue #6033 for the storage-layer analysis.

#![cfg(unix)]

use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

fn vibesql_binary() -> &'static str {
    env!("CARGO_BIN_EXE_vibesql")
}

/// Run a one-shot `vibesql <db> < script` invocation to completion (clean exit).
///
/// A clean exit in script mode writes a checkpoint and truncates the WAL on the
/// way out — the CLI's `\save` equivalent — so a subsequent invocation on the
/// same file re-opens from the checkpoint + any residual WAL.
fn run_script(binary: &str, db: &Path, home: &Path, script: &str) -> String {
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
        // Drop stdin (EOF) → script mode runs to completion and exits cleanly.
    }
    let out = child.wait_with_output().expect("vibesql did not exit");
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// Extract the `hex(col)` uppercase-hex tokens (one per data row) from a
/// pretty-printed `SELECT name, hex(col) ...` table, keyed by the name column.
/// Returns `(name, hexbytes)` pairs in table order. The parser is deliberately
/// simple: it keys off the fact that a JSONB blob renders as a long run of
/// `[0-9A-F]` and the name is a plain word in the first cell.
fn parse_name_hex_rows(table: &str) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    for line in table.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = trimmed.trim_matches('|').split('|').map(str::trim).collect();
        if cells.len() != 2 {
            continue;
        }
        let (name, hex) = (cells[0], cells[1]);
        // Skip the header row and the hex-column header.
        if hex.eq_ignore_ascii_case("hex(phoneb)") || name == "name" {
            continue;
        }
        // A data row's second cell must be pure uppercase hex.
        if !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            rows.push((name.to_string(), hex.to_string()));
        }
    }
    rows
}

/// A `jsonb()`-produced BLOB column survives WAL write + checkpoint (`\save`
/// equivalent) + reopen byte-for-byte, and `json_each` over the reloaded blob
/// still decodes correctly (mirrors json102-1000b against real file storage).
#[test]
fn test_jsonb_blob_survives_checkpoint_reopen_byte_identical() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("jsonb_durable.vbsql");
    let bin = vibesql_binary();

    // Process 1: build the table, materialise JSONB blobs via UPDATE ... SET
    // col=jsonb(other_col), and record the pre-checkpoint hex.
    let seed = "CREATE TABLE user(name,phone,phoneb);\n\
         INSERT INTO user(name,phone) VALUES\n\
           ('Alice','[\"604-555\"]'),\n\
           ('Bob','[\"604-666\"]'),\n\
           ('Cindy','[\"704-111\"]'),\n\
           ('Dave','[\"704-222\"]');\n\
         UPDATE user SET phoneb=jsonb(phone);\n\
         SELECT name, hex(phoneb) FROM user ORDER BY name;\n";
    let before = run_script(bin, &db_path, home.path(), seed);
    let before_rows = parse_name_hex_rows(&before);
    assert_eq!(
        before_rows.len(),
        4,
        "expected 4 JSONB blob rows before checkpoint; got:\n{before}"
    );
    // Sanity: the blobs are non-trivial (a JSONB blob is more than a couple of
    // bytes) so the byte-comparison is meaningful.
    assert!(
        before_rows.iter().all(|(_, h)| h.len() >= 4),
        "JSONB blobs should be non-trivial; got:\n{before}"
    );

    // Process 2: re-open the checkpointed file and re-read the hex — must be
    // byte-identical to the pre-checkpoint values.
    let after = run_script(
        bin,
        &db_path,
        home.path(),
        "SELECT name, hex(phoneb) FROM user ORDER BY name;\n",
    );
    let after_rows = parse_name_hex_rows(&after);
    assert_eq!(
        before_rows, after_rows,
        "JSONB blob bytes must survive checkpoint + reopen byte-identically.\n\
         before:\n{before}\nafter:\n{after}"
    );

    // Process 3: json_each over the reloaded JSONB blob still decodes correctly.
    // Mirrors json102-1000b: only the 704-* numbers match.
    let each = run_script(
        bin,
        &db_path,
        home.path(),
        "SELECT DISTINCT user.name FROM user, json_each(user.phoneb)\n\
           WHERE json_each.value LIKE '704-%' ORDER BY 1;\n",
    );
    assert!(
        each.contains("Cindy") && each.contains("Dave"),
        "json_each over a reloaded JSONB blob must still decode (expect Cindy, Dave); got:\n{each}"
    );
    assert!(
        !each.contains("Alice") && !each.contains("Bob"),
        "json_each filter must exclude the 604-* rows; got:\n{each}"
    );
}

/// `json_tree` over a reloaded JSONB blob decodes the nested structure
/// correctly (companion to the `json_each` read-back; mirrors json102-1000).
#[test]
fn test_jsonb_blob_json_tree_reads_back_after_reopen() {
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("jsonb_tree.vbsql");
    let bin = vibesql_binary();

    // Store a nested JSONB document, checkpoint on exit.
    run_script(
        bin,
        &db_path,
        home.path(),
        "CREATE TABLE t(id, doc);\n\
         INSERT INTO t(id) VALUES (1);\n\
         UPDATE t SET doc=jsonb('{\"a\":[10,20],\"b\":{\"c\":30}}');\n",
    );

    // Re-open and walk the tree: the leaf integer values must all be present.
    let out = run_script(
        bin,
        &db_path,
        home.path(),
        "SELECT json_tree.value FROM t, json_tree(t.doc)\n\
           WHERE json_tree.type NOT IN ('object','array')\n\
           ORDER BY CAST(json_tree.value AS INTEGER);\n",
    );
    for leaf in ["10", "20", "30"] {
        assert!(
            out.contains(leaf),
            "json_tree over a reloaded JSONB blob must yield leaf {leaf}; got:\n{out}"
        );
    }
}
