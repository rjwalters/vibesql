//! TPC-H Q7/Q8/Q9 SQLite-parity verification (strftime-dependent queries)
//!
//! The SQLite-flavored TPC-H query text in `vibesql-bench-common` uses
//! `strftime('%Y', date_col)` in place of `EXTRACT(YEAR FROM date_col)`
//! (see `crates/vibesql-bench-common/src/tpch/queries.rs`). This test runs
//! those queries against both VibeSQL and SQLite on identical SF 0.01 data
//! and asserts the results match, which is the acceptance criterion for
//! issue #5282 (Q8/Q9 previously failed with `no such function: strftime`).
//!
//! Requires the `sqlite` feature:
//!
//! ```sh
//! cargo test -p vibesql-executor --release --features sqlite \
//!     --test tpch_strftime_sqlite_parity
//! ```
//!
//! Note: run with `--release`; TPC-H joins are impractically slow in debug
//! builds even at SF 0.01.
#![cfg(feature = "sqlite")]

use vibesql_bench_common::tpch::{
    queries::{TPCH_Q7_SQLITE, TPCH_Q8_SQLITE, TPCH_Q9_SQLITE},
    schema::{load_sqlite, load_vibesql},
};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

const SCALE_FACTOR: f64 = 0.01;

fn vibesql_rows(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<String>> {
    let stmt = Parser::parse_sql(sql).unwrap();
    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("expected SELECT"),
    };
    let executor = SelectExecutor::new(db);
    let rows = executor.execute(&select).unwrap();
    rows.iter().map(|r| r.values.iter().map(normalize_vibesql).collect()).collect()
}

/// Normalize values to tagged strings so TEXT-vs-numeric mismatches are
/// detected (SQLite's strftime returns TEXT; VibeSQL must match).
fn normalize_vibesql(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Varchar(s) | SqlValue::Character(s) => format!("T:{}", s),
        SqlValue::Integer(n) => format!("N:{:.6}", *n as f64),
        SqlValue::Bigint(n) => format!("N:{:.6}", *n as f64),
        SqlValue::Smallint(n) => format!("N:{:.6}", *n as f64),
        SqlValue::Float(n) => format!("N:{:.6}", *n as f64),
        SqlValue::Double(n) | SqlValue::Real(n) | SqlValue::Numeric(n) => format!("N:{:.6}", n),
        other => format!("O:{:?}", other),
    }
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let ncols = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut out = Vec::with_capacity(ncols);
            for i in 0..ncols {
                let val = match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => "NULL".to_string(),
                    rusqlite::types::ValueRef::Integer(n) => format!("N:{:.6}", n as f64),
                    rusqlite::types::ValueRef::Real(f) => format!("N:{:.6}", f),
                    rusqlite::types::ValueRef::Text(t) => {
                        format!("T:{}", String::from_utf8_lossy(t))
                    }
                    rusqlite::types::ValueRef::Blob(_) => "BLOB".to_string(),
                };
                out.push(val);
            }
            Ok(out)
        })
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn values_match(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    // Numeric comparison with relative tolerance (float summation order differs)
    if let (Some(x), Some(y)) = (
        a.strip_prefix("N:").and_then(|s| s.parse::<f64>().ok()),
        b.strip_prefix("N:").and_then(|s| s.parse::<f64>().ok()),
    ) {
        let denom = x.abs().max(y.abs()).max(1.0);
        return ((x - y) / denom).abs() < 1e-6;
    }
    false
}

#[test]
fn tpch_q7_q8_q9_match_sqlite() {
    let vdb = load_vibesql(SCALE_FACTOR);
    let sconn = load_sqlite(SCALE_FACTOR);

    // At SF 0.01 the official Q7/Q8 filters are so selective that both engines
    // return 0 rows, which makes the parity check vacuous for those queries.
    // Relaxed variants keep the exact query shape (strftime in SELECT and
    // GROUP BY) while widening one data filter so rows flow through. The
    // widened filters stay bounded — do NOT relax Q7 to `n1 <> n2` (600 nation
    // pairs), which makes the join intermediate explode.
    let q7_relaxed = TPCH_Q7_SQLITE.replace(
        "((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')\n         OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))",
        "n1.n_name IN ('FRANCE', 'GERMANY', 'RUSSIA', 'ROMANIA', 'UNITED KINGDOM')\n         AND n2.n_name IN ('FRANCE', 'GERMANY', 'RUSSIA', 'ROMANIA', 'UNITED KINGDOM')",
    );
    assert_ne!(q7_relaxed, TPCH_Q7_SQLITE, "Q7 filter text changed; update the replacement");
    let q8_relaxed = TPCH_Q8_SQLITE
        .replace("AND p_type = 'ECONOMY ANODIZED STEEL'", "AND p_type LIKE '%STEEL%'");
    assert_ne!(q8_relaxed, TPCH_Q8_SQLITE, "Q8 filter text changed; update the replacement");

    let mut nonempty = 0;
    for (name, sql) in [
        ("Q7", TPCH_Q7_SQLITE),
        ("Q8", TPCH_Q8_SQLITE),
        ("Q9", TPCH_Q9_SQLITE),
        ("Q7-relaxed", q7_relaxed.as_str()),
        ("Q8-relaxed", q8_relaxed.as_str()),
    ] {
        let v = vibesql_rows(&vdb, sql);
        let s = sqlite_rows(&sconn, sql);
        assert_eq!(
            v.len(),
            s.len(),
            "{}: row count mismatch (vibesql={}, sqlite={})",
            name,
            v.len(),
            s.len()
        );
        for (i, (vr, sr)) in v.iter().zip(s.iter()).enumerate() {
            assert_eq!(vr.len(), sr.len(), "{} row {}: column count mismatch", name, i);
            for (j, (va, sa)) in vr.iter().zip(sr.iter()).enumerate() {
                assert!(
                    values_match(va, sa),
                    "{} row {} col {}: vibesql={} sqlite={}",
                    name,
                    i,
                    j,
                    va,
                    sa
                );
            }
        }
        if !v.is_empty() {
            nonempty += 1;
        }
        println!("{}: {} rows match SQLite", name, v.len());
    }

    // Guard against a vacuous pass: Q9 and the relaxed Q7/Q8 variants must
    // produce rows through the strftime GROUP BY at SF 0.01.
    assert!(nonempty >= 3, "expected >= 3 non-empty result sets, got {}", nonempty);
}
