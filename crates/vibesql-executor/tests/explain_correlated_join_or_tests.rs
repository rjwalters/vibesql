//! Tests for EXPLAIN QUERY PLAN rendering of the **correlated-join**
//! MULTI-INDEX OR access path (epic #5668, PR 5 — where9-3.1/3.2).
//!
//! When a 2-table (CROSS or LEFT) join has an OR join predicate
//! `(t1.c=t2.c AND t1.d=t2.d) OR t1.f=t2.f`, SQLite drives the outer table by
//! rowid and performs a correlated MULTI-INDEX OR on the inner table. EQP must
//! render:
//!
//! ```text
//! QUERY PLAN
//! |--SEARCH t1 USING INTEGER PRIMARY KEY (rowid=?)
//! `--MULTI-INDEX OR
//!    |--INDEX 1
//!    |  `--SEARCH t2 USING INDEX t2d (d=?)
//!    `--INDEX 3
//!       `--SEARCH t2 USING COVERING INDEX t2f (f=?)
//! ```
//!
//! Every expected shape below was verified live against sqlite3 3.51.0 on the
//! canonical where9.test fixture (the where9-3.1/3.2 conformance cases whose
//! harness skips were removed in this PR). The runtime already returns correct
//! rows via nested-loop scan; this PR fixes the EQP access-path rendering.

use std::sync::Mutex;

use vibesql_ast::Statement;
use vibesql_executor::ExplainExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Serializes the `MULTI_INDEX_OR_DISABLED` env-var mutation against the EQP
/// assertions, since `cargo test` runs the tests in this binary in parallel and
/// the kill switch is process-global.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn run(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateIndex(s) => {
            vibesql_executor::CreateIndexExecutor::execute(&s, db).unwrap();
        }
        Statement::Insert(i) => {
            vibesql_executor::InsertExecutor::execute(db, &i).unwrap();
        }
        other => panic!("unsupported setup statement: {other:?}"),
    }
}

fn eqp(db: &Database, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let stmt = Parser::parse_sql(&explain_sql).expect("Failed to parse SQL");
    if let Statement::Explain(explain_stmt) = stmt {
        let result = ExplainExecutor::execute(&explain_stmt, db).expect("EXPLAIN failed");
        result.to_sqlite_eqp()
    } else {
        panic!("Expected EXPLAIN statement");
    }
}

/// The canonical where9 t1/t2 fixture (sqlite3 where9.test): 99 rows over
/// (a,b,c,d,e,f,g). t1 has single-column indexes; t2 is a copy of t1 with the
/// multi-column index set the MULTI-INDEX OR branches resolve against
/// (t2c(c,e), t2d(d,g), t2f(f,b,d,c), ...). No ANALYZE is run, mirroring the
/// conformance harness; the inner-branch index choice is made deterministically
/// from the actual column cardinalities (d more selective than c → t2d).
const WHERE9_ROWS: &[&str] = &[
    "1,11,1001,1.001,100.1,'bcdefghij','yxwvuts'",
    "2,22,1001,2.002,100.1,'cdefghijk','yxwvuts'",
    "3,33,1001,3.003,100.1,'defghijkl','xwvutsr'",
    "4,44,2002,4.004,200.2,'efghijklm','xwvutsr'",
    "5,55,2002,5.005,200.2,'fghijklmn','xwvutsr'",
    "6,66,2002,6.006,200.2,'ghijklmno','xwvutsr'",
    "7,77,3003,7.007,300.3,'hijklmnop','xwvutsr'",
    "8,88,3003,8.008,300.3,'ijklmnopq','wvutsrq'",
    "9,99,3003,9.009,300.3,'jklmnopqr','wvutsrq'",
    "10,110,4004,10.01,400.4,'klmnopqrs','wvutsrq'",
    "11,121,4004,11.011,400.4,'lmnopqrst','wvutsrq'",
    "12,132,4004,12.012,400.4,'mnopqrstu','wvutsrq'",
    "13,143,5005,13.013,500.5,'nopqrstuv','vutsrqp'",
    "14,154,5005,14.014,500.5,'opqrstuvw','vutsrqp'",
    "15,165,5005,15.015,500.5,'pqrstuvwx','vutsrqp'",
    "16,176,6006,16.016,600.6,'qrstuvwxy','vutsrqp'",
    "17,187,6006,17.017,600.6,'rstuvwxyz','vutsrqp'",
    "18,198,6006,18.018,600.6,'stuvwxyza','utsrqpo'",
    "19,209,7007,19.019,700.7,'tuvwxyzab','utsrqpo'",
    "20,220,7007,20.02,700.7,'uvwxyzabc','utsrqpo'",
    "21,231,7007,21.021,700.7,'vwxyzabcd','utsrqpo'",
    "22,242,8008,22.022,800.8,'wxyzabcde','utsrqpo'",
    "23,253,8008,23.023,800.8,'xyzabcdef','tsrqpon'",
    "24,264,8008,24.024,800.8,'yzabcdefg','tsrqpon'",
    "25,275,9009,25.025,900.9,'zabcdefgh','tsrqpon'",
    "26,286,9009,26.026,900.9,'abcdefghi','tsrqpon'",
    "27,297,9009,27.027,900.9,'bcdefghij','tsrqpon'",
    "28,308,10010,28.028,1001.0,'cdefghijk','srqponm'",
    "29,319,10010,29.029,1001.0,'defghijkl','srqponm'",
    "30,330,10010,30.03,1001.0,'efghijklm','srqponm'",
    "31,341,11011,31.031,1101.1,'fghijklmn','srqponm'",
    "32,352,11011,32.032,1101.1,'ghijklmno','srqponm'",
    "33,363,11011,33.033,1101.1,'hijklmnop','rqponml'",
    "34,374,12012,34.034,1201.2,'ijklmnopq','rqponml'",
    "35,385,12012,35.035,1201.2,'jklmnopqr','rqponml'",
    "36,396,12012,36.036,1201.2,'klmnopqrs','rqponml'",
    "37,407,13013,37.037,1301.3,'lmnopqrst','rqponml'",
    "38,418,13013,38.038,1301.3,'mnopqrstu','qponmlk'",
    "39,429,13013,39.039,1301.3,'nopqrstuv','qponmlk'",
    "40,440,14014,40.04,1401.4,'opqrstuvw','qponmlk'",
    "41,451,14014,41.041,1401.4,'pqrstuvwx','qponmlk'",
    "42,462,14014,42.042,1401.4,'qrstuvwxy','qponmlk'",
    "43,473,15015,43.043,1501.5,'rstuvwxyz','ponmlkj'",
    "44,484,15015,44.044,1501.5,'stuvwxyza','ponmlkj'",
    "45,495,15015,45.045,1501.5,'tuvwxyzab','ponmlkj'",
    "46,506,16016,46.046,1601.6,'uvwxyzabc','ponmlkj'",
    "47,517,16016,47.047,1601.6,'vwxyzabcd','ponmlkj'",
    "48,528,16016,48.048,1601.6,'wxyzabcde','onmlkji'",
    "49,539,17017,49.049,1701.7,'xyzabcdef','onmlkji'",
    "50,550,17017,50.05,1701.7,'yzabcdefg','onmlkji'",
    "51,561,17017,51.051,1701.7,'zabcdefgh','onmlkji'",
    "52,572,18018,52.052,1801.8,'abcdefghi','onmlkji'",
    "53,583,18018,53.053,1801.8,'bcdefghij','nmlkjih'",
    "54,594,18018,54.054,1801.8,'cdefghijk','nmlkjih'",
    "55,605,19019,55.055,1901.9,'defghijkl','nmlkjih'",
    "56,616,19019,56.056,1901.9,'efghijklm','nmlkjih'",
    "57,627,19019,57.057,1901.9,'fghijklmn','nmlkjih'",
    "58,638,20020,58.058,2002.0,'ghijklmno','mlkjihg'",
    "59,649,20020,59.059,2002.0,'hijklmnop','mlkjihg'",
    "60,660,20020,60.06,2002.0,'ijklmnopq','mlkjihg'",
    "61,671,21021,61.061,2102.1,'jklmnopqr','mlkjihg'",
    "62,682,21021,62.062,2102.1,'klmnopqrs','mlkjihg'",
    "63,693,21021,63.063,2102.1,'lmnopqrst','lkjihgf'",
    "64,704,22022,64.064,2202.2,'mnopqrstu','lkjihgf'",
    "65,715,22022,65.065,2202.2,'nopqrstuv','lkjihgf'",
    "66,726,22022,66.066,2202.2,'opqrstuvw','lkjihgf'",
    "67,737,23023,67.067,2302.3,'pqrstuvwx','lkjihgf'",
    "68,748,23023,68.068,2302.3,'qrstuvwxy','kjihgfe'",
    "69,759,23023,69.069,2302.3,'rstuvwxyz','kjihgfe'",
    "70,770,24024,70.07,2402.4,'stuvwxyza','kjihgfe'",
    "71,781,24024,71.071,2402.4,'tuvwxyzab','kjihgfe'",
    "72,792,24024,72.072,2402.4,'uvwxyzabc','kjihgfe'",
    "73,803,25025,73.073,2502.5,'vwxyzabcd','jihgfed'",
    "74,814,25025,74.074,2502.5,'wxyzabcde','jihgfed'",
    "75,825,25025,75.075,2502.5,'xyzabcdef','jihgfed'",
    "76,836,26026,76.076,2602.6,'yzabcdefg','jihgfed'",
    "77,847,26026,77.077,2602.6,'zabcdefgh','jihgfed'",
    "78,858,26026,78.078,2602.6,'abcdefghi','ihgfedc'",
    "79,869,27027,79.079,2702.7,'bcdefghij','ihgfedc'",
    "80,880,27027,80.08,2702.7,'cdefghijk','ihgfedc'",
    "81,891,27027,81.081,2702.7,'defghijkl','ihgfedc'",
    "82,902,28028,82.082,2802.8,'efghijklm','ihgfedc'",
    "83,913,28028,83.083,2802.8,'fghijklmn','hgfedcb'",
    "84,924,28028,84.084,2802.8,'ghijklmno','hgfedcb'",
    "85,935,29029,85.085,2902.9,'hijklmnop','hgfedcb'",
    "86,946,29029,86.086,2902.9,'ijklmnopq','hgfedcb'",
    "87,957,29029,87.087,2902.9,'jklmnopqr','hgfedcb'",
    "88,968,30030,88.088,3003.0,'klmnopqrs','gfedcba'",
    "89,979,30030,89.089,3003.0,'lmnopqrst','gfedcba'",
    "90,NULL,30030,90.09,3003.0,'mnopqrstu','gfedcba'",
    "91,1001,NULL,91.091,3103.1,'nopqrstuv','gfedcba'",
    "92,1012,31031,NULL,3103.1,'opqrstuvw','gfedcba'",
    "93,1023,31031,93.093,NULL,'pqrstuvwx','fedcbaz'",
    "94,1034,32032,94.094,3203.2,NULL,'fedcbaz'",
    "95,1045,32032,95.095,3203.2,'rstuvwxyz',NULL",
    "96,NULL,NULL,96.096,3203.2,'stuvwxyza','fedcbaz'",
    "97,1067,33033,NULL,NULL,'tuvwxyzab','fedcbaz'",
    "98,1078,33033,98.098,3303.3,NULL,NULL",
    "99,NULL,NULL,NULL,NULL,NULL,NULL",
];

fn where9_join_db() -> Database {
    let mut db = Database::new();
    run(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY,b,c,d,e,f,g)");
    for v in WHERE9_ROWS {
        run(&mut db, &format!("INSERT INTO t1 VALUES({v})"));
    }
    run(&mut db, "CREATE INDEX t1b ON t1(b)");
    run(&mut db, "CREATE INDEX t1c ON t1(c)");
    run(&mut db, "CREATE INDEX t1d ON t1(d)");
    run(&mut db, "CREATE INDEX t1e ON t1(e)");
    run(&mut db, "CREATE INDEX t1f ON t1(f)");
    run(&mut db, "CREATE INDEX t1g ON t1(g)");

    run(&mut db, "CREATE TABLE t2(a INTEGER PRIMARY KEY,b,c,d,e,f,g)");
    for v in WHERE9_ROWS {
        run(&mut db, &format!("INSERT INTO t2 VALUES({v})"));
    }
    run(&mut db, "CREATE INDEX t2b ON t2(b,c)");
    run(&mut db, "CREATE INDEX t2c ON t2(c,e)");
    run(&mut db, "CREATE INDEX t2d ON t2(d,g)");
    run(&mut db, "CREATE INDEX t2e ON t2(e,f,g)");
    run(&mut db, "CREATE INDEX t2f ON t2(f,b,d,c)");
    run(&mut db, "CREATE INDEX t2g ON t2(g,f)");
    db
}

// where9-3.1: inner (CROSS) join with a correlated OR join condition. SQLite
// drives t1 by rowid (`t1.a=80`) and runs a MULTI-INDEX OR on t2: the
// `(t1.c=t2.c AND t1.d=t2.d)` branch seeks t2d (d more selective than c), and
// the `t1.f=t2.f` branch seeks the covering t2f. Branch ordinals are SQLite's
// WHERE-term slots: the first branch's two plain `col=col` conjuncts advance the
// slot to 3 for the second branch.
//
// sqlite3 3.51.0 (verified live):
//   QUERY PLAN
//   |--SEARCH t1 USING INTEGER PRIMARY KEY (rowid=?)
//   `--MULTI-INDEX OR
//      |--INDEX 1
//      |  `--SEARCH t2 USING INDEX t2d (d=?)
//      `--INDEX 3
//         `--SEARCH t2 USING COVERING INDEX t2f (f=?)
#[test]
fn where9_3_1_renders_correlated_join_multi_index_or() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let db = where9_join_db();
    let output = eqp(
        &db,
        "SELECT t2.a FROM t1, t2 \
         WHERE t1.a=80 AND ((t1.c=t2.c AND t1.d=t2.d) OR t1.f=t2.f)",
    );
    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SEARCH t1 USING INTEGER PRIMARY KEY (rowid=?)\n\
         `--MULTI-INDEX OR\n\
        \x20  |--INDEX 1\n\
        \x20  |  `--SEARCH t2 USING INDEX t2d (d=?)\n\
        \x20  `--INDEX 3\n\
        \x20     `--SEARCH t2 USING COVERING INDEX t2f (f=?)\n",
    );
}

// where9-3.2: LEFT JOIN with a correlated OR join condition whose terms are
// expressions (`t1.c+1=t2.c`, `(t1.f||'x')=t2.f`). Expression terms do not
// reserve extra WHERE-term slots, so the second branch is INDEX 2 (not 3), and
// every inner SEARCH line carries the ` LEFT-JOIN` suffix.
//
// sqlite3 3.51.0 (verified live):
//   QUERY PLAN
//   |--SEARCH t1 USING INTEGER PRIMARY KEY (rowid=?)
//   `--MULTI-INDEX OR
//      |--INDEX 1
//      |  `--SEARCH t2 USING INDEX t2d (d=?) LEFT-JOIN
//      `--INDEX 2
//         `--SEARCH t2 USING COVERING INDEX t2f (f=?) LEFT-JOIN
#[test]
fn where9_3_2_renders_left_join_correlated_multi_index_or() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let db = where9_join_db();
    let output = eqp(
        &db,
        "SELECT coalesce(t2.a,9999) \
         FROM t1 LEFT JOIN t2 ON (t1.c+1=t2.c AND t1.d=t2.d) OR (t1.f||'x')=t2.f \
         WHERE t1.a=80",
    );
    assert_eq!(
        output,
        "QUERY PLAN\n\
         |--SEARCH t1 USING INTEGER PRIMARY KEY (rowid=?)\n\
         `--MULTI-INDEX OR\n\
        \x20  |--INDEX 1\n\
        \x20  |  `--SEARCH t2 USING INDEX t2d (d=?) LEFT-JOIN\n\
        \x20  `--INDEX 2\n\
        \x20     `--SEARCH t2 USING COVERING INDEX t2f (f=?) LEFT-JOIN\n",
    );
}

// The MULTI_INDEX_OR_DISABLED kill switch must suppress the correlated-join
// path too, falling back to the generic join rendering (plain SCAN t2).
#[test]
fn kill_switch_disables_correlated_join_multi_index_or() {
    // Serialized against the EQP assertions via ENV_LOCK: the kill switch is
    // process-global, so it must not be observed by parallel tests.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("MULTI_INDEX_OR_DISABLED", "1");
    let db = where9_join_db();
    let output = eqp(
        &db,
        "SELECT t2.a FROM t1, t2 \
         WHERE t1.a=80 AND ((t1.c=t2.c AND t1.d=t2.d) OR t1.f=t2.f)",
    );
    std::env::remove_var("MULTI_INDEX_OR_DISABLED");
    assert!(
        !output.contains("MULTI-INDEX OR"),
        "kill switch must suppress the MULTI-INDEX OR subtree, got:\n{output}"
    );
}
