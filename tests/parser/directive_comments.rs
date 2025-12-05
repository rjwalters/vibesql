//! Test for issue #1311: Support comments on sqllogictest conditional directives

use sqllogictest::{parse, DefaultColumnType};

#[test]
fn test_onlyif_with_comment() {
    let script = r#"onlyif sqlite # empty RHS
query I
SELECT 1 IN ()
----
0
"#;

    let records = parse::<DefaultColumnType>(script)
        .expect("Should parse successfully with comment on onlyif directive");

    println!("Parsed {} records", records.len());
    assert!(records.len() > 0, "Should have parsed records");
}

#[test]
fn test_skipif_with_comment() {
    let script = r#"skipif mysql # this database doesn't support certain features
query I
SELECT 1
----
1
"#;

    let records = parse::<DefaultColumnType>(script).expect("Should parse skipif with comment");

    assert!(records.len() > 0, "Should have parsed records");
}

#[test]
fn test_multiple_directives_with_comments() {
    let script = r#"onlyif sqlite # test for sqlite only

skipif mysql # skip this on mysql

query I
SELECT 1
----
1
"#;

    let records =
        parse::<DefaultColumnType>(script).expect("Should parse multiple directives with comments");

    assert!(records.len() > 0, "Should have parsed records");
}

#[test]
fn test_directive_without_comment_still_works() {
    let script = r#"onlyif sqlite
query I
SELECT 1
----
1
"#;

    let records =
        parse::<DefaultColumnType>(script).expect("Should still parse directives without comments");

    assert!(records.len() > 0, "Should have parsed records");
}
