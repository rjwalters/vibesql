#!/usr/bin/env tclsh
#-----------------------------------------------------------------------------
# Unit tests for uses_sqlite_internals' MATCH-operator detector in
# scripts/tester_vibesql.tcl (Part of #6170)
#
# The FTS-MATCH skip check (`\sMATCH\s`) is a blanket regex intended to catch
# genuine FTS queries like `SELECT * FROM t1 WHERE content MATCH 'query'`.
# But `MATCH` is also the keyword introducing the SQL-standard FK match-type
# clause -- `FOREIGN KEY(...) REFERENCES tbl [(cols)] MATCH
# {SIMPLE|PARTIAL|FULL}` (R-24728-13230/R-24450-46174) -- which VibeSQL both
# parses and enforces correctly. The blanket regex incorrectly skipped every
# e_fkey-62.$zMatch.{1,2} test (12 tests across 6 case-variant MATCH keywords)
# as an "FTS feature" gap, hiding real, already-passing FK-MATCH-clause
# coverage.
#
# `do_test <name> {script} <expected>` passes `{script}` to
# uses_sqlite_internals BEFORE Tcl variable substitution happens (`{...}`
# suppresses it; substitution only happens later, at `uplevel` time in the
# caller's scope) -- so e_fkey.test's own
#   foreach zMatch {SIMPLE PARTIAL FULL ...} {
#     do_test e_fkey-62.$zMatch.1 { execsql "... MATCH $zMatch);" } {}
#   }
# reaches the detector as the literal, unsubstituted text `MATCH $zMatch`,
# never as `MATCH SIMPLE`. These tests cover both the literal-keyword shape
# and this pre-substitution shape, plus confirm genuine FTS MATCH usage (with
# or without an unrelated FK match-type clause elsewhere in the same script)
# is still correctly flagged.
#
# Run with: tclsh scripts/test_tcl_shim_fk_match_clause.tcl
# (Requires Tcl 8.5/8.6 — same as the shim itself. On macOS: /usr/bin/tclsh)
#-----------------------------------------------------------------------------

set _shim_dir [file dirname [file normalize [info script]]]
source [file join $_shim_dir tester_vibesql.tcl]

set ::_fkmatch_pass 0
set ::_fkmatch_fail 0

proc check {name script expect_skip} {
    set result [uses_sqlite_internals $script]
    set actual_skip [lindex $result 0]
    if {$actual_skip == $expect_skip} {
        incr ::_fkmatch_pass
        puts "ok - $name"
    } else {
        incr ::_fkmatch_fail
        puts "FAIL - $name"
        puts "    expected skip=$expect_skip"
        puts "    actual skip=$actual_skip (reason: [lindex $result 1])"
    }
}

#-----------------------------------------------------------------------------
# FK match-type clause forms must NOT be skipped (real, passing coverage).
#-----------------------------------------------------------------------------
check "literal MATCH SIMPLE not skipped" \
    {execsql "CREATE TABLE c(e, f, FOREIGN KEY(e, f) REFERENCES p MATCH SIMPLE)"} \
    0

check "literal MATCH PARTIAL not skipped" \
    {execsql "CREATE TABLE c(e, f, FOREIGN KEY(e, f) REFERENCES p MATCH PARTIAL)"} \
    0

check "literal MATCH FULL not skipped" \
    {execsql "CREATE TABLE c(e, f, FOREIGN KEY(e, f) REFERENCES p MATCH FULL)"} \
    0

check "mixed-case MATCH keyword (Simple) not skipped" \
    {execsql "CREATE TABLE c(e, f, FOREIGN KEY(e, f) REFERENCES p MATCH Simple)"} \
    0

check "MATCH clause with explicit parent columns not skipped" \
    {execsql "CREATE TABLE c(e,f, FOREIGN KEY(e,f) REFERENCES p(b,c) MATCH FULL)"} \
    0

# The actual e_fkey-62.test shape: do_test's {script} argument is brace-quoted
# so $zMatch is NOT substituted before uses_sqlite_internals sees it -- the
# detector must recognize this pre-substitution "MATCH $zMatch" form too.
check "pre-substitution MATCH \$zMatch (e_fkey-62 shape) not skipped" \
    {execsql "CREATE TABLE c(d, e, f, FOREIGN KEY(e, f) REFERENCES p MATCH $zMatch);"} \
    0

#-----------------------------------------------------------------------------
# Genuine FTS MATCH usage must still be skipped (unchanged behavior).
#-----------------------------------------------------------------------------
check "genuine FTS MATCH with string literal still skipped" \
    {execsql {SELECT * FROM t1 WHERE content MATCH 'query'}} \
    1

check "genuine FTS MATCH with bind parameter still skipped" \
    {execsql {SELECT * FROM t1 WHERE content MATCH ?}} \
    1

check "genuine FTS MATCH against a table name still skipped" \
    {execsql {SELECT * FROM ft1 WHERE ft1 MATCH 'hello'}} \
    1

# A script containing BOTH an FK match-type clause AND a genuine, unrelated
# FTS MATCH query must still be flagged -- the FK clause occurrence alone
# does not "launder" an unrelated real FTS usage in the same script.
check "FK match clause + unrelated genuine FTS MATCH still skipped" \
    {execsql "CREATE TABLE c(e,f, FOREIGN KEY(e,f) REFERENCES p MATCH SIMPLE); SELECT * FROM t1 WHERE x MATCH 'abc'"} \
    1

#-----------------------------------------------------------------------------
# Summary
#-----------------------------------------------------------------------------
puts ""
puts "fk-match-clause tests: $::_fkmatch_pass passed, $::_fkmatch_fail failed"
if {$::_fkmatch_fail > 0} {
    exit 1
}
exit 0
