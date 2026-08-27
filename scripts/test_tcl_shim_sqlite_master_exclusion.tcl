#!/usr/bin/env tclsh
#-----------------------------------------------------------------------------
# Unit tests for the same-batch TEMP-demotion sqlite_master exclusion rewrite
# in scripts/tester_vibesql.tcl (#6612).
#
# The shim demotes `CREATE TEMP TABLE <n>` to a genuinely persistent
# `CREATE TABLE <n>` (see strip_temp_table_keyword / ::temp_demoted_names,
# #5512) so an emulated TEMP table survives the shim's per-batch CLI-process
# respawns. Side effect: from the instant that CREATE TABLE runs, <n> is a real
# row in VibeSQL's own (unqualified) schema catalog — so a later statement in
# the SAME batch that reads unqualified `sqlite_master` "self-lists" the table
# whose own creation it is sitting behind. Real SQLite never shows a TEMP
# object there (TEMP lives in a separate `temp` schema).
#
# These tests pin both halves of the contract:
#   - a name demoted by THIS batch is excluded from that same batch's
#     unqualified sqlite_master/sqlite_schema reads, and
#   - a name demoted by an EARLIER batch is NOT excluded (the deliberate
#     cross-batch permanence alter-1.2/alter-1.5 depend on), and
#   - the rewrite is surgical: string literals, PRAGMA arguments,
#     schema-qualified references, DELETE FROM, and non-FROM/JOIN uses of the
#     identifier are all left byte-for-byte alone.
#
# Run with: tclsh scripts/test_tcl_shim_sqlite_master_exclusion.tcl
# (Requires Tcl 8.5/8.6 — same as the shim itself. On macOS: /usr/bin/tclsh)
#-----------------------------------------------------------------------------

set _shim_dir [file dirname [file normalize [info script]]]
source [file join $_shim_dir tester_vibesql.tcl]

set ::_pass 0
set ::_fail 0

proc check {name actual expected} {
    if {$actual eq $expected} {
        incr ::_pass
        puts "ok - $name"
    } else {
        incr ::_fail
        puts "FAIL - $name"
        puts "    expected: $expected"
        puts "    actual:   $actual"
    }
}

proc check_contains {name actual needle} {
    if {[string first $needle $actual] >= 0} {
        incr ::_pass
        puts "ok - $name"
    } else {
        incr ::_fail
        puts "FAIL - $name"
        puts "    expected to contain: $needle"
        puts "    actual:              $actual"
    }
}

proc check_not_contains {name actual needle} {
    if {[string first $needle $actual] < 0} {
        incr ::_pass
        puts "ok - $name"
    } else {
        incr ::_fail
        puts "FAIL - $name"
        puts "    expected NOT to contain: $needle"
        puts "    actual:                  $actual"
    }
}

# Reset every piece of cross-batch shim state these tests touch, so each case
# starts from a pristine "new test file" position.
proc reset_shim_state {} {
    set ::temp_demoted_names [dict create]
    set ::temp_replay_ddl [dict create]
    set ::temp_created_this_batch [dict create]
}

#-----------------------------------------------------------------------------
# 1. The #6612 defect itself: alter-1.6's batch shape.
#-----------------------------------------------------------------------------
reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE objlist(type, name, tbl_name);
    INSERT INTO objlist SELECT type, name, tbl_name FROM sqlite_master;
}]
check_contains "same-batch demotion filters unqualified sqlite_master" \
    $out "NOT IN ('objlist')"
check_not_contains "same-batch: no bare 'FROM sqlite_master' survives" \
    $out "tbl_name FROM sqlite_master"

#-----------------------------------------------------------------------------
# 2. Cross-batch permanence (alter-1.2 / alter-1.5) must NOT regress: a name
#    demoted in an EARLIER batch keeps appearing in later batches.
#-----------------------------------------------------------------------------
reset_shim_state
strip_temp_table_keyword {CREATE TEMP TABLE TempTab(e,f,g);}
set later {SELECT type, name, tbl_name FROM sqlite_master;}
check "earlier-batch demotion leaves a later batch untouched" \
    [strip_temp_table_keyword $later] $later

#-----------------------------------------------------------------------------
# 3. A batch with no TEMP-table demotion at all is never rewritten.
#-----------------------------------------------------------------------------
reset_shim_state
set plain {SELECT name FROM sqlite_master WHERE type='table';}
check "batch with no demotion is passed through verbatim" \
    [strip_temp_table_keyword $plain] $plain

#-----------------------------------------------------------------------------
# 4. Surgical scope: non-FROM/JOIN occurrences of the identifier.
#-----------------------------------------------------------------------------
reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    PRAGMA table_info(sqlite_master);
}]
check_contains "PRAGMA table_info(sqlite_master) is left alone" \
    $out "PRAGMA table_info(sqlite_master)"

reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    SELECT sql FROM sqlite_master WHERE sql LIKE '%FROM sqlite_master%';
}]
check_contains "a 'FROM sqlite_master' inside a string literal is left alone" \
    $out "LIKE '%FROM sqlite_master%'"

reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    DELETE FROM sqlite_master WHERE name='x';
}]
check_contains "DELETE FROM sqlite_master is left alone" \
    $out "DELETE FROM sqlite_master WHERE"

reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    SELECT name FROM temp.sqlite_master;
    SELECT name FROM sqlite_temp_master;
}]
check_contains "temp.sqlite_master is left alone" $out "FROM temp.sqlite_master"
check_contains "sqlite_temp_master is left alone" $out "FROM sqlite_temp_master"

#-----------------------------------------------------------------------------
# 5. Aliases survive the rewrite, so later qualified column refs still resolve.
#-----------------------------------------------------------------------------
reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    SELECT m.name FROM sqlite_master AS m;
}]
check_contains "explicit AS alias is preserved" $out ") AS m"

reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    SELECT m.name FROM sqlite_master m;
}]
check_contains "implicit alias is preserved" $out ") AS m"

reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    SELECT name FROM sqlite_master WHERE type='table';
}]
check_contains "no alias: reference keeps the sqlite_master name" \
    $out ") AS sqlite_master"
check_contains "no alias: trailing WHERE is not swallowed as an alias" \
    $out "AS sqlite_master WHERE type='table'"

#-----------------------------------------------------------------------------
# 6. sqlite_schema (the modern spelling) and JOIN form are handled too.
#-----------------------------------------------------------------------------
reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    SELECT name FROM sqlite_schema;
}]
check_contains "sqlite_schema is rewritten too" $out "SELECT * FROM sqlite_schema WHERE"

reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    SELECT a.name FROM t1 AS a JOIN sqlite_master AS b ON a.name=b.name;
}]
check_contains "JOIN form is rewritten" $out ") AS b ON a.name=b.name"

#-----------------------------------------------------------------------------
# 7. Objects owned by a same-batch-demoted table (indexes, triggers) are
#    excluded too — real SQLite keeps them in the temp schema as well.
#-----------------------------------------------------------------------------
reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE t9(x);
    CREATE INDEX i9 ON t9(x);
    SELECT name FROM sqlite_master;
}]
check_contains "rows owned by the demoted table are excluded via tbl_name" \
    $out "tbl_name NOT IN ('t9')"

#-----------------------------------------------------------------------------
# 8. Quoted / odd names are emitted as valid, properly-escaped SQL literals.
#-----------------------------------------------------------------------------
reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE [temp table](e,f,g);
    SELECT name FROM sqlite_master;
}]
check_contains "bracket-quoted name is emitted unquoted inside the literal" \
    $out "NOT IN ('temp table')"

reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TEMP TABLE "it's"(x);
    SELECT name FROM sqlite_master;
}]
check_contains "an apostrophe in the name is doubled in the SQL literal" \
    $out "NOT IN ('it''s')"

#-----------------------------------------------------------------------------
# 9. A TEMP table kept REAL by the #5591 coexist path is never added to the
#    exclusion list (it is not demoted, so it never self-lists).
#-----------------------------------------------------------------------------
reset_shim_state
set out [strip_temp_table_keyword {
    CREATE TABLE t1(a,b);
    CREATE TEMP TABLE t1(a,b);
    SELECT name FROM sqlite_master;
}]
check_contains "coexisting main+temp keeps the TEMP keyword" \
    $out "CREATE TEMP TABLE IF NOT EXISTS t1"
check_not_contains "coexist path adds no sqlite_master exclusion" \
    $out "NOT IN ('t1')"

#-----------------------------------------------------------------------------
# Summary
#-----------------------------------------------------------------------------
puts ""
puts "sqlite_master exclusion tests: $::_pass passed, $::_fail failed"
if {$::_fail > 0} {
    exit 1
}
exit 0
