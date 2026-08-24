#!/usr/bin/env tclsh
#-----------------------------------------------------------------------------
# Unit tests for the per-connection identity mechanism in
# scripts/tester_vibesql.tcl (#6537)
#
# Verifies that changes()/total_changes() tracking (set_last_changes/
# get_last_changes/get_total_changes, keyed by tcltest_conn_id) survives the
# `rename db db2; sqlite3 db :memory:; ...` connection-swap idiom used by
# e_expr.test: after the swap, the renamed-away original connection ("db2")
# and the freshly-opened replacement ("db") must be tracked independently,
# never sharing one counter slot (the #6532/#6537 bug).
#
# This test exercises the actual alias-creation one-liner used by both
# connection-open call sites in the real shim (`proc sqlite3`'s
# secondary-connection branch and the default "db" alias) directly via
# tcltest_next_conn_id + interp alias, so it does not require a real VibeSQL
# binary or database file -- `changes`/`total_changes` never touch the
# database, only the in-shim counters.
#
# Run with: tclsh scripts/test_tcl_shim_conn_id.tcl
# (Requires Tcl 8.5/8.6 — same as the shim itself. On macOS: /usr/bin/tclsh)
#-----------------------------------------------------------------------------

set _shim_dir [file dirname [file normalize [info script]]]
source [file join $_shim_dir tester_vibesql.tcl]

set ::_connid_pass 0
set ::_connid_fail 0

proc check {name actual expected} {
    if {$actual eq $expected} {
        incr ::_connid_pass
        puts "ok - $name"
    } else {
        incr ::_connid_fail
        puts "FAIL - $name"
        puts "    expected: $expected"
        puts "    actual:   $actual"
    }
}

# Mirrors the one-liner both real connection-open sites use (proc sqlite3's
# secondary-connection branch, and the default "db" alias) without any of
# proc sqlite3's filesystem/db-open side effects -- changes()/total_changes()
# tracking never touches the database.
proc open_conn {name} {
    if {[llength [info commands $name]] == 0} {
        interp alias {} $name {} ::tcltest_db_master [tcltest_next_conn_id]
    }
}

#-----------------------------------------------------------------------------
# 1. Basic (no rename): a single connection's changes()/total_changes() track
#    correctly and independently of a second, never-renamed connection.
#-----------------------------------------------------------------------------
proc test_basic_two_connections {} {
    open_conn db
    open_conn db2
    set_last_changes [tcltest_conn_id db] 5
    set_last_changes [tcltest_conn_id db2] 7
    check "basic: db changes" [db changes] 5
    check "basic: db2 changes" [db2 changes] 7
    check "basic: db total_changes" [db total_changes] 5
    check "basic: db2 total_changes" [db2 total_changes] 7
}
test_basic_two_connections

#-----------------------------------------------------------------------------
# 2. The e_expr.test connection-swap idiom:
#      rename db db2; sqlite3 db :memory:; ...
#    The saved-off original connection (now named "db2") must keep reporting
#    ITS OWN counts, not the freshly-opened replacement's (#6537's core
#    repro).
#-----------------------------------------------------------------------------
proc test_rename_swap_idiom {} {
    # Fresh namespace: use unique connection names so this doesn't collide
    # with test 1's "db"/"db2" state.
    open_conn rdb
    set_last_changes [tcltest_conn_id rdb] 5
    check "swap: before rename, rdb changes" [rdb changes] 5

    rename rdb rdb2
    # Simulate `sqlite3 db :memory:` reopening the now-free "rdb" name with a
    # brand new connection -- same alias-creation one-liner, fresh id.
    open_conn rdb
    set_last_changes [tcltest_conn_id rdb] 99

    check "swap: new rdb (post-reopen) changes == 99, not stale" \
        [rdb changes] 99
    check "swap: rdb2 (saved-off original) changes == 5, NOT 99 (#6537)" \
        [rdb2 changes] 5
    check "swap: new rdb total_changes == 99" [rdb total_changes] 99
    check "swap: rdb2 total_changes == 5, NOT clobbered by new conn (#6537)" \
        [rdb2 total_changes] 5

    # A second DML on the new "rdb" connection must accumulate onto its own
    # total, and must not leak into rdb2's total either.
    set_last_changes [tcltest_conn_id rdb] 1
    check "swap: new rdb total_changes accumulates (99+1=100)" \
        [rdb total_changes] 100
    check "swap: rdb2 total_changes still 5 after new-conn DML" \
        [rdb2 total_changes] 5
}
test_rename_swap_idiom

#-----------------------------------------------------------------------------
# 3. tcltest_conn_id resolves a plain connection NAME (as execsql passes it)
#    to whatever connection CURRENTLY answers to that name, matching direct
#    dispatch -- this is what keeps execsql's set_last_changes/get_last_changes
#    calls (which receive a name, not an id) consistent with `db changes`
#    after a rename.
#-----------------------------------------------------------------------------
proc test_conn_id_resolution_follows_rename {} {
    open_conn ndb
    set id_before [tcltest_conn_id ndb]
    check "resolution: \"\" resolves same as \"db\" for the default connection" \
        [tcltest_conn_id ""] [tcltest_conn_id db]

    rename ndb ndb2
    open_conn ndb
    set id_after [tcltest_conn_id ndb]
    set id_swapped [tcltest_conn_id ndb2]

    check "resolution: post-rename \"ndb\" resolves to a NEW id" \
        [expr {$id_after ne $id_before}] 1
    check "resolution: post-rename \"ndb2\" still resolves to the ORIGINAL id" \
        $id_swapped $id_before
}
test_conn_id_resolution_follows_rename

#-----------------------------------------------------------------------------
# Summary
#-----------------------------------------------------------------------------
puts ""
puts "conn-id tests: $::_connid_pass passed, $::_connid_fail failed"
if {$::_connid_fail > 0} {
    exit 1
}
exit 0
