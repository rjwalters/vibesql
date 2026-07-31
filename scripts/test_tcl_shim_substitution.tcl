#!/usr/bin/env tclsh
#-----------------------------------------------------------------------------
# Unit tests for substitute_tcl_vars in scripts/tester_vibesql.tcl (#6307)
#
# Verifies SQLite-tclsqlite-compatible parameter substitution semantics:
#   - Unset TCL variables bind SQL NULL (not left as literal text), and later
#     references in the same statement are still substituted — for all four
#     reference forms ($::var, $arr(elem), $var/${var}, :var).
#   - The scan is quote-aware: $word / :word inside single-quoted literals,
#     double-quoted identifiers, [bracketed] identifiers, and SQL comments
#     pass through verbatim (even when a TCL variable of that name IS set).
#   - Substituted text is never rescanned (no double substitution).
#
# Run with: tclsh scripts/test_tcl_shim_substitution.tcl
# (Requires Tcl 8.5/8.6 — same as the shim itself. On macOS: /usr/bin/tclsh)
#-----------------------------------------------------------------------------

set _shim_dir [file dirname [file normalize [info script]]]
source [file join $_shim_dir tester_vibesql.tcl]

set ::_subst_pass 0
set ::_subst_fail 0

proc check {name actual expected} {
    if {$actual eq $expected} {
        incr ::_subst_pass
        puts "ok - $name"
    } else {
        incr ::_subst_fail
        puts "FAIL - $name"
        puts "    expected: $expected"
        puts "    actual:   $actual"
    }
}

#-----------------------------------------------------------------------------
# 1. Unset-var -> NULL, order-independent, later vars still substituted
#    (the core #6307 defect, exercised from a proc scope like real tests)
#-----------------------------------------------------------------------------
proc test_unset_binds_null {} {
    set set_var 42
    unset -nocomplain missing_6307
    check "unset-first: later var still substituted" \
        [substitute_tcl_vars {INSERT INTO t VALUES($missing_6307, $set_var)}] \
        {INSERT INTO t VALUES(NULL, 42)}
    check "unset-last: still binds NULL" \
        [substitute_tcl_vars {INSERT INTO t VALUES($set_var, $missing_6307)}] \
        {INSERT INTO t VALUES(42, NULL)}
    check "unset-only reference binds NULL" \
        [substitute_tcl_vars {SELECT * FROM t WHERE a IS $missing_6307}] \
        {SELECT * FROM t WHERE a IS NULL}
}
test_unset_binds_null

#-----------------------------------------------------------------------------
# 2. :var named placeholders — same NULL/continue semantics
#-----------------------------------------------------------------------------
proc test_colon_form {} {
    set x 7
    unset -nocomplain missing_6307
    check ":var unset -> NULL, later :var still substituted" \
        [substitute_tcl_vars {SELECT * FROM t WHERE a IS :missing_6307 AND b = :x}] \
        {SELECT * FROM t WHERE a IS NULL AND b = 7}
}
test_colon_form

#-----------------------------------------------------------------------------
# 3. $::var global references
#-----------------------------------------------------------------------------
set ::gset_6307 11
unset -nocomplain ::gmissing_6307
proc test_global_form {} {
    check "\$::var unset -> NULL, later \$::var still substituted" \
        [substitute_tcl_vars {SELECT $::gmissing_6307, $::gset_6307}] \
        {SELECT NULL, 11}
}
test_global_form

#-----------------------------------------------------------------------------
# 4. $arr(elem) array-element references
#-----------------------------------------------------------------------------
proc test_array_form {} {
    array set a6307 {seq 3 name pk}
    check "\$arr(elem) unset element -> NULL, later elements still substituted" \
        [substitute_tcl_vars {INSERT INTO out VALUES($a6307(nope), $a6307(seq), $a6307(name))}] \
        {INSERT INTO out VALUES(NULL, 3, 'pk')}
}
test_array_form

#-----------------------------------------------------------------------------
# 5. ${var} braced form
#-----------------------------------------------------------------------------
proc test_braced_form {} {
    set braced_6307 5
    unset -nocomplain bmissing_6307
    check "\${var} set and unset" \
        [substitute_tcl_vars {SELECT ${bmissing_6307}, ${braced_6307}}] \
        {SELECT NULL, 5}
}
test_braced_form

#-----------------------------------------------------------------------------
# 6. Quote-awareness: single-quoted literals are never touched — even when a
#    TCL variable of the embedded name IS set (correctness must come from
#    literal-skipping, not from the old break-on-unset accident).
#-----------------------------------------------------------------------------
proc test_quote_awareness {} {
    set memory "SHOULD_NOT_APPEAR"
    set x 7
    set dollar "SHOULD_NOT_APPEAR"
    check "ATTACH ':memory:' unchanged (var 'memory' set)" \
        [substitute_tcl_vars {ATTACH ':memory:' AS aux}] \
        {ATTACH ':memory:' AS aux}
    check "colon token inside literal unchanged" \
        [substitute_tcl_vars {SELECT 'a:b'}] \
        {SELECT 'a:b'}
    check "\$ token inside literal unchanged (var set)" \
        [substitute_tcl_vars {SELECT 'price $x'}] \
        {SELECT 'price $x'}
    check "\$ token inside literal unchanged (var 'dollar' set)" \
        [substitute_tcl_vars {SELECT 'has $dollar'}] \
        {SELECT 'has $dollar'}
    check "'' escape stays inside the literal span" \
        [substitute_tcl_vars {SELECT 'it''s $x here', $x}] \
        {SELECT 'it''s $x here', 7}
    check "substitution still happens around literals" \
        [substitute_tcl_vars {SELECT 'a:b', $x, ':not_a_param', :x}] \
        {SELECT 'a:b', 7, ':not_a_param', 7}
}
test_quote_awareness

#-----------------------------------------------------------------------------
# 7. Other token spans real SQLite's tokenizer would never bind inside
#-----------------------------------------------------------------------------
proc test_other_spans {} {
    set x 7
    check "double-quoted identifier unchanged" \
        [substitute_tcl_vars {SELECT "a$x" FROM t}] \
        {SELECT "a$x" FROM t}
    check "bracketed identifier unchanged" \
        [substitute_tcl_vars {SELECT [a$x] FROM t}] \
        {SELECT [a$x] FROM t}
    check "line comment unchanged, next line still substituted" \
        [substitute_tcl_vars "SELECT 1 -- uses \$x\n, \$x"] \
        "SELECT 1 -- uses \$x\n, 7"
    check "block comment unchanged" \
        [substitute_tcl_vars {SELECT /* $x */ $x}] \
        {SELECT /* $x */ 7}
}
test_other_spans

#-----------------------------------------------------------------------------
# 8. Non-references: digit-leading $5, 12:34, lone $ / ::
#-----------------------------------------------------------------------------
proc test_non_references {} {
    check "digit-leading \$5 untouched" \
        [substitute_tcl_vars {SELECT a $5 FROM t}] \
        {SELECT a $5 FROM t}
    check "12:34 untouched (digit after colon)" \
        [substitute_tcl_vars {SELECT 12:34}] \
        {SELECT 12:34}
    check "trailing lone \$ untouched" \
        [substitute_tcl_vars {SELECT a FROM t WHERE b = 1 $}] \
        {SELECT a FROM t WHERE b = 1 $}
}
test_non_references

#-----------------------------------------------------------------------------
# 9. No double substitution: a substituted value whose CONTENTS contain $word
#    or :word text must not be rescanned.
#-----------------------------------------------------------------------------
proc test_no_double_substitution {} {
    set inner 99
    set holds_dollar {$inner}
    set holds_colon {:inner}
    check "value containing \$word not re-substituted" \
        [substitute_tcl_vars {SELECT $holds_dollar}] \
        {SELECT '$inner'}
    check "value containing :word not re-substituted" \
        [substitute_tcl_vars {SELECT $holds_colon}] \
        {SELECT ':inner'}
}
test_no_double_substitution

#-----------------------------------------------------------------------------
# 10. Value formatting + scope resolution sanity (pre-existing behavior kept)
#-----------------------------------------------------------------------------
set ::scope_6307 global_val
proc test_formatting_and_scope {} {
    set s "it's"
    set e ""
    set n NULL
    set f 1.5
    check "string value quoted with '' escaping" \
        [substitute_tcl_vars {INSERT INTO t VALUES($s)}] \
        {INSERT INTO t VALUES('it''s')}
    check "empty string value -> ''" \
        [substitute_tcl_vars {INSERT INTO t VALUES($e)}] \
        {INSERT INTO t VALUES('')}
    check "NULL keyword value stays unquoted" \
        [substitute_tcl_vars {INSERT INTO t VALUES($n)}] \
        {INSERT INTO t VALUES(NULL)}
    check "float passes through" \
        [substitute_tcl_vars {INSERT INTO t VALUES($f)}] \
        {INSERT INTO t VALUES(1.5)}
    # Innermost scope shadows the global of the same name
    set scope_6307 local_val
    check "innermost scope wins over global" \
        [substitute_tcl_vars {SELECT $scope_6307}] \
        {SELECT 'local_val'}
}
test_formatting_and_scope

# Nested procs: variable defined two frames up is still found (stack walk)
proc outer_6307 {} {
    set from_outer 8
    inner_6307
}
proc inner_6307 {} {
    check "variable from an outer (non-immediate) frame is found" \
        [substitute_tcl_vars {SELECT $from_outer}] \
        {SELECT 8}
}
outer_6307

#-----------------------------------------------------------------------------
# Summary
#-----------------------------------------------------------------------------
puts ""
puts "substitution tests: $::_subst_pass passed, $::_subst_fail failed"
if {$::_subst_fail > 0} {
    exit 1
}
exit 0
