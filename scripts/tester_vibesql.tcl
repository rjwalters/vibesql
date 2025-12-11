# VibeSQL Test Shim for SQLite TCL Tests
#
# This file provides compatibility layer to run SQLite's TCL test files
# against VibeSQL instead of SQLite. It implements the key testing
# commands: execsql, catchsql, do_test, do_execsql_test, do_catchsql_test
#
# Usage: tclsh tester_vibesql.tcl <test_file.test>
#
# NOTE: Unlike SQLite's tester.tcl which maintains a persistent database
# connection within the TCL process, this shim invokes vibesql as a separate
# process for each SQL execution. This means :memory: databases cannot work
# (each process gets a fresh empty memory database). We use temp files instead.

package require Tcl 8.5

# Configuration
set ::vibesql_path [file normalize "./target/release/vibesql"]
# Use temp file for persistence - :memory: won't work across process invocations
set ::db_file [file normalize "/tmp/vibesql_test_[pid].vbsql"]
set ::verbose 0

# Test counters
set ::nTest 0
set ::nPass 0
set ::nFail 0
set ::nSkip 0
set ::failList {}

# SQL statement accumulator for batching
set ::sql_batch {}
set ::in_transaction 0

#-----------------------------------------------------------------------------
# Core SQL execution
#-----------------------------------------------------------------------------

proc flush_batch {} {
    # Execute accumulated SQL statements
    if {[llength $::sql_batch] == 0} return

    set combined [join $::sql_batch ";\n"]
    set ::sql_batch {}

    if {$::db_file eq ""} {
        set cmd [list echo $combined | $::vibesql_path]
    } else {
        set cmd [list echo $combined | $::vibesql_path $::db_file]
    }

    if {[catch {exec {*}$cmd 2>@1} result]} {
        error $result
    }
    return $result
}

proc execsql {sql {db ""}} {
    # Execute SQL and return results as a TCL list

    # Skip SQLite-specific statements we don't support
    set sql_upper [string toupper [string trim $sql]]
    if {[string match "PRAGMA*" $sql_upper]} {
        return {}  ;# Skip PRAGMA statements
    }
    if {[string match "ANALYZE*" $sql_upper]} {
        return {}  ;# Skip ANALYZE statements
    }
    if {[string match "REINDEX*" $sql_upper]} {
        return {}  ;# Skip REINDEX statements
    }

    # Handle transaction batching
    if {$sql_upper eq "BEGIN" || [string match "BEGIN*" $sql_upper]} {
        set ::in_transaction 1
        lappend ::sql_batch $sql
        return {}
    } elseif {$sql_upper eq "COMMIT" || $sql_upper eq "ROLLBACK"} {
        lappend ::sql_batch $sql
        set ::in_transaction 0
        set result [flush_batch]
        return [parse_result $result]
    } elseif {$::in_transaction} {
        lappend ::sql_batch $sql
        return {}
    }

    # Direct execution for non-transaction SQL
    if {$::db_file eq ""} {
        set result [exec echo $sql | $::vibesql_path 2>@1]
    } else {
        set result [exec echo $sql | $::vibesql_path $::db_file 2>@1]
    }

    return [parse_result $result]
}

proc parse_result {output} {
    # Parse VibeSQL tabular output into TCL list
    set data {}
    set lines [split $output "\n"]
    set header_seen 0
    set separator_count 0

    foreach line $lines {
        # Skip empty lines and row count lines
        if {[string trim $line] eq ""} continue
        if {[regexp {^\d+ rows?$} $line]} continue
        if {[regexp {^=+$} $line]} continue
        if {[regexp {^Error} $line]} {
            error $line
        }

        # Count separators - header is between 1st and 2nd separator
        if {[regexp {^\+[-+]+\+$} $line]} {
            incr separator_count
            continue
        }

        # Extract data from pipe-delimited lines
        if {[regexp {^\|(.+)\|$} $line -> content]} {
            # Skip header row (first row, before 2nd separator)
            if {$separator_count < 2} {
                continue
            }
            set vals [split $content "|"]
            foreach v $vals {
                lappend data [string trim $v]
            }
        }
    }

    return $data
}

proc parse_result_with_headers {output} {
    # Parse VibeSQL tabular output, returning {headers rows} where rows is list of lists
    set headers {}
    set rows {}
    set lines [split $output "\n"]
    set separator_count 0

    foreach line $lines {
        # Skip empty lines and row count lines
        if {[string trim $line] eq ""} continue
        if {[regexp {^\d+ rows?$} $line]} continue
        if {[regexp {^=+$} $line]} continue
        if {[regexp {^Error} $line]} {
            error $line
        }

        # Count separators - header is between 1st and 2nd separator
        if {[regexp {^\+[-+]+\+$} $line]} {
            incr separator_count
            continue
        }

        # Extract data from pipe-delimited lines
        if {[regexp {^\|(.+)\|$} $line -> content]} {
            set vals {}
            foreach v [split $content "|"] {
                lappend vals [string trim $v]
            }

            if {$separator_count < 2} {
                # This is the header row
                set headers $vals
            } else {
                # This is a data row
                lappend rows $vals
            }
        }
    }

    return [list $headers $rows]
}

proc execsql_with_headers {sql {db ""}} {
    # Execute SQL and return {headers rows} for iteration
    if {$::db_file eq ""} {
        set result [exec echo $sql | $::vibesql_path 2>@1]
    } else {
        set result [exec echo $sql | $::vibesql_path $::db_file 2>@1]
    }

    return [parse_result_with_headers $result]
}

proc catchsql {sql {db ""}} {
    # Execute SQL and catch errors, return {errorcode result}
    if {[catch {execsql $sql $db} result]} {
        # Error occurred
        return [list 1 $result]
    } else {
        return [list 0 $result]
    }
}

#-----------------------------------------------------------------------------
# Test execution commands
#-----------------------------------------------------------------------------

proc do_test {name script expected} {
    # Run a test and compare result to expected
    incr ::nTest

    if {$::verbose} {
        puts -nonewline "  $name... "
        flush stdout
    }

    if {[catch {uplevel 1 $script} result]} {
        # Script error
        incr ::nFail
        lappend ::failList $name
        if {$::verbose} {
            puts "FAILED (error: $result)"
        }
        return
    }

    # Normalize for comparison
    set result_norm [normalize_result $result]
    set expected_norm [normalize_result $expected]

    if {$result_norm eq $expected_norm} {
        incr ::nPass
        if {$::verbose} {
            puts "ok"
        }
    } else {
        incr ::nFail
        lappend ::failList $name
        if {$::verbose} {
            puts "FAILED"
            puts "    Expected: $expected"
            puts "    Got:      $result"
        }
    }
}

proc do_execsql_test {name sql expected} {
    # Convenience wrapper for SQL execution tests
    do_test $name [list execsql $sql] $expected
}

proc do_catchsql_test {name sql expected} {
    # Test that expects a specific error
    do_test $name [list catchsql $sql] $expected
}

proc normalize_result {val} {
    # Normalize result for comparison
    # Convert to string and normalize whitespace
    set val [string trim $val]
    regsub -all {\s+} $val " " val
    return $val
}

#-----------------------------------------------------------------------------
# Capability checking (stub implementations)
#-----------------------------------------------------------------------------

proc ifcapable {capability script} {
    # Skip SQLite-specific capabilities that we don't support
    set skip_caps {wal vacuum_incr stat4 stat3 compound_select tclvar vtab fts3 fts4 fts5}
    if {$capability in $skip_caps} {
        return
    }
    uplevel 1 $script
}

proc capable {capability} {
    set skip_caps {wal vacuum_incr stat4 stat3}
    return [expr {$capability ni $skip_caps}]
}

#-----------------------------------------------------------------------------
# Database setup
#-----------------------------------------------------------------------------

proc sqlite3 {db filename args} {
    # Create/open a database
    # Note: :memory: is mapped to a temp file because we spawn separate processes
    # for each SQL execution, and memory databases don't persist across processes.
    if {$filename eq ":memory:" || $filename eq ""} {
        # Use temp file instead of :memory: for persistence
        set ::db_file [file normalize "/tmp/vibesql_test_[pid].vbsql"]
    } else {
        set ::db_file [file normalize $filename]
    }

    # Clean existing db file for fresh start
    if {[file exists $::db_file]} {
        file delete -force $::db_file
    }

    # Create db command alias - if name is not "db" (which already exists)
    # create an alias to the global db proc
    if {$db ne "db"} {
        interp alias {} $db {} db
    }
}

proc db {cmd args} {
    # Default db command - supports multiple call patterns:
    # db eval SQL                    - returns results as list
    # db eval SQL varname script     - iterates over rows, setting varname array
    # db one SQL                     - returns first column of first row
    switch $cmd {
        eval {
            set sql [lindex $args 0]
            if {[llength $args] == 1} {
                # Simple case: just return the results
                return [execsql $sql]
            } elseif {[llength $args] == 3} {
                # Iterator case: db eval $sql varname script
                set varname [lindex $args 1]
                set script [lindex $args 2]

                # Execute SQL and get column names + data
                set raw_result [execsql_with_headers $sql]
                set headers [lindex $raw_result 0]
                set rows [lindex $raw_result 1]

                # Iterate over each row
                foreach row $rows {
                    # Set up the array variable in caller's scope
                    upvar 1 $varname arr
                    set idx 0
                    foreach col $headers {
                        set arr($col) [lindex $row $idx]
                        incr idx
                    }
                    # Execute the script in caller's scope
                    uplevel 1 $script
                }
                return
            } else {
                # Two args case: db eval $sql varname (no script)
                return [execsql $sql]
            }
        }
        one {
            # Return single value (first column of first row)
            set sql [lindex $args 0]
            set result [execsql $sql]
            if {[llength $result] > 0} {
                return [lindex $result 0]
            }
            return ""
        }
        close {
            # No-op
        }
        nullvalue {
            # Sets the string used for NULL - ignore
            return
        }
        default {
            error "Unknown db command: $cmd"
        }
    }
}

#-----------------------------------------------------------------------------
# Utility commands
#-----------------------------------------------------------------------------

proc finish_test {} {
    # Clean up temp database
    if {$::db_file ne "" && [file exists $::db_file]} {
        file delete -force $::db_file
    }

    # Print summary
    puts ""
    puts "======================================"
    puts "Tests run:    $::nTest"
    puts "Tests passed: $::nPass"
    puts "Tests failed: $::nFail"
    puts "Tests skipped: $::nSkip"
    puts "======================================"

    if {[llength $::failList] > 0} {
        puts "\nFailed tests:"
        foreach t $::failList {
            puts "  - $t"
        }
        exit 1
    }
    exit 0
}

proc omit_test {name reason {append 0}} {
    incr ::nSkip
    if {$::verbose} {
        puts "  $name... SKIPPED ($reason)"
    }
}

proc reset_db {} {
    # Reset the database to a clean state
    if {$::db_file ne "" && [file exists $::db_file]} {
        file delete -force $::db_file
    }
}

proc forcedelete {args} {
    # Force delete files (SQLite test utility)
    foreach f $args {
        catch {file delete -force $f}
    }
}

proc file_control_chunksize_test {db size} {
    # SQLite-specific file control - ignore
    return
}

proc sqlite3_memdebug_pending {args} {
    # SQLite memory debugging - ignore
    return -1
}

proc breakpoint {} {
    # Debug breakpoint - ignore
    return
}

proc set_test_counter {counter {value ""}} {
    switch $counter {
        NTEST {
            if {$value ne ""} {set ::nTest $value}
            return $::nTest
        }
        NPASS {
            if {$value ne ""} {set ::nPass $value}
            return $::nPass
        }
        NFAIL {
            if {$value ne ""} {set ::nFail $value}
            return $::nFail
        }
    }
}

#-----------------------------------------------------------------------------
# Main entry point
#-----------------------------------------------------------------------------

proc run_test_file {filename} {
    # Clean any existing temp db
    if {$::db_file ne "" && [file exists $::db_file]} {
        file delete -force $::db_file
    }

    puts "Running: $filename"
    puts ""

    # Read and preprocess the test file
    # Replace "source $testdir/tester.tcl" with nothing (we already have our defs)
    set f [open $filename r]
    set content [read $f]
    close $f

    # The test file uses $argv0 and $testdir before sourcing tester.tcl
    # We need to inject our variable definitions BEFORE those lines
    # First, set up the variables that will be referenced
    set testdir [file dirname $filename]

    # Replace the set testdir and source lines with our setup
    # Common patterns:
    # set testdir [file dirname $argv0]
    # source $testdir/tester.tcl
    regsub {set testdir \[file dirname \$argv0\]} $content "set testdir \"$testdir\"" content
    regsub {source \$testdir/tester\.tcl} $content "# tester.tcl replaced by vibesql shim" content

    # Execute the modified content
    if {[catch {eval $content} err]} {
        puts "Error running test: $err"
        puts "Error info: $::errorInfo"
        exit 1
    }

    finish_test
}

# Parse command line
if {$argc > 0} {
    set test_file [lindex $argv 0]
    if {[lsearch $argv "--verbose"] >= 0 || [lsearch $argv "-v"] >= 0} {
        set ::verbose 1
    }
    run_test_file $test_file
} else {
    puts "Usage: tclsh tester_vibesql.tcl <test_file.test> \[--verbose\]"
    exit 1
}
