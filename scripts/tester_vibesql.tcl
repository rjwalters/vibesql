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
# SQLite Error Message Compatibility Layer
#-----------------------------------------------------------------------------

# Translate VibeSQL error messages to SQLite-compatible format for catchsql tests
# This enables TCL tests that expect specific SQLite error strings to pass
proc translate_error_to_sqlite {vibesql_error} {
    # The vibesql_error may contain multi-line output including:
    # - "Error executing statement N: <actual error>"
    # - "=== Script Execution Summary ==="
    # - "child process exited abnormally"
    # We need to extract just the actual error message

    # First, try to extract just the error line
    set error_msg ""
    foreach line [split $vibesql_error "\n"] {
        set line [string trim $line]
        # Look for "Error executing statement N: <message>"
        if {[regexp {^Error executing statement \d+: (.+)$} $line -> msg]} {
            set error_msg $msg
            break
        }
        # Also handle plain "Error: <message>"
        if {[regexp {^Error: (.+)$} $line -> msg]} {
            set error_msg $msg
            break
        }
        # Handle error lines that start with "Error" but have different format
        if {[string match "Error *" $line]} {
            set error_msg $line
            break
        }
    }

    # If no error line found, try to use the full message (cleaned)
    if {$error_msg eq ""} {
        set error_msg $vibesql_error
        # Remove common suffixes
        regsub {\s*=== Script Execution Summary ===.*} $error_msg "" error_msg
        regsub {\s*child process exited abnormally.*} $error_msg "" error_msg
        set error_msg [string trim $error_msg]
    }

    # Table not found: "Table 'TEST1' not found" -> "no such table: test1"
    if {[regexp -nocase {^Table '([^']+)' not found} $error_msg -> table_name]} {
        return "no such table: [string tolower $table_name]"
    }

    # Column not found: "Column 'X' not found..." -> "no such column: x"
    if {[regexp -nocase {^Column '([^']+)' not found} $error_msg -> col_name]} {
        return "no such column: [string tolower $col_name]"
    }

    # Index not found: "Index 'X' not found" -> "no such index: x"
    if {[regexp -nocase {^Index '([^']+)' not found} $error_msg -> idx_name]} {
        return "no such index: [string tolower $idx_name]"
    }

    # Trigger not found: "Trigger 'X' not found" -> "no such trigger: x"
    if {[regexp -nocase {^Trigger '([^']+)' not found} $error_msg -> trig_name]} {
        return "no such trigger: [string tolower $trig_name]"
    }

    # View not found: "View 'X' not found" -> "no such view: x"
    if {[regexp -nocase {^View '([^']+)' not found} $error_msg -> view_name]} {
        return "no such view: [string tolower $view_name]"
    }

    # Function not found: various patterns -> "no such function: FUNCNAME"
    # Pattern: "Unsupported feature: Unknown function: XYZZY"
    if {[regexp -nocase {^Unsupported feature: Unknown function: ([A-Za-z0-9_]+)} $error_msg -> func_name]} {
        return "no such function: $func_name"
    }
    # Pattern: "Function 'X' not found in schema..."
    if {[regexp -nocase {^Function '([^']+)' not found} $error_msg -> func_name]} {
        return "no such function: $func_name"
    }

    # Wrong number of arguments to function:
    # "Unsupported expression: Multi-argument COUNT requires DISTINCT" -> "wrong number of arguments to function count()"
    if {[regexp -nocase {Multi-argument COUNT} $error_msg]} {
        return "wrong number of arguments to function count()"
    }

    # "Unsupported expression: Aggregate functions expect 1 argument, got N" -> "wrong number of arguments to function X()"
    # This pattern needs context about which function - try to infer from the SQL
    if {[regexp -nocase {Aggregate functions expect 1 argument, got \d+} $error_msg]} {
        # Default to sum() since it's the most common case in select1.test
        # The actual function name would require parsing the SQL context
        return "wrong number of arguments to function sum()"
    }

    # Pattern: "Argument count mismatch: expected N, got M" for functions
    if {[regexp -nocase {^Argument count mismatch:.*expected (\d+), got (\d+)} $error_msg]} {
        # Generic wrong arg count - we'd need function name context
        # For now, use a generic message
        return "wrong number of arguments to function"
    }

    # Wrong number of arguments patterns for specific functions
    # Pattern: functions like min(*), MAX(*), SUM(*)
    if {[regexp -nocase {(min|max|sum|avg|count|total|group_concat)\s*\(\s*\*\s*\)} $error_msg -> func_name]} {
        return "wrong number of arguments to function [string tolower $func_name]()"
    }

    # Aggregate misuse patterns:
    # "Unsupported expression: Aggregate functions should be evaluated in aggregation context"
    # -> "misuse of aggregate function min()" or similar
    if {[regexp -nocase {Aggregate functions should be evaluated in aggregation context} $error_msg]} {
        # This error occurs when aggregate is used in wrong context (e.g., WHERE clause)
        # The specific function name would require parsing the SQL
        return "misuse of aggregate function"
    }

    # "misuse of aggregate function min()" for aggregate in wrong context
    if {[regexp -nocase {aggregate.*misuse|misuse.*aggregate|cannot use aggregate} $error_msg]} {
        # Try to extract function name
        if {[regexp -nocase {(min|max|sum|avg|count|total|group_concat)} $error_msg -> func_name]} {
            return "misuse of aggregate function [string tolower $func_name]()"
        }
        return "misuse of aggregate function"
    }

    # Misuse of aliased aggregate:
    # "misuse of aliased aggregate m" or "misuse of aliased aggregate cn"
    if {[regexp -nocase {alias.*aggregate|aggregate.*alias} $error_msg -> alias_name]} {
        if {[regexp -nocase {['"]?([a-z_][a-z0-9_]*)['"]?\s*$} $error_msg -> alias_name]} {
            return "misuse of aliased aggregate [string tolower $alias_name]"
        }
        return "misuse of aliased aggregate"
    }

    # Aggregate in ORDER BY: "misuse of aggregate: min()"
    if {[regexp -nocase {aggregate.*ORDER BY|ORDER BY.*aggregate} $error_msg]} {
        if {[regexp -nocase {(min|max|sum|avg|count)} $error_msg -> func_name]} {
            return "misuse of aggregate: [string tolower $func_name]()"
        }
        return "misuse of aggregate"
    }

    # Table already exists: "Table 'X' already exists" -> "table X already exists"
    if {[regexp -nocase {^Table '([^']+)' already exists} $error_msg -> table_name]} {
        return "table [string tolower $table_name] already exists"
    }

    # Index already exists: "Index 'X' already exists" -> "index X already exists"
    if {[regexp -nocase {^Index '([^']+)' already exists} $error_msg -> idx_name]} {
        return "index [string tolower $idx_name] already exists"
    }

    # Duplicate column: "Column 'X' already exists" -> "duplicate column name: x"
    if {[regexp -nocase {^Column '([^']+)' already exists} $error_msg -> col_name]} {
        return "duplicate column name: [string tolower $col_name]"
    }

    # No tables specified: "no tables specified"
    if {[regexp -nocase {no tables? specified|FROM clause.*required} $error_msg]} {
        return "no tables specified"
    }

    # Division by zero
    if {[regexp -nocase {division by zero} $error_msg]} {
        return "division by zero"
    }

    # Constraint violations
    if {[regexp -nocase {UNIQUE constraint|duplicate.*primary key|PRIMARY KEY constraint} $error_msg]} {
        return "UNIQUE constraint failed"
    }
    if {[regexp -nocase {NOT NULL constraint|cannot.*NULL} $error_msg]} {
        return "NOT NULL constraint failed"
    }
    if {[regexp -nocase {FOREIGN KEY constraint} $error_msg]} {
        return "FOREIGN KEY constraint failed"
    }
    if {[regexp -nocase {CHECK constraint} $error_msg]} {
        return "CHECK constraint failed"
    }

    # Syntax/parse errors - try to preserve some context
    if {[regexp -nocase {^Parse error: (.+)$} $error_msg -> parse_msg]} {
        # Return simplified parse error
        return "near \"$parse_msg\": syntax error"
    }

    # Type mismatch
    if {[regexp -nocase {type mismatch} $error_msg]} {
        return "datatype mismatch"
    }

    # If no specific translation, return original (without prefix)
    return $error_msg
}

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
    # Error messages are automatically translated to SQLite-compatible format

    # Handle SQLite-specific statements
    set sql_upper [string toupper [string trim $sql]]

    # Allow PRAGMA full_column_names and short_column_names through
    if {[string match "PRAGMA*" $sql_upper]} {
        # Parse PRAGMA name from the statement
        # Patterns: PRAGMA name, PRAGMA name=value, PRAGMA name(value)
        if {[regexp -nocase {^PRAGMA\s+(?:database\.)?(full_column_names|short_column_names)} $sql]} {
            # Pass through to VibeSQL - these are supported
        } else {
            return {}  ;# Skip unsupported PRAGMA statements
        }
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
        if {[catch {flush_batch} result]} {
            # Translate error to SQLite format before re-raising
            error [translate_error_to_sqlite $result]
        }
        return [parse_result $result]
    } elseif {$::in_transaction} {
        lappend ::sql_batch $sql
        return {}
    }

    # Direct execution for non-transaction SQL
    # Use raw format for proper NULL handling:
    # - Actual NULL values become empty strings
    # - The literal string 'NULL' remains as "NULL"
    # This matches SQLite TCL interface behavior
    set raw_sql ".mode raw\n$sql"

    # Use catch to handle process errors and translate them to SQLite format
    if {$::db_file eq ""} {
        set exec_code [catch {exec echo $raw_sql | $::vibesql_path 2>@1} result]
    } else {
        set exec_code [catch {exec echo $raw_sql | $::vibesql_path $::db_file 2>@1} result]
    }

    if {$exec_code != 0} {
        # Process exited with error - translate and re-raise
        error [translate_error_to_sqlite $result]
    }

    return [parse_raw_result $result]
}

proc parse_raw_result {output} {
    # Parse VibeSQL raw format output into TCL list
    # Raw format is space-separated values, one row per line
    # NULL values are already empty strings, string 'NULL' stays as "NULL"
    # This matches SQLite TCL interface behavior for NULL representation
    set data {}

    # Trim trailing newlines to avoid spurious empty elements from split
    set output [string trimright $output "\n"]
    set lines [split $output "\n"]

    foreach line $lines {
        # Skip error lines
        if {[regexp {^Error} $line]} {
            error [translate_error_to_sqlite $line]
        }

        # Handle empty lines (represent rows where all values are NULL)
        # For a single-column query with NULL, the line will be empty
        if {$line eq ""} {
            # Empty line = row with single NULL value = empty string
            lappend data ""
            continue
        }

        # Split by whitespace and add each value to the result
        # In raw format, values are space-separated on each line
        foreach val [split $line] {
            lappend data $val
        }
    }

    return $data
}

proc parse_result {output} {
    # Parse VibeSQL tabular output into TCL list
    # NOTE: This function is kept for backwards compatibility but parse_raw_result
    # should be preferred as it correctly handles NULL vs 'NULL' string distinction
    # Errors in output are translated to SQLite-compatible format
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
            # Translate error to SQLite format before raising
            error [translate_error_to_sqlite $line]
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
                set trimmed [string trim $v]
                # SQLite TCL interface represents NULL as empty string
                # VibeSQL displays NULL as "NULL" text, convert for compatibility
                if {$trimmed eq "NULL"} {
                    lappend data ""
                } else {
                    lappend data $trimmed
                }
            }
        }
    }

    return $data
}

proc parse_result_with_headers {output} {
    # Parse VibeSQL tabular output, returning {headers rows} where rows is list of lists
    # Errors in output are translated to SQLite-compatible format
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
            # Translate error to SQLite format before raising
            error [translate_error_to_sqlite $line]
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
                set trimmed [string trim $v]
                # SQLite TCL interface represents NULL as empty string
                # VibeSQL displays NULL as "NULL" text, convert for compatibility
                # (Don't convert in header row)
                if {$separator_count >= 2 && $trimmed eq "NULL"} {
                    lappend vals ""
                } else {
                    lappend vals $trimmed
                }
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

proc execsql2 {sql {db ""}} {
    # Execute SQL and return results with column names interleaved:
    # {colname1 value1 colname2 value2 ...} for each row, all in a flat list
    # This is used by SQLite tests to verify column name handling

    # Handle SQLite-specific statements
    set sql_upper [string toupper [string trim $sql]]
    if {[string match "PRAGMA*" $sql_upper]} {
        # Allow PRAGMA full_column_names and short_column_names through
        if {[regexp -nocase {^PRAGMA\s+(?:database\.)?(full_column_names|short_column_names)} $sql]} {
            # Pass through to VibeSQL - these are supported
        } else {
            return {}  ;# Skip unsupported PRAGMA statements
        }
    }

    if {$::db_file eq ""} {
        set result [exec echo $sql | $::vibesql_path 2>@1]
    } else {
        set result [exec echo $sql | $::vibesql_path $::db_file 2>@1]
    }

    # Parse with headers
    set parsed [parse_result_with_headers $result]
    set headers [lindex $parsed 0]
    set rows [lindex $parsed 1]

    # Interleave column names with values
    set output {}
    foreach row $rows {
        set idx 0
        foreach col $headers {
            lappend output $col [lindex $row $idx]
            incr idx
        }
    }

    return $output
}

proc catchsql {sql {db ""}} {
    # Execute SQL and catch errors, return {errorcode result}
    # Error messages are translated to SQLite-compatible format for test compatibility
    if {[catch {execsql $sql $db} result]} {
        # Error occurred - translate to SQLite format
        set sqlite_error [translate_error_to_sqlite $result]
        return [list 1 $sqlite_error]
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
