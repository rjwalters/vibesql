# VibeSQL Web UI - English (US)

# Page titles
page-title = VibeSQL - AI-Powered SQL:1999 Database
demo-title = VibeSQL Demo
benchmarks-title = Performance Benchmarks - VibeSQL
benchmarks-heading = VibeSQL - Performance Benchmarks
conformance-title = Conformance Report - VibeSQL
conformance-heading = Conformance Report
conformance-subtitle = SQL:1999 Standards Compliance Testing

# Navigation
nav-showcase = SQL:1999 Showcase
nav-conformance = View sqltest Results
nav-sqllogictest = View SQLLogicTest Results

# Editor section
editor-title = SQL Editor
editor-storage = Storage
editor-storage-init = Initializing...
editor-execute = Execute Query

# Results section
results-title = Results
results-empty = Execute a query to see results
results-loading = Loading...
results-rows = { $count } { $count ->
    [one] row
   *[other] rows
}
results-rows-with-time = { $count } { $count ->
    [one] row
   *[other] rows
} ({ $time }ms)
results-copy = Copy to clipboard
results-export = Export CSV
results-limit-warning = Showing first { $limit } of { $total } rows. Use LIMIT clause to refine your query.

# Examples sidebar
examples-title = Examples
examples-basic = Basic Queries
examples-advanced = Advanced Queries

# Database selector
db-select-label = Database

# Footer
footer-tagline = VibeSQL - SQL:1999 Database in WebAssembly
footer-deployed = Deployed: { $date }

# Theme
theme-toggle-dark = Switch to dark mode
theme-toggle-light = Switch to light mode

# Locale
locale-select = Select language

# Messages
msg-query-success = Query executed successfully
msg-rows-affected = { $count } { $count ->
    [one] row
   *[other] rows
} affected

# Errors
error-generic = An error occurred
error-query-failed = Query failed

# Editor
editor-placeholder = Enter SQL query here... (Ctrl+Enter or Cmd+Enter to execute)

# Navigation links
nav-terminal = SQL Terminal Demo
nav-compliance = SQL Test Compliance Report
nav-benchmarks = Performance Benchmarks
nav-github = GitHub Repository
nav-home = Home

# Results
results-success-zero = Query executed successfully (0 rows)
results-null = NULL

# Help Modal
help-title = Keyboard Shortcuts & Help
help-close = Close
help-editor-shortcuts = Editor Shortcuts
help-navigation = Navigation
help-results-actions = Results Actions
help-tips = Tips
help-shortcut-execute = Execute current query
help-shortcut-comment = Toggle line comment
help-shortcut-indent = Indent selection
help-shortcut-show-help = Show this help dialog
help-shortcut-close-help = Close help dialog
help-action-copy = Copy to clipboard
help-action-copy-desc = Copy results as tab-separated values
help-action-export = Export CSV
help-action-export-desc = Download results as CSV file
help-tip-limit = Results are limited to 1,000 rows for performance. Use LIMIT clause to refine queries.
help-tip-time = Execution time is shown with query results.
help-tip-syntax = The editor supports SQL syntax highlighting and auto-completion.
help-tip-theme = Toggle between light/dark themes using the theme button.
help-got-it = Got it!

# Showcase Navigation
showcase-title = Core SQL:1999 Showcase
showcase-description = Explore the implemented SQL:1999 Core features interactively
showcase-complete = { $percent }% Complete
showcase-categories = Feature Categories
showcase-legend = Status Legend
showcase-status-implemented = Fully Implemented
showcase-status-partial = Partially Implemented
showcase-status-planned = Planned

# Showcase category labels
showcase-cat-compliance = Compliance Dashboard
showcase-cat-data-types = Data Types
showcase-cat-dml = DML Operations
showcase-cat-predicates = Predicates & Operators
showcase-cat-joins = JOINs
showcase-cat-subqueries = Subqueries
showcase-cat-aggregates = Aggregates & GROUP BY
showcase-cat-ddl = DDL & Constraints

# Common showcase elements
showcase-interactive-examples = Interactive Examples
showcase-try-example = Try This Example
showcase-progress = { $implemented } of { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Category
showcase-table-description = Description
showcase-table-syntax = Syntax
showcase-table-use-case = Use Case

# Status labels
status-implemented = Implemented
status-partial = Partial
status-planned = Planned

# Aggregates Showcase
aggregates-title = SQL Aggregates and GROUP BY
aggregates-description = Core SQL:1999 aggregate functions and grouping capabilities
aggregates-reference = Aggregate Functions Reference
aggregates-table-function = Function
aggregates-progress-type = functions
aggregates-ex-basic = Basic Aggregate Functions
aggregates-ex-group-single = GROUP BY (Single Column)
aggregates-ex-group-multiple = GROUP BY (Multiple Columns)
aggregates-ex-having = HAVING Clause
aggregates-ex-orderby = ORDER BY with Aggregates
aggregates-ex-null = NULL Handling in Aggregates

# DML Operations Showcase
dml-title = DML Operations (Data Manipulation Language)
dml-description = Core SQL:1999 operations for querying and modifying data
dml-reference = DML Operations Reference
dml-table-operation = Operation
dml-progress-type = operations
dml-ex-select-basic = SELECT - Basic Queries
dml-ex-select-ordering = SELECT - Ordering and Limiting
dml-ex-insert = INSERT Operations
dml-ex-update = UPDATE Operations
dml-ex-delete = DELETE Operations
dml-ex-combined = Combined CRUD Workflow

# Data Types Showcase
datatypes-title = Core SQL:1999 Data Types
datatypes-description = Explore the fundamental data types defined in the SQL:1999 Core specification
datatypes-reference = Data Type Reference
datatypes-table-type = Type Name
datatypes-table-example = Example Values
datatypes-table-spec = Specification
datatypes-progress-type = types
datatypes-ex-numeric = Working with Numeric Types
datatypes-ex-null = NULL Handling & Three-Valued Logic
datatypes-ex-comparisons = Type Comparisons & Operations

# JOINs Showcase
joins-title = SQL JOINs
joins-description = Core SQL:1999 JOIN operations for combining data from multiple tables
joins-reference = JOIN Types Reference
joins-table-type = JOIN Type
joins-progress-type = JOIN types
joins-category-suffix = JOINs
joins-ex-sample = Sample Data Setup
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Multi-table JOIN

# Predicates Showcase
predicates-title = Predicates and Operators
predicates-description = SQL:1999 predicates for filtering and logical operations
predicates-reference = Predicates Reference
predicates-table-predicate = Predicate
predicates-progress-type = predicates
predicates-ex-comparison = Comparison Operators
predicates-ex-between = BETWEEN and Range Predicates
predicates-ex-null = NULL Predicates and Three-Valued Logic
predicates-ex-boolean = Boolean Logic (AND, OR, NOT)
predicates-ex-in = IN Predicate with Subqueries
predicates-ex-combined = Combined Predicate Operations

# Subqueries Showcase
subqueries-title = SQL Subqueries
subqueries-description = Core SQL:1999 subquery capabilities for nested query operations
subqueries-reference = Subquery Types Reference
subqueries-table-type = Subquery Type
subqueries-progress-type = subquery types
subqueries-ex-scalar-select = Scalar Subquery in SELECT
subqueries-ex-scalar-where = Scalar Subquery in WHERE
subqueries-ex-derived = Derived Tables (Subquery in FROM)
subqueries-ex-in = IN Predicate with Subquery
subqueries-ex-correlated = Correlated Subqueries
subqueries-ex-nested = Nested Subqueries
