# VibeSQL Web UI - Svenska

# Page titles
page-title = VibeSQL - AI-driven SQL:1999 Databas
demo-title = VibeSQL Demo
benchmarks-title = Prestandabenchmarks - VibeSQL
benchmarks-heading = VibeSQL - Prestandabenchmarks
conformance-title = Överensstämmelserapport - VibeSQL
conformance-heading = Överensstämmelserapport
conformance-subtitle = SQL:1999 Standardöverensstämmelsetest

# Navigation
nav-showcase = SQL:1999 Showcase
nav-conformance = Visa sqltest-resultat
nav-sqllogictest = Visa SQLLogicTest-resultat

# Editor section
editor-title = SQL-editor
editor-storage = Lagring
editor-storage-init = Initierar...
editor-execute = Kör fråga

# Results section
results-title = Resultat
results-empty = Kör en fråga för att se resultat
results-loading = Laddar...
results-rows = { $count } { $count ->
    [one] rad
   *[other] rader
}
results-rows-with-time = { $count } { $count ->
    [one] rad
   *[other] rader
} ({ $time }ms)
results-copy = Kopiera till urklipp
results-export = Exportera CSV
results-limit-warning = Visar de första { $limit } av { $total } rader. Använd LIMIT för att förfina din fråga.

# Examples sidebar
examples-title = Exempel
examples-basic = Grundläggande frågor
examples-advanced = Avancerade frågor

# Database selector
db-select-label = Databas

# Footer
footer-tagline = VibeSQL - SQL:1999 Databas i WebAssembly
footer-deployed = Distribuerad: { $date }

# Theme
theme-toggle-dark = Byt till mörkt läge
theme-toggle-light = Byt till ljust läge

# Locale
locale-select = Välj språk

# Messages
msg-query-success = Frågan kördes framgångsrikt
msg-rows-affected = { $count } { $count ->
    [one] rad påverkad
   *[other] rader påverkade
}

# Errors
error-generic = Ett fel uppstod
error-query-failed = Frågan misslyckades

# Editor
editor-placeholder = Skriv SQL-fråga här... (Ctrl+Enter eller Cmd+Enter för att köra)

# Navigation links
nav-terminal = SQL-terminaldemo
nav-compliance = SQL-testöverensstämmelserapport
nav-benchmarks = Prestandabenchmarks
nav-github = GitHub-arkiv
nav-home = Hem

# Results
results-success-zero = Frågan kördes framgångsrikt (0 rader)
results-null = NULL

# Help Modal
help-title = Tangentbordsgenvägar & Hjälp
help-close = Stäng
help-editor-shortcuts = Editorgenvägar
help-navigation = Navigering
help-results-actions = Resultatåtgärder
help-tips = Tips
help-shortcut-execute = Kör aktuell fråga
help-shortcut-comment = Växla radkommentar
help-shortcut-indent = Indentera markering
help-shortcut-show-help = Visa denna hjälpdialog
help-shortcut-close-help = Stäng hjälpdialog
help-action-copy = Kopiera till urklipp
help-action-copy-desc = Kopiera resultat som tabbseparerade värden
help-action-export = Exportera CSV
help-action-export-desc = Ladda ner resultat som CSV-fil
help-tip-limit = Resultat begränsade till 1 000 rader för prestanda. Använd LIMIT för att förfina frågor.
help-tip-time = Exekveringstid visas med frågeresultat.
help-tip-syntax = Editorn stöder SQL-syntaxmarkering och autokomplettering.
help-tip-theme = Växla mellan ljust/mörkt läge med temaknappen.
help-got-it = Uppfattat!

# Showcase Navigation
showcase-title = SQL:1999 Core Showcase
showcase-description = Utforska implementerade SQL:1999 Core-funktioner interaktivt
showcase-complete = { $percent }% färdigt
showcase-categories = Funktionskategorier
showcase-legend = Statusförklaring
showcase-status-implemented = Fullt implementerat
showcase-status-partial = Delvis implementerat
showcase-status-planned = Planerat

# Showcase category labels
showcase-cat-compliance = Överensstämmelsepanel
showcase-cat-data-types = Datatyper
showcase-cat-dml = DML-operationer
showcase-cat-predicates = Predikat & Operatorer
showcase-cat-joins = JOIN
showcase-cat-subqueries = Underfrågor
showcase-cat-aggregates = Aggregat & GROUP BY
showcase-cat-ddl = DDL & Begränsningar

# Common showcase elements
showcase-interactive-examples = Interaktiva exempel
showcase-try-example = Prova detta exempel
showcase-progress = { $implemented } av { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Kategori
showcase-table-description = Beskrivning
showcase-table-syntax = Syntax
showcase-table-use-case = Användningsfall

# Status labels
status-implemented = Implementerat
status-partial = Delvis
status-planned = Planerat

# Aggregates Showcase
aggregates-title = SQL Aggregat och GROUP BY
aggregates-description = SQL:1999 Core aggregatfunktioner och grupperingsmöjligheter
aggregates-reference = Aggregatfunktionsreferens
aggregates-table-function = Funktion
aggregates-progress-type = funktioner
aggregates-ex-basic = Grundläggande aggregatfunktioner
aggregates-ex-group-single = GROUP BY (en kolumn)
aggregates-ex-group-multiple = GROUP BY (flera kolumner)
aggregates-ex-having = HAVING-klausul
aggregates-ex-orderby = ORDER BY med aggregat
aggregates-ex-null = NULL-hantering i aggregat

# DML Operations Showcase
dml-title = DML-operationer (datamanipuleringsspråk)
dml-description = SQL:1999 Core operationer för att fråga och modifiera data
dml-reference = DML-operationsreferens
dml-table-operation = Operation
dml-progress-type = operationer
dml-ex-select-basic = SELECT - grundläggande frågor
dml-ex-select-ordering = SELECT - sortering och begränsning
dml-ex-insert = INSERT-operationer
dml-ex-update = UPDATE-operationer
dml-ex-delete = DELETE-operationer
dml-ex-combined = Kombinerat CRUD-arbetsflöde

# Data Types Showcase
datatypes-title = SQL:1999 Core Datatyper
datatypes-description = Utforska de grundläggande datatyperna definierade i SQL:1999 Core-specifikationen
datatypes-reference = Datatypsreferens
datatypes-table-type = Typnamn
datatypes-table-example = Exempelvärden
datatypes-table-spec = Specifikation
datatypes-progress-type = typer
datatypes-ex-numeric = Arbeta med numeriska typer
datatypes-ex-null = NULL-hantering & Trevärd logik
datatypes-ex-comparisons = Typjämförelser & Operationer

# JOINs Showcase
joins-title = SQL JOIN
joins-description = SQL:1999 Core JOIN-operationer för att kombinera data från flera tabeller
joins-reference = JOIN-typreferens
joins-table-type = JOIN-typ
joins-progress-type = JOIN-typer
joins-category-suffix = JOIN
joins-ex-sample = Exempeldatainställning
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Flertabells-JOIN

# Predicates Showcase
predicates-title = Predikat och Operatorer
predicates-description = SQL:1999 predikat för filtrering och logiska operationer
predicates-reference = Predikatreferens
predicates-table-predicate = Predikat
predicates-progress-type = predikat
predicates-ex-comparison = Jämförelseoperatorer
predicates-ex-between = BETWEEN och intervallpredikat
predicates-ex-null = NULL-predikat och trevärd logik
predicates-ex-boolean = Boolesk logik (AND, OR, NOT)
predicates-ex-in = IN-predikat med underfrågor
predicates-ex-combined = Kombinerade predikatoperationer

# Subqueries Showcase
subqueries-title = SQL Underfrågor
subqueries-description = SQL:1999 Core underfrågefunktioner för nästlade frågeoperationer
subqueries-reference = Underfrågetypreferens
subqueries-table-type = Underfrågetyp
subqueries-progress-type = underfrågetyper
subqueries-ex-scalar-select = Skalär underfråga i SELECT
subqueries-ex-scalar-where = Skalär underfråga i WHERE
subqueries-ex-derived = Härledda tabeller (underfråga i FROM)
subqueries-ex-in = IN-predikat med underfråga
subqueries-ex-correlated = Korrelerade underfrågor
subqueries-ex-nested = Nästlade underfrågor
