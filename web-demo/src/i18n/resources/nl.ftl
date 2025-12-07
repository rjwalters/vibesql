# VibeSQL Web UI - Nederlands

# Page titles
page-title = VibeSQL - AI-aangedreven SQL:1999 Database
demo-title = VibeSQL Demo
benchmarks-title = Prestatiebenchmarks - VibeSQL
benchmarks-heading = VibeSQL - Prestatiebenchmarks
conformance-title = Conformiteitsrapport - VibeSQL
conformance-heading = Conformiteitsrapport
conformance-subtitle = SQL:1999 Standaard Conformiteitstest

# Navigation
nav-showcase = SQL:1999 Showcase
nav-conformance = Bekijk sqltest Resultaten
nav-sqllogictest = Bekijk SQLLogicTest Resultaten

# Editor section
editor-title = SQL Editor
editor-storage = Opslag
editor-storage-init = Initialiseren...
editor-execute = Query Uitvoeren

# Results section
results-title = Resultaten
results-empty = Voer een query uit om resultaten te zien
results-loading = Laden...
results-rows = { $count } { $count ->
    [one] rij
   *[other] rijen
}
results-rows-with-time = { $count } { $count ->
    [one] rij
   *[other] rijen
} ({ $time }ms)
results-copy = Kopiëren naar klembord
results-export = CSV Exporteren
results-limit-warning = Toont de eerste { $limit } van { $total } rijen. Gebruik LIMIT om uw query te verfijnen.

# Examples sidebar
examples-title = Voorbeelden
examples-basic = Basis Queries
examples-advanced = Geavanceerde Queries

# Database selector
db-select-label = Database

# Footer
footer-tagline = VibeSQL - SQL:1999 Database in WebAssembly
footer-deployed = Geïmplementeerd: { $date }

# Theme
theme-toggle-dark = Schakel naar donkere modus
theme-toggle-light = Schakel naar lichte modus

# Locale
locale-select = Selecteer taal

# Messages
msg-query-success = Query succesvol uitgevoerd
msg-rows-affected = { $count } { $count ->
    [one] rij beïnvloed
   *[other] rijen beïnvloed
}

# Errors
error-generic = Er is een fout opgetreden
error-query-failed = Query mislukt

# Editor
editor-placeholder = Voer SQL-query hier in... (Ctrl+Enter of Cmd+Enter om uit te voeren)

# Navigation links
nav-terminal = SQL Terminal Demo
nav-compliance = SQL Test Conformiteitsrapport
nav-benchmarks = Prestatiebenchmarks
nav-github = GitHub Repository
nav-home = Home

# Results
results-success-zero = Query succesvol uitgevoerd (0 rijen)
results-null = NULL

# Help Modal
help-title = Sneltoetsen & Hulp
help-close = Sluiten
help-editor-shortcuts = Editor Sneltoetsen
help-navigation = Navigatie
help-results-actions = Resultaat Acties
help-tips = Tips
help-shortcut-execute = Huidige query uitvoeren
help-shortcut-comment = Regelcommentaar wisselen
help-shortcut-indent = Selectie inspringen
help-shortcut-show-help = Dit hulpdialoog tonen
help-shortcut-close-help = Hulpdialoog sluiten
help-action-copy = Kopiëren naar klembord
help-action-copy-desc = Resultaten kopiëren als tab-gescheiden waarden
help-action-export = CSV Exporteren
help-action-export-desc = Resultaten downloaden als CSV-bestand
help-tip-limit = Resultaten zijn beperkt tot 1.000 rijen voor prestaties. Gebruik LIMIT om queries te verfijnen.
help-tip-time = Uitvoeringstijd wordt getoond bij queryresultaten.
help-tip-syntax = De editor ondersteunt SQL-syntaxiskleuring en automatische aanvulling.
help-tip-theme = Schakel tussen lichte/donkere modus met de themaknop.
help-got-it = Begrepen!

# Showcase Navigation
showcase-title = SQL:1999 Core Showcase
showcase-description = Verken de geïmplementeerde SQL:1999 Core functies interactief
showcase-complete = { $percent }% Voltooid
showcase-categories = Functiecategorieën
showcase-legend = Statuslegende
showcase-status-implemented = Volledig Geïmplementeerd
showcase-status-partial = Gedeeltelijk Geïmplementeerd
showcase-status-planned = Gepland

# Showcase category labels
showcase-cat-compliance = Conformiteitsdashboard
showcase-cat-data-types = Gegevenstypen
showcase-cat-dml = DML-operaties
showcase-cat-predicates = Predicaten & Operatoren
showcase-cat-joins = JOINs
showcase-cat-subqueries = Subqueries
showcase-cat-aggregates = Aggregaten & GROUP BY
showcase-cat-ddl = DDL & Beperkingen

# Common showcase elements
showcase-interactive-examples = Interactieve Voorbeelden
showcase-try-example = Dit Voorbeeld Proberen
showcase-progress = { $implemented } van { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Categorie
showcase-table-description = Beschrijving
showcase-table-syntax = Syntaxis
showcase-table-use-case = Gebruiksscenario

# Status labels
status-implemented = Geïmplementeerd
status-partial = Gedeeltelijk
status-planned = Gepland

# Aggregates Showcase
aggregates-title = SQL Aggregaten en GROUP BY
aggregates-description = SQL:1999 Core aggregaatfuncties en groeperingsmogelijkheden
aggregates-reference = Aggregaatfuncties Referentie
aggregates-table-function = Functie
aggregates-progress-type = functies
aggregates-ex-basic = Basis Aggregaatfuncties
aggregates-ex-group-single = GROUP BY (Enkele Kolom)
aggregates-ex-group-multiple = GROUP BY (Meerdere Kolommen)
aggregates-ex-having = HAVING Clausule
aggregates-ex-orderby = ORDER BY met Aggregaten
aggregates-ex-null = NULL Afhandeling in Aggregaten

# DML Operations Showcase
dml-title = DML-operaties (Data Manipulation Language)
dml-description = SQL:1999 Core operaties voor het bevragen en wijzigen van gegevens
dml-reference = DML-operaties Referentie
dml-table-operation = Operatie
dml-progress-type = operaties
dml-ex-select-basic = SELECT - Basisqueries
dml-ex-select-ordering = SELECT - Sorteren en Beperken
dml-ex-insert = INSERT Operaties
dml-ex-update = UPDATE Operaties
dml-ex-delete = DELETE Operaties
dml-ex-combined = Gecombineerde CRUD Workflow

# Data Types Showcase
datatypes-title = SQL:1999 Core Gegevenstypen
datatypes-description = Verken de fundamentele gegevenstypen gedefinieerd in de SQL:1999 Core specificatie
datatypes-reference = Gegevenstypen Referentie
datatypes-table-type = Typenaam
datatypes-table-example = Voorbeeldwaarden
datatypes-table-spec = Specificatie
datatypes-progress-type = typen
datatypes-ex-numeric = Werken met Numerieke Typen
datatypes-ex-null = NULL Afhandeling & Driewaardige Logica
datatypes-ex-comparisons = Typevergelijkingen & Operaties

# JOINs Showcase
joins-title = SQL JOINs
joins-description = SQL:1999 Core JOIN-operaties voor het combineren van gegevens uit meerdere tabellen
joins-reference = JOIN-typen Referentie
joins-table-type = JOIN Type
joins-progress-type = JOIN-typen
joins-category-suffix = JOINs
joins-ex-sample = Voorbeeldgegevens Setup
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Multi-tabel JOIN

# Predicates Showcase
predicates-title = Predicaten en Operatoren
predicates-description = SQL:1999 predicaten voor filtering en logische operaties
predicates-reference = Predicaten Referentie
predicates-table-predicate = Predicaat
predicates-progress-type = predicaten
predicates-ex-comparison = Vergelijkingsoperatoren
predicates-ex-between = BETWEEN en Bereikpredicaten
predicates-ex-null = NULL Predicaten en Driewaardige Logica
predicates-ex-boolean = Booleaanse Logica (AND, OR, NOT)
predicates-ex-in = IN Predicaat met Subqueries
predicates-ex-combined = Gecombineerde Predicaatoperaties

# Subqueries Showcase
subqueries-title = SQL Subqueries
subqueries-description = SQL:1999 Core subquerymogelijkheden voor geneste queryoperaties
subqueries-reference = Subquerytypen Referentie
subqueries-table-type = Subquerytype
subqueries-progress-type = subquerytypen
subqueries-ex-scalar-select = Scalaire Subquery in SELECT
subqueries-ex-scalar-where = Scalaire Subquery in WHERE
subqueries-ex-derived = Afgeleide Tabellen (Subquery in FROM)
subqueries-ex-in = IN Predicaat met Subquery
subqueries-ex-correlated = Gecorreleerde Subqueries
subqueries-ex-nested = Geneste Subqueries
