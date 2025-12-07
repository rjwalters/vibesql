# VibeSQL Web UI - Deutsch

# Page titles
page-title = VibeSQL - KI-gestützte SQL:1999 Datenbank
demo-title = VibeSQL Demo
benchmarks-title = Leistungsbenchmarks - VibeSQL
benchmarks-heading = VibeSQL - Leistungsbenchmarks
conformance-title = Konformitätsbericht - VibeSQL
conformance-heading = Konformitätsbericht
conformance-subtitle = SQL:1999 Standardkonformitätstest

# Navigation
nav-showcase = SQL:1999 Showcase
nav-conformance = sqltest-Ergebnisse anzeigen
nav-sqllogictest = SQLLogicTest-Ergebnisse anzeigen

# Editor section
editor-title = SQL-Editor
editor-storage = Speicher
editor-storage-init = Initialisierung...
editor-execute = Abfrage ausführen

# Results section
results-title = Ergebnisse
results-empty = Führen Sie eine Abfrage aus, um Ergebnisse zu sehen
results-loading = Laden...
results-rows = { $count } { $count ->
    [one] Zeile
   *[other] Zeilen
}
results-rows-with-time = { $count } { $count ->
    [one] Zeile
   *[other] Zeilen
} ({ $time }ms)
results-copy = In Zwischenablage kopieren
results-export = Als CSV exportieren
results-limit-warning = Zeige die ersten { $limit } von { $total } Zeilen. Verwenden Sie LIMIT, um Ihre Abfrage einzugrenzen.

# Examples sidebar
examples-title = Beispiele
examples-basic = Grundlegende Abfragen
examples-advanced = Erweiterte Abfragen

# Database selector
db-select-label = Datenbank

# Footer
footer-tagline = VibeSQL - SQL:1999 Datenbank in WebAssembly
footer-deployed = Bereitgestellt: { $date }

# Theme
theme-toggle-dark = Zum Dunkelmodus wechseln
theme-toggle-light = Zum Hellmodus wechseln

# Locale
locale-select = Sprache auswählen

# Messages
msg-query-success = Abfrage erfolgreich ausgeführt
msg-rows-affected = { $count } { $count ->
    [one] Zeile betroffen
   *[other] Zeilen betroffen
}

# Errors
error-generic = Ein Fehler ist aufgetreten
error-query-failed = Abfrage fehlgeschlagen

# Editor
editor-placeholder = SQL-Abfrage hier eingeben... (Strg+Enter oder Cmd+Enter zum Ausführen)

# Navigation links
nav-terminal = SQL-Terminal Demo
nav-compliance = SQL-Konformitätsbericht
nav-benchmarks = Leistungsbenchmarks
nav-github = GitHub Repository
nav-home = Startseite

# Results
results-success-zero = Abfrage erfolgreich ausgeführt (0 Zeilen)
results-null = NULL

# Help Modal
help-title = Tastenkürzel & Hilfe
help-close = Schließen
help-editor-shortcuts = Editor-Tastenkürzel
help-navigation = Navigation
help-results-actions = Ergebnis-Aktionen
help-tips = Tipps
help-shortcut-execute = Aktuelle Abfrage ausführen
help-shortcut-comment = Zeilenkommentar umschalten
help-shortcut-indent = Auswahl einrücken
help-shortcut-show-help = Diesen Hilfedialog anzeigen
help-shortcut-close-help = Hilfedialog schließen
help-action-copy = In Zwischenablage kopieren
help-action-copy-desc = Ergebnisse als tabulatorgetrennte Werte kopieren
help-action-export = Als CSV exportieren
help-action-export-desc = Ergebnisse als CSV-Datei herunterladen
help-tip-limit = Ergebnisse sind auf 1.000 Zeilen begrenzt. Verwenden Sie LIMIT, um Abfragen einzugrenzen.
help-tip-time = Die Ausführungszeit wird mit den Abfrageergebnissen angezeigt.
help-tip-syntax = Der Editor unterstützt SQL-Syntaxhervorhebung und Autovervollständigung.
help-tip-theme = Wechseln Sie zwischen Hell-/Dunkelmodus mit der Thema-Schaltfläche.
help-got-it = Verstanden!

# Showcase Navigation
showcase-title = SQL:1999 Core Showcase
showcase-description = Erkunden Sie die implementierten SQL:1999 Core-Funktionen interaktiv
showcase-complete = { $percent }% Abgeschlossen
showcase-categories = Funktionskategorien
showcase-legend = Statuslegende
showcase-status-implemented = Vollständig Implementiert
showcase-status-partial = Teilweise Implementiert
showcase-status-planned = Geplant

# Showcase category labels
showcase-cat-compliance = Konformitäts-Dashboard
showcase-cat-data-types = Datentypen
showcase-cat-dml = DML-Operationen
showcase-cat-predicates = Prädikate & Operatoren
showcase-cat-joins = JOINs
showcase-cat-subqueries = Unterabfragen
showcase-cat-aggregates = Aggregate & GROUP BY
showcase-cat-ddl = DDL & Constraints

# Common showcase elements
showcase-interactive-examples = Interaktive Beispiele
showcase-try-example = Beispiel Ausprobieren
showcase-progress = { $implemented } von { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Kategorie
showcase-table-description = Beschreibung
showcase-table-syntax = Syntax
showcase-table-use-case = Anwendungsfall

# Status labels
status-implemented = Implementiert
status-partial = Teilweise
status-planned = Geplant

# Aggregates Showcase
aggregates-title = SQL-Aggregate und GROUP BY
aggregates-description = SQL:1999 Core Aggregatfunktionen und Gruppierungsfähigkeiten
aggregates-reference = Aggregatfunktionen-Referenz
aggregates-table-function = Funktion
aggregates-progress-type = Funktionen
aggregates-ex-basic = Grundlegende Aggregatfunktionen
aggregates-ex-group-single = GROUP BY (Einzelne Spalte)
aggregates-ex-group-multiple = GROUP BY (Mehrere Spalten)
aggregates-ex-having = HAVING-Klausel
aggregates-ex-orderby = ORDER BY mit Aggregaten
aggregates-ex-null = NULL-Behandlung in Aggregaten

# DML Operations Showcase
dml-title = DML-Operationen (Datenmanipulationssprache)
dml-description = SQL:1999 Core-Operationen zum Abfragen und Ändern von Daten
dml-reference = DML-Operationen Referenz
dml-table-operation = Operation
dml-progress-type = Operationen
dml-ex-select-basic = SELECT - Grundlegende Abfragen
dml-ex-select-ordering = SELECT - Sortierung und Limitierung
dml-ex-insert = INSERT-Operationen
dml-ex-update = UPDATE-Operationen
dml-ex-delete = DELETE-Operationen
dml-ex-combined = Kombinierter CRUD-Workflow

# Data Types Showcase
datatypes-title = SQL:1999 Core Datentypen
datatypes-description = Erkunden Sie die grundlegenden Datentypen der SQL:1999 Core-Spezifikation
datatypes-reference = Datentypen-Referenz
datatypes-table-type = Typname
datatypes-table-example = Beispielwerte
datatypes-table-spec = Spezifikation
datatypes-progress-type = Typen
datatypes-ex-numeric = Arbeiten mit numerischen Typen
datatypes-ex-null = NULL-Behandlung & Dreiwertige Logik
datatypes-ex-comparisons = Typvergleiche & Operationen

# JOINs Showcase
joins-title = SQL JOINs
joins-description = SQL:1999 Core JOIN-Operationen zum Kombinieren von Daten aus mehreren Tabellen
joins-reference = JOIN-Typen Referenz
joins-table-type = JOIN-Typ
joins-progress-type = JOIN-Typen
joins-category-suffix = JOINs
joins-ex-sample = Beispieldaten-Setup
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Multi-Tabellen JOIN

# Predicates Showcase
predicates-title = Prädikate und Operatoren
predicates-description = SQL:1999 Prädikate für Filterung und logische Operationen
predicates-reference = Prädikaten-Referenz
predicates-table-predicate = Prädikat
predicates-progress-type = Prädikate
predicates-ex-comparison = Vergleichsoperatoren
predicates-ex-between = BETWEEN und Bereichsprädikate
predicates-ex-null = NULL-Prädikate und Dreiwertige Logik
predicates-ex-boolean = Boolesche Logik (AND, OR, NOT)
predicates-ex-in = IN-Prädikat mit Unterabfragen
predicates-ex-combined = Kombinierte Prädikat-Operationen

# Subqueries Showcase
subqueries-title = SQL-Unterabfragen
subqueries-description = SQL:1999 Core Unterabfrage-Fähigkeiten für verschachtelte Abfrageoperationen
subqueries-reference = Unterabfragetypen-Referenz
subqueries-table-type = Unterabfragetyp
subqueries-progress-type = Unterabfragetypen
subqueries-ex-scalar-select = Skalare Unterabfrage in SELECT
subqueries-ex-scalar-where = Skalare Unterabfrage in WHERE
subqueries-ex-derived = Abgeleitete Tabellen (Unterabfrage in FROM)
subqueries-ex-in = IN-Prädikat mit Unterabfrage
subqueries-ex-correlated = Korrelierte Unterabfragen
subqueries-ex-nested = Verschachtelte Unterabfragen
