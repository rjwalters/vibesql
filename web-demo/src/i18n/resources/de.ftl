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

# =============================================================================
# Benchmark-Seite
# =============================================================================

# Abschnittsüberschriften
bench-section-embedded = Eingebettet
bench-section-server = Server
bench-results-title = Benchmark-Ergebnisse
bench-perf-comparison = Leistungsvergleich
bench-methodology-title = Methodik
bench-analysis-roadmap = Analyse & Roadmap

# Zusammenfassungskarten
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = Getestete Operationen
bench-last-updated = Zuletzt aktualisiert
bench-avg-speedup = durchschnittliche Beschleunigung
bench-from-main = vom main-Branch
bench-loading = Lädt...
bench-na = N/V
bench-faster = { $value }x schneller
bench-slower = { $value }x langsamer
bench-speedup = { $value }x
bench-startup-time-label = Startzeit
bench-download-size = Downloadgröße
bench-uncompressed = unkomprimiert
bench-size-metrics = Größenmetriken
bench-failed = FEHLGESCHLAGEN
bench-failed-title = Abfrage fehlgeschlagen (Zeitüberschreitung oder Fehler)
bench-no-wasm-data = Keine WASM-Daten verfügbar

# Tabellenüberschriften
bench-table-operation = Operation
bench-table-query = Query
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = Lade Benchmark-Ergebnisse...
bench-vibesql-server-title = VibeSQL über PostgreSQL-Protokoll

# Allgemeine Benchmark-Begriffe
bench-hardware = Hardware
bench-benchmark-framework = Benchmark-Framework
bench-scale-factor = Skalierungsfaktor
bench-data = Daten
bench-databases-tested = Getestete Datenbanken
bench-execution-mode = Ausführungsmodus
bench-measurement = Messung
bench-workload = Arbeitslast
bench-transaction-mix = Transaktionsmix
bench-warehouses = Lager
bench-concurrency = Nebenläufigkeit
bench-acid-compliance = ACID-Konformität
bench-mode = Modus
bench-workload-types = Arbeitslasttypen
bench-table-size = Tabellengröße
bench-index-types = Indextypen
bench-operations = Operationen
bench-databases = Datenbanken
bench-protocol-overhead = Protokoll-Overhead
bench-binary-size = Binärgröße
bench-startup-time = Startzeit
bench-peak-memory = Maximaler Speicher
bench-schema = Schema
bench-query-count = Abfrageanzahl
bench-query-types = Abfragetypen
bench-sql-features = SQL-Funktionen
bench-wasm-size = WASM-Größe
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H spezifisch
bench-tpch-name = TPC-H
bench-tpch-title = TPC-H Entscheidungsunterstützungs-Benchmark
bench-tpch-description = Diese Benchmarks verwenden die branchenübliche <strong>TPC-H Benchmark-Suite</strong>, die reale Entscheidungsunterstützungs-Arbeitslasten mit komplexen analytischen Abfragen simuliert, einschließlich Aggregationen, Joins, Unterabfragen und Sortierung.
bench-tpch-ops-label = TPC-H-Abfragen
bench-tpch-note-intro = Alle Benchmarks messen die End-to-End-Abfrageausführungszeit einschließlich Parsing, Planung, Ausführung und Ergebnismaterialisierung. Dies repräsentiert die <strong>reale SQL-Engine-Leistung</strong> für analytische Arbeitslasten.
bench-tpch-note-queries = <strong>Hinweis:</strong> TPC-H-Abfragen testen verschiedene Aspekte der SQL-Leistung: einfache Aggregationen (Q1, Q6), komplexe Joins (Q2-Q5, Q7-Q10), Unterabfragen (Q11-Q15) und erweiterte Analytik (Q16-Q22). Fahren Sie mit der Maus über die Abfragenamen in der Tabelle für Beschreibungen.

# TPC-H Diskussion
bench-tpch-disc-excels-title = Wo VibeSQL glänzt
bench-tpch-disc-excels = VibeSQL zeigt starke Leistung bei <strong>scan-intensiven Aggregationsabfragen</strong> (Q1, Q6, Q14, Q15, Q20), wo unsere spaltenorientierte Ausführungsengine und SIMD-beschleunigte Aggregationen hervorstechen. Diese Abfragen beinhalten das Filtern großer Tabellen und die Berechnung von Aggregaten ohne komplexe Join-Muster.
bench-tpch-disc-targets-title = Aktuelle Optimierungsziele
bench-tpch-disc-targets = Mehrfach-Join-Abfragen (Q3, Q5, Q7-Q10, Q18, Q19, Q21) zeigen derzeit SQLite vorne. Der Hauptengpass ist unsere Hash-Join-Implementierung, die noch nicht das gleiche Optimierungsniveau wie SQLites jahrzehntelang verfeinerte B-Tree-Joins erreicht. Spezifische Bereiche in aktiver Entwicklung:
bench-tpch-disc-join-ordering = Verbesserte Kardinalitätsschätzung für bessere Join-Reihenfolgeauswahl
bench-tpch-disc-hash-sizing = Adaptives Hash-Tabellen-Wachstum und Spill-to-Disk für große Joins
bench-tpch-disc-vectorized = Batch-Verarbeitung in der Join-Innenschleife zur Verbesserung der Cache-Nutzung
bench-tpch-disc-inl-joins = Nutzung von B-Tree-Indizes wenn vorteilhaft
bench-tpch-disc-path-title = Weg zur Führungsposition
bench-tpch-disc-path = VibeSQLs Architektur ist für moderne Hardware konzipiert mit Funktionen wie spaltenorientierter Speicherung, vektorisierter Ausführung und sperrenfreier Nebenläufigkeit. Mit der Reifung dieser Optimierungen erwarten wir, dass VibeSQL eine konsistente Führungsposition bei allen TPC-H-Abfragen erreicht. Das grundlegende Design unterstützt Parallelismus und SIMD, die traditionelle zeilenbasierte Datenbanken nicht einfach nachrüsten können.

# TPC-H Abfragebeschreibungen
bench-tpch-q1 = Preiszusammenfassungsbericht - Preisaggregation mit GROUP BY und ORDER BY
bench-tpch-q2 = Minimalkostenlieferant - 3-Tabellen-JOIN mit ORDER BY und LIMIT
bench-tpch-q3 = Versandpriorität - 3-Tabellen-JOIN mit Aggregation
bench-tpch-q4 = Bestellprioritätsprüfung - Korrelierte EXISTS-Unterabfrage
bench-tpch-q5 = Lokales Lieferantenvolumen - 6-Tabellen-JOIN mit komplexer Filterung
bench-tpch-q6 = Umsatzänderungsprognose - WHERE-Filter mit BETWEEN und SUM
bench-tpch-q7 = Versandvolumen - 6-Tabellen-JOIN mit SUBSTR und Datumsfilterung
bench-tpch-q8 = Nationaler Marktanteil - 7-Tabellen-JOIN mit CASE-Ausdrücken
bench-tpch-q9 = Produkttyp-Gewinnmessung - 4-Tabellen-JOIN mit Aggregation
bench-tpch-q10 = Rückgabeartikel-Berichterstattung - 4-Tabellen-JOIN mit TOP-N LIMIT
bench-tpch-q11 = Wichtige Bestandsidentifikation - Unterabfrage in HAVING-Klausel
bench-tpch-q12 = Versandmodus-Priorität - CASE-Aggregation mit Datumslogik
bench-tpch-q13 = Kundenverteilung - LEFT OUTER JOIN mit Unterabfrage
bench-tpch-q14 = Promotion-Effekt - Bedingte Aggregation mit CASE
bench-tpch-q15 = Top-Lieferant - Verschachtelte Unterabfragen mit MAX
bench-tpch-q16 = Teile/Lieferanten-Beziehung - NOT IN-Unterabfrage mit DISTINCT
bench-tpch-q17 = Kleinmengenbestellungs-Umsatz - Korrelierte Unterabfrage in WHERE
bench-tpch-q18 = Großvolumen-Kunde - GROUP BY mit HAVING
bench-tpch-q19 = Rabatt-Umsatz - Komplexe OR-Bedingungen
bench-tpch-q20 = Potenzielle Teilepromotion - IN-Unterabfrage mit GROUP BY/HAVING
bench-tpch-q21 = Lieferanten mit Bestellverzögerung - Mehrfach-Tabellen EXISTS
bench-tpch-q22 = Globale Verkaufschance - SUBSTR mit NOT EXISTS-Unterabfrage

# TPC-DS spezifisch
bench-tpcds-name = TPC-DS
bench-tpcds-title = TPC-DS Entscheidungsunterstützungs-Benchmark
bench-tpcds-description = <strong>TPC-DS</strong> ist der Nachfolger von TPC-H mit 99 Abfragen, die ein modernes Entscheidungsunterstützungssystem modellieren mit deutlich komplexeren Abfragemustern einschließlich mehrerer Faktentabellen, Schneeflockenschema und erweiterten SQL-Funktionen.
bench-tpcds-ops-label = TPC-DS-Abfragen
bench-tpcds-note-intro = TPC-DS-Abfragen sind wesentlich komplexer als TPC-H und testen erweiterte SQL-Funktionen wie Fensterfunktionen, Common Table Expressions (WITH-Klausel) und komplexe Join-Muster über mehrere Fakten- und Dimensionstabellen.
bench-tpcds-note-remaining = <strong>Hinweis:</strong> Verbleibende nicht unterstützte Abfragen erfordern Funktionen wie INTERSECT/EXCEPT oder spezifische Datumsarithmetikfunktionen, die noch nicht implementiert sind.

# TPC-DS Diskussion
bench-tpcds-disc-coverage-title = SQL:1999 Funktionsabdeckung
bench-tpcds-disc-coverage = TPC-DS testet die anspruchsvollsten SQL-Funktionen. VibeSQL besteht <strong>88 von 99 Abfragen</strong> und demonstriert breite Abdeckung von SQL:1999 einschließlich ROLLUP, CUBE, GROUPING(), Fensterfunktionen mit komplexem Framing und rekursiven CTEs. Die verbleibenden Abfragen erfordern INTERSECT/EXCEPT-Mengenoperationen.
bench-tpcds-disc-optimization-title = Komplexe Abfrageoptimierung
bench-tpcds-disc-optimization = TPC-DS-Abfragen verbinden oft mehr als 10 Tabellen mit korrelierten Unterabfragen. Aktuelle Fokusgebiete:
bench-tpcds-disc-cte = Intelligente Entscheidung zwischen materialisierten und Inline-CTEs
bench-tpcds-disc-decorrelation = Konvertierung korrelierter Unterabfragen in Joins wenn vorteilhaft
bench-tpcds-disc-star = Fakten-Dimensions-Join-Reihenfolge für analytische Muster
bench-tpcds-disc-toward-title = Auf dem Weg zu 99/99
bench-tpcds-disc-toward = INTERSECT und EXCEPT sind geplante Ergänzungen, die die verbleibenden Abfragen ermöglichen werden. Diese Mengenoperationen fügen sich natürlich in unsere bestehende Abfragealgebra ein und werden als hash-basierte Operatoren ähnlich unserer DISTINCT-Verarbeitung implementiert.

# TPC-C spezifisch
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online-Transaktionsverarbeitungs-Benchmark
bench-tpcc-description = Der <strong>TPC-C-Benchmark</strong> simuliert eine vollständige Bestelleingabeumgebung mit einer Mischung aus komplexen Transaktionen einschließlich Bestelleingabe, Zahlungsverarbeitung, Bestellstatusabfragen, Lieferverarbeitung und Lagerbestandsüberwachung.
bench-tpcc-ops-label = TPC-C-Transaktionen
bench-tpcc-transactions-label = ausgeführte Transaktionen
bench-tpcc-note-intro = TPC-C misst Transaktionen pro Minute (tpmC) und testet die Fähigkeit der Datenbank, konkurrierende Transaktionen mit komplexer Geschäftslogik zu verarbeiten. Dieser Benchmark ist kritisch für die Bewertung der <strong>transaktionalen Arbeitslastleistung</strong>.
bench-tpcc-note-results = <strong>Hinweis:</strong> Ergebnisse zeigen die durchschnittliche Transaktionslatenz. Niedriger ist besser. TPC-C ist besonders anspruchsvoll für schreibintensive Arbeitslasten mit strengen Konsistenzanforderungen.

# TPC-C Transaktionsbeschreibungen
bench-tpcc-new-order = Neue Bestellung - Komplexe Transaktion mit Bestandsprüfungen und Bestellerstellung
bench-tpcc-payment = Zahlung - Aktualisierung von Kundensaldo und Lager-/Bezirkssummen
bench-tpcc-order-status = Bestellstatus - Nur-Lese-Abfrage für Kundenbestellhistorie
bench-tpcc-delivery = Lieferung - Stapelverarbeitung ausstehender Bestellungen
bench-tpcc-stock-level = Lagerbestand - Zählen von Artikeln unter dem Schwellenwert in aktuellen Bestellungen

# TPC-C Diskussion
bench-tpcc-disc-faster-title = 42x schneller als SQLite
bench-tpcc-disc-faster = VibeSQL erreicht <strong>~79.000 Transaktionen pro Sekunde</strong> im Vergleich zu SQLites ~1.900 TPS, eine 42-fache Verbesserung. Diese dramatische Beschleunigung kommt von unserer sperrenfreien MVCC-Architektur, die SQLites grobkörnige Sperrung bei jeder Schreiboperation vermeidet.
bench-tpcc-disc-dominates-title = Warum VibeSQL OLTP dominiert
bench-tpcc-disc-lockfree = MVCC ermöglicht Lesern und Schreibern gleichzeitiges Fortschreiten ohne Blockierung
bench-tpcc-disc-optimistic = Transaktionen kollidieren nur zum Commit-Zeitpunkt, nicht während der Ausführung
bench-tpcc-disc-btree = Zweckgebaute Indexstruktur optimiert für In-Memory-Arbeitslasten
bench-tpcc-disc-prepared = Abfragepläne werden einmal kompiliert und wiederverwendet
bench-tpcc-disc-scaling-title = Weitere Skalierung
bench-tpcc-disc-scaling = Aktuelle Ergebnisse sind single-threaded. VibeSQLs Architektur unterstützt Multi-Thread-Transaktionsverarbeitung, und wir erwarten nahezu lineare Skalierung mit der Hinzufügung paralleler Ausführungsunterstützung. Unser Ziel ist 500K+ TPS auf moderner Multi-Core-Hardware.
bench-tpcc-disc-duckdb-title = Why DuckDB Lags on OLTP
bench-tpcc-disc-duckdb = DuckDB achieves only ~385 TPS on TPC-C (60x slower than VibeSQL, 12x slower than SQLite). This is expected: DuckDB is an <strong>analytical (OLAP) database</strong> optimized for large batch operations, not single-row transactions. Its columnar storage format excels at scanning millions of rows but adds overhead for point lookups and small updates that dominate OLTP workloads like TPC-C.

# Sysbench Eingebettet spezifisch
bench-sysbench-embedded-name = Sysbench (Eingebettet)
bench-sysbench-embedded-title = Sysbench Mikro-Benchmarks (Eingebettet)
bench-sysbench-embedded-description = <strong>Sysbench</strong> bietet fokussierte Mikro-Benchmarks, die spezifische Datenbankoperationen isolieren. Diese Tests messen die Rohleistung für grundlegende Operationen ohne die Komplexität vollständiger Transaktionsarbeitslasten.
bench-sysbench-embedded-ops-label = Sysbench-Operationen
bench-sysbench-embedded-note = Der eingebettete Modus führt die Datenbank im Prozess mit null Netzwerk-Overhead aus, ideal für Single-Process-Anwendungen, bei denen minimale Latenz kritisch ist.

# Sysbench Operationsbeschreibungen
bench-sysbench-point-select = Punktabfrage - Einzelzeilensuche nach Primärschlüssel
bench-sysbench-insert = Einfügen - Neue Zeilen in Tabelle einfügen
bench-sysbench-update-index = Index-Update - Indizierte Spalte aktualisieren (k = k + 1)
bench-sysbench-update-non-index = Nicht-Index-Update - Nicht-indizierte Spalte aktualisieren
bench-sysbench-delete = Löschen - Zeilen nach Primärschlüssel entfernen
bench-sysbench-range-queries = Bereichsabfragen - Einfache, SUM, ORDER BY und DISTINCT Bereichsscans

# Sysbench Eingebettet Diskussion
bench-sysbench-emb-disc-point-title = Punktabfragen: VibeSQL führt
bench-sysbench-emb-disc-point = VibeSQLs direkte API erreicht <strong>~137ns pro Punktabfrage</strong>, gleichauf mit SQLite und weit vor DuckDB (~140µs). Unsere B-Tree-Implementierung ist für Einzelzeilensuchen mit minimalem Zeigerverfolgung und cache-freundlichen Knotenlayouts optimiert.
bench-sysbench-emb-disc-index-title = Index-Updates: 2x schneller
bench-sysbench-emb-disc-index = VibeSQLs indizierte Updates laufen bei <strong>~740ns vs SQLites ~1,6µs</strong>. Unser MVCC-Design ermöglicht In-Place-Index-Updates ohne Write-Ahead-Logging-Overhead für jede Operation.
bench-sysbench-emb-disc-improve-title = Verbesserungsbereiche
bench-sysbench-emb-disc-bulk = SQLites Batch-Insert-Pfad ist hochoptimiert; wir fügen Batch-B-Tree-Operationen hinzu
bench-sysbench-emb-disc-nonindex = Vollständige Tabellenscans für nicht-indizierte Spalten benötigen Prädikat-Pushdown-Optimierung
bench-sysbench-emb-disc-deletes = Unsere Tombstone-basierte Löschung hat Bereinigungsoverhead; Kompaktierungsverbesserungen sind geplant
bench-sysbench-emb-disc-duckdb-title = DuckDB-Vergleich
bench-sysbench-emb-disc-duckdb = DuckDB ist für analytische Arbeitslasten optimiert, nicht für Mikrooperationen. Seine 100-1000x langsameren Ergebnisse hier spiegeln architektonische Entscheidungen wider (spaltenorientierte Speicherung, vektorisierte Ausführung), die Einzelzeilenlatenz gegen Massendurchsatz eintauschen. VibeSQL zielt auf beide Anwendungsfälle.

# Sysbench Server spezifisch
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Mikro-Benchmarks (Server)
bench-sysbench-server-description = <strong>Sysbench</strong> Server-Benchmarks vergleichen VibeSQL Server (PostgreSQL-Protokoll) mit MySQL und messen die Leistung für Multi-Client-Datenbankbereitstellungen.
bench-sysbench-server-ops-label = Sysbench-Operationen
bench-sysbench-server-note = Der Server-Modus verwendet das PostgreSQL-Protokoll und ermöglicht Multi-Client-Zugriff und Kompatibilität mit bestehenden PostgreSQL-Tools und -Treibern.

# Sysbench Server Diskussion
bench-sysbench-srv-disc-protocol-title = PostgreSQL-Protokoll
bench-sysbench-srv-disc-protocol = VibeSQL Server implementiert das PostgreSQL-Protokoll und ermöglicht Kompatibilität mit bestehenden PostgreSQL-Treibern und -Tools. Dies fügt ~10-50µs Protokoll-Overhead pro Abfrage im Vergleich zum eingebetteten Modus hinzu, ermöglicht aber Multi-Client-Bereitstellungen.
bench-sysbench-srv-disc-mysql-title = MySQL-Vergleich
bench-sysbench-srv-disc-mysql = Server-Benchmarks vergleichen mit MySQL, um VibeSQL als Drop-in-Ersatz für traditionelle Client-Server-Datenbanken zu bewerten. Ergebnisse variieren je nach Operationstyp, wobei VibeSQL Vorteile bei leseintensiven Arbeitslasten zeigt.
bench-sysbench-srv-disc-roadmap-title = Server-Roadmap
bench-sysbench-srv-disc-pooling = Reduzierung des Verbindungsaufbau-Overheads für Hochdurchsatzszenarien
bench-sysbench-srv-disc-caching = Serverseitiges Caching von Abfrageplänen über Verbindungen hinweg
bench-sysbench-srv-disc-extended = Vollständige PostgreSQL Extended Query Protocol-Unterstützung für Batch-Operationen

# Footprint Eingebettet spezifisch
bench-footprint-embedded-name = Footprint (Eingebettet)
bench-footprint-embedded-title = Native Binär-Footprint
bench-footprint-embedded-description = <strong>Eingebettete Footprint-Benchmarks</strong> messen die Ressourceneffizienz nativer Datenbankbinärdateien und vergleichen Binärgröße, Kaltstartzeit und maximale Speichernutzung.
bench-footprint-embedded-ops-label = verglichene Datenbanken
bench-footprint-embedded-note = Der native Binär-Footprint ist kritisch für <strong>eingebettete und Edge-Bereitstellungen</strong>, bei denen Binärgröße, Startlatenz und Speicherverbrauch die Bereitstellungsmachbarkeit direkt beeinflussen.

# Footprint Eingebettet Beschreibungen
bench-footprint-binary-size = Binärgröße - Größe der kompilierten Datenbankbinärdatei auf der Festplatte
bench-footprint-startup-time = Startzeit - Zeit für Kaltstart und Ausführung der ersten Abfrage
bench-footprint-peak-memory = Maximaler Speicher - Maximale Resident-Set-Größe während der Initialisierung

# Footprint Eingebettet Diskussion
bench-footprint-emb-disc-size-title = Binärgröße: Goldener Mittelweg
bench-footprint-emb-disc-size = VibeSQL mit <strong>~17 MB</strong> liegt zwischen SQLite (~5 MB) und DuckDB (~45 MB). Dies spiegelt unsere Entscheidung wider, erweiterte Funktionen (Fensterfunktionen, CTEs, spaltenorientierte Ausführung) einzuschließen und gleichzeitig die Binärdatei für eingebettete Bereitstellungen handhabbar zu halten.
bench-footprint-emb-disc-startup-title = Start: Schnellster Kaltstart
bench-footprint-emb-disc-startup = VibeSQL erreicht <strong>~7,7 ms Kaltstart</strong>, etwas schneller als SQLite (~8,2 ms) und deutlich schneller als DuckDB (~14,6 ms). Unser minimaler Initialisierungspfad lädt beim Start nur wesentliche Metadatenstrukturen.
bench-footprint-emb-disc-memory-title = Speichereffizienz
bench-footprint-emb-disc-memory = Der maximale Speicher beim Start beträgt ~7 MB für VibeSQL vs ~3 MB für SQLite und ~11 MB für DuckDB. Der Unterschied zu SQLite spiegelt unseren ausgefeilteren Abfrageoptimierer und die spaltenorientierte Ausführungsinfrastruktur wider, die vorab allokiert wird.
bench-footprint-emb-disc-roadmap-title = Größenreduzierungs-Roadmap
bench-footprint-emb-disc-flags = Kompilierzeit-Funktionsauswahl zum Ausschließen ungenutzter Funktionalität
bench-footprint-emb-disc-lto = Gesamtprogramm-Link-Time-Optimierung zur Eliminierung toten Codes
bench-footprint-emb-disc-modular = Trennung der Kern-Engine von optionalen Funktionen (z.B. Fensterfunktionen)

# Footprint Server/WASM spezifisch
bench-footprint-server-name = Footprint (Server/WASM)
bench-footprint-server-title = WASM-Footprint
bench-footprint-server-description = <strong>WASM-Footprint-Benchmarks</strong> messen die WebAssembly-Modulgröße für Browser-Bereitstellung, kritisch für Webanwendungen, bei denen die Downloadgröße die Benutzererfahrung beeinflusst.
bench-footprint-server-ops-label = Bereitstellungsziele
bench-footprint-server-note = WASM-Größen sind kritisch für <strong>Web-Bereitstellungen</strong>, bei denen die Downloadzeit direkt die Zeit bis zur Interaktivität beeinflusst. Gzip-Größen sind am relevantesten, da Browser gzip-Inhalte automatisch dekomprimieren.
bench-footprint-server-note2 = <strong>Hinweis:</strong> VibeSQL WASM ist für minimale Downloadgröße konzipiert bei gleichzeitiger Beibehaltung voller SQL:1999-Konformität im Browser.

# Footprint Server Beschreibungen
bench-footprint-wasm-size = WASM-Größe - Größe des WebAssembly-Moduls für Browser-Bereitstellung
bench-footprint-wasm-gzip = WASM (gzip) - Komprimierte Größe für Web-Auslieferung

# Footprint Server Diskussion
bench-footprint-srv-disc-wasm-title = WASM: 2,2 MB komprimiert
bench-footprint-srv-disc-wasm = VibeSQLs WebAssembly-Modul komprimiert auf <strong>~2,2 MB gzipped</strong> und ermöglicht schnelle initiale Seitenladevorgänge. Dies ist eine vollständige SQL:1999-Datenbank mit Fensterfunktionen, CTEs und ACID-Transaktionen, die vollständig im Browser läuft.
bench-footprint-srv-disc-included-title = Was enthalten ist
bench-footprint-srv-disc-parser = Vollständiger SQL-Parser und Abfrageoptimierer
bench-footprint-srv-disc-btree = B-Tree-Speicher-Engine mit MVCC
bench-footprint-srv-disc-window = Fensterfunktionen und erweiterte Aggregationen
bench-footprint-srv-disc-cte = Common Table Expressions (WITH-Klausel)
bench-footprint-srv-disc-acid = Volle ACID-Transaktionsunterstützung
bench-footprint-srv-disc-benefits-title = Vorteile der Browser-Bereitstellung
bench-footprint-srv-disc-benefits = SQL im Browser ausführen eliminiert Round-Trip-Latenz zu Servern, ermöglicht Offline-First-Anwendungen und hält sensible Daten auf dem Gerät des Benutzers. VibeSQLs WASM-Build ist für diesen Anwendungsfall mit minimalen Abhängigkeiten und effizienter Speichernutzung konzipiert.
bench-footprint-srv-disc-roadmap-title = WASM-Roadmap
bench-footprint-srv-disc-streaming = Ausführung beginnen während das Modul herunterlädt
bench-footprint-srv-disc-indexeddb = Dauerhafte Speicherung über Browser-Sitzungen hinweg
bench-footprint-srv-disc-worker = Abfragen außerhalb des Hauptthreads für reaktive UIs ausführen

# Aufzählungspunkt-Labels (verwendet mit Beschreibungen)
bench-bullet-join-ordering = Join-Reihenfolge
bench-bullet-hash-sizing = Hash-Tabellen-Dimensionierung
bench-bullet-vectorized = Vektorisierte Joins
bench-bullet-inl-joins = Index-Nested-Loop-Joins
bench-bullet-cte-materialization = CTE-Materialisierung
bench-bullet-decorrelation = Unterabfragen-Dekorrelation
bench-bullet-star-optimization = Sternschema-Optimierung
bench-bullet-lock-free = Sperrenfreie Lesezugriffe
bench-bullet-optimistic = Optimistische Nebenläufigkeit
bench-bullet-btree = In-Memory B-Tree
bench-bullet-prepared = Prepared-Statement-Caching
bench-bullet-bulk-inserts = Masseneinfügungen
bench-bullet-non-indexed = Nicht-indizierte Updates
bench-bullet-deletes = Löschungen
bench-bullet-connection-pooling = Verbindungspooling
bench-bullet-stmt-caching = Prepared-Statement-Caching
bench-bullet-extended-protocol = Extended Query Protocol
bench-bullet-feature-flags = Feature-Flags
bench-bullet-lto = LTO-Optimierung
bench-bullet-modular = Modulare Builds
bench-bullet-streaming = Streaming-Kompilierung
bench-bullet-indexeddb = IndexedDB-Persistenz
bench-bullet-worker = Worker-Thread-Unterstützung

# =============================================================================
# Konformitätsseite
# =============================================================================

# Übersichtssektion
conformance-sql-conformance = SQL-Konformität
conformance-testing-against = Tests gegen SQLLogicTest - die branchenübliche SQL-Testsuite
conformance-full-pass-rate = 100% Datei-Erfolgsrate erreicht!
conformance-tests-passing = Bestandene Tests
conformance-files-passing = Bestandene Dateien
conformance-loading = Lade Konformitätsbericht...
conformance-error-loading = Fehler beim Laden des Berichts
conformance-no-data = Keine Konformitätsdaten verfügbar

# Kategorieaufschlüsselung
conformance-category-title = Testabdeckung nach Kategorie
conformance-category-header = Kategorie
conformance-pass-rate-header = Erfolgsrate
conformance-progress-header = Fortschritt
conformance-tests-header = Tests
conformance-cat-select = SELECT-Abfragen
conformance-cat-aggregates = Aggregate
conformance-cat-joins = JOINs
conformance-cat-expressions = Ausdrücke
conformance-cat-subqueries = Unterabfragen
conformance-cat-index = Index-Operationen
conformance-cat-ddl = DDL-Anweisungen
conformance-cat-evidence = Evidenz-Tests
conformance-cat-random = Zufallstests
conformance-cat-other = Andere Tests

# Zeitachse
conformance-timeline-title = Erfolgsraten-Verlauf
conformance-timeline-desc = Konformitätsfortschritt der letzten 90 Tage
conformance-timeline-loading = Lade Diagrammdaten...

# Meilensteine
conformance-milestones-title = Meilensteine

# Tests lokal ausführen
conformance-running-locally-title = Tests lokal ausführen
conformance-run-sqltest = # SQL:1999 Konformitätstests ausführen
conformance-run-sqllogictest = # SQLLogicTest-Suite ausführen (dauert Stunden)
conformance-generate-coverage = # Abdeckungsbericht generieren
conformance-open-coverage = # Abdeckungsbericht öffnen

# Legacy sqltest Sektion
conformance-sqltest-title = sqltest-Ergebnisse
conformance-sqltest-desc = Ergebnisse von <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a> - eine Community-gepflegte BNF-gesteuerte Konformitäts-Testsuite, abgeleitet vom SQL:1999-Standard, mit 739 Tests für Core- und Foundation-Funktionen.
conformance-overall-pass-rate = Gesamterfolgsrate
conformance-tests-of-passing = { $passed } von { $total } Tests bestanden
conformance-passed = Bestanden
conformance-failed = Fehlgeschlagen
conformance-errors = Fehler
conformance-test-coverage = Testabdeckung
conformance-core-features = Core-Funktionen (E-Serie)
conformance-additional-features = Zusätzliche Funktionen

# Funktionscodes
conformance-e011 = Numerische Datentypen
conformance-e021 = Zeichenketten-Typen
conformance-e031 = Bezeichner
conformance-e051 = Grundlegende Abfragespezifikation
conformance-e061 = Grundlegende Prädikate und Suchbedingungen
conformance-e071 = Grundlegende Abfrageausdrücke
conformance-e081 = Grundlegende Privilegien
conformance-e091 = Mengenfunktionen
conformance-e101 = Grundlegende Datenmanipulation
conformance-e111 = Einzelzeilen-SELECT-Anweisung
conformance-e121 = Grundlegende Cursor-Unterstützung
conformance-e131 = NULL-Wert-Unterstützung
conformance-e141 = Grundlegende Integritätsbedingungen
conformance-e151 = Transaktionsunterstützung
conformance-e161 = SQL-Kommentare
conformance-f031 = Grundlegende Schema-Manipulation

# SQLLogicTest Sektion
conformance-slt-title = SQLLogicTest-Ergebnisse
conformance-slt-desc = Ergebnisse der umfassenden <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a>-Suite mit ~5,9 Millionen Tests in 623 Testdateien aus dem offiziellen SQLite-Korpus.
conformance-files-of-passing = { $passed } von { $total } Testdateien bestanden
conformance-test-categories = Testkategorien
conformance-slt-select = SELECT-Tests
conformance-slt-evidence = Evidenz-Tests
conformance-slt-index = Index-Tests
conformance-slt-random = Zufallstests
conformance-slt-ddl = DDL-Tests
conformance-slt-other = Andere Tests
conformance-slt-note = <strong>Hinweis:</strong> SQLLogicTest bietet eine andere Perspektive als sqltest. Während sich sqltest auf BNF-Grammatikkonformität der SQL:1999-Spezifikation konzentriert, enthält SQLLogicTest Millionen realer SQL-Abfragen, die praktische Korrektheit in einer Vielzahl von Szenarien testen.

# Erklärungssektion
conformance-explanation-title = Unsere Testsuites verstehen
conformance-what-is-sqltest = Was ist sqltest?
conformance-sqltest-explanation = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a> ist eine Community-gepflegte Testsuite von Elliot Chance, die BNF-gesteuerte Konformitätstests basierend auf dem SQL:1999-Standard bereitstellt. Sie enthält 739 Tests für Core- und Foundation-Funktionen in E- und F-Serien-Testkategorien. Diese Suite testet, ob unsere Implementierung der SQL:1999-Grammatikspezifikation entspricht.
conformance-what-is-slt = Was ist SQLLogicTest?
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> ist eine umfassende Testsuite, ursprünglich für SQLite entwickelt, mit ~5,9 Millionen SQL-Testfällen in 623 Testdateien. Sie testet praktische Korrektheit durch Ausführung realer Abfragen und Validierung der Ergebnisse. Diese Suite konzentriert sich auf semantische Korrektheit und Grenzfälle statt auf reine Grammatikkonformität.
conformance-how-complement = Wie ergänzen sie sich?
conformance-sqltest-validates = <span class="font-medium">sqltest (BNF-gesteuert):</span> Validiert Grammatikkonformität zu SQL:1999-Standardspezifikationen
conformance-slt-validates = <span class="font-medium">SQLLogicTest (Ergebnis-gesteuert):</span> Validiert semantische Korrektheit mit Millionen realer Abfragen
conformance-coverage-point = <span class="font-medium">Abdeckung:</span> sqltest umfasst 739 Standard-Funktionstests; SQLLogicTest umfasst praktische Szenarien
conformance-philosophy-point = <span class="font-medium">Philosophie:</span> sqltest fragt "Können Sie das parsen?"; SQLLogicTest fragt "Funktioniert das korrekt?"
conformance-what-is-core = Was ist SQL:1999 Core?
conformance-core-explanation = SQL:1999 Core ist die offizielle obligatorische Funktionsmenge, definiert im SQL:1999-Standard (ISO/IEC 9075:1999). Sie besteht aus etwa 169 erforderlichen Funktionen, die jede Datenbank, die Core-Konformität beansprucht, implementieren muss. Offizielle Core-Konformität wird durch die NIST SQL-Testsuite verifiziert, nicht durch Community-Testsuites.
conformance-what-mean = Was bedeuten unsere Erfolgsraten?
conformance-pass-rates-mean = Unsere <strong>sqltest-Erfolgsrate von { $sqltestRate }%</strong> ({ $sqltestPassed }/{ $sqltestTotal } Tests) demonstriert starke SQL:1999-Grammatikkonformität. { $sltInfo } Zusammen zeigen diese Ergebnisse umfassende SQL:1999-Konformität, obwohl sie keine offizielle Core-Zertifizierung darstellen.
conformance-slt-pass-info = Unsere <strong>SQLLogicTest-Erfolgsrate von { $sltRate }%</strong> ({ $sltPassed }/{ $sltTotal } Testdateien) zeigt, dass wir reale Abfragen korrekt verarbeiten.
conformance-bottom-line = <strong>Fazit:</strong> Wir verwenden zwei komplementäre Testsuites, um sowohl Standardkonformität (sqltest) als auch praktische Korrektheit (SQLLogicTest) sicherzustellen. Hohe Erfolgsraten in beiden demonstrieren ernsthafte SQL:1999-Implementierungsqualität, obwohl formelle Core-Zertifizierung Tests gegen offizielle NIST-Suites erfordern würde.

# Fehlgeschlagene Tests Sektion
conformance-failing-tests-title = Fehlgeschlagene Tests
conformance-failing-tests-desc = Die folgenden Tests schlagen derzeit fehl. Klicken Sie zum Erweitern der Details.
conformance-view-failing = Details der fehlgeschlagenen Tests anzeigen ({ $count } Tests)
conformance-error-label = Fehler:

# Metadaten
conformance-generated = Generiert:
conformance-commit = Commit:
conformance-status = Status:

