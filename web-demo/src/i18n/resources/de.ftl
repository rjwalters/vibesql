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
