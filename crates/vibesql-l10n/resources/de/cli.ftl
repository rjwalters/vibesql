# VibeSQL CLI Lokalisierung - Deutsch
# Diese Datei enthält alle benutzerfreundlichen Zeichenketten für die VibeSQL-Kommandozeilenschnittstelle.

# =============================================================================
# REPL-Banner und Grundlegende Nachrichten
# =============================================================================

cli-banner = VibeSQL v{ $version } - Datenbank mit VOLLSTÄNDIGER SQL:1999-Konformität
cli-help-hint = Geben Sie \help für Hilfe ein, \quit zum Beenden
cli-goodbye = Auf Wiedersehen!
locale-changed = Sprache geändert zu { $locale }

# =============================================================================
# Befehlshilfetext (Clap-Argumente)
# =============================================================================

cli-about = VibeSQL - Datenbank mit VOLLSTÄNDIGER SQL:1999-Konformität

cli-long-about = VibeSQL-Kommandozeilenschnittstelle

    VERWENDUNGSMODI:
      Interaktive REPL:     vibesql (--database <DATEI>)
      Befehl ausführen:     vibesql -c "SELECT * FROM benutzer"
      Datei ausführen:      vibesql -f skript.sql
      Von stdin ausführen:  cat daten.sql | vibesql
      Typen generieren:     vibesql codegen --schema schema.sql --output types.ts

    INTERAKTIVE REPL:
      Wenn ohne -c, -f oder Pipe-Eingabe gestartet, wechselt VibeSQL in eine
      interaktive REPL mit Readline-Unterstützung, Befehlshistorie und Meta-Befehlen wie:
        \d (tabelle)  - Tabelle beschreiben oder alle Tabellen auflisten
        \dt           - Tabellen auflisten
        \f <format>   - Ausgabeformat festlegen
        \copy         - CSV/JSON importieren/exportieren
        \help         - Alle REPL-Befehle anzeigen

    UNTERBEFEHLE:
      codegen           TypeScript-Typen aus Datenbankschema generieren

    KONFIGURATION:
      Einstellungen können in ~/.vibesqlrc (TOML-Format) konfiguriert werden.
      Abschnitte: display, database, history, query

    BEISPIELE:
      # Interaktive REPL mit In-Memory-Datenbank starten
      vibesql

      # Persistente Datenbankdatei verwenden
      vibesql --database meinedaten.db

      # Einzelnen Befehl ausführen
      vibesql -c "CREATE TABLE benutzer (id INT, name VARCHAR(100))"

      # SQL-Skriptdatei ausführen
      vibesql -f schema.sql -v

      # Daten aus CSV importieren
      echo "\copy benutzer FROM 'daten.csv'" | vibesql --database meinedaten.db

      # Abfrageergebnisse als JSON exportieren
      vibesql -d meinedaten.db -c "SELECT * FROM benutzer" --format json

      # TypeScript-Typen aus einer Schema-Datei generieren
      vibesql codegen --schema schema.sql --output src/types.ts

      # TypeScript-Typen aus einer laufenden Datenbank generieren
      vibesql codegen --database meinedaten.db --output src/types.ts

# Argument-Hilfezeichenketten
arg-database-help = Pfad zur Datenbankdatei (wenn nicht angegeben, wird In-Memory-Datenbank verwendet)
arg-file-help = SQL-Befehle aus Datei ausführen
arg-command-help = SQL-Befehl direkt ausführen und beenden
arg-stdin-help = SQL-Befehle von stdin lesen (automatisch erkannt bei Pipe-Verwendung)
arg-verbose-help = Detaillierte Ausgabe während der Datei-/stdin-Ausführung anzeigen
arg-format-help = Ausgabeformat für Abfrageergebnisse
arg-lang-help = Anzeigesprache festlegen (z.B. en-US, es, de)

# =============================================================================
# Codegen-Unterbefehl
# =============================================================================

codegen-about = TypeScript-Typen aus Datenbankschema generieren

codegen-long-about = TypeScript-Typdefinitionen aus einem VibeSQL-Datenbankschema generieren.

    Dieser Befehl erstellt TypeScript-Interfaces für alle Tabellen in der Datenbank,
    zusammen mit Metadatenobjekten für Laufzeit-Typprüfung und IDE-Unterstützung.

    EINGABEQUELLEN:
      --database <DATEI>  Aus einer bestehenden Datenbankdatei generieren
      --schema <DATEI>    Aus einer SQL-Schema-Datei generieren (CREATE TABLE-Anweisungen)

    AUSGABE:
      --output <DATEI>    Generierte Typen in diese Datei schreiben (Standard: types.ts)

    OPTIONEN:
      --camel-case        Spaltennamen in camelCase konvertieren
      --no-metadata       Tabellen-Metadatenobjekt nicht generieren

    BEISPIELE:
      # Aus einer Datenbankdatei
      vibesql codegen --database meinedaten.db --output src/db/types.ts

      # Aus einer SQL-Schema-Datei
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Mit camelCase-Eigenschaftsnamen
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = SQL-Schema-Datei mit CREATE TABLE-Anweisungen
codegen-output-help = Ausgabedateipfad für generiertes TypeScript
codegen-camel-case-help = Spaltennamen in camelCase konvertieren
codegen-no-metadata-help = Tabellen-Metadatenobjekt nicht generieren

codegen-from-schema = Generiere TypeScript-Typen aus Schema-Datei: { $path }
codegen-from-database = Generiere TypeScript-Typen aus Datenbank: { $path }
codegen-written = TypeScript-Typen geschrieben nach: { $path }
codegen-error-no-source = Entweder --database oder --schema muss angegeben werden.
    Verwenden Sie 'vibesql codegen --help' für Nutzungsinformationen.

# =============================================================================
# Meta-Befehlshilfe (\help-Ausgabe)
# =============================================================================

help-title = Meta-Befehle:
help-describe = \d (tabelle)   - Tabelle beschreiben oder alle Tabellen auflisten
help-tables = \dt             - Tabellen auflisten
help-schemas = \ds             - Schemata auflisten
help-indexes = \di             - Indizes auflisten
help-roles = \du             - Rollen/Benutzer auflisten
help-format = \f <format>     - Ausgabeformat festlegen (table, json, csv, markdown, html)
help-timing = \timing         - Abfragezeitmessung umschalten
help-copy-to = \copy <tabelle> TO <datei>   - Tabelle in CSV/JSON-Datei exportieren
help-copy-from = \copy <tabelle> FROM <datei> - CSV-Datei in Tabelle importieren
help-save = \save (datei)   - Datenbank in SQL-Dump-Datei speichern
help-errors = \errors         - Aktuelle Fehlerhistorie anzeigen
help-help = \h, \help      - Diese Hilfe anzeigen
help-quit = \q, \quit      - Beenden

help-sql-title = SQL-Introspektion:
help-show-tables = SHOW TABLES                  - Alle Tabellen auflisten
help-show-databases = SHOW DATABASES               - Alle Schemata/Datenbanken auflisten
help-show-columns = SHOW COLUMNS FROM <tabelle>  - Tabellenspalten anzeigen
help-show-index = SHOW INDEX FROM <tabelle>    - Tabellenindizes anzeigen
help-show-create = SHOW CREATE TABLE <tabelle>  - CREATE TABLE-Anweisung anzeigen
help-describe-sql = DESCRIBE <tabelle>           - Alias für SHOW COLUMNS

help-examples-title = Beispiele:
help-example-create = CREATE TABLE benutzer (id INT PRIMARY KEY, name VARCHAR(100));
help-example-insert = INSERT INTO benutzer VALUES (1, 'Alice'), (2, 'Bob');
help-example-select = SELECT * FROM benutzer;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM benutzer;
help-example-describe = DESCRIBE benutzer;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy benutzer TO '/tmp/benutzer.csv'
help-example-copy-from = \copy benutzer FROM '/tmp/benutzer.csv'
help-example-copy-json = \copy benutzer TO '/tmp/benutzer.json'
help-example-errors = \errors

# =============================================================================
# Statusnachrichten
# =============================================================================

format-changed = Ausgabeformat festgelegt auf: { $format }
database-saved = Datenbank gespeichert unter: { $path }
no-database-file = Fehler: Keine Datenbankdatei angegeben. Verwenden Sie \save <dateiname> oder starten Sie mit dem --database-Flag

# =============================================================================
# Fehleranzeige
# =============================================================================

no-errors = Keine Fehler in dieser Sitzung.
recent-errors = Aktuelle Fehler:

# =============================================================================
# Skriptausführungsnachrichten
# =============================================================================

script-no-statements = Keine SQL-Anweisungen im Skript gefunden
script-executing = Führe Anweisung { $current } von { $total } aus...
script-error = Fehler beim Ausführen von Anweisung { $index }: { $error }
script-summary-title = === Zusammenfassung der Skriptausführung ===
script-total = Anweisungen gesamt: { $count }
script-successful = Erfolgreich: { $count }
script-failed = Fehlgeschlagen: { $count }
script-failed-error = { $count } Anweisungen fehlgeschlagen

# =============================================================================
# Ausgabeformatierung
# =============================================================================

rows-with-time = { $count } Zeilen im Ergebnis ({ $time }s)
rows-count = { $count } Zeilen

# =============================================================================
# Warnungen
# =============================================================================

warning-config-load = Warnung: Konfigurationsdatei konnte nicht geladen werden: { $error }
warning-auto-save-failed = Warnung: Automatisches Speichern der Datenbank fehlgeschlagen: { $error }
warning-save-on-exit-failed = Warnung: Speichern der Datenbank beim Beenden fehlgeschlagen: { $error }

# =============================================================================
# Dateioperationen
# =============================================================================

file-read-error = Fehler beim Lesen der Datei '{ $path }': { $error }
stdin-read-error = Fehler beim Lesen von stdin: { $error }
database-load-error = Fehler beim Laden der Datenbank: { $error }
