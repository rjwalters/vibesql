# VibeSQL CLI Localization - Dutch (Nederlands)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - SQL:1999 VOLLEDIG Compliant Database
cli-help-hint = Typ \help voor hulp, \quit om af te sluiten
cli-goodbye = Tot ziens!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - SQL:1999 VOLLEDIG Compliant Database

cli-long-about = VibeSQL opdrachtregelinterface

    GEBRUIKSMODI:
      Interactieve REPL:    vibesql (--database <BESTAND>)
      Opdracht uitvoeren:   vibesql -c "SELECT * FROM users"
      Bestand uitvoeren:    vibesql -f script.sql
      Uitvoeren via stdin:  cat data.sql | vibesql
      Types genereren:      vibesql codegen --schema schema.sql --output types.ts

    INTERACTIEVE REPL:
      Wanneer gestart zonder -c, -f, of gepipete invoer, gaat VibeSQL naar een
      interactieve REPL met readline-ondersteuning, opdrachtgeschiedenis en
      meta-opdrachten zoals:
        \d (tabel)  - Beschrijf tabel of toon alle tabellen
        \dt         - Toon tabellen
        \f <format> - Stel uitvoerformaat in
        \copy       - Importeer/exporteer CSV/JSON
        \help       - Toon alle REPL-opdrachten

    SUBOPDRACHTEN:
      codegen           Genereer TypeScript-types uit databaseschema

    CONFIGURATIE:
      Instellingen kunnen worden geconfigureerd in ~/.vibesqlrc (TOML-formaat).
      Secties: display, database, history, query

    VOORBEELDEN:
      # Start interactieve REPL met in-memory database
      vibesql

      # Gebruik persistent databasebestand
      vibesql --database mydata.db

      # Voer enkele opdracht uit
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Voer SQL-scriptbestand uit
      vibesql -f schema.sql -v

      # Importeer gegevens uit CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Exporteer queryresultaten als JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Genereer TypeScript-types uit schemabestand
      vibesql codegen --schema schema.sql --output src/types.ts

      # Genereer TypeScript-types uit draaiende database
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = Databasebestandspad (indien niet opgegeven, wordt in-memory database gebruikt)
arg-file-help = Voer SQL-opdrachten uit vanuit bestand
arg-command-help = Voer SQL-opdracht direct uit en sluit af
arg-stdin-help = Lees SQL-opdrachten van stdin (automatisch gedetecteerd bij piping)
arg-verbose-help = Toon gedetailleerde uitvoer tijdens bestand/stdin-uitvoering
arg-format-help = Uitvoerformaat voor queryresultaten
arg-lang-help = Stel de weergavetaal in (bijv. en-US, es, ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Genereer TypeScript-types uit databaseschema

codegen-long-about = Genereer TypeScript-typedefinities uit een VibeSQL-databaseschema.

    Deze opdracht maakt TypeScript-interfaces voor alle tabellen in de database,
    samen met metadata-objecten voor runtime typecontrole en IDE-ondersteuning.

    INVOERBRONNEN:
      --database <BESTAND>  Genereer uit een bestaand databasebestand
      --schema <BESTAND>    Genereer uit een SQL-schemabestand (CREATE TABLE-statements)

    UITVOER:
      --output <BESTAND>    Schrijf gegenereerde types naar dit bestand (standaard: types.ts)

    OPTIES:
      --camel-case       Converteer kolomnamen naar camelCase
      --no-metadata      Sla het genereren van het tabellen-metadata-object over

    VOORBEELDEN:
      # Vanuit een databasebestand
      vibesql codegen --database mydata.db --output src/db/types.ts

      # Vanuit een SQL-schemabestand
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Met camelCase eigenschapnamen
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = SQL-schemabestand met CREATE TABLE-statements
codegen-output-help = Uitvoerbestandspad voor gegenereerd TypeScript
codegen-camel-case-help = Converteer kolomnamen naar camelCase
codegen-no-metadata-help = Sla het genereren van tabel-metadata-object over

codegen-from-schema = TypeScript-types genereren uit schemabestand: { $path }
codegen-from-database = TypeScript-types genereren uit database: { $path }
codegen-written = TypeScript-types geschreven naar: { $path }
codegen-error-no-source = --database of --schema moet worden opgegeven.
    Gebruik 'vibesql codegen --help' voor gebruiksinformatie.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Meta-opdrachten:
help-describe = \d (tabel)      - Beschrijf tabel of toon alle tabellen
help-tables = \dt             - Toon tabellen
help-schemas = \ds             - Toon schema's
help-indexes = \di             - Toon indexen
help-roles = \du             - Toon rollen/gebruikers
help-format = \f <format>     - Stel uitvoerformaat in (table, json, csv, markdown, html)
help-timing = \timing         - Schakel querytiming in/uit
help-copy-to = \copy <tabel> TO <bestand>   - Exporteer tabel naar CSV/JSON-bestand
help-copy-from = \copy <tabel> FROM <bestand> - Importeer CSV-bestand in tabel
help-save = \save (bestand) - Sla database op naar SQL-dumpbestand
help-errors = \errors         - Toon recente foutgeschiedenis
help-help = \h, \help      - Toon deze hulp
help-quit = \q, \quit      - Afsluiten

help-sql-title = SQL-introspectie:
help-show-tables = SHOW TABLES                  - Toon alle tabellen
help-show-databases = SHOW DATABASES               - Toon alle schema's/databases
help-show-columns = SHOW COLUMNS FROM <tabel>    - Toon tabelkolommen
help-show-index = SHOW INDEX FROM <tabel>      - Toon tabelindexen
help-show-create = SHOW CREATE TABLE <tabel>    - Toon CREATE TABLE-statement
help-describe-sql = DESCRIBE <tabel>             - Alias voor SHOW COLUMNS

help-examples-title = Voorbeelden:
help-example-create = CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
help-example-insert = INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
help-example-select = SELECT * FROM users;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM users;
help-example-describe = DESCRIBE users;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy users TO '/tmp/users.csv'
help-example-copy-from = \copy users FROM '/tmp/users.csv'
help-example-copy-json = \copy users TO '/tmp/users.json'
help-example-errors = \errors

# =============================================================================
# Status Messages
# =============================================================================

format-changed = Uitvoerformaat ingesteld op: { $format }
database-saved = Database opgeslagen naar: { $path }
no-database-file = Fout: Geen databasebestand opgegeven. Gebruik \save <bestandsnaam> of start met --database vlag

# =============================================================================
# Error Display
# =============================================================================

no-errors = Geen fouten in deze sessie.
recent-errors = Recente fouten:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = Geen SQL-statements gevonden in script
script-executing = Statement { $current } van { $total } uitvoeren...
script-error = Fout bij uitvoeren van statement { $index }: { $error }
script-summary-title = === Samenvatting Scriptuitvoering ===
script-total = Totaal statements: { $count }
script-successful = Succesvol: { $count }
script-failed = Mislukt: { $count }
script-failed-error = { $count } statements mislukt

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } rijen in set ({ $time }s)
rows-count = { $count } rijen

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Waarschuwing: Kon configuratiebestand niet laden: { $error }
warning-auto-save-failed = Waarschuwing: Automatisch opslaan van database mislukt: { $error }
warning-save-on-exit-failed = Waarschuwing: Opslaan van database bij afsluiten mislukt: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Lezen van bestand '{ $path }' mislukt: { $error }
stdin-read-error = Lezen van stdin mislukt: { $error }
database-load-error = Laden van database mislukt: { $error }
