# VibeSQL CLI Localization - Swedish (Svenska)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - SQL:1999 FULL-kompatibel databas
cli-help-hint = Skriv \help för hjälp, \quit för att avsluta
cli-goodbye = Hej då!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - SQL:1999 FULL-kompatibel databas

cli-long-about = VibeSQL kommandoradsgränssnitt

    ANVÄNDNINGSLÄGEN:
      Interaktiv REPL:    vibesql (--database <FIL>)
      Kör kommando:       vibesql -c "SELECT * FROM users"
      Kör fil:            vibesql -f script.sql
      Kör från stdin:     cat data.sql | vibesql
      Generera typer:     vibesql codegen --schema schema.sql --output types.ts

    INTERAKTIV REPL:
      När den startas utan -c, -f eller pipad indata går VibeSQL in i en interaktiv
      REPL med readline-stöd, kommandohistorik och metakommandon som:
        \d (tabell)  - Beskriv tabell eller lista alla tabeller
        \dt          - Lista tabeller
        \f <format>  - Ställ in utdataformat
        \copy        - Importera/exportera CSV/JSON
        \help        - Visa alla REPL-kommandon

    UNDERKOMMANDON:
      codegen           Generera TypeScript-typer från databasschema

    KONFIGURATION:
      Inställningar kan konfigureras i ~/.vibesqlrc (TOML-format).
      Sektioner: display, database, history, query

    EXEMPEL:
      # Starta interaktiv REPL med minnesdatabas
      vibesql

      # Använd beständig databasfil
      vibesql --database mydata.db

      # Kör enstaka kommando
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Kör SQL-skriptfil
      vibesql -f schema.sql -v

      # Importera data från CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Exportera frågeresultat som JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Generera TypeScript-typer från schemafil
      vibesql codegen --schema schema.sql --output src/types.ts

      # Generera TypeScript-typer från körande databas
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = Sökväg till databasfil (om ej angiven används minnesdatabas)
arg-file-help = Kör SQL-kommandon från fil
arg-command-help = Kör SQL-kommando direkt och avsluta
arg-stdin-help = Läs SQL-kommandon från stdin (autodetekteras vid pipning)
arg-verbose-help = Visa detaljerad utdata under fil-/stdin-körning
arg-format-help = Utdataformat för frågeresultat
arg-lang-help = Ställ in visningsspråk (t.ex. en-US, es, ja, sv)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Generera TypeScript-typer från databasschema

codegen-long-about = Generera TypeScript-typdefinitioner från ett VibeSQL-databasschema.

    Detta kommando skapar TypeScript-gränssnitt för alla tabeller i databasen,
    tillsammans med metadataobjekt för körtidstypkontroll och IDE-stöd.

    INDATAKÄLLOR:
      --database <FIL>   Generera från befintlig databasfil
      --schema <FIL>     Generera från SQL-schemafil (CREATE TABLE-satser)

    UTDATA:
      --output <FIL>     Skriv genererade typer till denna fil (standard: types.ts)

    ALTERNATIV:
      --camel-case       Konvertera kolumnnamn till camelCase
      --no-metadata      Hoppa över generering av tabellmetadataobjekt

    EXEMPEL:
      # Från en databasfil
      vibesql codegen --database mydata.db --output src/db/types.ts

      # Från en SQL-schemafil
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Med camelCase-egenskapsnamn
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = SQL-schemafil med CREATE TABLE-satser
codegen-output-help = Sökväg till utdatafil för genererad TypeScript
codegen-camel-case-help = Konvertera kolumnnamn till camelCase
codegen-no-metadata-help = Hoppa över generering av tabellmetadataobjekt

codegen-from-schema = Genererar TypeScript-typer från schemafil: { $path }
codegen-from-database = Genererar TypeScript-typer från databas: { $path }
codegen-written = TypeScript-typer skrivna till: { $path }
codegen-error-no-source = Antingen --database eller --schema måste anges.
    Använd 'vibesql codegen --help' för användningsinformation.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Metakommandon:
help-describe = \d (tabell)      - Beskriv tabell eller lista alla tabeller
help-tables = \dt             - Lista tabeller
help-schemas = \ds             - Lista scheman
help-indexes = \di             - Lista index
help-roles = \du             - Lista roller/användare
help-format = \f <format>     - Ställ in utdataformat (table, json, csv, markdown, html)
help-timing = \timing         - Växla frågetiming
help-copy-to = \copy <tabell> TO <fil>   - Exportera tabell till CSV/JSON-fil
help-copy-from = \copy <tabell> FROM <fil> - Importera CSV-fil till tabell
help-save = \save (fil)     - Spara databas till SQL-dumpfil
help-errors = \errors         - Visa senaste felhistorik
help-help = \h, \help       - Visa denna hjälp
help-quit = \q, \quit       - Avsluta

help-sql-title = SQL-introspektion:
help-show-tables = SHOW TABLES                  - Lista alla tabeller
help-show-databases = SHOW DATABASES               - Lista alla scheman/databaser
help-show-columns = SHOW COLUMNS FROM <tabell>   - Visa tabellkolumner
help-show-index = SHOW INDEX FROM <tabell>     - Visa tabellindex
help-show-create = SHOW CREATE TABLE <tabell>   - Visa CREATE TABLE-sats
help-describe-sql = DESCRIBE <tabell>            - Alias för SHOW COLUMNS

help-examples-title = Exempel:
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

format-changed = Utdataformat ändrat till: { $format }
database-saved = Databas sparad till: { $path }
no-database-file = Fel: Ingen databasfil angiven. Använd \save <filnamn> eller starta med --database-flagga

# =============================================================================
# Error Display
# =============================================================================

no-errors = Inga fel i denna session.
recent-errors = Senaste fel:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = Inga SQL-satser hittades i skriptet
script-executing = Kör sats { $current } av { $total }...
script-error = Fel vid körning av sats { $index }: { $error }
script-summary-title = === Sammanfattning av skriptkörning ===
script-total = Totalt antal satser: { $count }
script-successful = Lyckade: { $count }
script-failed = Misslyckade: { $count }
script-failed-error = { $count } satser misslyckades

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } rader ({ $time }s)
rows-count = { $count } rader

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Varning: Kunde inte läsa konfigurationsfil: { $error }
warning-auto-save-failed = Varning: Kunde inte automatiskt spara databas: { $error }
warning-save-on-exit-failed = Varning: Kunde inte spara databas vid avslutning: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Kunde inte läsa fil '{ $path }': { $error }
stdin-read-error = Kunde inte läsa från stdin: { $error }
database-load-error = Kunde inte läsa in databas: { $error }
