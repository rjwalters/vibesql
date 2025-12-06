# VibeSQL CLI Localization - Italian (it)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - Database con conformità FULL allo standard SQL:1999
cli-help-hint = Digita \help per la guida, \quit per uscire
cli-goodbye = Arrivederci!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - Database con conformità FULL allo standard SQL:1999

cli-long-about = Interfaccia a riga di comando di VibeSQL

    MODALITÀ D'USO:
      REPL interattivo:      vibesql (--database <FILE>)
      Esegui comando:        vibesql -c "SELECT * FROM users"
      Esegui file:           vibesql -f script.sql
      Esegui da stdin:       cat data.sql | vibesql
      Genera tipi:           vibesql codegen --schema schema.sql --output types.ts

    REPL INTERATTIVO:
      Quando avviato senza -c, -f o input da pipe, VibeSQL entra in un REPL
      interattivo con supporto readline, cronologia comandi e meta-comandi come:
        \d (tabella)  - Descrivi tabella o elenca tutte le tabelle
        \dt           - Elenca tabelle
        \f <formato>  - Imposta formato output
        \copy         - Importa/esporta CSV/JSON
        \help         - Mostra tutti i comandi REPL

    SOTTOCOMANDI:
      codegen           Genera tipi TypeScript dallo schema del database

    CONFIGURAZIONE:
      Le impostazioni possono essere configurate in ~/.vibesqlrc (formato TOML).
      Sezioni: display, database, history, query

    ESEMPI:
      # Avvia REPL interattivo con database in memoria
      vibesql

      # Usa file database persistente
      vibesql --database mydata.db

      # Esegui singolo comando
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Esegui file script SQL
      vibesql -f schema.sql -v

      # Importa dati da CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Esporta risultati query come JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Genera tipi TypeScript da un file schema
      vibesql codegen --schema schema.sql --output src/types.ts

      # Genera tipi TypeScript da un database in esecuzione
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = Percorso file database (se non specificato, usa database in memoria)
arg-file-help = Esegui comandi SQL da file
arg-command-help = Esegui comando SQL direttamente ed esci
arg-stdin-help = Leggi comandi SQL da stdin (rilevato automaticamente quando in pipe)
arg-verbose-help = Mostra output dettagliato durante l'esecuzione di file/stdin
arg-format-help = Formato output per i risultati delle query
arg-lang-help = Imposta la lingua di visualizzazione (es. en-US, es, ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Genera tipi TypeScript dallo schema del database

codegen-long-about = Genera definizioni di tipi TypeScript dallo schema di un database VibeSQL.

    Questo comando crea interfacce TypeScript per tutte le tabelle nel database,
    insieme a oggetti metadati per il controllo dei tipi a runtime e supporto IDE.

    SORGENTI INPUT:
      --database <FILE>  Genera da un file database esistente
      --schema <FILE>    Genera da un file schema SQL (istruzioni CREATE TABLE)

    OUTPUT:
      --output <FILE>    Scrivi i tipi generati in questo file (default: types.ts)

    OPZIONI:
      --camel-case       Converti nomi colonne in camelCase
      --no-metadata      Salta la generazione dell'oggetto metadati delle tabelle

    ESEMPI:
      # Da un file database
      vibesql codegen --database mydata.db --output src/db/types.ts

      # Da un file schema SQL
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Con nomi proprietà in camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = File schema SQL contenente istruzioni CREATE TABLE
codegen-output-help = Percorso file output per il TypeScript generato
codegen-camel-case-help = Converti nomi colonne in camelCase
codegen-no-metadata-help = Salta la generazione dell'oggetto metadati delle tabelle

codegen-from-schema = Generazione tipi TypeScript dal file schema: { $path }
codegen-from-database = Generazione tipi TypeScript dal database: { $path }
codegen-written = Tipi TypeScript scritti in: { $path }
codegen-error-no-source = Deve essere specificato --database o --schema.
    Usa 'vibesql codegen --help' per informazioni sull'utilizzo.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Meta-comandi:
help-describe = \d (tabella)    - Descrivi tabella o elenca tutte le tabelle
help-tables = \dt             - Elenca tabelle
help-schemas = \ds             - Elenca schemi
help-indexes = \di             - Elenca indici
help-roles = \du             - Elenca ruoli/utenti
help-format = \f <formato>    - Imposta formato output (table, json, csv, markdown, html)
help-timing = \timing         - Attiva/disattiva misurazione tempo query
help-copy-to = \copy <tabella> TO <file>   - Esporta tabella in file CSV/JSON
help-copy-from = \copy <tabella> FROM <file> - Importa file CSV in tabella
help-save = \save (file)    - Salva database in file dump SQL
help-errors = \errors         - Mostra cronologia errori recenti
help-help = \h, \help      - Mostra questa guida
help-quit = \q, \quit      - Esci

help-sql-title = Introspezione SQL:
help-show-tables = SHOW TABLES                  - Elenca tutte le tabelle
help-show-databases = SHOW DATABASES               - Elenca tutti gli schemi/database
help-show-columns = SHOW COLUMNS FROM <tabella>  - Mostra colonne della tabella
help-show-index = SHOW INDEX FROM <tabella>    - Mostra indici della tabella
help-show-create = SHOW CREATE TABLE <tabella>  - Mostra istruzione CREATE TABLE
help-describe-sql = DESCRIBE <tabella>           - Alias per SHOW COLUMNS

help-examples-title = Esempi:
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

format-changed = Formato output impostato a: { $format }
database-saved = Database salvato in: { $path }
no-database-file = Errore: Nessun file database specificato. Usa \save <nomefile> o avvia con il flag --database

# =============================================================================
# Error Display
# =============================================================================

no-errors = Nessun errore in questa sessione.
recent-errors = Errori recenti:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = Nessuna istruzione SQL trovata nello script
script-executing = Esecuzione istruzione { $current } di { $total }...
script-error = Errore nell'esecuzione dell'istruzione { $index }: { $error }
script-summary-title = === Riepilogo Esecuzione Script ===
script-total = Istruzioni totali: { $count }
script-successful = Riuscite: { $count }
script-failed = Fallite: { $count }
script-failed-error = { $count } istruzioni fallite

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } righe nel risultato ({ $time }s)
rows-count = { $count } righe

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Avviso: Impossibile caricare il file di configurazione: { $error }
warning-auto-save-failed = Avviso: Salvataggio automatico del database fallito: { $error }
warning-save-on-exit-failed = Avviso: Salvataggio del database all'uscita fallito: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Lettura del file '{ $path }' fallita: { $error }
stdin-read-error = Lettura da stdin fallita: { $error }
database-load-error = Caricamento database fallito: { $error }
