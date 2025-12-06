# VibeSQL CLI Localization - Polski (Polish)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - Baza danych z pełną zgodnością SQL:1999
cli-help-hint = Wpisz \help aby uzyskać pomoc, \quit aby wyjść
cli-goodbye = Do widzenia!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - Baza danych z pełną zgodnością SQL:1999

cli-long-about = Interfejs wiersza poleceń VibeSQL

    TRYBY UŻYCIA:
      Interaktywny REPL:  vibesql (--database <PLIK>)
      Wykonaj polecenie:  vibesql -c "SELECT * FROM users"
      Wykonaj plik:       vibesql -f skrypt.sql
      Wykonaj ze stdin:   cat dane.sql | vibesql
      Generuj typy:       vibesql codegen --schema schema.sql --output types.ts

    INTERAKTYWNY REPL:
      Po uruchomieniu bez -c, -f lub wejścia z potoku, VibeSQL wchodzi w tryb
      interaktywnego REPL z obsługą readline, historią poleceń i meta-poleceniami:
        \d (tabela)  - Opisz tabelę lub wyświetl wszystkie tabele
        \dt          - Wyświetl tabele
        \f <format>  - Ustaw format wyjściowy
        \copy        - Importuj/eksportuj CSV/JSON
        \help        - Pokaż wszystkie polecenia REPL

    PODKOMENDY:
      codegen           Generuj typy TypeScript ze schematu bazy danych

    KONFIGURACJA:
      Ustawienia można skonfigurować w ~/.vibesqlrc (format TOML).
      Sekcje: display, database, history, query

    PRZYKŁADY:
      # Uruchom interaktywny REPL z bazą w pamięci
      vibesql

      # Użyj trwałego pliku bazy danych
      vibesql --database mojedane.db

      # Wykonaj pojedyncze polecenie
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Uruchom plik skryptu SQL
      vibesql -f schema.sql -v

      # Importuj dane z CSV
      echo "\copy users FROM 'dane.csv'" | vibesql --database mojedane.db

      # Eksportuj wyniki zapytania jako JSON
      vibesql -d mojedane.db -c "SELECT * FROM users" --format json

      # Generuj typy TypeScript z pliku schematu
      vibesql codegen --schema schema.sql --output src/types.ts

      # Generuj typy TypeScript z działającej bazy danych
      vibesql codegen --database mojedane.db --output src/types.ts

# Argument help strings
arg-database-help = Ścieżka do pliku bazy danych (jeśli nie podano, używana jest baza w pamięci)
arg-file-help = Wykonaj polecenia SQL z pliku
arg-command-help = Wykonaj polecenie SQL bezpośrednio i zakończ
arg-stdin-help = Odczytaj polecenia SQL ze standardowego wejścia (wykrywane automatycznie przy potoku)
arg-verbose-help = Pokaż szczegółowe informacje podczas wykonywania pliku/stdin
arg-format-help = Format wyjściowy dla wyników zapytań
arg-lang-help = Ustaw język wyświetlania (np. en-US, es, ja, pl)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Generuj typy TypeScript ze schematu bazy danych

codegen-long-about = Generuj definicje typów TypeScript ze schematu bazy danych VibeSQL.

    To polecenie tworzy interfejsy TypeScript dla wszystkich tabel w bazie danych,
    wraz z obiektami metadanych do sprawdzania typów w czasie wykonania i wsparcia IDE.

    ŹRÓDŁA WEJŚCIOWE:
      --database <PLIK>  Generuj z istniejącego pliku bazy danych
      --schema <PLIK>    Generuj z pliku schematu SQL (instrukcje CREATE TABLE)

    WYJŚCIE:
      --output <PLIK>    Zapisz wygenerowane typy do tego pliku (domyślnie: types.ts)

    OPCJE:
      --camel-case       Konwertuj nazwy kolumn na camelCase
      --no-metadata      Pomiń generowanie obiektu metadanych tabel

    PRZYKŁADY:
      # Z pliku bazy danych
      vibesql codegen --database mojedane.db --output src/db/types.ts

      # Z pliku schematu SQL
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Z nazwami właściwości w camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = Plik schematu SQL zawierający instrukcje CREATE TABLE
codegen-output-help = Ścieżka pliku wyjściowego dla wygenerowanego TypeScript
codegen-camel-case-help = Konwertuj nazwy kolumn na camelCase
codegen-no-metadata-help = Pomiń generowanie obiektu metadanych tabel

codegen-from-schema = Generowanie typów TypeScript z pliku schematu: { $path }
codegen-from-database = Generowanie typów TypeScript z bazy danych: { $path }
codegen-written = Typy TypeScript zapisane do: { $path }
codegen-error-no-source = Należy podać --database lub --schema.
    Użyj 'vibesql codegen --help' aby uzyskać informacje o użyciu.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Meta-polecenia:
help-describe = \d (tabela)     - Opisz tabelę lub wyświetl wszystkie tabele
help-tables = \dt             - Wyświetl tabele
help-schemas = \ds             - Wyświetl schematy
help-indexes = \di             - Wyświetl indeksy
help-roles = \du             - Wyświetl role/użytkowników
help-format = \f <format>     - Ustaw format wyjściowy (table, json, csv, markdown, html)
help-timing = \timing         - Przełącz pomiar czasu zapytań
help-copy-to = \copy <tabela> TO <plik>   - Eksportuj tabelę do pliku CSV/JSON
help-copy-from = \copy <tabela> FROM <plik> - Importuj plik CSV do tabeli
help-save = \save (plik)    - Zapisz bazę danych do pliku zrzutu SQL
help-errors = \errors         - Pokaż ostatnią historię błędów
help-help = \h, \help      - Pokaż tę pomoc
help-quit = \q, \quit      - Wyjdź

help-sql-title = Introspekcja SQL:
help-show-tables = SHOW TABLES                  - Wyświetl wszystkie tabele
help-show-databases = SHOW DATABASES               - Wyświetl wszystkie schematy/bazy danych
help-show-columns = SHOW COLUMNS FROM <tabela>   - Pokaż kolumny tabeli
help-show-index = SHOW INDEX FROM <tabela>     - Pokaż indeksy tabeli
help-show-create = SHOW CREATE TABLE <tabela>   - Pokaż instrukcję CREATE TABLE
help-describe-sql = DESCRIBE <tabela>            - Alias dla SHOW COLUMNS

help-examples-title = Przykłady:
help-example-create = CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
help-example-insert = INSERT INTO users VALUES (1, 'Alicja'), (2, 'Robert');
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

format-changed = Format wyjściowy ustawiony na: { $format }
database-saved = Baza danych zapisana do: { $path }
no-database-file = Błąd: Nie podano pliku bazy danych. Użyj \save <nazwa_pliku> lub uruchom z flagą --database

# =============================================================================
# Error Display
# =============================================================================

no-errors = Brak błędów w tej sesji.
recent-errors = Ostatnie błędy:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = Nie znaleziono instrukcji SQL w skrypcie
script-executing = Wykonywanie instrukcji { $current } z { $total }...
script-error = Błąd wykonywania instrukcji { $index }: { $error }
script-summary-title = === Podsumowanie wykonania skryptu ===
script-total = Łączna liczba instrukcji: { $count }
script-successful = Udane: { $count }
script-failed = Nieudane: { $count }
script-failed-error = { $count } instrukcji nie powiodło się

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } wierszy w zbiorze ({ $time }s)
rows-count = { $count } wierszy

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Ostrzeżenie: Nie można załadować pliku konfiguracji: { $error }
warning-auto-save-failed = Ostrzeżenie: Nie udało się automatycznie zapisać bazy danych: { $error }
warning-save-on-exit-failed = Ostrzeżenie: Nie udało się zapisać bazy danych przy wyjściu: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Nie udało się odczytać pliku '{ $path }': { $error }
stdin-read-error = Nie udało się odczytać ze standardowego wejścia: { $error }
database-load-error = Nie udało się załadować bazy danych: { $error }
