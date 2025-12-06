# VibeSQL CLI Localization - Ukrainian (Українська)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - База даних з повною відповідністю SQL:1999
cli-help-hint = Введіть \help для довідки, \quit для виходу
cli-goodbye = До побачення!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - База даних з повною відповідністю SQL:1999

cli-long-about = Інтерфейс командного рядка VibeSQL

    РЕЖИМИ ВИКОРИСТАННЯ:
      Інтерактивний REPL:  vibesql (--database <ФАЙЛ>)
      Виконати команду:    vibesql -c "SELECT * FROM users"
      Виконати файл:       vibesql -f script.sql
      Виконати з stdin:    cat data.sql | vibesql
      Генерація типів:     vibesql codegen --schema schema.sql --output types.ts

    ІНТЕРАКТИВНИЙ REPL:
      При запуску без -c, -f або переданих даних, VibeSQL входить в інтерактивний
      REPL з підтримкою readline, історією команд та мета-командами:
        \d (таблиця) - Описати таблицю або показати всі таблиці
        \dt          - Показати таблиці
        \f <формат>  - Встановити формат виводу
        \copy        - Імпорт/експорт CSV/JSON
        \help        - Показати всі команди REPL

    ПІДКОМАНДИ:
      codegen           Генерувати TypeScript типи зі схеми бази даних

    КОНФІГУРАЦІЯ:
      Налаштування можна вказати в ~/.vibesqlrc (формат TOML).
      Секції: display, database, history, query

    ПРИКЛАДИ:
      # Запустити інтерактивний REPL з базою даних в пам'яті
      vibesql

      # Використати постійний файл бази даних
      vibesql --database mydata.db

      # Виконати одну команду
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Запустити SQL-скрипт
      vibesql -f schema.sql -v

      # Імпортувати дані з CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Експортувати результати запиту як JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Генерувати TypeScript типи зі схеми
      vibesql codegen --schema schema.sql --output src/types.ts

      # Генерувати TypeScript типи з бази даних
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = Шлях до файлу бази даних (якщо не вказано, використовується база в пам'яті)
arg-file-help = Виконати SQL-команди з файлу
arg-command-help = Виконати SQL-команду напряму та вийти
arg-stdin-help = Читати SQL-команди з stdin (автоматично визначається при перенаправленні)
arg-verbose-help = Показувати детальний вивід під час виконання файлу/stdin
arg-format-help = Формат виводу для результатів запиту
arg-lang-help = Встановити мову відображення (напр., en-US, es, ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Генерувати TypeScript типи зі схеми бази даних

codegen-long-about = Генерувати визначення TypeScript типів зі схеми бази даних VibeSQL.

    Ця команда створює TypeScript інтерфейси для всіх таблиць бази даних,
    а також об'єкти метаданих для перевірки типів та підтримки IDE.

    ДЖЕРЕЛА ВВОДУ:
      --database <ФАЙЛ>  Генерувати з існуючого файлу бази даних
      --schema <ФАЙЛ>    Генерувати з SQL-схеми (оператори CREATE TABLE)

    ВИВІД:
      --output <ФАЙЛ>    Записати згенеровані типи в цей файл (за замовчуванням: types.ts)

    ОПЦІЇ:
      --camel-case       Конвертувати назви колонок в camelCase
      --no-metadata      Пропустити генерацію об'єкта метаданих таблиць

    ПРИКЛАДИ:
      # З файлу бази даних
      vibesql codegen --database mydata.db --output src/db/types.ts

      # З SQL-схеми
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # З властивостями в camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = SQL-файл схеми з операторами CREATE TABLE
codegen-output-help = Шлях до файлу для згенерованого TypeScript
codegen-camel-case-help = Конвертувати назви колонок в camelCase
codegen-no-metadata-help = Пропустити генерацію об'єкта метаданих таблиць

codegen-from-schema = Генерація TypeScript типів зі схеми: { $path }
codegen-from-database = Генерація TypeScript типів з бази даних: { $path }
codegen-written = TypeScript типи записано в: { $path }
codegen-error-no-source = Потрібно вказати --database або --schema.
    Використовуйте 'vibesql codegen --help' для інформації про використання.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Мета-команди:
help-describe = \d (таблиця)   - Описати таблицю або показати всі таблиці
help-tables = \dt             - Показати таблиці
help-schemas = \ds             - Показати схеми
help-indexes = \di             - Показати індекси
help-roles = \du             - Показати ролі/користувачів
help-format = \f <формат>     - Встановити формат виводу (table, json, csv, markdown, html)
help-timing = \timing         - Увімкнути/вимкнути вимірювання часу запиту
help-copy-to = \copy <таблиця> TO <файл>   - Експортувати таблицю в CSV/JSON файл
help-copy-from = \copy <таблиця> FROM <файл> - Імпортувати CSV файл в таблицю
help-save = \save (файл)    - Зберегти базу даних в SQL-дамп
help-errors = \errors         - Показати останні помилки
help-help = \h, \help      - Показати цю довідку
help-quit = \q, \quit      - Вийти

help-sql-title = SQL-інтроспекція:
help-show-tables = SHOW TABLES                  - Показати всі таблиці
help-show-databases = SHOW DATABASES               - Показати всі схеми/бази даних
help-show-columns = SHOW COLUMNS FROM <таблиця>  - Показати колонки таблиці
help-show-index = SHOW INDEX FROM <таблиця>    - Показати індекси таблиці
help-show-create = SHOW CREATE TABLE <таблиця>  - Показати оператор CREATE TABLE
help-describe-sql = DESCRIBE <таблиця>           - Аліас для SHOW COLUMNS

help-examples-title = Приклади:
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

format-changed = Формат виводу встановлено: { $format }
database-saved = Базу даних збережено в: { $path }
no-database-file = Помилка: Файл бази даних не вказано. Використовуйте \save <ім'я_файлу> або запустіть з прапорцем --database

# =============================================================================
# Error Display
# =============================================================================

no-errors = Помилок у цій сесії немає.
recent-errors = Останні помилки:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = SQL-операторів у скрипті не знайдено
script-executing = Виконання оператора { $current } з { $total }...
script-error = Помилка виконання оператора { $index }: { $error }
script-summary-title = === Підсумок виконання скрипта ===
script-total = Всього операторів: { $count }
script-successful = Успішних: { $count }
script-failed = Невдалих: { $count }
script-failed-error = { $count } операторів завершились помилкою

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } рядків у наборі ({ $time }с)
rows-count = { $count } рядків

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Попередження: Не вдалося завантажити файл конфігурації: { $error }
warning-auto-save-failed = Попередження: Не вдалося автоматично зберегти базу даних: { $error }
warning-save-on-exit-failed = Попередження: Не вдалося зберегти базу даних при виході: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Не вдалося прочитати файл '{ $path }': { $error }
stdin-read-error = Не вдалося прочитати з stdin: { $error }
database-load-error = Не вдалося завантажити базу даних: { $error }
