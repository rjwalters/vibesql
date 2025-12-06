# VibeSQL CLI Localization - Russian (ru)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - База данных с полным соответствием SQL:1999
cli-help-hint = Введите \help для справки, \quit для выхода
cli-goodbye = До свидания!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - База данных с полным соответствием SQL:1999

cli-long-about = Интерфейс командной строки VibeSQL

    РЕЖИМЫ ИСПОЛЬЗОВАНИЯ:
      Интерактивный REPL:    vibesql (--database <ФАЙЛ>)
      Выполнение команды:    vibesql -c "SELECT * FROM users"
      Выполнение файла:      vibesql -f script.sql
      Выполнение из stdin:   cat data.sql | vibesql
      Генерация типов:       vibesql codegen --schema schema.sql --output types.ts

    ИНТЕРАКТИВНЫЙ REPL:
      При запуске без -c, -f или переданных данных VibeSQL входит в интерактивный
      REPL с поддержкой readline, историей команд и мета-командами:
        \d (таблица) - Описание таблицы или список всех таблиц
        \dt          - Список таблиц
        \f <формат>  - Установка формата вывода
        \copy        - Импорт/экспорт CSV/JSON
        \help        - Показать все команды REPL

    ПОДКОМАНДЫ:
      codegen           Генерация типов TypeScript из схемы базы данных

    КОНФИГУРАЦИЯ:
      Настройки могут быть указаны в ~/.vibesqlrc (формат TOML).
      Секции: display, database, history, query

    ПРИМЕРЫ:
      # Запуск интерактивного REPL с базой данных в памяти
      vibesql

      # Использование постоянного файла базы данных
      vibesql --database mydata.db

      # Выполнение одной команды
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Запуск SQL-скрипта из файла
      vibesql -f schema.sql -v

      # Импорт данных из CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Экспорт результатов запроса в JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Генерация типов TypeScript из файла схемы
      vibesql codegen --schema schema.sql --output src/types.ts

      # Генерация типов TypeScript из работающей базы данных
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = Путь к файлу базы данных (если не указан, используется база данных в памяти)
arg-file-help = Выполнение SQL-команд из файла
arg-command-help = Выполнение SQL-команды напрямую и выход
arg-stdin-help = Чтение SQL-команд из stdin (автоматически определяется при перенаправлении)
arg-verbose-help = Показать подробный вывод при выполнении файла/stdin
arg-format-help = Формат вывода результатов запроса
arg-lang-help = Установить язык отображения (например, en-US, es, ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Генерация типов TypeScript из схемы базы данных

codegen-long-about = Генерация определений типов TypeScript из схемы базы данных VibeSQL.

    Эта команда создаёт интерфейсы TypeScript для всех таблиц в базе данных,
    а также объекты метаданных для проверки типов во время выполнения и поддержки IDE.

    ИСТОЧНИКИ ВВОДА:
      --database <ФАЙЛ>  Генерация из существующего файла базы данных
      --schema <ФАЙЛ>    Генерация из файла SQL-схемы (операторы CREATE TABLE)

    ВЫВОД:
      --output <ФАЙЛ>    Записать сгенерированные типы в этот файл (по умолчанию: types.ts)

    ПАРАМЕТРЫ:
      --camel-case       Преобразовать имена столбцов в camelCase
      --no-metadata      Пропустить генерацию объекта метаданных таблиц

    ПРИМЕРЫ:
      # Из файла базы данных
      vibesql codegen --database mydata.db --output src/db/types.ts

      # Из файла SQL-схемы
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # С именами свойств в camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = Файл SQL-схемы, содержащий операторы CREATE TABLE
codegen-output-help = Путь к выходному файлу для сгенерированного TypeScript
codegen-camel-case-help = Преобразовать имена столбцов в camelCase
codegen-no-metadata-help = Пропустить генерацию объекта метаданных таблиц

codegen-from-schema = Генерация типов TypeScript из файла схемы: { $path }
codegen-from-database = Генерация типов TypeScript из базы данных: { $path }
codegen-written = Типы TypeScript записаны в: { $path }
codegen-error-no-source = Необходимо указать --database или --schema.
    Используйте 'vibesql codegen --help' для информации об использовании.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Мета-команды:
help-describe = \d (таблица)   - Описание таблицы или список всех таблиц
help-tables = \dt             - Список таблиц
help-schemas = \ds             - Список схем
help-indexes = \di             - Список индексов
help-roles = \du             - Список ролей/пользователей
help-format = \f <формат>     - Установить формат вывода (table, json, csv, markdown, html)
help-timing = \timing         - Переключить отображение времени запроса
help-copy-to = \copy <таблица> TO <файл>   - Экспорт таблицы в CSV/JSON файл
help-copy-from = \copy <таблица> FROM <файл> - Импорт CSV файла в таблицу
help-save = \save (файл)    - Сохранить базу данных в SQL-дамп
help-errors = \errors         - Показать историю недавних ошибок
help-help = \h, \help       - Показать эту справку
help-quit = \q, \quit       - Выход

help-sql-title = SQL-интроспекция:
help-show-tables = SHOW TABLES                  - Список всех таблиц
help-show-databases = SHOW DATABASES               - Список всех схем/баз данных
help-show-columns = SHOW COLUMNS FROM <таблица>  - Показать столбцы таблицы
help-show-index = SHOW INDEX FROM <таблица>    - Показать индексы таблицы
help-show-create = SHOW CREATE TABLE <таблица>  - Показать оператор CREATE TABLE
help-describe-sql = DESCRIBE <таблица>           - Псевдоним для SHOW COLUMNS

help-examples-title = Примеры:
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

format-changed = Формат вывода установлен: { $format }
database-saved = База данных сохранена в: { $path }
no-database-file = Ошибка: Не указан файл базы данных. Используйте \save <имя_файла> или запустите с флагом --database

# =============================================================================
# Error Display
# =============================================================================

no-errors = Нет ошибок в этой сессии.
recent-errors = Недавние ошибки:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = SQL-операторы не найдены в скрипте
script-executing = Выполнение оператора { $current } из { $total }...
script-error = Ошибка выполнения оператора { $index }: { $error }
script-summary-title = === Сводка выполнения скрипта ===
script-total = Всего операторов: { $count }
script-successful = Успешно: { $count }
script-failed = Неуспешно: { $count }
script-failed-error = { $count } операторов завершились с ошибкой

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } строк в наборе ({ $time }с)
rows-count = { $count } строк

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Предупреждение: Не удалось загрузить файл конфигурации: { $error }
warning-auto-save-failed = Предупреждение: Не удалось автоматически сохранить базу данных: { $error }
warning-save-on-exit-failed = Предупреждение: Не удалось сохранить базу данных при выходе: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Не удалось прочитать файл '{ $path }': { $error }
stdin-read-error = Не удалось прочитать из stdin: { $error }
database-load-error = Не удалось загрузить базу данных: { $error }
