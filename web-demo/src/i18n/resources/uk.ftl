# VibeSQL Web UI - Українська

# Page titles
page-title = VibeSQL - База даних SQL:1999 з ШІ
demo-title = Демо VibeSQL
benchmarks-title = Тести продуктивності - VibeSQL
benchmarks-heading = VibeSQL - Тести продуктивності
conformance-title = Звіт про відповідність - VibeSQL
conformance-heading = Звіт про відповідність
conformance-subtitle = Тестування відповідності стандарту SQL:1999

# Navigation
nav-showcase = Демонстрація SQL:1999
nav-conformance = Результати sqltest
nav-sqllogictest = Результати SQLLogicTest

# Editor section
editor-title = Редактор SQL
editor-storage = Сховище
editor-storage-init = Ініціалізація...
editor-execute = Виконати запит

# Results section
results-title = Результати
results-empty = Виконайте запит для перегляду результатів
results-loading = Завантаження...
results-rows = { $count } { $count ->
    [one] рядок
    [few] рядки
   *[other] рядків
}
results-rows-with-time = { $count } { $count ->
    [one] рядок
    [few] рядки
   *[other] рядків
} ({ $time }мс)
results-copy = Копіювати в буфер обміну
results-export = Експорт у CSV
results-limit-warning = Показано перші { $limit } з { $total } рядків. Використовуйте LIMIT для уточнення запиту.

# Examples sidebar
examples-title = Приклади
examples-basic = Базові запити
examples-advanced = Розширені запити

# Database selector
db-select-label = База даних

# Footer
footer-tagline = VibeSQL - База даних SQL:1999 у WebAssembly
footer-deployed = Розгорнуто: { $date }

# Theme
theme-toggle-dark = Перейти на темну тему
theme-toggle-light = Перейти на світлу тему

# Locale
locale-select = Вибрати мову

# Messages
msg-query-success = Запит виконано успішно
msg-rows-affected = { $count } { $count ->
    [one] рядок змінено
    [few] рядки змінено
   *[other] рядків змінено
}

# Errors
error-generic = Сталася помилка
error-query-failed = Помилка виконання запиту

# Editor
editor-placeholder = Введіть SQL-запит тут... (Ctrl+Enter або Cmd+Enter для виконання)

# Navigation links
nav-terminal = Демо SQL-терміналу
nav-compliance = Звіт про відповідність SQL
nav-benchmarks = Тести продуктивності
nav-github = Репозиторій GitHub
nav-home = Головна

# Results
results-success-zero = Запит виконано успішно (0 рядків)
results-null = NULL

# Help Modal
help-title = Гарячі клавіші та довідка
help-close = Закрити
help-editor-shortcuts = Гарячі клавіші редактора
help-navigation = Навігація
help-results-actions = Дії з результатами
help-tips = Поради
help-shortcut-execute = Виконати поточний запит
help-shortcut-comment = Перемкнути коментар рядка
help-shortcut-indent = Відступ виділення
help-shortcut-show-help = Показати цю довідку
help-shortcut-close-help = Закрити довідку
help-action-copy = Копіювати в буфер обміну
help-action-copy-desc = Копіювати результати як значення, розділені табуляцією
help-action-export = Експорт у CSV
help-action-export-desc = Завантажити результати як CSV-файл
help-tip-limit = Результати обмежені 1000 рядками для продуктивності. Використовуйте LIMIT для уточнення запитів.
help-tip-time = Час виконання відображається з результатами запиту.
help-tip-syntax = Редактор підтримує підсвічування синтаксису SQL та автозавершення.
help-tip-theme = Перемикайтесь між світлою/темною темою за допомогою кнопки теми.
help-got-it = Зрозуміло!

# Showcase Navigation
showcase-title = Демонстрація SQL:1999 Core
showcase-description = Інтерактивне вивчення реалізованих функцій SQL:1999 Core
showcase-complete = { $percent }% завершено
showcase-categories = Категорії функцій
showcase-legend = Легенда статусів
showcase-status-implemented = Повністю реалізовано
showcase-status-partial = Частково реалізовано
showcase-status-planned = Заплановано

# Showcase category labels
showcase-cat-compliance = Панель відповідності
showcase-cat-data-types = Типи даних
showcase-cat-dml = DML-операції
showcase-cat-predicates = Предикати та оператори
showcase-cat-joins = JOIN
showcase-cat-subqueries = Підзапити
showcase-cat-aggregates = Агрегати та GROUP BY
showcase-cat-ddl = DDL та обмеження

# Common showcase elements
showcase-interactive-examples = Інтерактивні приклади
showcase-try-example = Спробувати приклад
showcase-progress = { $implemented } з { $total } { $type } ({ $percent }%)
showcase-table-status = Статус
showcase-table-category = Категорія
showcase-table-description = Опис
showcase-table-syntax = Синтаксис
showcase-table-use-case = Приклад використання

# Status labels
status-implemented = Реалізовано
status-partial = Частково
status-planned = Заплановано

# Aggregates Showcase
aggregates-title = Агрегати SQL та GROUP BY
aggregates-description = Агрегатні функції SQL:1999 Core та можливості групування
aggregates-reference = Довідник агрегатних функцій
aggregates-table-function = Функція
aggregates-progress-type = функцій
aggregates-ex-basic = Базові агрегатні функції
aggregates-ex-group-single = GROUP BY (одна колонка)
aggregates-ex-group-multiple = GROUP BY (кілька колонок)
aggregates-ex-having = Умова HAVING
aggregates-ex-orderby = ORDER BY з агрегатами
aggregates-ex-null = Обробка NULL в агрегатах

# DML Operations Showcase
dml-title = DML-операції (мова маніпулювання даними)
dml-description = Операції SQL:1999 Core для запиту та зміни даних
dml-reference = Довідник DML-операцій
dml-table-operation = Операція
dml-progress-type = операцій
dml-ex-select-basic = SELECT - базові запити
dml-ex-select-ordering = SELECT - сортування та обмеження
dml-ex-insert = Операції INSERT
dml-ex-update = Операції UPDATE
dml-ex-delete = Операції DELETE
dml-ex-combined = Комбінований робочий процес CRUD

# Data Types Showcase
datatypes-title = Типи даних SQL:1999 Core
datatypes-description = Вивчення фундаментальних типів даних специфікації SQL:1999 Core
datatypes-reference = Довідник типів даних
datatypes-table-type = Ім'я типу
datatypes-table-example = Приклади значень
datatypes-table-spec = Специфікація
datatypes-progress-type = типів
datatypes-ex-numeric = Робота з числовими типами
datatypes-ex-null = Обробка NULL та тризначна логіка
datatypes-ex-comparisons = Порівняння типів та операції

# JOINs Showcase
joins-title = SQL JOIN
joins-description = Операції JOIN SQL:1999 Core для об'єднання даних з кількох таблиць
joins-reference = Довідник типів JOIN
joins-table-type = Тип JOIN
joins-progress-type = типів JOIN
joins-category-suffix = JOIN
joins-ex-sample = Налаштування тестових даних
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Багатотабличний JOIN

# Predicates Showcase
predicates-title = Предикати та оператори
predicates-description = Предикати SQL:1999 для фільтрації та логічних операцій
predicates-reference = Довідник предикатів
predicates-table-predicate = Предикат
predicates-progress-type = предикатів
predicates-ex-comparison = Оператори порівняння
predicates-ex-between = BETWEEN та предикати діапазону
predicates-ex-null = Предикати NULL та тризначна логіка
predicates-ex-boolean = Булева логіка (AND, OR, NOT)
predicates-ex-in = Предикат IN з підзапитами
predicates-ex-combined = Комбіновані операції з предикатами

# Subqueries Showcase
subqueries-title = SQL-підзапити
subqueries-description = Можливості підзапитів SQL:1999 Core для вкладених запитів
subqueries-reference = Довідник типів підзапитів
subqueries-table-type = Тип підзапиту
subqueries-progress-type = типів підзапитів
subqueries-ex-scalar-select = Скалярний підзапит в SELECT
subqueries-ex-scalar-where = Скалярний підзапит в WHERE
subqueries-ex-derived = Похідні таблиці (підзапит в FROM)
subqueries-ex-in = Предикат IN з підзапитом
subqueries-ex-correlated = Корельовані підзапити
subqueries-ex-nested = Вкладені підзапити
