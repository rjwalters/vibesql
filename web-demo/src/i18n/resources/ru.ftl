# VibeSQL Web UI - Русский

# Page titles
page-title = VibeSQL - База данных SQL:1999 с ИИ
demo-title = Демо VibeSQL
benchmarks-title = Тесты производительности - VibeSQL
benchmarks-heading = VibeSQL - Тесты производительности
conformance-title = Отчёт о соответствии - VibeSQL
conformance-heading = Отчёт о соответствии
conformance-subtitle = Тестирование соответствия стандарту SQL:1999

# Navigation
nav-showcase = Демонстрация SQL:1999
nav-conformance = Результаты sqltest
nav-sqllogictest = Результаты SQLLogicTest

# Editor section
editor-title = Редактор SQL
editor-storage = Хранилище
editor-storage-init = Инициализация...
editor-execute = Выполнить запрос

# Results section
results-title = Результаты
results-empty = Выполните запрос для просмотра результатов
results-loading = Загрузка...
results-rows = { $count } { $count ->
    [one] строка
    [few] строки
   *[other] строк
}
results-rows-with-time = { $count } { $count ->
    [one] строка
    [few] строки
   *[other] строк
} ({ $time }мс)
results-copy = Копировать в буфер обмена
results-export = Экспорт в CSV
results-limit-warning = Показаны первые { $limit } из { $total } строк. Используйте LIMIT для уточнения запроса.

# Examples sidebar
examples-title = Примеры
examples-basic = Базовые запросы
examples-advanced = Продвинутые запросы

# Database selector
db-select-label = База данных

# Footer
footer-tagline = VibeSQL - База данных SQL:1999 в WebAssembly
footer-deployed = Развёрнуто: { $date }

# Theme
theme-toggle-dark = Переключить на тёмную тему
theme-toggle-light = Переключить на светлую тему

# Locale
locale-select = Выбрать язык

# Messages
msg-query-success = Запрос выполнен успешно
msg-rows-affected = { $count } { $count ->
    [one] строка затронута
    [few] строки затронуты
   *[other] строк затронуто
}

# Errors
error-generic = Произошла ошибка
error-query-failed = Ошибка выполнения запроса

# Editor
editor-placeholder = Введите SQL-запрос здесь... (Ctrl+Enter или Cmd+Enter для выполнения)

# Navigation links
nav-terminal = Демо SQL-терминала
nav-compliance = Отчёт о соответствии SQL
nav-benchmarks = Тесты производительности
nav-github = Репозиторий GitHub
nav-home = Главная

# Results
results-success-zero = Запрос выполнен успешно (0 строк)
results-null = NULL

# Help Modal
help-title = Горячие клавиши и справка
help-close = Закрыть
help-editor-shortcuts = Горячие клавиши редактора
help-navigation = Навигация
help-results-actions = Действия с результатами
help-tips = Советы
help-shortcut-execute = Выполнить текущий запрос
help-shortcut-comment = Переключить комментарий строки
help-shortcut-indent = Отступ выделения
help-shortcut-show-help = Показать эту справку
help-shortcut-close-help = Закрыть справку
help-action-copy = Копировать в буфер обмена
help-action-copy-desc = Копировать результаты как значения, разделённые табуляцией
help-action-export = Экспорт в CSV
help-action-export-desc = Скачать результаты как CSV-файл
help-tip-limit = Результаты ограничены 1000 строками для производительности. Используйте LIMIT для уточнения запросов.
help-tip-time = Время выполнения отображается с результатами запроса.
help-tip-syntax = Редактор поддерживает подсветку синтаксиса SQL и автодополнение.
help-tip-theme = Переключайтесь между светлой/тёмной темой с помощью кнопки темы.
help-got-it = Понятно!

# Showcase Navigation
showcase-title = Демонстрация SQL:1999 Core
showcase-description = Интерактивное изучение реализованных функций SQL:1999 Core
showcase-complete = { $percent }% завершено
showcase-categories = Категории функций
showcase-legend = Легенда статусов
showcase-status-implemented = Полностью реализовано
showcase-status-partial = Частично реализовано
showcase-status-planned = Планируется

# Showcase category labels
showcase-cat-compliance = Панель соответствия
showcase-cat-data-types = Типы данных
showcase-cat-dml = DML-операции
showcase-cat-predicates = Предикаты и операторы
showcase-cat-joins = JOIN
showcase-cat-subqueries = Подзапросы
showcase-cat-aggregates = Агрегаты и GROUP BY
showcase-cat-ddl = DDL и ограничения

# Common showcase elements
showcase-interactive-examples = Интерактивные примеры
showcase-try-example = Попробовать пример
showcase-progress = { $implemented } из { $total } { $type } ({ $percent }%)
showcase-table-status = Статус
showcase-table-category = Категория
showcase-table-description = Описание
showcase-table-syntax = Синтаксис
showcase-table-use-case = Применение

# Status labels
status-implemented = Реализовано
status-partial = Частично
status-planned = Планируется

# Aggregates Showcase
aggregates-title = Агрегаты SQL и GROUP BY
aggregates-description = Агрегатные функции SQL:1999 Core и возможности группировки
aggregates-reference = Справочник агрегатных функций
aggregates-table-function = Функция
aggregates-progress-type = функций
aggregates-ex-basic = Базовые агрегатные функции
aggregates-ex-group-single = GROUP BY (одна колонка)
aggregates-ex-group-multiple = GROUP BY (несколько колонок)
aggregates-ex-having = Условие HAVING
aggregates-ex-orderby = ORDER BY с агрегатами
aggregates-ex-null = Обработка NULL в агрегатах

# DML Operations Showcase
dml-title = DML-операции (язык манипулирования данными)
dml-description = Операции SQL:1999 Core для запроса и изменения данных
dml-reference = Справочник DML-операций
dml-table-operation = Операция
dml-progress-type = операций
dml-ex-select-basic = SELECT - базовые запросы
dml-ex-select-ordering = SELECT - сортировка и ограничение
dml-ex-insert = Операции INSERT
dml-ex-update = Операции UPDATE
dml-ex-delete = Операции DELETE
dml-ex-combined = Комбинированный рабочий процесс CRUD

# Data Types Showcase
datatypes-title = Типы данных SQL:1999 Core
datatypes-description = Изучение фундаментальных типов данных спецификации SQL:1999 Core
datatypes-reference = Справочник типов данных
datatypes-table-type = Имя типа
datatypes-table-example = Примеры значений
datatypes-table-spec = Спецификация
datatypes-progress-type = типов
datatypes-ex-numeric = Работа с числовыми типами
datatypes-ex-null = Обработка NULL и трёхзначная логика
datatypes-ex-comparisons = Сравнение типов и операции

# JOINs Showcase
joins-title = SQL JOIN
joins-description = Операции JOIN SQL:1999 Core для объединения данных из нескольких таблиц
joins-reference = Справочник типов JOIN
joins-table-type = Тип JOIN
joins-progress-type = типов JOIN
joins-category-suffix = JOIN
joins-ex-sample = Настройка тестовых данных
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Многотабличный JOIN

# Predicates Showcase
predicates-title = Предикаты и операторы
predicates-description = Предикаты SQL:1999 для фильтрации и логических операций
predicates-reference = Справочник предикатов
predicates-table-predicate = Предикат
predicates-progress-type = предикатов
predicates-ex-comparison = Операторы сравнения
predicates-ex-between = BETWEEN и предикаты диапазона
predicates-ex-null = Предикаты NULL и трёхзначная логика
predicates-ex-boolean = Булева логика (AND, OR, NOT)
predicates-ex-in = Предикат IN с подзапросами
predicates-ex-combined = Комбинированные операции с предикатами

# Subqueries Showcase
subqueries-title = SQL-подзапросы
subqueries-description = Возможности подзапросов SQL:1999 Core для вложенных запросов
subqueries-reference = Справочник типов подзапросов
subqueries-table-type = Тип подзапроса
subqueries-progress-type = типов подзапросов
subqueries-ex-scalar-select = Скалярный подзапрос в SELECT
subqueries-ex-scalar-where = Скалярный подзапрос в WHERE
subqueries-ex-derived = Производные таблицы (подзапрос в FROM)
subqueries-ex-in = Предикат IN с подзапросом
subqueries-ex-correlated = Коррелированные подзапросы
subqueries-ex-nested = Вложенные подзапросы
