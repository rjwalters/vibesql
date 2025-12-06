# VibeSQL Web UI - Русский

# Page titles
page-title = VibeSQL - База данных SQL:1999 с ИИ
demo-title = Демо VibeSQL

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
