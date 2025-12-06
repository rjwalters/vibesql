# VibeSQL Web UI - Українська

# Page titles
page-title = VibeSQL - База даних SQL:1999 з ШІ
demo-title = Демо VibeSQL

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
