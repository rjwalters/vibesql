# VibeSQL Web UI - Polski

# Page titles
page-title = VibeSQL - Baza danych SQL:1999 z AI
demo-title = Demo VibeSQL

# Navigation
nav-showcase = Prezentacja SQL:1999
nav-conformance = Zobacz wyniki sqltest
nav-sqllogictest = Zobacz wyniki SQLLogicTest

# Editor section
editor-title = Edytor SQL
editor-storage = Pamięć
editor-storage-init = Inicjalizacja...
editor-execute = Wykonaj zapytanie

# Results section
results-title = Wyniki
results-empty = Wykonaj zapytanie, aby zobaczyć wyniki
results-loading = Ładowanie...
results-rows = { $count } { $count ->
    [one] wiersz
    [few] wiersze
   *[other] wierszy
}
results-rows-with-time = { $count } { $count ->
    [one] wiersz
    [few] wiersze
   *[other] wierszy
} ({ $time }ms)
results-copy = Kopiuj do schowka
results-export = Eksportuj CSV
results-limit-warning = Wyświetlanie pierwszych { $limit } z { $total } wierszy. Użyj LIMIT, aby zawęzić zapytanie.

# Examples sidebar
examples-title = Przykłady
examples-basic = Podstawowe zapytania
examples-advanced = Zaawansowane zapytania

# Database selector
db-select-label = Baza danych

# Footer
footer-tagline = VibeSQL - Baza danych SQL:1999 w WebAssembly
footer-deployed = Wdrożono: { $date }

# Theme
theme-toggle-dark = Przełącz na tryb ciemny
theme-toggle-light = Przełącz na tryb jasny

# Locale
locale-select = Wybierz język

# Messages
msg-query-success = Zapytanie wykonane pomyślnie
msg-rows-affected = { $count } { $count ->
    [one] wiersz zmieniony
    [few] wiersze zmienione
   *[other] wierszy zmienionych
}

# Errors
error-generic = Wystąpił błąd
error-query-failed = Zapytanie nie powiodło się
