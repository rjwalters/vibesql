# VibeSQL Web UI - Polski

# Page titles
page-title = VibeSQL - Baza danych SQL:1999 z AI
demo-title = Demo VibeSQL
benchmarks-title = Testy Wydajności - VibeSQL
benchmarks-heading = VibeSQL - Testy Wydajności
conformance-title = Raport Zgodności - VibeSQL
conformance-heading = Raport Zgodności
conformance-subtitle = Testy Zgodności ze Standardem SQL:1999

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

# Editor
editor-placeholder = Wprowadź zapytanie SQL tutaj... (Ctrl+Enter lub Cmd+Enter aby wykonać)

# Navigation links
nav-terminal = Demo terminala SQL
nav-compliance = Raport zgodności SQL
nav-benchmarks = Testy wydajności
nav-github = Repozytorium GitHub
nav-home = Strona główna

# Results
results-success-zero = Zapytanie wykonane pomyślnie (0 wierszy)
results-null = NULL

# Help Modal
help-title = Skróty klawiszowe i pomoc
help-close = Zamknij
help-editor-shortcuts = Skróty edytora
help-navigation = Nawigacja
help-results-actions = Akcje wyników
help-tips = Wskazówki
help-shortcut-execute = Wykonaj bieżące zapytanie
help-shortcut-comment = Przełącz komentarz linii
help-shortcut-indent = Wcięcie zaznaczenia
help-shortcut-show-help = Pokaż to okno pomocy
help-shortcut-close-help = Zamknij okno pomocy
help-action-copy = Kopiuj do schowka
help-action-copy-desc = Kopiuj wyniki jako wartości rozdzielone tabulatorem
help-action-export = Eksportuj CSV
help-action-export-desc = Pobierz wyniki jako plik CSV
help-tip-limit = Wyniki ograniczone do 1000 wierszy dla wydajności. Użyj LIMIT do zawężenia zapytań.
help-tip-time = Czas wykonania wyświetlany jest z wynikami zapytania.
help-tip-syntax = Edytor obsługuje podświetlanie składni SQL i autouzupełnianie.
help-tip-theme = Przełączaj między jasnym/ciemnym trybem za pomocą przycisku motywu.
help-got-it = Rozumiem!

# Showcase Navigation
showcase-title = Prezentacja SQL:1999 Core
showcase-description = Interaktywne odkrywanie zaimplementowanych funkcji SQL:1999 Core
showcase-complete = { $percent }% ukończone
showcase-categories = Kategorie funkcji
showcase-legend = Legenda statusów
showcase-status-implemented = W pełni zaimplementowane
showcase-status-partial = Częściowo zaimplementowane
showcase-status-planned = Planowane

# Showcase category labels
showcase-cat-compliance = Panel zgodności
showcase-cat-data-types = Typy danych
showcase-cat-dml = Operacje DML
showcase-cat-predicates = Predykaty i operatory
showcase-cat-joins = JOIN
showcase-cat-subqueries = Podzapytania
showcase-cat-aggregates = Agregaty i GROUP BY
showcase-cat-ddl = DDL i ograniczenia

# Common showcase elements
showcase-interactive-examples = Interaktywne przykłady
showcase-try-example = Wypróbuj ten przykład
showcase-progress = { $implemented } z { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Kategoria
showcase-table-description = Opis
showcase-table-syntax = Składnia
showcase-table-use-case = Przypadek użycia

# Status labels
status-implemented = Zaimplementowane
status-partial = Częściowe
status-planned = Planowane

# Aggregates Showcase
aggregates-title = Agregaty SQL i GROUP BY
aggregates-description = Funkcje agregujące SQL:1999 Core i możliwości grupowania
aggregates-reference = Dokumentacja funkcji agregujących
aggregates-table-function = Funkcja
aggregates-progress-type = funkcji
aggregates-ex-basic = Podstawowe funkcje agregujące
aggregates-ex-group-single = GROUP BY (jedna kolumna)
aggregates-ex-group-multiple = GROUP BY (wiele kolumn)
aggregates-ex-having = Klauzula HAVING
aggregates-ex-orderby = ORDER BY z agregatami
aggregates-ex-null = Obsługa NULL w agregatach

# DML Operations Showcase
dml-title = Operacje DML (język manipulacji danymi)
dml-description = Operacje SQL:1999 Core do zapytań i modyfikacji danych
dml-reference = Dokumentacja operacji DML
dml-table-operation = Operacja
dml-progress-type = operacji
dml-ex-select-basic = SELECT - podstawowe zapytania
dml-ex-select-ordering = SELECT - sortowanie i ograniczanie
dml-ex-insert = Operacje INSERT
dml-ex-update = Operacje UPDATE
dml-ex-delete = Operacje DELETE
dml-ex-combined = Połączony workflow CRUD

# Data Types Showcase
datatypes-title = Typy danych SQL:1999 Core
datatypes-description = Odkryj podstawowe typy danych zdefiniowane w specyfikacji SQL:1999 Core
datatypes-reference = Dokumentacja typów danych
datatypes-table-type = Nazwa typu
datatypes-table-example = Przykładowe wartości
datatypes-table-spec = Specyfikacja
datatypes-progress-type = typów
datatypes-ex-numeric = Praca z typami numerycznymi
datatypes-ex-null = Obsługa NULL i logika trójwartościowa
datatypes-ex-comparisons = Porównania typów i operacje

# JOINs Showcase
joins-title = SQL JOIN
joins-description = Operacje JOIN SQL:1999 Core do łączenia danych z wielu tabel
joins-reference = Dokumentacja typów JOIN
joins-table-type = Typ JOIN
joins-progress-type = typów JOIN
joins-category-suffix = JOIN
joins-ex-sample = Konfiguracja przykładowych danych
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Wielotabelowy JOIN

# Predicates Showcase
predicates-title = Predykaty i operatory
predicates-description = Predykaty SQL:1999 do filtrowania i operacji logicznych
predicates-reference = Dokumentacja predykatów
predicates-table-predicate = Predykat
predicates-progress-type = predykatów
predicates-ex-comparison = Operatory porównania
predicates-ex-between = BETWEEN i predykaty zakresu
predicates-ex-null = Predykaty NULL i logika trójwartościowa
predicates-ex-boolean = Logika boolowska (AND, OR, NOT)
predicates-ex-in = Predykat IN z podzapytaniami
predicates-ex-combined = Połączone operacje predykatów

# Subqueries Showcase
subqueries-title = Podzapytania SQL
subqueries-description = Możliwości podzapytań SQL:1999 Core dla zagnieżdżonych operacji
subqueries-reference = Dokumentacja typów podzapytań
subqueries-table-type = Typ podzapytania
subqueries-progress-type = typów podzapytań
subqueries-ex-scalar-select = Skalarne podzapytanie w SELECT
subqueries-ex-scalar-where = Skalarne podzapytanie w WHERE
subqueries-ex-derived = Tabele pochodne (podzapytanie w FROM)
subqueries-ex-in = Predykat IN z podzapytaniem
subqueries-ex-correlated = Skorelowane podzapytania
subqueries-ex-nested = Zagnieżdżone podzapytania
