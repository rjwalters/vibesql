# VibeSQL Web UI - Italiano

# Page titles
page-title = VibeSQL - Database SQL:1999 con IA
demo-title = Demo VibeSQL
benchmarks-title = Benchmark Prestazionali - VibeSQL
benchmarks-heading = VibeSQL - Benchmark Prestazionali
conformance-title = Report di Conformità - VibeSQL
conformance-heading = Report di Conformità
conformance-subtitle = Test di Conformità allo Standard SQL:1999

# Navigation
nav-showcase = Showcase SQL:1999
nav-conformance = Vedi risultati sqltest
nav-sqllogictest = Vedi risultati SQLLogicTest

# Editor section
editor-title = Editor SQL
editor-storage = Archiviazione
editor-storage-init = Inizializzazione...
editor-execute = Esegui Query

# Results section
results-title = Risultati
results-empty = Esegui una query per vedere i risultati
results-loading = Caricamento...
results-rows = { $count } { $count ->
    [one] riga
   *[other] righe
}
results-rows-with-time = { $count } { $count ->
    [one] riga
   *[other] righe
} ({ $time }ms)
results-copy = Copia negli appunti
results-export = Esporta CSV
results-limit-warning = Visualizzazione delle prime { $limit } di { $total } righe. Usa LIMIT per affinare la tua query.

# Examples sidebar
examples-title = Esempi
examples-basic = Query di Base
examples-advanced = Query Avanzate

# Database selector
db-select-label = Database

# Footer
footer-tagline = VibeSQL - Database SQL:1999 in WebAssembly
footer-deployed = Distribuito: { $date }

# Theme
theme-toggle-dark = Passa alla modalità scura
theme-toggle-light = Passa alla modalità chiara

# Locale
locale-select = Seleziona lingua

# Messages
msg-query-success = Query eseguita con successo
msg-rows-affected = { $count } { $count ->
    [one] riga interessata
   *[other] righe interessate
}

# Errors
error-generic = Si è verificato un errore
error-query-failed = Query fallita

# Editor
editor-placeholder = Inserisci la tua query SQL qui... (Ctrl+Invio o Cmd+Invio per eseguire)

# Navigation links
nav-terminal = Demo Terminale SQL
nav-compliance = Report di Conformità SQL
nav-benchmarks = Benchmark Prestazionali
nav-github = Repository GitHub
nav-home = Home

# Results
results-success-zero = Query eseguita con successo (0 righe)
results-null = NULL

# Help Modal
help-title = Scorciatoie da Tastiera e Aiuto
help-close = Chiudi
help-editor-shortcuts = Scorciatoie Editor
help-navigation = Navigazione
help-results-actions = Azioni Risultati
help-tips = Suggerimenti
help-shortcut-execute = Esegui query corrente
help-shortcut-comment = Attiva/disattiva commento riga
help-shortcut-indent = Indenta selezione
help-shortcut-show-help = Mostra questa finestra di aiuto
help-shortcut-close-help = Chiudi finestra di aiuto
help-action-copy = Copia negli appunti
help-action-copy-desc = Copia i risultati come valori separati da tabulazione
help-action-export = Esporta CSV
help-action-export-desc = Scarica i risultati come file CSV
help-tip-limit = I risultati sono limitati a 1.000 righe per prestazioni. Usa LIMIT per affinare le query.
help-tip-time = Il tempo di esecuzione è mostrato con i risultati della query.
help-tip-syntax = L'editor supporta l'evidenziazione della sintassi SQL e il completamento automatico.
help-tip-theme = Passa tra tema chiaro/scuro usando il pulsante tema.
help-got-it = Capito!

# Showcase Navigation
showcase-title = Showcase SQL:1999 Core
showcase-description = Esplora interattivamente le funzionalità SQL:1999 Core implementate
showcase-complete = { $percent }% Completato
showcase-categories = Categorie Funzionalità
showcase-legend = Legenda Stati
showcase-status-implemented = Completamente Implementato
showcase-status-partial = Parzialmente Implementato
showcase-status-planned = Pianificato

# Showcase category labels
showcase-cat-compliance = Dashboard Conformità
showcase-cat-data-types = Tipi di Dati
showcase-cat-dml = Operazioni DML
showcase-cat-predicates = Predicati e Operatori
showcase-cat-joins = JOIN
showcase-cat-subqueries = Subquery
showcase-cat-aggregates = Aggregati e GROUP BY
showcase-cat-ddl = DDL e Vincoli

# Common showcase elements
showcase-interactive-examples = Esempi Interattivi
showcase-try-example = Prova Questo Esempio
showcase-progress = { $implemented } di { $total } { $type } ({ $percent }%)
showcase-table-status = Stato
showcase-table-category = Categoria
showcase-table-description = Descrizione
showcase-table-syntax = Sintassi
showcase-table-use-case = Caso d'Uso

# Status labels
status-implemented = Implementato
status-partial = Parziale
status-planned = Pianificato

# Aggregates Showcase
aggregates-title = Aggregati SQL e GROUP BY
aggregates-description = Funzioni di aggregazione SQL:1999 Core e capacità di raggruppamento
aggregates-reference = Riferimento Funzioni di Aggregazione
aggregates-table-function = Funzione
aggregates-progress-type = funzioni
aggregates-ex-basic = Funzioni di Aggregazione Base
aggregates-ex-group-single = GROUP BY (Colonna Singola)
aggregates-ex-group-multiple = GROUP BY (Colonne Multiple)
aggregates-ex-having = Clausola HAVING
aggregates-ex-orderby = ORDER BY con Aggregati
aggregates-ex-null = Gestione NULL negli Aggregati

# DML Operations Showcase
dml-title = Operazioni DML (Linguaggio di Manipolazione Dati)
dml-description = Operazioni SQL:1999 Core per interrogare e modificare dati
dml-reference = Riferimento Operazioni DML
dml-table-operation = Operazione
dml-progress-type = operazioni
dml-ex-select-basic = SELECT - Query Base
dml-ex-select-ordering = SELECT - Ordinamento e Limitazione
dml-ex-insert = Operazioni INSERT
dml-ex-update = Operazioni UPDATE
dml-ex-delete = Operazioni DELETE
dml-ex-combined = Workflow CRUD Combinato

# Data Types Showcase
datatypes-title = Tipi di Dati SQL:1999 Core
datatypes-description = Esplora i tipi di dati fondamentali definiti nella specifica SQL:1999 Core
datatypes-reference = Riferimento Tipi di Dati
datatypes-table-type = Nome Tipo
datatypes-table-example = Valori di Esempio
datatypes-table-spec = Specifica
datatypes-progress-type = tipi
datatypes-ex-numeric = Lavorare con Tipi Numerici
datatypes-ex-null = Gestione NULL e Logica a Tre Valori
datatypes-ex-comparisons = Confronti e Operazioni sui Tipi

# JOINs Showcase
joins-title = JOIN SQL
joins-description = Operazioni JOIN SQL:1999 Core per combinare dati da più tabelle
joins-reference = Riferimento Tipi di JOIN
joins-table-type = Tipo JOIN
joins-progress-type = tipi di JOIN
joins-category-suffix = JOIN
joins-ex-sample = Setup Dati di Esempio
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN Multi-tabella

# Predicates Showcase
predicates-title = Predicati e Operatori
predicates-description = Predicati SQL:1999 per filtraggio e operazioni logiche
predicates-reference = Riferimento Predicati
predicates-table-predicate = Predicato
predicates-progress-type = predicati
predicates-ex-comparison = Operatori di Confronto
predicates-ex-between = BETWEEN e Predicati di Intervallo
predicates-ex-null = Predicati NULL e Logica a Tre Valori
predicates-ex-boolean = Logica Booleana (AND, OR, NOT)
predicates-ex-in = Predicato IN con Subquery
predicates-ex-combined = Operazioni Predicati Combinati

# Subqueries Showcase
subqueries-title = Subquery SQL
subqueries-description = Capacità di subquery SQL:1999 Core per operazioni di query annidate
subqueries-reference = Riferimento Tipi di Subquery
subqueries-table-type = Tipo Subquery
subqueries-progress-type = tipi di subquery
subqueries-ex-scalar-select = Subquery Scalare in SELECT
subqueries-ex-scalar-where = Subquery Scalare in WHERE
subqueries-ex-derived = Tabelle Derivate (Subquery in FROM)
subqueries-ex-in = Predicato IN con Subquery
subqueries-ex-correlated = Subquery Correlate
subqueries-ex-nested = Subquery Annidate
