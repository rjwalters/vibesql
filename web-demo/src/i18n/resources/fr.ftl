# VibeSQL Web UI - Français

# Page titles
page-title = VibeSQL - Base de Données SQL:1999 avec IA
demo-title = Démo VibeSQL
benchmarks-title = Benchmarks de Performance - VibeSQL
benchmarks-heading = VibeSQL - Benchmarks de Performance
conformance-title = Rapport de Conformité - VibeSQL
conformance-heading = Rapport de Conformité
conformance-subtitle = Tests de Conformité au Standard SQL:1999

# Navigation
nav-showcase = Démonstration SQL:1999
nav-conformance = Voir les résultats sqltest
nav-sqllogictest = Voir les résultats SQLLogicTest

# Editor section
editor-title = Éditeur SQL
editor-storage = Stockage
editor-storage-init = Initialisation...
editor-execute = Exécuter la Requête

# Results section
results-title = Résultats
results-empty = Exécutez une requête pour voir les résultats
results-loading = Chargement...
results-rows = { $count } { $count ->
    [one] ligne
   *[other] lignes
}
results-rows-with-time = { $count } { $count ->
    [one] ligne
   *[other] lignes
} ({ $time }ms)
results-copy = Copier dans le presse-papiers
results-export = Exporter en CSV
results-limit-warning = Affichage des { $limit } premières lignes sur { $total }. Utilisez LIMIT pour affiner votre requête.

# Examples sidebar
examples-title = Exemples
examples-basic = Requêtes de Base
examples-advanced = Requêtes Avancées

# Database selector
db-select-label = Base de données

# Footer
footer-tagline = VibeSQL - Base de Données SQL:1999 en WebAssembly
footer-deployed = Déployé le : { $date }

# Theme
theme-toggle-dark = Passer en mode sombre
theme-toggle-light = Passer en mode clair

# Locale
locale-select = Choisir la langue

# Messages
msg-query-success = Requête exécutée avec succès
msg-rows-affected = { $count } { $count ->
    [one] ligne affectée
   *[other] lignes affectées
}

# Errors
error-generic = Une erreur s'est produite
error-query-failed = La requête a échoué
error-no-databases = Aucune base de données disponible

# Loading states
loading-initializing-theme = Initialisation du thème
loading-preparing-editor = Préparation de l'éditeur
loading-database-engine = Chargement du moteur de base de données
loading-setting-up-ui = Configuration de l'interface utilisateur
loading-editor = Chargement de l'éditeur...
loading-compliance-data = Chargement des données de conformité...
loading-conformance-report = Chargement du rapport de conformité...

# Editor
editor-placeholder = Entrez votre requête SQL ici... (Ctrl+Entrée ou Cmd+Entrée pour exécuter)

# Navigation links
nav-terminal = Terminal SQL Démo
nav-compliance = Rapport de Conformité SQL
nav-benchmarks = Benchmarks de Performance
nav-github = Dépôt GitHub
nav-home = Accueil
nav-trends = Tendances de Performance

# Trends page
trends-title = Tendances de Performance - VibeSQL
trends-heading = VibeSQL - Tendances de Performance
trends-total-runs = Exécutions Totales
trends-across-suites = sur toutes les suites
trends-date-range = Plage de Dates
trends-first-to-last = première à dernière exécution
trends-latest-commit = Dernier Commit
trends-most-recent = benchmark le plus récent
trends-generated = Généré
trends-last-export = dernière exportation de données

# Results
results-success-zero = Requête exécutée avec succès (0 lignes)
results-null = NULL

# Help Modal
help-title = Raccourcis Clavier et Aide
help-close = Fermer
help-editor-shortcuts = Raccourcis de l'Éditeur
help-navigation = Navigation
help-results-actions = Actions sur les Résultats
help-tips = Conseils
help-shortcut-execute = Exécuter la requête actuelle
help-shortcut-comment = Basculer le commentaire de ligne
help-shortcut-indent = Indenter la sélection
help-shortcut-show-help = Afficher cette aide
help-shortcut-close-help = Fermer l'aide
help-action-copy = Copier dans le presse-papiers
help-action-copy-desc = Copier les résultats en valeurs séparées par tabulation
help-action-export = Exporter en CSV
help-action-export-desc = Télécharger les résultats au format CSV
help-tip-limit = Les résultats sont limités à 1 000 lignes pour la performance. Utilisez LIMIT pour affiner les requêtes.
help-tip-time = Le temps d'exécution est affiché avec les résultats.
help-tip-syntax = L'éditeur prend en charge la coloration syntaxique SQL et l'auto-complétion.
help-tip-theme = Basculez entre les thèmes clair/sombre avec le bouton de thème.
help-got-it = Compris !

# Showcase Navigation
showcase-title = Démonstration SQL:1999 Core
showcase-description = Explorez les fonctionnalités SQL:1999 Core implémentées de manière interactive
showcase-complete = { $percent }% Terminé
showcase-categories = Catégories de Fonctionnalités
showcase-legend = Légende des États
showcase-status-implemented = Entièrement Implémenté
showcase-status-partial = Partiellement Implémenté
showcase-status-planned = Planifié

# Showcase category labels
showcase-cat-compliance = Tableau de Bord de Conformité
showcase-cat-data-types = Types de Données
showcase-cat-dml = Opérations DML
showcase-cat-predicates = Prédicats et Opérateurs
showcase-cat-joins = JOINs
showcase-cat-subqueries = Sous-requêtes
showcase-cat-aggregates = Agrégats et GROUP BY
showcase-cat-ddl = DDL et Contraintes

# Common showcase elements
showcase-interactive-examples = Exemples Interactifs
showcase-try-example = Essayer Cet Exemple
showcase-progress = { $implemented } sur { $total } { $type } ({ $percent }%)
showcase-table-status = État
showcase-table-category = Catégorie
showcase-table-description = Description
showcase-table-syntax = Syntaxe
showcase-table-use-case = Cas d'Utilisation

# Status labels
status-implemented = Implémenté
status-partial = Partiel
status-planned = Planifié

# Aggregates Showcase
aggregates-title = Agrégats SQL et GROUP BY
aggregates-description = Fonctions d'agrégation SQL:1999 Core et capacités de regroupement
aggregates-reference = Référence des Fonctions d'Agrégation
aggregates-table-function = Fonction
aggregates-progress-type = fonctions
aggregates-ex-basic = Fonctions d'Agrégation de Base
aggregates-ex-group-single = GROUP BY (Colonne Unique)
aggregates-ex-group-multiple = GROUP BY (Colonnes Multiples)
aggregates-ex-having = Clause HAVING
aggregates-ex-orderby = ORDER BY avec Agrégats
aggregates-ex-null = Gestion des NULL dans les Agrégats

# DML Operations Showcase
dml-title = Opérations DML (Langage de Manipulation de Données)
dml-description = Opérations SQL:1999 Core pour interroger et modifier les données
dml-reference = Référence des Opérations DML
dml-table-operation = Opération
dml-progress-type = opérations
dml-ex-select-basic = SELECT - Requêtes de Base
dml-ex-select-ordering = SELECT - Tri et Limitation
dml-ex-insert = Opérations INSERT
dml-ex-update = Opérations UPDATE
dml-ex-delete = Opérations DELETE
dml-ex-combined = Workflow CRUD Combiné

# Data Types Showcase
datatypes-title = Types de Données SQL:1999 Core
datatypes-description = Explorez les types de données fondamentaux définis dans la spécification SQL:1999 Core
datatypes-reference = Référence des Types de Données
datatypes-table-type = Nom du Type
datatypes-table-example = Valeurs d'Exemple
datatypes-table-spec = Spécification
datatypes-progress-type = types
datatypes-ex-numeric = Travailler avec les Types Numériques
datatypes-ex-null = Gestion des NULL et Logique Ternaire
datatypes-ex-comparisons = Comparaisons et Opérations de Types

# JOINs Showcase
joins-title = JOINs SQL
joins-description = Opérations JOIN SQL:1999 Core pour combiner des données de plusieurs tables
joins-reference = Référence des Types de JOIN
joins-table-type = Type de JOIN
joins-progress-type = types de JOIN
joins-category-suffix = JOINs
joins-ex-sample = Configuration des Données d'Exemple
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN Multi-tables

# Predicates Showcase
predicates-title = Prédicats et Opérateurs
predicates-description = Prédicats SQL:1999 pour le filtrage et les opérations logiques
predicates-reference = Référence des Prédicats
predicates-table-predicate = Prédicat
predicates-progress-type = prédicats
predicates-ex-comparison = Opérateurs de Comparaison
predicates-ex-between = BETWEEN et Prédicats de Plage
predicates-ex-null = Prédicats NULL et Logique Ternaire
predicates-ex-boolean = Logique Booléenne (AND, OR, NOT)
predicates-ex-in = Prédicat IN avec Sous-requêtes
predicates-ex-combined = Opérations de Prédicats Combinées

# Subqueries Showcase
subqueries-title = Sous-requêtes SQL
subqueries-description = Capacités de sous-requêtes SQL:1999 Core pour les opérations de requêtes imbriquées
subqueries-reference = Référence des Types de Sous-requêtes
subqueries-table-type = Type de Sous-requête
subqueries-progress-type = types de sous-requête
subqueries-ex-scalar-select = Sous-requête Scalaire dans SELECT
subqueries-ex-scalar-where = Sous-requête Scalaire dans WHERE
subqueries-ex-derived = Tables Dérivées (Sous-requête dans FROM)
subqueries-ex-in = Prédicat IN avec Sous-requête
subqueries-ex-correlated = Sous-requêtes Corrélées
subqueries-ex-nested = Sous-requêtes Imbriquées

# =============================================================================
# Page des Benchmarks
# =============================================================================

# En-têtes de section
bench-section-embedded = Embarqué
bench-section-server = Serveur
bench-results-title = Résultats des Benchmarks
bench-perf-comparison = Comparaison des Performances
bench-methodology-title = Méthodologie
bench-analysis-roadmap = Analyse et Feuille de Route

# Cartes récapitulatives
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = Opérations Testées
bench-last-updated = Dernière Mise à Jour
bench-avg-speedup = accélération moyenne
bench-from-main = depuis la branche main
bench-loading = Chargement...
bench-na = N/D
bench-faster = { $value }x plus rapide
bench-slower = { $value }x plus lent
bench-speedup = { $value }x
bench-startup-time-label = temps de démarrage
bench-download-size = taille de téléchargement
bench-uncompressed = non compressé
bench-size-metrics = métriques de taille
bench-failed = ÉCHEC
bench-failed-title = Requête échouée (délai dépassé ou erreur)
bench-no-wasm-data = Aucune donnée WASM disponible
bench-no-server-data = Aucune donnée de benchmark serveur Sysbench disponible
bench-no-server-data-hint = Les benchmarks serveur nécessitent l'exécution de sysbench_server avec la comparaison MySQL activée.

# En-têtes de tableau
bench-table-operation = Opération
bench-table-query = Query
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = Chargement des résultats de benchmark...
bench-vibesql-server-title = VibeSQL via protocole PostgreSQL

# Termes communs de benchmark
bench-hardware = Matériel
bench-benchmark-framework = Framework de Benchmark
bench-scale-factor = Facteur d'Échelle
bench-data = Données
bench-databases-tested = Bases de Données Testées
bench-execution-mode = Mode d'Exécution
bench-measurement = Mesure
bench-workload = Charge de Travail
bench-transaction-mix = Mix de Transactions
bench-warehouses = Entrepôts
bench-concurrency = Concurrence
bench-acid-compliance = Conformité ACID
bench-mode = Mode
bench-workload-types = Types de Charge de Travail
bench-table-size = Taille de Table
bench-index-types = Types d'Index
bench-operations = Opérations
bench-databases = Bases de Données
bench-protocol-overhead = Surcharge Protocole
bench-binary-size = Taille du Binaire
bench-startup-time = Temps de Démarrage
bench-peak-memory = Mémoire Maximale
bench-schema = Schéma
bench-query-count = Nombre de Requêtes
bench-query-types = Types de Requêtes
bench-sql-features = Fonctionnalités SQL
bench-wasm-size = Taille WASM
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H spécifique
bench-tpch-name = TPC-H
bench-tpch-title = Benchmark d'Aide à la Décision TPC-H
bench-tpch-description = Ces benchmarks utilisent le <strong>suite de benchmarks TPC-H</strong> standard de l'industrie, qui simule des charges de travail d'aide à la décision réelles avec des requêtes analytiques complexes impliquant agrégations, jointures, sous-requêtes et tri.
bench-tpch-ops-label = requêtes TPC-H
bench-tpch-note-intro = Tous les benchmarks mesurent le temps d'exécution de bout en bout des requêtes, incluant l'analyse, la planification, l'exécution et la matérialisation des résultats. Cela représente la <strong>performance réelle du moteur SQL</strong> pour les charges analytiques.
bench-tpch-note-queries = <strong>Note :</strong> Les requêtes TPC-H testent différents aspects de la performance SQL : agrégations simples (Q1, Q6), jointures complexes (Q2-Q5, Q7-Q10), sous-requêtes (Q11-Q15) et analytique avancée (Q16-Q22). Survolez les noms des requêtes dans le tableau pour les descriptions.

# Discussion TPC-H
bench-tpch-disc-excels-title = Où VibeSQL Excelle
bench-tpch-disc-excels = VibeSQL montre de solides performances sur les <strong>requêtes d'agrégation intensives en scan</strong> (Q1, Q6, Q14, Q15, Q20) où notre moteur d'exécution colonnaire et les agrégations accélérées par SIMD brillent. Ces requêtes impliquent le filtrage de grandes tables et le calcul d'agrégats sans patterns de jointure complexes.
bench-tpch-disc-targets-title = Objectifs d'Optimisation Actuels
bench-tpch-disc-targets = Les requêtes à jointures multiples (Q3, Q5, Q7-Q10, Q18, Q19, Q21) montrent actuellement SQLite en tête. Le goulot d'étranglement principal est notre implémentation de hash join, qui n'emploie pas encore le même niveau d'optimisation que les jointures B-tree affinées pendant des décennies de SQLite. Domaines spécifiques en développement actif :
bench-tpch-disc-join-ordering = Estimation de cardinalité améliorée pour une meilleure sélection de l'ordre des jointures
bench-tpch-disc-hash-sizing = Croissance adaptative des tables de hachage et débordement sur disque pour les grandes jointures
bench-tpch-disc-vectorized = Traitement par lots dans la boucle interne des jointures pour améliorer l'utilisation du cache
bench-tpch-disc-inl-joins = Exploitation des index B-tree quand c'est bénéfique
bench-tpch-disc-path-title = Vers le Leadership
bench-tpch-disc-path = L'architecture de VibeSQL est conçue pour le matériel moderne avec des fonctionnalités comme le stockage colonnaire, l'exécution vectorisée et la concurrence sans verrou. À mesure que ces optimisations mûrissent, nous nous attendons à ce que VibeSQL atteigne un leadership constant sur toutes les requêtes TPC-H. La conception fondamentale supporte le parallélisme et SIMD que les bases de données traditionnelles orientées lignes ne peuvent pas facilement adapter.

# Descriptions des Requêtes TPC-H
bench-tpch-q1 = Rapport Récapitulatif des Prix - Agrégation de prix avec GROUP BY et ORDER BY
bench-tpch-q2 = Fournisseur au Coût Minimum - JOIN de 3 tables avec ORDER BY et LIMIT
bench-tpch-q3 = Priorité d'Expédition - JOIN de 3 tables avec agrégation
bench-tpch-q4 = Vérification de Priorité de Commande - Sous-requête EXISTS corrélée
bench-tpch-q5 = Volume Fournisseur Local - JOIN de 6 tables avec filtrage complexe
bench-tpch-q6 = Prévision de Changement de Revenus - Filtres WHERE avec BETWEEN et SUM
bench-tpch-q7 = Volume d'Expédition - JOIN de 6 tables avec SUBSTR et filtrage de dates
bench-tpch-q8 = Part de Marché Nationale - JOIN de 7 tables avec expressions CASE
bench-tpch-q9 = Mesure de Profit par Type de Produit - JOIN de 4 tables avec agrégation
bench-tpch-q10 = Rapport des Articles Retournés - JOIN de 4 tables avec TOP-N LIMIT
bench-tpch-q11 = Identification de Stock Important - Sous-requête dans clause HAVING
bench-tpch-q12 = Priorité des Modes d'Expédition - Agrégation CASE avec logique de dates
bench-tpch-q13 = Distribution des Clients - LEFT OUTER JOIN avec sous-requête
bench-tpch-q14 = Effet Promotion - Agrégation conditionnelle avec CASE
bench-tpch-q15 = Meilleur Fournisseur - Sous-requêtes imbriquées avec MAX
bench-tpch-q16 = Relation Pièces/Fournisseur - Sous-requête NOT IN avec DISTINCT
bench-tpch-q17 = Revenus Petites Commandes - Sous-requête corrélée dans WHERE
bench-tpch-q18 = Client Grand Volume - GROUP BY avec HAVING
bench-tpch-q19 = Revenus Remisés - Conditions OR complexes
bench-tpch-q20 = Promotion Potentielle de Pièces - Sous-requête IN avec GROUP BY/HAVING
bench-tpch-q21 = Fournisseurs qui ont Retardé les Commandes - EXISTS multi-tables
bench-tpch-q22 = Opportunité de Ventes Globale - SUBSTR avec sous-requête NOT EXISTS

# TPC-DS spécifique
bench-tpcds-name = TPC-DS
bench-tpcds-title = Benchmark d'Aide à la Décision TPC-DS
bench-tpcds-description = <strong>TPC-DS</strong> est le successeur de TPC-H, comportant 99 requêtes qui modélisent un système moderne d'aide à la décision avec des patterns de requêtes significativement plus complexes incluant plusieurs tables de faits, schéma en flocon de neige et fonctionnalités SQL avancées.
bench-tpcds-ops-label = requêtes TPC-DS
bench-tpcds-note-intro = Les requêtes TPC-DS sont substantiellement plus complexes que TPC-H, testant des fonctionnalités SQL avancées comme les fonctions de fenêtrage, les expressions de table communes (clause WITH) et des patterns de jointures complexes à travers plusieurs tables de faits et de dimensions.
bench-tpcds-note-remaining = <strong>Note :</strong> Les requêtes restantes non supportées nécessitent des fonctionnalités comme INTERSECT/EXCEPT ou des fonctions arithmétiques de dates spécifiques pas encore implémentées.

# Discussion TPC-DS
bench-tpcds-disc-coverage-title = Couverture des Fonctionnalités SQL:1999
bench-tpcds-disc-coverage = TPC-DS exerce les fonctionnalités SQL les plus exigeantes. VibeSQL passe <strong>88 des 99 requêtes</strong>, démontrant une large couverture de SQL:1999 incluant ROLLUP, CUBE, GROUPING(), fonctions de fenêtrage avec cadrage complexe et CTEs récursifs. Les requêtes restantes nécessitent les opérations d'ensemble INTERSECT/EXCEPT.
bench-tpcds-disc-optimization-title = Optimisation des Requêtes Complexes
bench-tpcds-disc-optimization = Les requêtes TPC-DS joignent souvent plus de 10 tables avec des sous-requêtes corrélées. Domaines de focus actuels :
bench-tpcds-disc-cte = Décision intelligente entre CTEs matérialisés et inline
bench-tpcds-disc-decorrelation = Conversion des sous-requêtes corrélées en jointures quand bénéfique
bench-tpcds-disc-star = Ordonnancement des jointures fait-dimension pour les patterns analytiques
bench-tpcds-disc-toward-title = Vers 99/99
bench-tpcds-disc-toward = INTERSECT et EXCEPT sont des ajouts planifiés qui permettront les requêtes restantes. Ces opérations d'ensemble s'intègrent naturellement dans notre algèbre de requêtes existante et seront implémentées comme des opérateurs basés sur le hachage similaires à notre traitement DISTINCT.

# TPC-C spécifique
bench-tpcc-name = TPC-C
bench-tpcc-title = Benchmark de Traitement Transactionnel en Ligne TPC-C
bench-tpcc-description = Le <strong>benchmark TPC-C</strong> simule un environnement complet de saisie de commandes avec un mix de transactions complexes incluant la saisie de commandes, le traitement des paiements, les requêtes de statut de commande, le traitement des livraisons et la surveillance du niveau de stock.
bench-tpcc-ops-label = transactions TPC-C
bench-tpcc-transactions-label = transactions exécutées
bench-tpcc-note-intro = TPC-C mesure les transactions par minute (tpmC) et teste la capacité de la base de données à gérer des transactions concurrentes avec une logique métier complexe. Ce benchmark est critique pour évaluer la <strong>performance des charges transactionnelles</strong>.
bench-tpcc-note-results = <strong>Note :</strong> Les résultats montrent la latence moyenne des transactions. Plus bas est mieux. TPC-C est particulièrement exigeant pour les charges intensives en écriture avec des exigences strictes de cohérence.

# Descriptions des Transactions TPC-C
bench-tpcc-new-order = Nouvelle Commande - Transaction complexe avec vérifications d'inventaire et création de commande
bench-tpcc-payment = Paiement - Mise à jour du solde client et des totaux entrepôt/district
bench-tpcc-order-status = Statut de Commande - Requête en lecture seule pour l'historique des commandes client
bench-tpcc-delivery = Livraison - Traitement par lots des commandes en attente
bench-tpcc-stock-level = Niveau de Stock - Compter les articles sous le seuil dans les commandes récentes

# Discussion TPC-C
bench-tpcc-disc-faster-title = { $speedup } Plus Rapide que SQLite
bench-tpcc-disc-faster = VibeSQL atteint <strong>~79 000 transactions par seconde</strong> comparé à ~1 900 TPS de SQLite, une amélioration de 42x. Cette accélération spectaculaire vient de notre architecture MVCC sans verrou qui évite le verrouillage à gros grain de SQLite sur chaque opération d'écriture.
bench-tpcc-disc-dominates-title = Pourquoi VibeSQL Domine l'OLTP
bench-tpcc-disc-lockfree = MVCC permet aux lecteurs et écrivains de procéder concurremment sans blocage
bench-tpcc-disc-optimistic = Les transactions n'entrent en conflit qu'au moment du commit, pas pendant l'exécution
bench-tpcc-disc-btree = Structure d'index construite sur mesure optimisée pour les charges en mémoire
bench-tpcc-disc-prepared = Les plans de requêtes sont compilés une fois et réutilisés
bench-tpcc-disc-scaling-title = Passage à l'Échelle
bench-tpcc-disc-scaling = Les résultats actuels sont mono-thread. L'architecture de VibeSQL supporte le traitement transactionnel multi-thread, et nous nous attendons à un passage à l'échelle quasi-linéaire à mesure que nous ajoutons le support d'exécution parallèle. Notre objectif est d'atteindre plus de 500K TPS sur du matériel multi-cœur moderne.
bench-tpcc-disc-duckdb-title = Pourquoi DuckDB est à la traîne sur OLTP
bench-tpcc-disc-duckdb = DuckDB n'atteint que ~385 TPS sur TPC-C (60x plus lent que VibeSQL, 12x plus lent que SQLite). C'est attendu : DuckDB est une <strong>base de données analytique (OLAP)</strong> optimisée pour les opérations par lots volumineuses, pas pour les transactions mono-ligne. Son format de stockage en colonnes excelle dans le scan de millions de lignes mais ajoute une surcharge pour les recherches ponctuelles et les petites mises à jour qui dominent les charges OLTP comme TPC-C.

# Sysbench Embarqué spécifique
bench-sysbench-embedded-name = Sysbench (Embarqué)
bench-sysbench-embedded-title = Micro-Benchmarks Sysbench (Embarqué)
bench-sysbench-embedded-description = <strong>Sysbench</strong> fournit des micro-benchmarks ciblés qui isolent des opérations de base de données spécifiques. Ces tests mesurent la performance brute pour des opérations fondamentales sans la complexité des charges transactionnelles complètes.
bench-sysbench-embedded-ops-label = opérations Sysbench
bench-sysbench-embedded-note = Le mode embarqué exécute la base de données dans le processus avec zéro surcharge réseau, idéal pour les applications mono-processus où une latence minimale est critique.

# Descriptions des Opérations Sysbench
bench-sysbench-point-select = Sélection Point - Recherche d'une seule ligne par clé primaire
bench-sysbench-insert = Insertion - Insérer de nouvelles lignes dans la table
bench-sysbench-update-index = Mise à Jour Index - Mettre à jour une colonne indexée (k = k + 1)
bench-sysbench-update-non-index = Mise à Jour Sans Index - Mettre à jour une colonne non indexée
bench-sysbench-delete = Suppression - Supprimer des lignes par clé primaire
bench-sysbench-range-queries = Requêtes de Plage - Scans de plage Simple, SUM, ORDER BY et DISTINCT

# Discussion Sysbench Embarqué
bench-sysbench-emb-disc-point-title = Recherches Point : Écart de { $pointRatio }
bench-sysbench-emb-disc-point = Les sélections point de VibeSQL s'exécutent à <strong>~{ $pointVibesqlUs }µs vs ~{ $pointSqliteUs }µs pour SQLite</strong>. Cet écart de { $pointRatio } représente notre objectif principal d'optimisation OLTP - nous étudions la disposition des nœuds B-tree et les chemins de lecture sans verrou pour combler cet écart.
bench-sysbench-emb-disc-index-title = Mises à Jour d'Index : Écart de { $indexRatio }
bench-sysbench-emb-disc-index = Les mises à jour indexées de VibeSQL s'exécutent à <strong>~{ $indexVibesqlUs }µs vs ~{ $indexSqliteUs }µs pour SQLite</strong>. C'est un domaine d'optimisation car notre conception MVCC ajoute une surcharge pour la maintenance d'index que nous travaillons à réduire.
bench-sysbench-emb-disc-improve-title = Domaines d'Amélioration
bench-sysbench-emb-disc-bulk = Le chemin d'insertion par lots de SQLite est hautement optimisé ; nous ajoutons des opérations B-tree par lots
bench-sysbench-emb-disc-nonindex = Les mises à jour non indexées montrent VibeSQL à ~{ $nonIndexVibesqlUs }µs vs ~{ $nonIndexSqliteUs }µs pour SQLite
bench-sysbench-emb-disc-deletes = Les opérations de suppression montrent VibeSQL à ~{ $deleteVibesqlUs }µs vs ~{ $deleteSqliteUs }µs pour SQLite
bench-sysbench-emb-disc-architecture-title = Compromis Architecturaux
bench-sysbench-emb-disc-architecture = L'architecture hybride de VibeSQL cible à la fois les charges OLTP et OLAP. Notre stockage B-tree offre des performances de recherche ponctuelle compétitives avec SQLite, tandis que l'exécution en colonnes gère efficacement les requêtes analytiques. Ceci diffère des bases de données purement OLAP comme DuckDB qui optimisent exclusivement pour les opérations en masse au détriment de la latence mono-ligne.

# Sysbench Serveur spécifique
bench-sysbench-server-name = Sysbench (Serveur)
bench-sysbench-server-title = Micro-Benchmarks Sysbench (Serveur)
bench-sysbench-server-description = Les benchmarks serveur <strong>Sysbench</strong> comparent VibeSQL Server (protocole PostgreSQL) à MySQL, mesurant la performance pour les déploiements de bases de données multi-clients.
bench-sysbench-server-ops-label = opérations Sysbench
bench-sysbench-server-note = Le mode serveur utilise le protocole PostgreSQL, permettant l'accès multi-clients et la compatibilité avec les outils et drivers PostgreSQL existants.

# Discussion Sysbench Serveur
bench-sysbench-srv-disc-protocol-title = Protocole PostgreSQL
bench-sysbench-srv-disc-protocol = VibeSQL Server implémente le protocole PostgreSQL, permettant la compatibilité avec les drivers et outils PostgreSQL existants. Cela ajoute ~10-50µs de surcharge protocolaire par requête comparé au mode embarqué, mais permet les déploiements multi-clients.
bench-sysbench-srv-disc-mysql-title = Comparaison avec MySQL
bench-sysbench-srv-disc-mysql = Les benchmarks serveur comparent avec MySQL pour évaluer VibeSQL comme remplacement direct pour les bases de données client-serveur traditionnelles. VibeSQL Server surpasse MySQL sur toutes les opérations Sysbench, avec des accélérations allant de <strong>2,4x</strong> (requêtes de plage) à <strong>12,8x</strong> (mises à jour indexées).
bench-sysbench-srv-disc-perf-title = Pourquoi VibeSQL Server est plus rapide
bench-sysbench-srv-disc-perf-arch = L'architecture de VibeSQL diffère fondamentalement de la conception RDBMS traditionnelle de MySQL
bench-sysbench-srv-disc-perf-storage = VibeSQL utilise un moteur de stockage colonnaire en mémoire optimisé pour les charges analytiques et OLTP, évitant la surcharge de gestion de pages InnoDB sur disque de MySQL
bench-sysbench-srv-disc-perf-locking = Pas de verrouillage lourd au niveau des lignes ni de comptabilité MVCC—VibeSQL utilise un contrôle de concurrence léger conçu pour les CPU multi-cœurs modernes
bench-sysbench-srv-disc-perf-protocol = Implémentation efficace du protocole PostgreSQL avec une surcharge de sérialisation minimale comparée au protocole MySQL
bench-sysbench-srv-disc-perf-writes = Les opérations d'écriture (insertions/mises à jour) montrent les gains les plus importants (<strong>8-12x</strong>) car VibeSQL évite la synchronisation du redo log, undo log et doublewrite buffer de MySQL
bench-sysbench-srv-disc-perf-reads = Les opérations de lecture montrent des gains plus faibles mais constants (<strong>2-3x</strong>) grâce à des modèles d'accès colonnaires efficaces en cache et zéro E/S disque
bench-sysbench-srv-disc-roadmap-title = Feuille de Route Serveur
bench-sysbench-srv-disc-pooling = Réduire la surcharge d'établissement de connexion pour les scénarios à haut débit
bench-sysbench-srv-disc-caching = Mise en cache côté serveur des plans de requêtes entre connexions
bench-sysbench-srv-disc-extended = Support complet du protocole de requête étendu PostgreSQL pour les opérations par lots

# TPC-H Serveur spécifique
bench-tpch-server-name = TPC-H (Serveur)
bench-tpch-server-title = Benchmark Analytique TPC-H (Serveur)
bench-tpch-server-description = Les <strong>benchmarks serveur TPC-H</strong> comparent VibeSQL Server (protocole PostgreSQL) à MySQL pour les charges de travail de requêtes analytiques, mesurant les performances OLAP dans les déploiements client-serveur.
bench-tpch-server-ops-label = requêtes TPC-H
bench-tpch-server-note-intro = Les benchmarks serveur testent l'implémentation du <strong>protocole PostgreSQL</strong>, mesurant la latence de requête de bout en bout incluant la surcharge réseau.
bench-tpch-server-note-queries = Les requêtes testent les JOINs complexes, sous-requêtes et agrégations typiques des charges de travail de business intelligence.

# Discussion TPC-H Serveur
bench-tpch-srv-disc-protocol-title = Protocole PostgreSQL
bench-tpch-srv-disc-protocol = VibeSQL Server parle le protocole PostgreSQL, permettant l'utilisation de drivers et outils PostgreSQL standard. Ce benchmark mesure la latence de bout en bout complète incluant la surcharge du protocole.
bench-tpch-srv-disc-comparison-title = Comparaison avec MySQL
bench-tpch-srv-disc-comparison = La comparaison avec MySQL fournit une référence pour les bases de données client-serveur traditionnelles sur les charges de travail analytiques. Le moteur d'exécution colonnaire de VibeSQL offre des avantages pour les agrégations et jointures complexes.
bench-tpch-srv-disc-roadmap-title = Feuille de Route OLAP Serveur
bench-tpch-srv-disc-prepared = Réutiliser les plans de requêtes compilés entre connexions
bench-tpch-srv-disc-pooling = Gestion efficace des connexions pour les scénarios à haut débit
bench-tpch-srv-disc-scale = Test de jeux de données plus grands (SF 0.1, SF 1.0) pour validation à l'échelle production

# TPC-C Serveur spécifique
bench-tpcc-server-name = TPC-C (Serveur)
bench-tpcc-server-title = Benchmark OLTP TPC-C (Serveur)
bench-tpcc-server-description = Les <strong>benchmarks serveur TPC-C</strong> comparent VibeSQL Server (protocole PostgreSQL) à MySQL pour les charges de travail transactionnelles OLTP, mesurant le débit pour les déploiements de bases de données multi-clients.
bench-tpcc-server-ops-label = transactions TPC-C
bench-tpcc-server-note-intro = Les benchmarks serveur testent l'implémentation du <strong>protocole PostgreSQL</strong>, mesurant le débit transactionnel incluant la surcharge réseau.
bench-tpcc-server-note-results = Les résultats rapportent les transactions par seconde (TPS) pour le mix de transactions TPC-C standard.
bench-tpcc-mixed = Charge Mixte - Mix de transactions TPC-C standard (45% Nouvelle-Commande, 43% Paiement, 4% Statut-Commande, 4% Livraison, 4% Niveau-Stock)

# Discussion TPC-C Serveur
bench-tpcc-srv-disc-protocol-title = Protocole PostgreSQL
bench-tpcc-srv-disc-protocol = VibeSQL Server parle le protocole PostgreSQL, permettant l'utilisation de drivers et outils PostgreSQL standard. Ce benchmark mesure la latence transactionnelle de bout en bout complète incluant la surcharge du protocole.
bench-tpcc-srv-disc-comparison-title = Comparaison avec MySQL
bench-tpcc-srv-disc-comparison = La comparaison avec MySQL fournit une référence pour les bases de données client-serveur traditionnelles sur les charges de travail OLTP. MySQL est le standard de l'industrie pour les charges de travail transactionnelles, et TPC-C est le point fort de MySQL.
bench-tpcc-srv-disc-roadmap-title = Feuille de Route OLTP Serveur
bench-tpcc-srv-disc-prepared = Réutiliser les plans de requêtes compilés entre connexions
bench-tpcc-srv-disc-pooling = Gestion efficace des connexions pour les scénarios à haut débit
bench-tpcc-srv-disc-parallel = Traitement concurrent de transactions multi-clients

# Empreinte Embarquée spécifique
bench-footprint-embedded-name = Empreinte (Embarqué)
bench-footprint-embedded-title = Empreinte du Binaire Natif
bench-footprint-embedded-description = Les <strong>benchmarks d'empreinte embarquée</strong> mesurent l'efficacité des ressources des binaires de base de données natifs, comparant la taille du binaire, le temps de démarrage à froid et l'utilisation mémoire maximale.
bench-footprint-embedded-ops-label = bases de données comparées
bench-footprint-embedded-note = L'empreinte du binaire natif est critique pour les <strong>déploiements embarqués et edge</strong> où la taille du binaire, la latence de démarrage et la consommation mémoire impactent directement la faisabilité du déploiement.

# Descriptions d'Empreinte Embarquée
bench-footprint-binary-size = Taille du Binaire - Taille du binaire de base de données compilé sur disque
bench-footprint-startup-time = Temps de Démarrage - Temps pour démarrer à froid et exécuter la première requête
bench-footprint-peak-memory = Mémoire Maximale - Taille maximale de l'ensemble résident pendant l'initialisation

# Discussion Empreinte Embarquée
bench-footprint-emb-disc-size-title = Taille du Binaire : Juste Milieu
bench-footprint-emb-disc-size = VibeSQL à <strong>~17 Mo</strong> se situe entre SQLite (~5 Mo) et DuckDB (~45 Mo). Cela reflète notre choix d'inclure des fonctionnalités avancées (fonctions de fenêtrage, CTEs, exécution colonnaire) tout en gardant le binaire gérable pour les déploiements embarqués.
bench-footprint-emb-disc-startup-title = Démarrage : Le Plus Rapide à Froid
bench-footprint-emb-disc-startup = VibeSQL atteint <strong>~7,7ms de démarrage à froid</strong>, légèrement plus rapide que SQLite (~8,2ms) et significativement plus rapide que DuckDB (~14,6ms). Notre chemin d'initialisation minimal ne charge que les structures de métadonnées essentielles au démarrage.
bench-footprint-emb-disc-memory-title = Efficacité Mémoire
bench-footprint-emb-disc-memory = La mémoire maximale au démarrage est ~7 Mo pour VibeSQL vs ~3 Mo pour SQLite et ~11 Mo pour DuckDB. La différence avec SQLite reflète notre optimiseur de requêtes plus sophistiqué et notre infrastructure d'exécution colonnaire allouée dès le départ.
bench-footprint-emb-disc-roadmap-title = Feuille de Route de Réduction de Taille
bench-footprint-emb-disc-flags = Sélection de fonctionnalités à la compilation pour exclure les fonctionnalités non utilisées
bench-footprint-emb-disc-lto = Optimisation au temps de liaison du programme complet pour l'élimination du code mort
bench-footprint-emb-disc-modular = Séparer le moteur central des fonctionnalités optionnelles (ex. fonctions de fenêtrage)

# Empreinte Serveur/WASM spécifique
bench-footprint-server-name = Empreinte (Serveur/WASM)
bench-footprint-server-title = Empreinte WASM
bench-footprint-server-description = Les <strong>benchmarks d'empreinte WASM</strong> mesurent la taille du module WebAssembly pour le déploiement navigateur, critique pour les applications web où la taille de téléchargement impacte l'expérience utilisateur.
bench-footprint-server-ops-label = cibles de déploiement
bench-footprint-server-note = Les tailles WASM sont critiques pour les <strong>déploiements web</strong> où le temps de téléchargement impacte directement le temps avant interactivité. Les tailles gzip sont les plus pertinentes car les navigateurs décompressent automatiquement le contenu gzip.
bench-footprint-server-note2 = <strong>Note :</strong> Le WASM VibeSQL est conçu pour une taille de téléchargement minimale tout en maintenant la conformité complète SQL:1999 dans le navigateur.

# Descriptions d'Empreinte Serveur
bench-footprint-wasm-size = Taille WASM - Taille du module WebAssembly pour déploiement navigateur
bench-footprint-wasm-gzip = WASM (gzip) - Taille compressée pour livraison web

# Discussion Empreinte Serveur
bench-footprint-srv-disc-wasm-title = WASM : 2,2 Mo Compressé
bench-footprint-srv-disc-wasm = Le module WebAssembly de VibeSQL se compresse à <strong>~2,2 Mo gzippé</strong>, permettant des chargements de page initiaux rapides. C'est une base de données SQL:1999 complète avec fonctions de fenêtrage, CTEs et transactions ACID s'exécutant entièrement dans le navigateur.
bench-footprint-srv-disc-included-title = Ce qui est Inclus
bench-footprint-srv-disc-parser = Analyseur SQL complet et optimiseur de requêtes
bench-footprint-srv-disc-btree = Moteur de stockage B-tree avec MVCC
bench-footprint-srv-disc-window = Fonctions de fenêtrage et agrégations avancées
bench-footprint-srv-disc-cte = Expressions de table communes (clause WITH)
bench-footprint-srv-disc-acid = Support complet des transactions ACID
bench-footprint-srv-disc-benefits-title = Avantages du Déploiement Navigateur
bench-footprint-srv-disc-benefits = Exécuter SQL dans le navigateur élimine la latence aller-retour vers les serveurs, permet les applications offline-first et garde les données sensibles sur l'appareil de l'utilisateur. La version WASM de VibeSQL est conçue pour ce cas d'usage avec des dépendances minimales et une utilisation mémoire efficace.
bench-footprint-srv-disc-roadmap-title = Feuille de Route WASM
bench-footprint-srv-disc-streaming = Commencer l'exécution pendant le téléchargement du module
bench-footprint-srv-disc-indexeddb = Stockage durable entre sessions navigateur
bench-footprint-srv-disc-worker = Exécuter les requêtes hors du thread principal pour des UIs réactives

# Étiquettes de points (utilisées avec descriptions)
bench-bullet-join-ordering = Ordonnancement des jointures
bench-bullet-hash-sizing = Dimensionnement des tables de hachage
bench-bullet-vectorized = Jointures vectorisées
bench-bullet-inl-joins = Jointures index-nested-loop
bench-bullet-cte-materialization = Matérialisation CTE
bench-bullet-decorrelation = Décorrélation des sous-requêtes
bench-bullet-star-optimization = Optimisation schéma en étoile
bench-bullet-lock-free = Lectures sans verrou
bench-bullet-optimistic = Concurrence optimiste
bench-bullet-btree = B-tree en mémoire
bench-bullet-prepared = Cache de requêtes préparées
bench-bullet-bulk-inserts = Insertions en masse
bench-bullet-non-indexed = Mises à jour non indexées
bench-bullet-deletes = Suppressions
bench-bullet-connection-pooling = Pool de connexions
bench-bullet-stmt-caching = Cache de requêtes préparées
bench-bullet-extended-protocol = Protocole de requête étendu
bench-bullet-concurrency = Concurrence légère
bench-bullet-protocol = Efficacité du protocole
bench-bullet-writes = Opérations d'écriture
bench-bullet-reads = Opérations de lecture
bench-bullet-feature-flags = Drapeaux de fonctionnalités
bench-bullet-lto = Optimisation LTO
bench-bullet-modular = Builds modulaires
bench-bullet-streaming = Compilation streaming
bench-bullet-indexeddb = Persistance IndexedDB
bench-bullet-worker = Support des worker threads
bench-bullet-prepared-stmts = Requêtes préparées
bench-bullet-larger-scale = Facteurs d'échelle plus grands
bench-bullet-parallel-clients = Clients parallèles

# =============================================================================
# Page de Conformité
# =============================================================================

# Section de vue d'ensemble
conformance-sql-conformance = Conformité SQL
conformance-testing-against = Tests contre SQLLogicTest - la suite de tests SQL standard de l'industrie
conformance-full-pass-rate = Taux de Réussite 100% Atteint !
conformance-tests-passing = Tests Réussis
conformance-files-passing = Fichiers Réussis
conformance-loading = Chargement du rapport de conformité...
conformance-error-loading = Erreur de Chargement du Rapport
conformance-no-data = Aucune donnée de conformité disponible

# Répartition par catégorie
conformance-category-title = Couverture des Tests par Catégorie
conformance-category-header = Catégorie
conformance-pass-rate-header = Taux de Réussite
conformance-progress-header = Progression
conformance-tests-header = Tests
conformance-cat-select = Requêtes SELECT
conformance-cat-aggregates = Agrégats
conformance-cat-joins = JOINs
conformance-cat-expressions = Expressions
conformance-cat-subqueries = Sous-requêtes
conformance-cat-index = Opérations d'Index
conformance-cat-ddl = Instructions DDL
conformance-cat-evidence = Tests de Preuve
conformance-cat-random = Tests Aléatoires
conformance-cat-other = Autres Tests

# Chronologie
conformance-timeline-title = Historique du Taux de Réussite
conformance-timeline-desc = Progression de la conformité sur les 90 derniers jours
conformance-timeline-loading = Chargement des données du graphique...

# Jalons
conformance-milestones-title = Jalons

# Exécution locale des tests
conformance-running-locally-title = Exécuter les Tests Localement
conformance-run-sqltest = # Exécuter les tests de conformité SQL:1999
conformance-run-sqllogictest = # Exécuter la suite SQLLogicTest (prend des heures)
conformance-generate-coverage = # Générer le rapport de couverture
conformance-open-coverage = # Ouvrir le rapport de couverture

# Section sqltest héritée
conformance-sqltest-title = Résultats sqltest
conformance-sqltest-desc = Résultats de <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a> - une suite de tests de conformité BNF maintenue par la communauté dérivée du standard SQL:1999, contenant 739 tests couvrant les fonctionnalités Core et Foundation.
conformance-overall-pass-rate = Taux de Réussite Global
conformance-tests-of-passing = { $passed } sur { $total } tests réussis
conformance-passed = Réussi
conformance-failed = Échoué
conformance-errors = Erreurs
conformance-test-coverage = Couverture des Tests
conformance-core-features = Fonctionnalités Core (Série E)
conformance-additional-features = Fonctionnalités Additionnelles

# Codes de fonctionnalités
conformance-e011 = Types de données numériques
conformance-e021 = Types de chaînes de caractères
conformance-e031 = Identifiants
conformance-e051 = Spécification de requête de base
conformance-e061 = Prédicats et conditions de recherche de base
conformance-e071 = Expressions de requête de base
conformance-e081 = Privilèges de base
conformance-e091 = Fonctions d'ensemble
conformance-e101 = Manipulation de données de base
conformance-e111 = Instruction SELECT mono-ligne
conformance-e121 = Support de curseur de base
conformance-e131 = Support des valeurs NULL
conformance-e141 = Contraintes d'intégrité de base
conformance-e151 = Support des transactions
conformance-e161 = Commentaires SQL
conformance-f031 = Manipulation de schéma de base

# Section SQLLogicTest
conformance-slt-title = Résultats SQLLogicTest
conformance-slt-desc = Résultats de la suite complète <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a> contenant ~5,9 millions de tests répartis sur 623 fichiers de test du corpus officiel SQLite.
conformance-files-of-passing = { $passed } sur { $total } fichiers de test réussis
conformance-test-categories = Catégories de Tests
conformance-slt-select = Tests SELECT
conformance-slt-evidence = Tests de Preuve
conformance-slt-index = Tests d'Index
conformance-slt-random = Tests Aléatoires
conformance-slt-ddl = Tests DDL
conformance-slt-other = Autres Tests
conformance-slt-note = <strong>Note :</strong> SQLLogicTest offre une perspective différente de sqltest. Alors que sqltest se concentre sur la conformité grammaticale BNF de la spécification SQL:1999, SQLLogicTest contient des millions de requêtes SQL réelles testant la correction pratique dans un large éventail de scénarios.

# Section d'explication
conformance-explanation-title = Comprendre Nos Suites de Tests
conformance-what-is-sqltest = Qu'est-ce que sqltest ?
conformance-sqltest-explanation = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a> est une suite de tests maintenue par la communauté par Elliot Chance qui fournit des tests de conformité BNF dérivés du standard SQL:1999. Elle contient 739 tests couvrant les fonctionnalités Core et Foundation dans les catégories de tests séries E et F. Cette suite teste si notre implémentation est conforme à la spécification grammaticale SQL:1999.
conformance-what-is-slt = Qu'est-ce que SQLLogicTest ?
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> est une suite de tests complète développée à l'origine pour SQLite, contenant ~5,9 millions de cas de test SQL répartis sur 623 fichiers de test. Elle teste la correction pratique en exécutant des requêtes réelles et en validant les résultats. Cette suite se concentre sur la correction sémantique et les cas limites plutôt que sur la pure conformité grammaticale.
conformance-how-complement = Comment se complètent-elles ?
conformance-sqltest-validates = <span class="font-medium">sqltest (BNF) :</span> Valide la conformité grammaticale aux spécifications du standard SQL:1999
conformance-slt-validates = <span class="font-medium">SQLLogicTest (Résultats) :</span> Valide la correction sémantique avec des millions de requêtes réelles
conformance-coverage-point = <span class="font-medium">Couverture :</span> sqltest couvre 739 tests de fonctionnalités standard ; SQLLogicTest couvre des scénarios pratiques
conformance-philosophy-point = <span class="font-medium">Philosophie :</span> sqltest demande « pouvez-vous parser ceci ? » ; SQLLogicTest demande « cela fonctionne-t-il correctement ? »
conformance-what-is-core = Qu'est-ce que SQL:1999 Core ?
conformance-core-explanation = SQL:1999 Core est l'ensemble de fonctionnalités obligatoires officiel défini dans le standard SQL:1999 (ISO/IEC 9075:1999). Il comprend environ 169 fonctionnalités requises que toute base de données revendiquant la conformité Core doit implémenter. La conformité Core officielle est vérifiée par la Suite de Tests SQL NIST, pas par des suites de tests communautaires.
conformance-what-mean = Que signifient nos taux de réussite ?
conformance-pass-rates-mean = Notre <strong>taux de réussite sqltest de { $sqltestRate }%</strong> ({ $sqltestPassed }/{ $sqltestTotal } tests) démontre une forte conformité grammaticale SQL:1999. { $sltInfo } Ensemble, ces résultats indiquent une conformité SQL:1999 complète, bien qu'ils ne constituent pas une certification Core officielle.
conformance-slt-pass-info = Notre <strong>taux de réussite SQLLogicTest de { $sltRate }%</strong> ({ $sltPassed }/{ $sltTotal } fichiers de test) montre que nous traitons correctement les requêtes réelles.
conformance-bottom-line = <strong>Conclusion :</strong> Nous utilisons deux suites de tests complémentaires pour assurer à la fois la conformité aux standards (sqltest) et la correction pratique (SQLLogicTest). Des taux de réussite élevés dans les deux démontrent une qualité d'implémentation SQL:1999 sérieuse, bien que la certification Core formelle nécessiterait des tests contre les suites officielles NIST.

# Section des tests échouant
conformance-failing-tests-title = Tests Échouant
conformance-failing-tests-desc = Les tests suivants échouent actuellement. Cliquez pour développer les détails.
conformance-view-failing = Voir les détails des tests échouant ({ $count } tests)
conformance-error-label = Erreur :

# Tests de Régression PostgreSQL
conformance-pgsql-title = Tests de Régression PostgreSQL
conformance-pgsql-desc = Résultats de l'exécution de la <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suite de tests de régression PostgreSQL</a> - la suite de tests canonique utilisée pour valider la compatibilité PostgreSQL.
conformance-pgsql-tests-passing = tests réussis
conformance-pgsql-tests-excluded = tests exclus
conformance-pgsql-pass-rate = Taux de Réussite
conformance-pgsql-excluded-reason = Les tests exclus utilisent des fonctionnalités spécifiques à PostgreSQL non applicables à VibeSQL
conformance-pgsql-note = <strong>Note :</strong> Les tests de régression PostgreSQL valident le comportement SQL par rapport à l'implémentation de référence PostgreSQL. Les tests exclus concernent des fonctionnalités spécifiques à PostgreSQL comme les catalogues système, les langages procéduraux ou les modules d'extension.

# Section Suite de Tests TCL SQLite
conformance-tcl-title = Suite de Tests TCL SQLite
conformance-tcl-desc = Résultats de la <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suite de tests TCL</a> canonique de SQLite contenant { $fileCount } fichiers de test. Cette suite est la référence absolue pour les tests de compatibilité SQLite.
conformance-tcl-overall-rate = Taux de Réussite Global
conformance-tcl-tests-passing = { $passed } sur { $total } tests réussis
conformance-tcl-passed = Réussis
conformance-tcl-failed = Échoués
conformance-tcl-skipped = Ignorés
conformance-tcl-total = Total
conformance-tcl-categories-title = Catégories de Tests
conformance-tcl-category = Catégorie
conformance-tcl-rate = Taux
conformance-tcl-progress = Progression
conformance-tcl-tests = Tests
conformance-tcl-common-failures = Échecs Courants
conformance-tcl-failure-patterns = Top { $count } motifs d'échec par nombre d'occurrences
conformance-tcl-about-title = À propos des Tests TCL :
conformance-tcl-about-text = La suite de tests TCL de SQLite est le test de conformité canonique pour la compatibilité SQLite. Elle teste les comportements spécifiques de SQLite, les particularités et les cas limites qui peuvent ne pas être couverts par les suites de tests SQL standard. Des taux de réussite élevés indiquent une forte compatibilité SQLite pour les scénarios de migration d'applications.

# Métadonnées
conformance-generated = Généré :
conformance-commit = Commit :
conformance-status = Statut :

# =============================================================================
# Page d'Accueil
# =============================================================================

home-page-title = VibeSQL — Une Base de Données SQL Pure Rust Conçue pour la Vitesse

# Section héros
home-hero-title = Une base de données SQL pure Rust<br>conçue pour la vitesse
home-hero-subtitle = VibeSQL échange l'efficacité de stockage contre la performance. Stockage hybride lignes + colonnes, exécution vectorisée et zéro code unsafe. Optimisée pour les jeux de données tenant en mémoire.
home-hero-subtext = Conforme SQL:1999. S'exécute nativement, en WebAssembly et comme bibliothèque embarquée.
home-btn-demo = Essayer dans le Navigateur
home-btn-github = GitHub
home-btn-crates = crates.io

# Section pourquoi VibeSQL
home-why-title = Pourquoi VibeSQL ?
home-hybrid-title = Stockage Hybride
home-hybrid-text = Stockage en lignes et en colonnes dans un seul moteur. Format lignes pour les recherches ponctuelles rapides et l'OLTP. Format colonnes pour les scans analytiques avec exécution vectorisée. Pas besoin de choisir.
home-speed-title = Vitesse Avant Stockage
home-speed-text = Échange délibérément l'espace disque contre la performance des requêtes. Layouts de stockage redondants, mise en cache agressive et index précalculés font que les bases de données plus petites s'exécutent aussi vite que possible.
home-rust-title = Rust Pur, Zéro Unsafe
home-rust-text = Écrit entièrement en Rust sûr. Aucune dépendance C, pas de FFI, pas de blocs unsafe. Se compile en binaires natifs et WebAssembly depuis le même code source.

# Section architecture
home-arch-title = Architecture
home-pipeline-title = Pipeline de Requêtes
home-pipeline-parser = <strong>Parser</strong> — Grammaire SQL:1999 complète, AST alloué en arena
home-pipeline-planner = <strong>Planificateur</strong> — Optimiseur basé sur les coûts avec réordonnancement des jointures
home-pipeline-executor = <strong>Exécuteur</strong> — Exécution vectorisée avec traitement par lots
home-pipeline-storage = <strong>Stockage</strong> — Hybride lignes/colonnes avec index B-tree
home-features-title = Fonctionnalités Clés
home-feature-window = Fonctions de fenêtrage (ROW_NUMBER, RANK, LEAD/LAG, NTILE, ...)
home-feature-cte = Expressions de table communes (WITH, CTEs récursifs)
home-feature-subquery = Sous-requêtes (corrélées, EXISTS, IN, scalaires)
home-feature-join = Support complet des JOIN (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL)
home-feature-triggers = Triggers, vues, clés étrangères, contraintes CHECK
home-feature-wasm = Cible WASM avec stockage persistant OPFS

# Section performance
home-perf-title = Performance
home-perf-full = Benchmarks complets →
home-stat-tpch-label = Requêtes TPC-H Réussies
home-stat-tpch-sub = Benchmark d'aide à la décision
home-stat-conformance-label = Taux de Réussite SQLLogicTest
home-stat-conformance-sub = Plus de 6M d'assertions de test
home-stat-tpcds-label = Requêtes TPC-DS Réussies
home-stat-tpcds-sub = Benchmark d'analytique complexe
home-perf-note = Comparé à SQLite, DuckDB et MySQL sur des charges de travail équivalentes. <a href="benchmarks.html" class="text-blue-600 dark:text-blue-400 hover:underline">Voir les résultats complets.</a>

# Section démarrer
home-start-title = Démarrer
home-demo-title = Démo Interactive
home-demo-text = Exécutez des requêtes SQL dans votre navigateur. Moteur de base de données complet compilé en WebAssembly avec stockage persistant via OPFS. Aucune installation requise.
home-install-title = Installer
home-install-cargo = Cargo
home-install-library = En tant que bibliothèque

# Section explorer
home-explore-conformance-title = Rapport de Conformité
home-explore-conformance-text = Analyse détaillée de la conformité aux standards SQL:1999 sur 622 fichiers de test.
home-explore-bench-title = Benchmarks de Performance
home-explore-bench-text = Résultats TPC-H, TPC-DS, TPC-C et Sysbench face à SQLite, DuckDB et MySQL.

# Pied de page
home-footer = VibeSQL — Une base de données SQL pure Rust conçue pour la vitesse

