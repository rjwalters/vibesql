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

# Editor
editor-placeholder = Entrez votre requête SQL ici... (Ctrl+Entrée ou Cmd+Entrée pour exécuter)

# Navigation links
nav-terminal = Terminal SQL Démo
nav-compliance = Rapport de Conformité SQL
nav-benchmarks = Benchmarks de Performance
nav-github = Dépôt GitHub
nav-home = Accueil

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
