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
