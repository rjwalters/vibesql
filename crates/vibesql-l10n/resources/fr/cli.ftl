# VibeSQL CLI Localisation - Français
# Ce fichier contient toutes les chaînes visibles par l'utilisateur pour l'interface en ligne de commande.

# =============================================================================
# Bannière du REPL et Messages de Base
# =============================================================================

cli-banner = VibeSQL v{ $version } - Base de Données Conforme SQL:1999 COMPLÈTE
cli-help-hint = Tapez \help pour l'aide, \quit pour quitter
cli-goodbye = Au revoir !
locale-changed = Langue changée en { $locale }

# =============================================================================
# Texte d'Aide des Commandes (Arguments Clap)
# =============================================================================

cli-about = VibeSQL - Base de Données Conforme SQL:1999 COMPLÈTE

cli-long-about = Interface en ligne de commande VibeSQL

    MODES D'UTILISATION :
      REPL Interactif :      vibesql (--database <FICHIER>)
      Exécuter Commande :    vibesql -c "SELECT * FROM utilisateurs"
      Exécuter Fichier :     vibesql -f script.sql
      Exécuter depuis stdin : cat donnees.sql | vibesql
      Générer Types :        vibesql codegen --schema schema.sql --output types.ts

    REPL INTERACTIF :
      Lorsqu'il est lancé sans -c, -f ou entrée par tube, VibeSQL entre dans un REPL
      interactif avec support readline, historique des commandes et méta-commandes comme :
        \d (table)   - Décrire une table ou lister toutes les tables
        \dt          - Lister les tables
        \f <format>  - Définir le format de sortie
        \copy        - Importer/exporter CSV/JSON
        \help        - Afficher toutes les commandes du REPL

    SOUS-COMMANDES :
      codegen           Générer des types TypeScript depuis le schéma de base de données

    CONFIGURATION :
      Les paramètres peuvent être configurés dans ~/.vibesqlrc (format TOML).
      Sections : display, database, history, query

    EXEMPLES :
      # Démarrer le REPL interactif avec base de données en mémoire
      vibesql

      # Utiliser un fichier de base de données persistant
      vibesql --database mesdonnees.db

      # Exécuter une seule commande
      vibesql -c "CREATE TABLE utilisateurs (id INT, nom VARCHAR(100))"

      # Exécuter un fichier de script SQL
      vibesql -f schema.sql -v

      # Importer des données depuis CSV
      echo "\copy utilisateurs FROM 'donnees.csv'" | vibesql --database mesdonnees.db

      # Exporter les résultats de requête en JSON
      vibesql -d mesdonnees.db -c "SELECT * FROM utilisateurs" --format json

      # Générer des types TypeScript depuis un fichier de schéma
      vibesql codegen --schema schema.sql --output src/types.ts

      # Générer des types TypeScript depuis une base de données en cours d'exécution
      vibesql codegen --database mesdonnees.db --output src/types.ts

# Chaînes d'aide des arguments
arg-database-help = Chemin du fichier de base de données (si non spécifié, utilise une base de données en mémoire)
arg-file-help = Exécuter les commandes SQL depuis un fichier
arg-command-help = Exécuter une commande SQL directement et quitter
arg-stdin-help = Lire les commandes SQL depuis stdin (détecté automatiquement lors d'un tube)
arg-verbose-help = Afficher une sortie détaillée pendant l'exécution du fichier/stdin
arg-format-help = Format de sortie pour les résultats de requête
arg-lang-help = Définir la langue d'affichage (ex. : en-US, es, fr, ja)

# =============================================================================
# Sous-commande Codegen
# =============================================================================

codegen-about = Générer des types TypeScript depuis le schéma de base de données

codegen-long-about = Générer des définitions de types TypeScript depuis un schéma de base de données VibeSQL.

    Cette commande crée des interfaces TypeScript pour toutes les tables de la base de données,
    ainsi que des objets de métadonnées pour la vérification de types à l'exécution et le support IDE.

    SOURCES D'ENTRÉE :
      --database <FICHIER>  Générer depuis un fichier de base de données existant
      --schema <FICHIER>    Générer depuis un fichier de schéma SQL (instructions CREATE TABLE)

    SORTIE :
      --output <FICHIER>    Écrire les types générés dans ce fichier (par défaut : types.ts)

    OPTIONS :
      --camel-case          Convertir les noms de colonnes en camelCase
      --no-metadata         Ne pas générer l'objet de métadonnées des tables

    EXEMPLES :
      # Depuis un fichier de base de données
      vibesql codegen --database mesdonnees.db --output src/db/types.ts

      # Depuis un fichier de schéma SQL
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Avec noms de propriétés en camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = Fichier de schéma SQL contenant les instructions CREATE TABLE
codegen-output-help = Chemin du fichier de sortie pour le TypeScript généré
codegen-camel-case-help = Convertir les noms de colonnes en camelCase
codegen-no-metadata-help = Ne pas générer l'objet de métadonnées des tables

codegen-from-schema = Génération des types TypeScript depuis le fichier de schéma : { $path }
codegen-from-database = Génération des types TypeScript depuis la base de données : { $path }
codegen-written = Types TypeScript écrits dans : { $path }
codegen-error-no-source = --database ou --schema doit être spécifié.
    Utilisez 'vibesql codegen --help' pour les informations d'utilisation.

# =============================================================================
# Aide des Méta-commandes (sortie de \help)
# =============================================================================

help-title = Méta-commandes :
help-describe = \d (table)      - Décrire une table ou lister toutes les tables
help-tables = \dt             - Lister les tables
help-schemas = \ds             - Lister les schémas
help-indexes = \di             - Lister les index
help-roles = \du             - Lister les rôles/utilisateurs
help-format = \f <format>    - Définir le format de sortie (table, json, csv, markdown, html)
help-timing = \timing         - Activer/désactiver le chronométrage des requêtes
help-copy-to = \copy <table> TO <fichier>   - Exporter une table vers un fichier CSV/JSON
help-copy-from = \copy <table> FROM <fichier> - Importer un fichier CSV dans une table
help-save = \save (fichier) - Sauvegarder la base de données dans un fichier de vidage SQL
help-errors = \errors         - Afficher l'historique des erreurs récentes
help-help = \h, \help      - Afficher cette aide
help-quit = \q, \quit      - Quitter

help-sql-title = Introspection SQL :
help-show-tables = SHOW TABLES                  - Lister toutes les tables
help-show-databases = SHOW DATABASES               - Lister tous les schémas/bases de données
help-show-columns = SHOW COLUMNS FROM <table>    - Afficher les colonnes d'une table
help-show-index = SHOW INDEX FROM <table>      - Afficher les index d'une table
help-show-create = SHOW CREATE TABLE <table>    - Afficher l'instruction CREATE TABLE
help-describe-sql = DESCRIBE <table>             - Alias pour SHOW COLUMNS

help-examples-title = Exemples :
help-example-create = CREATE TABLE utilisateurs (id INT PRIMARY KEY, nom VARCHAR(100));
help-example-insert = INSERT INTO utilisateurs VALUES (1, 'Alice'), (2, 'Bob');
help-example-select = SELECT * FROM utilisateurs;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM utilisateurs;
help-example-describe = DESCRIBE utilisateurs;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy utilisateurs TO '/tmp/utilisateurs.csv'
help-example-copy-from = \copy utilisateurs FROM '/tmp/utilisateurs.csv'
help-example-copy-json = \copy utilisateurs TO '/tmp/utilisateurs.json'
help-example-errors = \errors

# =============================================================================
# Messages d'État
# =============================================================================

format-changed = Format de sortie défini à : { $format }
database-saved = Base de données sauvegardée dans : { $path }
no-database-file = Erreur : Aucun fichier de base de données spécifié. Utilisez \save <nom_fichier> ou démarrez avec l'option --database

# =============================================================================
# Affichage des Erreurs
# =============================================================================

no-errors = Aucune erreur dans cette session.
recent-errors = Erreurs récentes :

# =============================================================================
# Messages d'Exécution de Script
# =============================================================================

script-no-statements = Aucune instruction SQL trouvée dans le script
script-executing = Exécution de l'instruction { $current } sur { $total }...
script-error = Erreur lors de l'exécution de l'instruction { $index } : { $error }
script-summary-title = === Résumé de l'Exécution du Script ===
script-total = Total des instructions : { $count }
script-successful = Réussies : { $count }
script-failed = Échouées : { $count }
script-failed-error = { $count } instructions ont échoué

# =============================================================================
# Formatage de Sortie
# =============================================================================

rows-with-time = { $count } lignes dans l'ensemble ({ $time }s)
rows-count = { $count } lignes

# =============================================================================
# Avertissements
# =============================================================================

warning-config-load = Avertissement : Impossible de charger le fichier de configuration : { $error }
warning-auto-save-failed = Avertissement : Échec de la sauvegarde automatique de la base de données : { $error }
warning-save-on-exit-failed = Avertissement : Échec de la sauvegarde de la base de données à la fermeture : { $error }

# =============================================================================
# Opérations sur les Fichiers
# =============================================================================

file-read-error = Échec de la lecture du fichier '{ $path }' : { $error }
stdin-read-error = Échec de la lecture depuis stdin : { $error }
database-load-error = Échec du chargement de la base de données : { $error }
