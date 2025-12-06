# VibeSQL Messages d'Erreur de l'Exécuteur - Français
# Ce fichier contient tous les messages d'erreur pour le crate vibesql-executor.

# =============================================================================
# Erreurs de Table
# =============================================================================

executor-table-not-found = Table '{ $name }' introuvable
executor-table-already-exists = La table '{ $name }' existe déjà

# =============================================================================
# Erreurs de Colonne
# =============================================================================

executor-column-not-found-simple = Colonne '{ $column_name }' introuvable dans la table '{ $table_name }'
executor-column-not-found-searched = Colonne '{ $column_name }' introuvable (tables recherchées : { $searched_tables })
executor-column-not-found-with-available = Colonne '{ $column_name }' introuvable (tables recherchées : { $searched_tables }). Colonnes disponibles : { $available_columns }
executor-invalid-table-qualifier = Qualificateur de table invalide '{ $qualifier }' pour la colonne '{ $column }'. Tables disponibles : { $available_tables }
executor-column-already-exists = La colonne '{ $name }' existe déjà
executor-column-index-out-of-bounds = Index de colonne { $index } hors limites

# =============================================================================
# Erreurs d'Index
# =============================================================================

executor-index-not-found = Index '{ $name }' introuvable
executor-index-already-exists = L'index '{ $name }' existe déjà
executor-invalid-index-definition = Définition d'index invalide : { $message }

# =============================================================================
# Erreurs de Déclencheur
# =============================================================================

executor-trigger-not-found = Déclencheur '{ $name }' introuvable
executor-trigger-already-exists = Le déclencheur '{ $name }' existe déjà

# =============================================================================
# Erreurs de Schéma
# =============================================================================

executor-schema-not-found = Schéma '{ $name }' introuvable
executor-schema-already-exists = Le schéma '{ $name }' existe déjà
executor-schema-not-empty = Impossible de supprimer le schéma '{ $name }' : le schéma n'est pas vide

# =============================================================================
# Erreurs de Rôle et de Permission
# =============================================================================

executor-role-not-found = Rôle '{ $name }' introuvable
executor-permission-denied = Permission refusée : le rôle '{ $role }' n'a pas le privilège { $privilege } sur { $object }
executor-dependent-privileges-exist = Des privilèges dépendants existent : { $message }

# =============================================================================
# Erreurs de Type
# =============================================================================

executor-type-not-found = Type '{ $name }' introuvable
executor-type-already-exists = Le type '{ $name }' existe déjà
executor-type-in-use = Impossible de supprimer le type '{ $name }' : le type est encore utilisé
executor-type-mismatch = Incompatibilité de type : { $left } { $op } { $right }
executor-type-error = Erreur de type : { $message }
executor-cast-error = Impossible de convertir { $from_type } en { $to_type }
executor-type-conversion-error = Impossible de convertir { $from } en { $to }

# =============================================================================
# Erreurs d'Expression et de Requête
# =============================================================================

executor-division-by-zero = Division par zéro
executor-invalid-where-clause = Clause WHERE invalide : { $message }
executor-unsupported-expression = Expression non supportée : { $message }
executor-unsupported-feature = Fonctionnalité non supportée : { $message }
executor-parse-error = Erreur d'analyse : { $message }

# =============================================================================
# Erreurs de Sous-requête
# =============================================================================

executor-subquery-returned-multiple-rows = La sous-requête scalaire a retourné { $actual } lignes, { $expected } attendue
executor-subquery-column-count-mismatch = La sous-requête a retourné { $actual } colonnes, { $expected } attendues
executor-column-count-mismatch = La liste de colonnes dérivées a { $provided } colonnes mais la requête produit { $expected } colonnes

# =============================================================================
# Erreurs de Contrainte
# =============================================================================

executor-constraint-violation = Violation de contrainte : { $message }
executor-multiple-primary-keys = Les contraintes PRIMARY KEY multiples ne sont pas autorisées
executor-cannot-drop-column = Impossible de supprimer la colonne : { $message }
executor-constraint-not-found = Contrainte '{ $constraint_name }' introuvable dans la table '{ $table_name }'

# =============================================================================
# Erreurs de Limite de Ressources
# =============================================================================

executor-expression-depth-exceeded = Limite de profondeur d'expression dépassée : { $depth } > { $max_depth } (prévient le dépassement de pile)
executor-query-timeout-exceeded = Délai d'attente de la requête dépassé : { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Limite de traitement de lignes dépassée : { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Limite de mémoire dépassée : { $used_gb } Go > { $max_gb } Go

# =============================================================================
# Erreurs Procédurales/Variables
# =============================================================================

executor-variable-not-found-simple = Variable '{ $variable_name }' introuvable
executor-variable-not-found-with-available = Variable '{ $variable_name }' introuvable. Variables disponibles : { $available_variables }
executor-label-not-found = Étiquette '{ $name }' introuvable

# =============================================================================
# Erreurs SELECT INTO
# =============================================================================

executor-select-into-row-count = Le SELECT INTO procédural doit retourner exactement { $expected } ligne, { $actual } ligne{ $plural } obtenue(s)
executor-select-into-column-count = Incompatibilité du nombre de colonnes pour SELECT INTO procédural : { $expected } variable{ $expected_plural } mais la requête a retourné { $actual } colonne{ $actual_plural }

# =============================================================================
# Erreurs de Procédure et de Fonction
# =============================================================================

executor-procedure-not-found-simple = Procédure '{ $procedure_name }' introuvable dans le schéma '{ $schema_name }'
executor-procedure-not-found-with-available = Procédure '{ $procedure_name }' introuvable dans le schéma '{ $schema_name }'
    .available = Procédures disponibles : { $available_procedures }
executor-procedure-not-found-with-suggestion = Procédure '{ $procedure_name }' introuvable dans le schéma '{ $schema_name }'
    .available = Procédures disponibles : { $available_procedures }
    .suggestion = Vouliez-vous dire '{ $suggestion }' ?

executor-function-not-found-simple = Fonction '{ $function_name }' introuvable dans le schéma '{ $schema_name }'
executor-function-not-found-with-available = Fonction '{ $function_name }' introuvable dans le schéma '{ $schema_name }'
    .available = Fonctions disponibles : { $available_functions }
executor-function-not-found-with-suggestion = Fonction '{ $function_name }' introuvable dans le schéma '{ $schema_name }'
    .available = Fonctions disponibles : { $available_functions }
    .suggestion = Vouliez-vous dire '{ $suggestion }' ?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' attend { $expected } paramètre{ $expected_plural } ({ $parameter_signature }), { $actual } argument{ $actual_plural } reçu(s)
executor-parameter-type-mismatch = Le paramètre '{ $parameter_name }' attend { $expected_type }, reçu { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Incompatibilité du nombre d'arguments : { $expected } attendus, { $actual } reçus

executor-recursion-limit-exceeded = Profondeur de récursion maximale ({ $max_depth }) dépassée : { $message }
executor-recursion-call-stack = Pile d'appels :
executor-function-must-return = La fonction doit retourner une valeur
executor-invalid-control-flow = Flux de contrôle invalide : { $message }
executor-invalid-function-body = Corps de fonction invalide : { $message }
executor-function-read-only-violation = Violation de lecture seule de la fonction : { $message }

# =============================================================================
# Erreurs EXTRACT
# =============================================================================

executor-invalid-extract-field = Impossible d'extraire { $field } d'une valeur de type { $value_type }

# =============================================================================
# Erreurs Colonnes/Arrow
# =============================================================================

executor-arrow-downcast-error = Échec de la conversion descendante du tableau Arrow vers { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Types incompatibles pour { $operation } : { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Type incompatible pour { $operation } : { $left_type }
executor-simd-operation-failed = Opération SIMD { $operation } échouée : { $reason }
executor-columnar-column-not-found = Index de colonne { $column_index } hors limites (le lot a { $batch_columns } colonnes)
executor-columnar-column-not-found-by-name = Colonne introuvable : { $column_name }
executor-columnar-length-mismatch = Incompatibilité de longueur de colonne dans { $context } : { $expected } attendu, { $actual } obtenu
executor-unsupported-array-type = Type de tableau non supporté pour { $operation } : { $array_type }

# =============================================================================
# Erreurs Spatiales
# =============================================================================

executor-spatial-geometry-error = { $function_name } : { $message }
executor-spatial-operation-failed = { $function_name } : { $message }
executor-spatial-argument-error = { $function_name } attend { $expected }, reçu { $actual }

# =============================================================================
# Erreurs de Curseur
# =============================================================================

executor-cursor-already-exists = Le curseur '{ $name }' existe déjà
executor-cursor-not-found = Curseur '{ $name }' introuvable
executor-cursor-already-open = Le curseur '{ $name }' est déjà ouvert
executor-cursor-not-open = Le curseur '{ $name }' n'est pas ouvert
executor-cursor-not-scrollable = Le curseur '{ $name }' n'est pas défilable (SCROLL non spécifié)

# =============================================================================
# Erreurs de Stockage et Générales
# =============================================================================

executor-storage-error = Erreur de stockage : { $message }
executor-other = { $message }
