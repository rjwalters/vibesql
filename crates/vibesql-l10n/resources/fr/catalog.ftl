# VibeSQL Messages d'Erreur du Catalogue - Français
# Ce fichier contient tous les messages d'erreur pour le crate vibesql-catalog.

# =============================================================================
# Erreurs de Table
# =============================================================================

catalog-table-already-exists = La table '{ $name }' existe déjà
catalog-table-not-found = Table '{ $table_name }' introuvable

# =============================================================================
# Erreurs de Colonne
# =============================================================================

catalog-column-already-exists = La colonne '{ $name }' existe déjà
catalog-column-not-found = Colonne '{ $column_name }' introuvable dans la table '{ $table_name }'

# =============================================================================
# Erreurs de Schéma
# =============================================================================

catalog-schema-already-exists = Le schéma '{ $name }' existe déjà
catalog-schema-not-found = Schéma '{ $name }' introuvable
catalog-schema-not-empty = Le schéma '{ $name }' n'est pas vide

# =============================================================================
# Erreurs de Rôle
# =============================================================================

catalog-role-already-exists = Le rôle '{ $name }' existe déjà
catalog-role-not-found = Rôle '{ $name }' introuvable

# =============================================================================
# Erreurs de Domaine
# =============================================================================

catalog-domain-already-exists = Le domaine '{ $name }' existe déjà
catalog-domain-not-found = Domaine '{ $name }' introuvable
catalog-domain-in-use = Le domaine '{ $domain_name }' est encore utilisé par { $count } colonne(s) : { $columns }

# =============================================================================
# Erreurs de Séquence
# =============================================================================

catalog-sequence-already-exists = La séquence '{ $name }' existe déjà
catalog-sequence-not-found = Séquence '{ $name }' introuvable
catalog-sequence-in-use = La séquence '{ $sequence_name }' est encore utilisée par { $count } colonne(s) : { $columns }

# =============================================================================
# Erreurs de Type
# =============================================================================

catalog-type-already-exists = Le type '{ $name }' existe déjà
catalog-type-not-found = Type '{ $name }' introuvable
catalog-type-in-use = Le type '{ $name }' est encore utilisé par une ou plusieurs tables

# =============================================================================
# Erreurs de Collation et de Jeu de Caractères
# =============================================================================

catalog-collation-already-exists = La collation '{ $name }' existe déjà
catalog-collation-not-found = Collation '{ $name }' introuvable
catalog-character-set-already-exists = Le jeu de caractères '{ $name }' existe déjà
catalog-character-set-not-found = Jeu de caractères '{ $name }' introuvable
catalog-translation-already-exists = La traduction '{ $name }' existe déjà
catalog-translation-not-found = Traduction '{ $name }' introuvable

# =============================================================================
# Erreurs de Vue
# =============================================================================

catalog-view-already-exists = La vue '{ $name }' existe déjà
catalog-view-not-found = Vue '{ $name }' introuvable
catalog-view-in-use = La vue ou table '{ $view_name }' est encore utilisée par { $count } vue(s) : { $views }

# =============================================================================
# Erreurs de Déclencheur
# =============================================================================

catalog-trigger-already-exists = Le déclencheur '{ $name }' existe déjà
catalog-trigger-not-found = Déclencheur '{ $name }' introuvable

# =============================================================================
# Erreurs d'Assertion
# =============================================================================

catalog-assertion-already-exists = L'assertion '{ $name }' existe déjà
catalog-assertion-not-found = Assertion '{ $name }' introuvable

# =============================================================================
# Erreurs de Fonction et de Procédure
# =============================================================================

catalog-function-already-exists = La fonction '{ $name }' existe déjà
catalog-function-not-found = Fonction '{ $name }' introuvable
catalog-procedure-already-exists = La procédure '{ $name }' existe déjà
catalog-procedure-not-found = Procédure '{ $name }' introuvable

# =============================================================================
# Erreurs de Contrainte
# =============================================================================

catalog-constraint-already-exists = La contrainte '{ $name }' existe déjà
catalog-constraint-not-found = Contrainte '{ $name }' introuvable

# =============================================================================
# Erreurs d'Index
# =============================================================================

catalog-index-already-exists = L'index '{ $index_name }' sur la table '{ $table_name }' existe déjà
catalog-index-not-found = Index '{ $index_name }' sur la table '{ $table_name }' introuvable

# =============================================================================
# Erreurs de Clé Étrangère
# =============================================================================

catalog-circular-foreign-key = Dépendance circulaire de clé étrangère détectée pour la table '{ $table_name }' : { $message }
