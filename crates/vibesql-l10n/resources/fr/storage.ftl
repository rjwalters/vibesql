# VibeSQL Messages d'Erreur de Stockage - Français
# Ce fichier contient tous les messages d'erreur pour le crate vibesql-storage.

# =============================================================================
# Erreurs de Table
# =============================================================================

storage-table-not-found = Table '{ $name }' introuvable

# =============================================================================
# Erreurs de Colonne
# =============================================================================

storage-column-count-mismatch = Incompatibilité du nombre de colonnes : { $expected } attendues, { $actual } reçues
storage-column-index-out-of-bounds = Index de colonne { $index } hors limites
storage-column-not-found = Colonne '{ $column_name }' introuvable dans la table '{ $table_name }'

# =============================================================================
# Erreurs d'Index
# =============================================================================

storage-index-already-exists = L'index '{ $name }' existe déjà
storage-index-not-found = Index '{ $name }' introuvable
storage-invalid-index-column = { $message }

# =============================================================================
# Erreurs de Contrainte
# =============================================================================

storage-null-constraint-violation = Violation de contrainte NOT NULL : la colonne '{ $column }' ne peut pas être NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Erreurs de Type
# =============================================================================

storage-type-mismatch = Incompatibilité de type dans la colonne '{ $column }' : { $expected } attendu, { $actual } reçu

# =============================================================================
# Erreurs de Transaction et de Catalogue
# =============================================================================

storage-catalog-error = Erreur de catalogue : { $message }
storage-transaction-error = Erreur de transaction : { $message }
storage-row-not-found = Ligne introuvable

# =============================================================================
# Erreurs d'E/S et de Page
# =============================================================================

storage-io-error = Erreur d'E/S : { $message }
storage-invalid-page-size = Taille de page invalide : { $expected } attendue, { $actual } reçue
storage-invalid-page-id = ID de page invalide : { $page_id }
storage-lock-error = Erreur de verrou : { $message }

# =============================================================================
# Erreurs de Mémoire
# =============================================================================

storage-memory-budget-exceeded = Budget mémoire dépassé : { $used } octets utilisés, budget de { $budget } octets
storage-no-index-to-evict = Aucun index disponible à évincer (tous les index sont déjà sur disque)

# =============================================================================
# Erreurs Générales
# =============================================================================

storage-not-implemented = Non implémenté : { $message }
storage-other = { $message }
