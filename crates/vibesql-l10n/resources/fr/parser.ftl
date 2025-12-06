# VibeSQL Localisation Analyseur/Lexer - Français
# Ce fichier contient toutes les chaînes visibles par l'utilisateur pour les erreurs du lexer et de l'analyseur.

# =============================================================================
# En-tête d'Erreur du Lexer
# =============================================================================

lexer-error-at-position = Erreur du lexer à la position { $position } : { $message }

# =============================================================================
# Erreurs de Chaînes Littérales
# =============================================================================

lexer-unterminated-string = Chaîne littérale non terminée

# =============================================================================
# Erreurs d'Identificateurs
# =============================================================================

lexer-unterminated-delimited-identifier = Identificateur délimité non terminé
lexer-empty-delimited-identifier = Un identificateur délimité vide n'est pas autorisé

# =============================================================================
# Erreurs de Nombres Littéraux
# =============================================================================

lexer-invalid-scientific-notation = Notation scientifique invalide : chiffres attendus après 'E'

# =============================================================================
# Erreurs de Paramètres Positionnels
# =============================================================================

lexer-expected-digit-after-dollar = Chiffre attendu après '$' pour un paramètre positionnel numéroté
lexer-invalid-numbered-placeholder = Paramètre positionnel numéroté invalide : ${ $placeholder }
lexer-numbered-placeholder-zero = Le paramètre positionnel numéroté doit être $1 ou supérieur (pas de $0)
lexer-expected-identifier-after-colon = Identificateur attendu après ':' pour un paramètre positionnel nommé

# =============================================================================
# Erreurs de Variables
# =============================================================================

lexer-expected-variable-after-at-at = Nom de variable attendu après @@
lexer-expected-variable-after-at = Nom de variable attendu après @

# =============================================================================
# Erreurs d'Opérateurs
# =============================================================================

lexer-unexpected-pipe = Caractère inattendu : '|' (vouliez-vous dire '||' ?)

# =============================================================================
# Erreurs Générales
# =============================================================================

lexer-unexpected-character = Caractère inattendu : '{ $character }'
