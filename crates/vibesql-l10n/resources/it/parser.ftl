# VibeSQL Parser/Lexer Localization - Italian (it)
# This file contains all user-facing strings for lexer and parser errors.

# =============================================================================
# Lexer Error Header
# =============================================================================

lexer-error-at-position = Errore del lexer alla posizione { $position }: { $message }

# =============================================================================
# String Literal Errors
# =============================================================================

lexer-unterminated-string = Stringa letterale non terminata

# =============================================================================
# Identifier Errors
# =============================================================================

lexer-unterminated-delimited-identifier = Identificatore delimitato non terminato
lexer-empty-delimited-identifier = L'identificatore delimitato vuoto non è consentito

# =============================================================================
# Number Literal Errors
# =============================================================================

lexer-invalid-scientific-notation = Notazione scientifica non valida: previste cifre dopo 'E'

# =============================================================================
# Placeholder Errors
# =============================================================================

lexer-expected-digit-after-dollar = Prevista cifra dopo '$' per placeholder numerato
lexer-invalid-numbered-placeholder = Placeholder numerato non valido: ${ $placeholder }
lexer-numbered-placeholder-zero = Il placeholder numerato deve essere $1 o superiore (non $0)
lexer-expected-identifier-after-colon = Previsto identificatore dopo ':' per placeholder nominato

# =============================================================================
# Variable Errors
# =============================================================================

lexer-expected-variable-after-at-at = Previsto nome variabile dopo @@
lexer-expected-variable-after-at = Previsto nome variabile dopo @

# =============================================================================
# Operator Errors
# =============================================================================

lexer-unexpected-pipe = Carattere inatteso: '|' (forse intendevi '||'?)

# =============================================================================
# General Errors
# =============================================================================

lexer-unexpected-character = Carattere inatteso: '{ $character }'
