# VibeSQL Parser/Lexer Localization - Dutch (Nederlands)
# This file contains all user-facing strings for lexer and parser errors.

# =============================================================================
# Lexer Error Header
# =============================================================================

lexer-error-at-position = Lexerfout op positie { $position }: { $message }

# =============================================================================
# String Literal Errors
# =============================================================================

lexer-unterminated-string = Niet-afgesloten string-literal

# =============================================================================
# Identifier Errors
# =============================================================================

lexer-unterminated-delimited-identifier = Niet-afgesloten gescheiden identificatie
lexer-empty-delimited-identifier = Lege gescheiden identificatie is niet toegestaan

# =============================================================================
# Number Literal Errors
# =============================================================================

lexer-invalid-scientific-notation = Ongeldige wetenschappelijke notatie: cijfers verwacht na 'E'

# =============================================================================
# Placeholder Errors
# =============================================================================

lexer-expected-digit-after-dollar = Cijfer verwacht na '$' voor genummerde placeholder
lexer-invalid-numbered-placeholder = Ongeldige genummerde placeholder: ${ $placeholder }
lexer-numbered-placeholder-zero = Genummerde placeholder moet $1 of hoger zijn (geen $0)
lexer-expected-identifier-after-colon = Identificatie verwacht na ':' voor benoemde placeholder

# =============================================================================
# Variable Errors
# =============================================================================

lexer-expected-variable-after-at-at = Variabelenaam verwacht na @@
lexer-expected-variable-after-at = Variabelenaam verwacht na @

# =============================================================================
# Operator Errors
# =============================================================================

lexer-unexpected-pipe = Onverwacht teken: '|' (bedoelde u '||'?)

# =============================================================================
# General Errors
# =============================================================================

lexer-unexpected-character = Onverwacht teken: '{ $character }'
