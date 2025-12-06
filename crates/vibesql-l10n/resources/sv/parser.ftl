# VibeSQL Parser/Lexer Localization - Swedish (Svenska)
# This file contains all user-facing strings for lexer and parser errors.

# =============================================================================
# Lexer Error Header
# =============================================================================

lexer-error-at-position = Lexerfel vid position { $position }: { $message }

# =============================================================================
# String Literal Errors
# =============================================================================

lexer-unterminated-string = Oavslutad stränglitteral

# =============================================================================
# Identifier Errors
# =============================================================================

lexer-unterminated-delimited-identifier = Oavslutad avgränsad identifierare
lexer-empty-delimited-identifier = Tom avgränsad identifierare är inte tillåten

# =============================================================================
# Number Literal Errors
# =============================================================================

lexer-invalid-scientific-notation = Ogiltig vetenskaplig notation: siffror förväntades efter 'E'

# =============================================================================
# Placeholder Errors
# =============================================================================

lexer-expected-digit-after-dollar = Siffra förväntades efter '$' för numrerad platshållare
lexer-invalid-numbered-placeholder = Ogiltig numrerad platshållare: ${ $placeholder }
lexer-numbered-placeholder-zero = Numrerad platshållare måste vara $1 eller högre (ingen $0)
lexer-expected-identifier-after-colon = Identifierare förväntades efter ':' för namngiven platshållare

# =============================================================================
# Variable Errors
# =============================================================================

lexer-expected-variable-after-at-at = Variabelnamn förväntades efter @@
lexer-expected-variable-after-at = Variabelnamn förväntades efter @

# =============================================================================
# Operator Errors
# =============================================================================

lexer-unexpected-pipe = Oväntat tecken: '|' (menade du '||'?)

# =============================================================================
# General Errors
# =============================================================================

lexer-unexpected-character = Oväntat tecken: '{ $character }'
