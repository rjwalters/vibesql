# VibeSQL Parser/Lexer Localization - Polski (Polish)
# This file contains all user-facing strings for lexer and parser errors.

# =============================================================================
# Lexer Error Header
# =============================================================================

lexer-error-at-position = Błąd leksera na pozycji { $position }: { $message }

# =============================================================================
# String Literal Errors
# =============================================================================

lexer-unterminated-string = Niezakończony literał znakowy

# =============================================================================
# Identifier Errors
# =============================================================================

lexer-unterminated-delimited-identifier = Niezakończony identyfikator ograniczony
lexer-empty-delimited-identifier = Pusty identyfikator ograniczony jest niedozwolony

# =============================================================================
# Number Literal Errors
# =============================================================================

lexer-invalid-scientific-notation = Nieprawidłowa notacja naukowa: oczekiwano cyfr po 'E'

# =============================================================================
# Placeholder Errors
# =============================================================================

lexer-expected-digit-after-dollar = Oczekiwano cyfry po '$' dla numerowanego symbolu zastępczego
lexer-invalid-numbered-placeholder = Nieprawidłowy numerowany symbol zastępczy: ${ $placeholder }
lexer-numbered-placeholder-zero = Numerowany symbol zastępczy musi być $1 lub wyższy (brak $0)
lexer-expected-identifier-after-colon = Oczekiwano identyfikatora po ':' dla nazwanego symbolu zastępczego

# =============================================================================
# Variable Errors
# =============================================================================

lexer-expected-variable-after-at-at = Oczekiwano nazwy zmiennej po @@
lexer-expected-variable-after-at = Oczekiwano nazwy zmiennej po @

# =============================================================================
# Operator Errors
# =============================================================================

lexer-unexpected-pipe = Nieoczekiwany znak: '|' (czy chodziło o '||'?)

# =============================================================================
# General Errors
# =============================================================================

lexer-unexpected-character = Nieoczekiwany znak: '{ $character }'
