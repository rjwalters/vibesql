# VibeSQL Parser/Lexer Localization - English (US)
# This file contains all user-facing strings for lexer and parser errors.

# =============================================================================
# Lexer Error Header
# =============================================================================

lexer-error-at-position = Lexer error at position { $position }: { $message }

# =============================================================================
# String Literal Errors
# =============================================================================

lexer-unterminated-string = Unterminated string literal

# =============================================================================
# Identifier Errors
# =============================================================================

lexer-unterminated-delimited-identifier = Unterminated delimited identifier
lexer-empty-delimited-identifier = Empty delimited identifier is not allowed

# =============================================================================
# Number Literal Errors
# =============================================================================

lexer-invalid-scientific-notation = Invalid scientific notation: expected digits after 'E'

# =============================================================================
# Placeholder Errors
# =============================================================================

lexer-expected-digit-after-dollar = Expected digit after '$' for numbered placeholder
lexer-invalid-numbered-placeholder = Invalid numbered placeholder: ${ $placeholder }
lexer-numbered-placeholder-zero = Numbered placeholder must be $1 or higher (no $0)
lexer-expected-identifier-after-colon = Expected identifier after ':' for named placeholder

# =============================================================================
# Variable Errors
# =============================================================================

lexer-expected-variable-after-at-at = Expected variable name after @@
lexer-expected-variable-after-at = Expected variable name after @

# =============================================================================
# Operator Errors
# =============================================================================

lexer-unexpected-pipe = Unexpected character: '|' (did you mean '||'?)

# =============================================================================
# General Errors
# =============================================================================

lexer-unexpected-character = Unexpected character: '{ $character }'
