# VibeSQL Parser/Lexer Localization - Indonesian (id)
# This file contains all user-facing strings for lexer and parser errors.

# =============================================================================
# Lexer Error Header
# =============================================================================

lexer-error-at-position = Kesalahan lexer pada posisi { $position }: { $message }

# =============================================================================
# String Literal Errors
# =============================================================================

lexer-unterminated-string = Literal string tidak diakhiri

# =============================================================================
# Identifier Errors
# =============================================================================

lexer-unterminated-delimited-identifier = Identifier dibatasi tidak diakhiri
lexer-empty-delimited-identifier = Identifier dibatasi kosong tidak diperbolehkan

# =============================================================================
# Number Literal Errors
# =============================================================================

lexer-invalid-scientific-notation = Notasi ilmiah tidak valid: digit diharapkan setelah 'E'

# =============================================================================
# Placeholder Errors
# =============================================================================

lexer-expected-digit-after-dollar = Digit diharapkan setelah '$' untuk placeholder bernomor
lexer-invalid-numbered-placeholder = Placeholder bernomor tidak valid: ${ $placeholder }
lexer-numbered-placeholder-zero = Placeholder bernomor harus $1 atau lebih tinggi (bukan $0)
lexer-expected-identifier-after-colon = Identifier diharapkan setelah ':' untuk placeholder bernama

# =============================================================================
# Variable Errors
# =============================================================================

lexer-expected-variable-after-at-at = Nama variabel diharapkan setelah @@
lexer-expected-variable-after-at = Nama variabel diharapkan setelah @

# =============================================================================
# Operator Errors
# =============================================================================

lexer-unexpected-pipe = Karakter tidak terduga: '|' (apakah Anda maksud '||'?)

# =============================================================================
# General Errors
# =============================================================================

lexer-unexpected-character = Karakter tidak terduga: '{ $character }'
