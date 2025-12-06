# VibeSQL Parser/Lexer Localization - Vietnamese (Tiếng Việt)
# This file contains all user-facing strings for lexer and parser errors.

# =============================================================================
# Lexer Error Header
# =============================================================================

lexer-error-at-position = Lỗi lexer tại vị trí { $position }: { $message }

# =============================================================================
# String Literal Errors
# =============================================================================

lexer-unterminated-string = Chuỗi ký tự chưa kết thúc

# =============================================================================
# Identifier Errors
# =============================================================================

lexer-unterminated-delimited-identifier = Định danh phân cách chưa kết thúc
lexer-empty-delimited-identifier = Không cho phép định danh phân cách rỗng

# =============================================================================
# Number Literal Errors
# =============================================================================

lexer-invalid-scientific-notation = Ký hiệu khoa học không hợp lệ: mong đợi các chữ số sau 'E'

# =============================================================================
# Placeholder Errors
# =============================================================================

lexer-expected-digit-after-dollar = Mong đợi chữ số sau '$' cho placeholder đánh số
lexer-invalid-numbered-placeholder = Placeholder đánh số không hợp lệ: ${ $placeholder }
lexer-numbered-placeholder-zero = Placeholder đánh số phải là $1 trở lên (không có $0)
lexer-expected-identifier-after-colon = Mong đợi định danh sau ':' cho placeholder có tên

# =============================================================================
# Variable Errors
# =============================================================================

lexer-expected-variable-after-at-at = Mong đợi tên biến sau @@
lexer-expected-variable-after-at = Mong đợi tên biến sau @

# =============================================================================
# Operator Errors
# =============================================================================

lexer-unexpected-pipe = Ký tự không mong đợi: '|' (có phải bạn muốn nói '||'?)

# =============================================================================
# General Errors
# =============================================================================

lexer-unexpected-character = Ký tự không mong đợi: '{ $character }'
