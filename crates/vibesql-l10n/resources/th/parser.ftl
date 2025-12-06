# VibeSQL Parser/Lexer Localization - Thai (ภาษาไทย)
# This file contains all user-facing strings for lexer and parser errors.

# =============================================================================
# Lexer Error Header
# =============================================================================

lexer-error-at-position = ข้อผิดพลาด Lexer ที่ตำแหน่ง { $position }: { $message }

# =============================================================================
# String Literal Errors
# =============================================================================

lexer-unterminated-string = String literal ไม่ถูกปิด

# =============================================================================
# Identifier Errors
# =============================================================================

lexer-unterminated-delimited-identifier = Delimited identifier ไม่ถูกปิด
lexer-empty-delimited-identifier = ไม่อนุญาตให้ใช้ delimited identifier ว่าง

# =============================================================================
# Number Literal Errors
# =============================================================================

lexer-invalid-scientific-notation = Scientific notation ไม่ถูกต้อง: คาดหวังตัวเลขหลัง 'E'

# =============================================================================
# Placeholder Errors
# =============================================================================

lexer-expected-digit-after-dollar = คาดหวังตัวเลขหลัง '$' สำหรับ numbered placeholder
lexer-invalid-numbered-placeholder = Numbered placeholder ไม่ถูกต้อง: ${ $placeholder }
lexer-numbered-placeholder-zero = Numbered placeholder ต้องเป็น $1 หรือสูงกว่า (ไม่มี $0)
lexer-expected-identifier-after-colon = คาดหวัง identifier หลัง ':' สำหรับ named placeholder

# =============================================================================
# Variable Errors
# =============================================================================

lexer-expected-variable-after-at-at = คาดหวังชื่อตัวแปรหลัง @@
lexer-expected-variable-after-at = คาดหวังชื่อตัวแปรหลัง @

# =============================================================================
# Operator Errors
# =============================================================================

lexer-unexpected-pipe = อักขระไม่คาดคิด: '|' (หมายถึง '||' หรือไม่?)

# =============================================================================
# General Errors
# =============================================================================

lexer-unexpected-character = อักขระไม่คาดคิด: '{ $character }'
