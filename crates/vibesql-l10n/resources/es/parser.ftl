# VibeSQL Parser/Lexer Localización - Español
# Este archivo contiene todas las cadenas visibles para el usuario de errores del lexer y parser.

# =============================================================================
# Encabezado de Error del Lexer
# =============================================================================

lexer-error-at-position = Error del lexer en la posición { $position }: { $message }

# =============================================================================
# Errores de Literales de Cadena
# =============================================================================

lexer-unterminated-string = Literal de cadena sin terminar

# =============================================================================
# Errores de Identificadores
# =============================================================================

lexer-unterminated-delimited-identifier = Identificador delimitado sin terminar
lexer-empty-delimited-identifier = No se permite un identificador delimitado vacío

# =============================================================================
# Errores de Literales Numéricos
# =============================================================================

lexer-invalid-scientific-notation = Notación científica inválida: se esperaban dígitos después de 'E'

# =============================================================================
# Errores de Marcadores de Posición
# =============================================================================

lexer-expected-digit-after-dollar = Se esperaba un dígito después de '$' para marcador de posición numerado
lexer-invalid-numbered-placeholder = Marcador de posición numerado inválido: ${ $placeholder }
lexer-numbered-placeholder-zero = El marcador de posición numerado debe ser $1 o superior (no $0)
lexer-expected-identifier-after-colon = Se esperaba un identificador después de ':' para marcador de posición con nombre

# =============================================================================
# Errores de Variables
# =============================================================================

lexer-expected-variable-after-at-at = Se esperaba nombre de variable después de @@
lexer-expected-variable-after-at = Se esperaba nombre de variable después de @

# =============================================================================
# Errores de Operadores
# =============================================================================

lexer-unexpected-pipe = Carácter inesperado: '|' (¿quiso decir '||'?)

# =============================================================================
# Errores Generales
# =============================================================================

lexer-unexpected-character = Carácter inesperado: '{ $character }'
