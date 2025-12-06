# VibeSQL Parser/Lexer Localização - Português Brasileiro (pt-BR)
# Este arquivo contém todas as strings visíveis ao usuário de erros do lexer e parser.

# =============================================================================
# Cabeçalho de Erro do Lexer
# =============================================================================

lexer-error-at-position = Erro do lexer na posição { $position }: { $message }

# =============================================================================
# Erros de Literais de String
# =============================================================================

lexer-unterminated-string = Literal de string não terminado

# =============================================================================
# Erros de Identificadores
# =============================================================================

lexer-unterminated-delimited-identifier = Identificador delimitado não terminado
lexer-empty-delimited-identifier = Identificador delimitado vazio não é permitido

# =============================================================================
# Erros de Literais Numéricos
# =============================================================================

lexer-invalid-scientific-notation = Notação científica inválida: esperados dígitos após 'E'

# =============================================================================
# Erros de Marcadores de Posição
# =============================================================================

lexer-expected-digit-after-dollar = Esperado dígito após '$' para marcador de posição numerado
lexer-invalid-numbered-placeholder = Marcador de posição numerado inválido: ${ $placeholder }
lexer-numbered-placeholder-zero = Marcador de posição numerado deve ser $1 ou superior (não $0)
lexer-expected-identifier-after-colon = Esperado identificador após ':' para marcador de posição nomeado

# =============================================================================
# Erros de Variáveis
# =============================================================================

lexer-expected-variable-after-at-at = Esperado nome de variável após @@
lexer-expected-variable-after-at = Esperado nome de variável após @

# =============================================================================
# Erros de Operadores
# =============================================================================

lexer-unexpected-pipe = Caractere inesperado: '|' (você quis dizer '||'?)

# =============================================================================
# Erros Gerais
# =============================================================================

lexer-unexpected-character = Caractere inesperado: '{ $character }'
