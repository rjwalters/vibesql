# VibeSQL Mensagens de Erro do Executor - Português Brasileiro (pt-BR)
# Este arquivo contém todas as mensagens de erro do crate vibesql-executor.

# =============================================================================
# Erros de Tabela
# =============================================================================

executor-table-not-found = Tabela '{ $name }' não encontrada
executor-table-already-exists = Tabela '{ $name }' já existe

# =============================================================================
# Erros de Coluna
# =============================================================================

executor-column-not-found-simple = Coluna '{ $column_name }' não encontrada na tabela '{ $table_name }'
executor-column-not-found-searched = Coluna '{ $column_name }' não encontrada (tabelas pesquisadas: { $searched_tables })
executor-column-not-found-with-available = Coluna '{ $column_name }' não encontrada (tabelas pesquisadas: { $searched_tables }). Colunas disponíveis: { $available_columns }
executor-invalid-table-qualifier = Qualificador de tabela '{ $qualifier }' inválido para coluna '{ $column }'. Tabelas disponíveis: { $available_tables }
executor-column-already-exists = Coluna '{ $name }' já existe
executor-column-index-out-of-bounds = Índice de coluna { $index } fora dos limites

# =============================================================================
# Erros de Índice
# =============================================================================

executor-index-not-found = Índice '{ $name }' não encontrado
executor-index-already-exists = Índice '{ $name }' já existe
executor-invalid-index-definition = Definição de índice inválida: { $message }

# =============================================================================
# Erros de Trigger
# =============================================================================

executor-trigger-not-found = Trigger '{ $name }' não encontrado
executor-trigger-already-exists = Trigger '{ $name }' já existe

# =============================================================================
# Erros de Schema
# =============================================================================

executor-schema-not-found = Schema '{ $name }' não encontrado
executor-schema-already-exists = Schema '{ $name }' já existe
executor-schema-not-empty = Não é possível remover o schema '{ $name }': schema não está vazio

# =============================================================================
# Erros de Role e Permissão
# =============================================================================

executor-role-not-found = Role '{ $name }' não encontrada
executor-permission-denied = Permissão negada: role '{ $role }' não possui privilégio { $privilege } em { $object }
executor-dependent-privileges-exist = Privilégios dependentes existem: { $message }

# =============================================================================
# Erros de Tipo
# =============================================================================

executor-type-not-found = Tipo '{ $name }' não encontrado
executor-type-already-exists = Tipo '{ $name }' já existe
executor-type-in-use = Não é possível remover o tipo '{ $name }': tipo ainda está em uso
executor-type-mismatch = Incompatibilidade de tipos: { $left } { $op } { $right }
executor-type-error = Erro de tipo: { $message }
executor-cast-error = Não é possível converter { $from_type } para { $to_type }
executor-type-conversion-error = Não é possível converter { $from } para { $to }

# =============================================================================
# Erros de Expressão e Consulta
# =============================================================================

executor-division-by-zero = Divisão por zero
executor-invalid-where-clause = Cláusula WHERE inválida: { $message }
executor-unsupported-expression = Expressão não suportada: { $message }
executor-unsupported-feature = Recurso não suportado: { $message }
executor-parse-error = Erro de análise: { $message }

# =============================================================================
# Erros de Subconsulta
# =============================================================================

executor-subquery-returned-multiple-rows = Subconsulta escalar retornou { $actual } linhas, esperada { $expected }
executor-subquery-column-count-mismatch = Subconsulta retornou { $actual } colunas, esperadas { $expected }
executor-column-count-mismatch = Lista de colunas derivadas possui { $provided } colunas, mas a consulta produz { $expected } colunas

# =============================================================================
# Erros de Restrição
# =============================================================================

executor-constraint-violation = Violação de restrição: { $message }
executor-multiple-primary-keys = Múltiplas restrições PRIMARY KEY não são permitidas
executor-cannot-drop-column = Não é possível remover coluna: { $message }
executor-constraint-not-found = Restrição '{ $constraint_name }' não encontrada na tabela '{ $table_name }'

# =============================================================================
# Erros de Limite de Recursos
# =============================================================================

executor-expression-depth-exceeded = Limite de profundidade de expressão excedido: { $depth } > { $max_depth } (previne estouro de pilha)
executor-query-timeout-exceeded = Tempo limite da consulta excedido: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Limite de processamento de linhas excedido: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Limite de memória excedido: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Erros de Procedimento/Variável
# =============================================================================

executor-variable-not-found-simple = Variável '{ $variable_name }' não encontrada
executor-variable-not-found-with-available = Variável '{ $variable_name }' não encontrada. Variáveis disponíveis: { $available_variables }
executor-label-not-found = Rótulo '{ $name }' não encontrado

# =============================================================================
# Erros de SELECT INTO
# =============================================================================

executor-select-into-row-count = SELECT INTO procedural deve retornar exatamente { $expected } linha, obtidas { $actual } linha{ $plural }
executor-select-into-column-count = Incompatibilidade de contagem de colunas no SELECT INTO procedural: { $expected } variável{ $expected_plural } mas a consulta retornou { $actual } coluna{ $actual_plural }

# =============================================================================
# Erros de Procedure e Function
# =============================================================================

executor-procedure-not-found-simple = Procedure '{ $procedure_name }' não encontrada no schema '{ $schema_name }'
executor-procedure-not-found-with-available = Procedure '{ $procedure_name }' não encontrada no schema '{ $schema_name }'
    .available = Procedures disponíveis: { $available_procedures }
executor-procedure-not-found-with-suggestion = Procedure '{ $procedure_name }' não encontrada no schema '{ $schema_name }'
    .available = Procedures disponíveis: { $available_procedures }
    .suggestion = Você quis dizer '{ $suggestion }'?

executor-function-not-found-simple = Function '{ $function_name }' não encontrada no schema '{ $schema_name }'
executor-function-not-found-with-available = Function '{ $function_name }' não encontrada no schema '{ $schema_name }'
    .available = Functions disponíveis: { $available_functions }
executor-function-not-found-with-suggestion = Function '{ $function_name }' não encontrada no schema '{ $schema_name }'
    .available = Functions disponíveis: { $available_functions }
    .suggestion = Você quis dizer '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' espera { $expected } parâmetro{ $expected_plural } ({ $parameter_signature }), recebidos { $actual } argumento{ $actual_plural }
executor-parameter-type-mismatch = Parâmetro '{ $parameter_name }' espera { $expected_type }, recebido { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Incompatibilidade na contagem de argumentos: esperados { $expected }, recebidos { $actual }

executor-recursion-limit-exceeded = Profundidade máxima de recursão ({ $max_depth }) excedida: { $message }
executor-recursion-call-stack = Pilha de chamadas:
executor-function-must-return = Function deve retornar um valor
executor-invalid-control-flow = Fluxo de controle inválido: { $message }
executor-invalid-function-body = Corpo de function inválido: { $message }
executor-function-read-only-violation = Violação de somente leitura da function: { $message }

# =============================================================================
# Erros de EXTRACT
# =============================================================================

executor-invalid-extract-field = Não é possível extrair { $field } de valor do tipo { $value_type }

# =============================================================================
# Erros Columnar/Arrow
# =============================================================================

executor-arrow-downcast-error = Falha ao fazer downcast do array Arrow para { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Tipos incompatíveis para { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Tipo incompatível para { $operation }: { $left_type }
executor-simd-operation-failed = Operação SIMD { $operation } falhou: { $reason }
executor-columnar-column-not-found = Índice de coluna { $column_index } fora dos limites (lote tem { $batch_columns } colunas)
executor-columnar-column-not-found-by-name = Coluna não encontrada: { $column_name }
executor-columnar-length-mismatch = Incompatibilidade de comprimento de coluna em { $context }: esperado { $expected }, obtido { $actual }
executor-unsupported-array-type = Tipo de array não suportado para { $operation }: { $array_type }

# =============================================================================
# Erros Espaciais
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } espera { $expected }, recebido { $actual }

# =============================================================================
# Erros de Cursor
# =============================================================================

executor-cursor-already-exists = Cursor '{ $name }' já existe
executor-cursor-not-found = Cursor '{ $name }' não encontrado
executor-cursor-already-open = Cursor '{ $name }' já está aberto
executor-cursor-not-open = Cursor '{ $name }' não está aberto
executor-cursor-not-scrollable = Cursor '{ $name }' não é rolável (SCROLL não especificado)

# =============================================================================
# Erros de Storage e Gerais
# =============================================================================

executor-storage-error = Erro de armazenamento: { $message }
executor-other = { $message }
