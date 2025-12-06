# VibeSQL Mensagens de Erro de Storage - Português Brasileiro (pt-BR)
# Este arquivo contém todas as mensagens de erro do crate vibesql-storage.

# =============================================================================
# Erros de Tabela
# =============================================================================

storage-table-not-found = Tabela '{ $name }' não encontrada

# =============================================================================
# Erros de Coluna
# =============================================================================

storage-column-count-mismatch = Incompatibilidade na contagem de colunas: esperadas { $expected }, obtidas { $actual }
storage-column-index-out-of-bounds = Índice de coluna { $index } fora dos limites
storage-column-not-found = Coluna '{ $column_name }' não encontrada na tabela '{ $table_name }'

# =============================================================================
# Erros de Índice
# =============================================================================

storage-index-already-exists = Índice '{ $name }' já existe
storage-index-not-found = Índice '{ $name }' não encontrado
storage-invalid-index-column = { $message }

# =============================================================================
# Erros de Restrição
# =============================================================================

storage-null-constraint-violation = Violação de restrição NOT NULL: coluna '{ $column }' não pode ser NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Erros de Tipo
# =============================================================================

storage-type-mismatch = Incompatibilidade de tipo na coluna '{ $column }': esperado { $expected }, obtido { $actual }

# =============================================================================
# Erros de Transação e Catálogo
# =============================================================================

storage-catalog-error = Erro de catálogo: { $message }
storage-transaction-error = Erro de transação: { $message }
storage-row-not-found = Linha não encontrada

# =============================================================================
# Erros de E/S e Página
# =============================================================================

storage-io-error = Erro de E/S: { $message }
storage-invalid-page-size = Tamanho de página inválido: esperado { $expected }, obtido { $actual }
storage-invalid-page-id = ID de página inválido: { $page_id }
storage-lock-error = Erro de bloqueio: { $message }

# =============================================================================
# Erros de Memória
# =============================================================================

storage-memory-budget-exceeded = Orçamento de memória excedido: usando { $used } bytes, orçamento é { $budget } bytes
storage-no-index-to-evict = Nenhum índice disponível para remoção (todos os índices já estão em disco)

# =============================================================================
# Erros Gerais
# =============================================================================

storage-not-implemented = Não implementado: { $message }
storage-other = { $message }
