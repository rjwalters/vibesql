# VibeSQL Mensagens de Erro do Catálogo - Português Brasileiro (pt-BR)
# Este arquivo contém todas as mensagens de erro do crate vibesql-catalog.

# =============================================================================
# Erros de Tabela
# =============================================================================

catalog-table-already-exists = Tabela '{ $name }' já existe
catalog-table-not-found = Tabela '{ $table_name }' não encontrada

# =============================================================================
# Erros de Coluna
# =============================================================================

catalog-column-already-exists = Coluna '{ $name }' já existe
catalog-column-not-found = Coluna '{ $column_name }' não encontrada na tabela '{ $table_name }'

# =============================================================================
# Erros de Schema
# =============================================================================

catalog-schema-already-exists = Schema '{ $name }' já existe
catalog-schema-not-found = Schema '{ $name }' não encontrado
catalog-schema-not-empty = Schema '{ $name }' não está vazio

# =============================================================================
# Erros de Role
# =============================================================================

catalog-role-already-exists = Role '{ $name }' já existe
catalog-role-not-found = Role '{ $name }' não encontrada

# =============================================================================
# Erros de Domain
# =============================================================================

catalog-domain-already-exists = Domain '{ $name }' já existe
catalog-domain-not-found = Domain '{ $name }' não encontrado
catalog-domain-in-use = Domain '{ $domain_name }' ainda está em uso por { $count } coluna(s): { $columns }

# =============================================================================
# Erros de Sequence
# =============================================================================

catalog-sequence-already-exists = Sequence '{ $name }' já existe
catalog-sequence-not-found = Sequence '{ $name }' não encontrada
catalog-sequence-in-use = Sequence '{ $sequence_name }' ainda está em uso por { $count } coluna(s): { $columns }

# =============================================================================
# Erros de Tipo
# =============================================================================

catalog-type-already-exists = Tipo '{ $name }' já existe
catalog-type-not-found = Tipo '{ $name }' não encontrado
catalog-type-in-use = Tipo '{ $name }' ainda está em uso por uma ou mais tabelas

# =============================================================================
# Erros de Collation e Character Set
# =============================================================================

catalog-collation-already-exists = Collation '{ $name }' já existe
catalog-collation-not-found = Collation '{ $name }' não encontrada
catalog-character-set-already-exists = Character set '{ $name }' já existe
catalog-character-set-not-found = Character set '{ $name }' não encontrado
catalog-translation-already-exists = Translation '{ $name }' já existe
catalog-translation-not-found = Translation '{ $name }' não encontrada

# =============================================================================
# Erros de View
# =============================================================================

catalog-view-already-exists = View '{ $name }' já existe
catalog-view-not-found = View '{ $name }' não encontrada
catalog-view-in-use = View ou tabela '{ $view_name }' ainda está em uso por { $count } view(s): { $views }

# =============================================================================
# Erros de Trigger
# =============================================================================

catalog-trigger-already-exists = Trigger '{ $name }' já existe
catalog-trigger-not-found = Trigger '{ $name }' não encontrado

# =============================================================================
# Erros de Assertion
# =============================================================================

catalog-assertion-already-exists = Assertion '{ $name }' já existe
catalog-assertion-not-found = Assertion '{ $name }' não encontrada

# =============================================================================
# Erros de Function e Procedure
# =============================================================================

catalog-function-already-exists = Function '{ $name }' já existe
catalog-function-not-found = Function '{ $name }' não encontrada
catalog-procedure-already-exists = Procedure '{ $name }' já existe
catalog-procedure-not-found = Procedure '{ $name }' não encontrada

# =============================================================================
# Erros de Constraint
# =============================================================================

catalog-constraint-already-exists = Constraint '{ $name }' já existe
catalog-constraint-not-found = Constraint '{ $name }' não encontrada

# =============================================================================
# Erros de Índice
# =============================================================================

catalog-index-already-exists = Índice '{ $index_name }' na tabela '{ $table_name }' já existe
catalog-index-not-found = Índice '{ $index_name }' na tabela '{ $table_name }' não encontrado

# =============================================================================
# Erros de Foreign Key
# =============================================================================

catalog-circular-foreign-key = Dependência circular de foreign key detectada para tabela '{ $table_name }': { $message }
