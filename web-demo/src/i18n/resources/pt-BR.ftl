# VibeSQL Web UI - Português (Brasil)

# Page titles
page-title = VibeSQL - Banco de Dados SQL:1999 com IA
demo-title = Demo do VibeSQL
benchmarks-title = Benchmarks de Desempenho - VibeSQL
benchmarks-heading = VibeSQL - Benchmarks de Desempenho
conformance-title = Relatório de Conformidade - VibeSQL
conformance-heading = Relatório de Conformidade
conformance-subtitle = Testes de Conformidade com o Padrão SQL:1999

# Navigation
nav-showcase = Demonstração SQL:1999
nav-conformance = Ver resultados do sqltest
nav-sqllogictest = Ver resultados do SQLLogicTest

# Editor section
editor-title = Editor SQL
editor-storage = Armazenamento
editor-storage-init = Inicializando...
editor-execute = Executar Consulta

# Results section
results-title = Resultados
results-empty = Execute uma consulta para ver os resultados
results-loading = Carregando...
results-rows = { $count } { $count ->
    [one] linha
   *[other] linhas
}
results-rows-with-time = { $count } { $count ->
    [one] linha
   *[other] linhas
} ({ $time }ms)
results-copy = Copiar para área de transferência
results-export = Exportar CSV
results-limit-warning = Mostrando as primeiras { $limit } de { $total } linhas. Use a cláusula LIMIT para refinar sua consulta.

# Examples sidebar
examples-title = Exemplos
examples-basic = Consultas Básicas
examples-advanced = Consultas Avançadas

# Database selector
db-select-label = Banco de dados

# Footer
footer-tagline = VibeSQL - Banco de Dados SQL:1999 em WebAssembly
footer-deployed = Implantado: { $date }

# Theme
theme-toggle-dark = Mudar para modo escuro
theme-toggle-light = Mudar para modo claro

# Locale
locale-select = Selecionar idioma

# Messages
msg-query-success = Consulta executada com sucesso
msg-rows-affected = { $count } { $count ->
    [one] linha afetada
   *[other] linhas afetadas
}

# Errors
error-generic = Ocorreu um erro
error-query-failed = A consulta falhou

# Editor
editor-placeholder = Digite sua consulta SQL aqui... (Ctrl+Enter ou Cmd+Enter para executar)

# Navigation links
nav-terminal = Demo do Terminal SQL
nav-compliance = Relatório de Conformidade SQL
nav-benchmarks = Benchmarks de Desempenho
nav-github = Repositório GitHub
nav-home = Início

# Results
results-success-zero = Consulta executada com sucesso (0 linhas)
results-null = NULO

# Help Modal
help-title = Atalhos de Teclado e Ajuda
help-close = Fechar
help-editor-shortcuts = Atalhos do Editor
help-navigation = Navegação
help-results-actions = Ações de Resultados
help-tips = Dicas
help-shortcut-execute = Executar consulta atual
help-shortcut-comment = Alternar comentário de linha
help-shortcut-indent = Indentar seleção
help-shortcut-show-help = Mostrar este diálogo de ajuda
help-shortcut-close-help = Fechar diálogo de ajuda
help-action-copy = Copiar para área de transferência
help-action-copy-desc = Copiar resultados como valores separados por tabulação
help-action-export = Exportar CSV
help-action-export-desc = Baixar resultados como arquivo CSV
help-tip-limit = Os resultados são limitados a 1.000 linhas por desempenho. Use LIMIT para refinar consultas.
help-tip-time = O tempo de execução é mostrado com os resultados da consulta.
help-tip-syntax = O editor suporta realce de sintaxe SQL e autocompletar.
help-tip-theme = Alterne entre temas claro/escuro usando o botão de tema.
help-got-it = Entendi!

# Showcase Navigation
showcase-title = Demonstração SQL:1999 Core
showcase-description = Explore interativamente os recursos SQL:1999 Core implementados
showcase-complete = { $percent }% Completo
showcase-categories = Categorias de Recursos
showcase-legend = Legenda de Status
showcase-status-implemented = Totalmente Implementado
showcase-status-partial = Parcialmente Implementado
showcase-status-planned = Planejado

# Showcase category labels
showcase-cat-compliance = Painel de Conformidade
showcase-cat-data-types = Tipos de Dados
showcase-cat-dml = Operações DML
showcase-cat-predicates = Predicados e Operadores
showcase-cat-joins = JOINs
showcase-cat-subqueries = Subconsultas
showcase-cat-aggregates = Agregados e GROUP BY
showcase-cat-ddl = DDL e Restrições

# Common showcase elements
showcase-interactive-examples = Exemplos Interativos
showcase-try-example = Experimente Este Exemplo
showcase-progress = { $implemented } de { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Categoria
showcase-table-description = Descrição
showcase-table-syntax = Sintaxe
showcase-table-use-case = Caso de Uso

# Status labels
status-implemented = Implementado
status-partial = Parcial
status-planned = Planejado

# Aggregates Showcase
aggregates-title = Agregados SQL e GROUP BY
aggregates-description = Funções de agregação SQL:1999 Core e capacidades de agrupamento
aggregates-reference = Referência de Funções de Agregação
aggregates-table-function = Função
aggregates-progress-type = funções
aggregates-ex-basic = Funções de Agregação Básicas
aggregates-ex-group-single = GROUP BY (Coluna Única)
aggregates-ex-group-multiple = GROUP BY (Múltiplas Colunas)
aggregates-ex-having = Cláusula HAVING
aggregates-ex-orderby = ORDER BY com Agregados
aggregates-ex-null = Tratamento de NULL em Agregados

# DML Operations Showcase
dml-title = Operações DML (Linguagem de Manipulação de Dados)
dml-description = Operações SQL:1999 Core para consultar e modificar dados
dml-reference = Referência de Operações DML
dml-table-operation = Operação
dml-progress-type = operações
dml-ex-select-basic = SELECT - Consultas Básicas
dml-ex-select-ordering = SELECT - Ordenação e Limitação
dml-ex-insert = Operações INSERT
dml-ex-update = Operações UPDATE
dml-ex-delete = Operações DELETE
dml-ex-combined = Fluxo de Trabalho CRUD Combinado

# Data Types Showcase
datatypes-title = Tipos de Dados SQL:1999 Core
datatypes-description = Explore os tipos de dados fundamentais definidos na especificação SQL:1999 Core
datatypes-reference = Referência de Tipos de Dados
datatypes-table-type = Nome do Tipo
datatypes-table-example = Valores de Exemplo
datatypes-table-spec = Especificação
datatypes-progress-type = tipos
datatypes-ex-numeric = Trabalhando com Tipos Numéricos
datatypes-ex-null = Tratamento de NULL e Lógica de Três Valores
datatypes-ex-comparisons = Comparações e Operações de Tipos

# JOINs Showcase
joins-title = JOINs SQL
joins-description = Operações JOIN SQL:1999 Core para combinar dados de múltiplas tabelas
joins-reference = Referência de Tipos de JOIN
joins-table-type = Tipo de JOIN
joins-progress-type = tipos de JOIN
joins-category-suffix = JOINs
joins-ex-sample = Configuração de Dados de Exemplo
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN Multi-tabelas

# Predicates Showcase
predicates-title = Predicados e Operadores
predicates-description = Predicados SQL:1999 para filtragem e operações lógicas
predicates-reference = Referência de Predicados
predicates-table-predicate = Predicado
predicates-progress-type = predicados
predicates-ex-comparison = Operadores de Comparação
predicates-ex-between = BETWEEN e Predicados de Intervalo
predicates-ex-null = Predicados NULL e Lógica de Três Valores
predicates-ex-boolean = Lógica Booleana (AND, OR, NOT)
predicates-ex-in = Predicado IN com Subconsultas
predicates-ex-combined = Operações de Predicados Combinadas

# Subqueries Showcase
subqueries-title = Subconsultas SQL
subqueries-description = Capacidades de subconsultas SQL:1999 Core para operações de consultas aninhadas
subqueries-reference = Referência de Tipos de Subconsulta
subqueries-table-type = Tipo de Subconsulta
subqueries-progress-type = tipos de subconsulta
subqueries-ex-scalar-select = Subconsulta Escalar em SELECT
subqueries-ex-scalar-where = Subconsulta Escalar em WHERE
subqueries-ex-derived = Tabelas Derivadas (Subconsulta em FROM)
subqueries-ex-in = Predicado IN com Subconsulta
subqueries-ex-correlated = Subconsultas Correlacionadas
subqueries-ex-nested = Subconsultas Aninhadas
