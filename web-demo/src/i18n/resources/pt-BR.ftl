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
error-no-databases = Nenhum banco de dados disponível

# Loading states
loading-initializing-theme = Inicializando tema
loading-preparing-editor = Preparando editor
loading-database-engine = Carregando mecanismo de banco de dados
loading-setting-up-ui = Configurando interface do usuário
loading-editor = Carregando editor...
loading-compliance-data = Carregando dados de conformidade...
loading-conformance-report = Carregando relatório de conformidade...

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

# =============================================================================
# Benchmarks Page
# =============================================================================

# Cabeçalhos de seção
bench-section-embedded = Embutido
bench-section-server = Servidor
bench-results-title = Resultados de Benchmark
bench-perf-comparison = Comparação de Desempenho
bench-methodology-title = Metodologia
bench-analysis-roadmap = Análise e Roadmap

# Cartões de resumo
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = Operações Testadas
bench-last-updated = Última Atualização
bench-avg-speedup = aceleração média
bench-from-main = da branch main
bench-loading = Carregando...
bench-na = N/D
bench-faster = { $value }x mais rápido
bench-slower = { $value }x mais lento
bench-speedup = { $value }x
bench-startup-time-label = tempo de inicialização
bench-download-size = tamanho do download
bench-uncompressed = descomprimido
bench-size-metrics = métricas de tamanho
bench-failed = FALHOU
bench-failed-title = Consulta falhou (timeout ou erro)
bench-no-wasm-data = Nenhum dado WASM disponível
bench-no-server-data = Nenhum dado de benchmark de servidor Sysbench disponível
bench-no-server-data-hint = Os benchmarks de servidor requerem a execução do sysbench_server com a comparação MySQL habilitada.

# Table headers
bench-table-operation = Operação
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = Carregando resultados de benchmark...
bench-vibesql-server-title = VibeSQL via protocolo wire PostgreSQL

# Termos comuns de benchmark
bench-hardware = Hardware
bench-benchmark-framework = Framework de Benchmark
bench-scale-factor = Fator de Escala
bench-data = Dados
bench-databases-tested = Bancos de Dados Testados
bench-execution-mode = Modo de Execução
bench-measurement = Medição
bench-workload = Carga de Trabalho
bench-transaction-mix = Mix de Transações
bench-warehouses = Armazéns
bench-concurrency = Concorrência
bench-acid-compliance = Conformidade ACID
bench-mode = Modo
bench-workload-types = Tipos de Carga de Trabalho
bench-table-size = Tamanho da Tabela
bench-index-types = Tipos de Índice
bench-operations = Operações
bench-databases = Bancos de Dados
bench-protocol-overhead = Overhead de Protocolo
bench-binary-size = Tamanho do Binário
bench-startup-time = Tempo de Inicialização
bench-peak-memory = Memória de Pico
bench-schema = Esquema
bench-query-count = Contagem de Consultas
bench-query-types = Tipos de Consulta
bench-sql-features = Recursos SQL
bench-wasm-size = Tamanho WASM
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H específico
bench-tpch-name = TPC-H
bench-tpch-title = Benchmark de Suporte à Decisão TPC-H
bench-tpch-description = Esses benchmarks usam a suíte de benchmark padrão da indústria <strong>TPC-H</strong>, que simula cargas de trabalho de suporte à decisão do mundo real com consultas analíticas complexas envolvendo agregações, joins, subconsultas e ordenação.
bench-tpch-ops-label = Consultas TPC-H
bench-tpch-note-intro = Todos os benchmarks medem o tempo de execução de consulta de ponta a ponta, incluindo análise, planejamento, execução e materialização de resultados. Isso representa o <strong>desempenho real do motor SQL</strong> para cargas de trabalho analíticas.
bench-tpch-note-queries = <strong>Nota:</strong> As consultas TPC-H testam diferentes aspectos do desempenho SQL: agregações simples (Q1, Q6), joins complexos (Q2-Q5, Q7-Q10), subconsultas (Q11-Q15) e análises avançadas (Q16-Q22). Passe o mouse sobre os nomes das consultas na tabela acima para ver descrições.

# Discussão TPC-H
bench-tpch-disc-excels-title = Onde o VibeSQL se Destaca
bench-tpch-disc-excels = O VibeSQL mostra forte desempenho em <strong>consultas de agregação pesadas em varredura</strong> (Q1, Q6, Q14, Q15, Q20) onde nosso motor de execução colunar e agregações aceleradas por SIMD se destacam. Essas consultas envolvem filtragem de tabelas grandes e cálculo de agregados sem padrões de join complexos.
bench-tpch-disc-targets-title = Alvos de Otimização Atuais
bench-tpch-disc-targets = Consultas de join multi-way (Q3, Q5, Q7-Q10, Q18, Q19, Q21) atualmente mostram SQLite à frente. O principal gargalo é nossa implementação de hash join, que ainda não emprega o mesmo nível de otimização dos joins B-tree refinados por décadas do SQLite. Áreas específicas em desenvolvimento ativo:
bench-tpch-disc-join-ordering = Estimativa de cardinalidade aprimorada para melhor seleção de ordem de join
bench-tpch-disc-hash-sizing = Crescimento adaptativo de tabela hash e spill-to-disk para joins grandes
bench-tpch-disc-vectorized = Processamento em lote no loop interno do join para melhorar utilização de cache
bench-tpch-disc-inl-joins = Aproveitamento de índices B-tree quando benéfico
bench-tpch-disc-path-title = Caminho para Liderança
bench-tpch-disc-path = A arquitetura do VibeSQL é projetada para hardware moderno com recursos como armazenamento colunar, execução vetorizada e concorrência livre de locks. Conforme essas otimizações amadurecem, esperamos que o VibeSQL alcance liderança consistente em todas as consultas TPC-H. O design fundamental suporta paralelismo e SIMD que bancos de dados row-store tradicionais não podem facilmente adaptar.

# Descrições de Consultas TPC-H
bench-tpch-q1 = Relatório Resumo de Preços - Agregação de preços com GROUP BY e ORDER BY
bench-tpch-q2 = Fornecedor de Menor Custo - JOIN de 3 tabelas com ORDER BY e LIMIT
bench-tpch-q3 = Prioridade de Envio - JOIN de 3 tabelas com agregação
bench-tpch-q4 = Verificação de Prioridade de Pedido - Subconsulta EXISTS correlacionada
bench-tpch-q5 = Volume do Fornecedor Local - JOIN de 6 tabelas com filtragem complexa
bench-tpch-q6 = Previsão de Mudança de Receita - Filtros WHERE com BETWEEN e SUM
bench-tpch-q7 = Volume de Envio - JOIN de 6 tabelas com SUBSTR e filtragem de data
bench-tpch-q8 = Participação no Mercado Nacional - JOIN de 7 tabelas com expressões CASE
bench-tpch-q9 = Medida de Lucro por Tipo de Produto - JOIN de 4 tabelas com agregação
bench-tpch-q10 = Relatório de Itens Devolvidos - JOIN de 4 tabelas com TOP-N LIMIT
bench-tpch-q11 = Identificação de Estoque Importante - Subconsulta em cláusula HAVING
bench-tpch-q12 = Prioridade de Modos de Envio - Agregação CASE com lógica de data
bench-tpch-q13 = Distribuição de Clientes - LEFT OUTER JOIN com subconsulta
bench-tpch-q14 = Efeito de Promoção - Agregação condicional com CASE
bench-tpch-q15 = Principal Fornecedor - Subconsultas aninhadas com MAX
bench-tpch-q16 = Relacionamento Peças/Fornecedor - Subconsulta NOT IN com DISTINCT
bench-tpch-q17 = Receita de Pedidos de Pequena Quantidade - Subconsulta correlacionada em WHERE
bench-tpch-q18 = Cliente de Grande Volume - GROUP BY com HAVING
bench-tpch-q19 = Receita com Desconto - Condições OR complexas
bench-tpch-q20 = Promoção Potencial de Peças - Subconsulta IN com GROUP BY/HAVING
bench-tpch-q21 = Fornecedores que Atrasaram Pedidos - EXISTS multi-tabela
bench-tpch-q22 = Oportunidade de Vendas Global - SUBSTR com subconsulta NOT EXISTS

# TPC-DS específico
bench-tpcds-name = TPC-DS
bench-tpcds-title = Benchmark de Suporte à Decisão TPC-DS
bench-tpcds-description = <strong>TPC-DS</strong> é o sucessor do TPC-H, apresentando 99 consultas que modelam um sistema moderno de suporte à decisão com padrões de consulta significativamente mais complexos, incluindo múltiplas tabelas de fatos, esquema floco de neve e recursos SQL avançados.
bench-tpcds-ops-label = Consultas TPC-DS
bench-tpcds-note-intro = As consultas TPC-DS são substancialmente mais complexas que TPC-H, testando recursos SQL avançados como funções de janela, expressões de tabela comuns (cláusula WITH) e padrões de join complexos entre múltiplas tabelas de fatos e dimensões.
bench-tpcds-note-remaining = <strong>Nota:</strong> As consultas restantes não suportadas requerem recursos como INTERSECT/EXCEPT ou funções específicas de aritmética de datas ainda não implementadas.

# Discussão TPC-DS
bench-tpcds-disc-coverage-title = Cobertura de Recursos SQL:1999
bench-tpcds-disc-coverage = O TPC-DS exercita os recursos SQL mais exigentes. O VibeSQL passa <strong>88 de 99 consultas</strong>, demonstrando ampla cobertura de SQL:1999 incluindo ROLLUP, CUBE, GROUPING(), funções de janela com framing complexo e CTEs recursivos. As consultas restantes requerem operações de conjunto INTERSECT/EXCEPT.
bench-tpcds-disc-optimization-title = Otimização de Consultas Complexas
bench-tpcds-disc-optimization = Consultas TPC-DS frequentemente juntam 10+ tabelas com subconsultas correlacionadas. Áreas de foco atuais:
bench-tpcds-disc-cte = Decisão inteligente entre CTEs materializados e inline
bench-tpcds-disc-decorrelation = Converter subconsultas correlacionadas em joins quando benéfico
bench-tpcds-disc-star = Ordenação de joins fato-dimensão para padrões analíticos
bench-tpcds-disc-toward-title = Rumo a 99/99
bench-tpcds-disc-toward = INTERSECT e EXCEPT são adições planejadas que habilitarão as consultas restantes. Essas operações de conjunto se encaixam naturalmente em nossa álgebra de consulta existente e serão implementadas como operadores baseados em hash, similares ao nosso processamento DISTINCT.

# TPC-C específico
bench-tpcc-name = TPC-C
bench-tpcc-title = Benchmark de Processamento de Transações Online TPC-C
bench-tpcc-description = O <strong>benchmark TPC-C</strong> simula um ambiente completo de entrada de pedidos com uma mistura de transações complexas incluindo entrada de pedidos, processamento de pagamentos, consultas de status de pedidos, processamento de entregas e monitoramento de nível de estoque.
bench-tpcc-ops-label = Transações TPC-C
bench-tpcc-note-intro = O TPC-C mede transações por minuto (tpmC) e testa a capacidade do banco de dados de lidar com transações concorrentes com lógica de negócios complexa. Este benchmark é crítico para avaliar o <strong>desempenho de cargas de trabalho transacionais</strong>.
bench-tpcc-note-results = <strong>Nota:</strong> Os resultados mostram a latência média de transação. Menor é melhor. O TPC-C é particularmente exigente para cargas de trabalho com escrita intensiva com requisitos estritos de consistência.

# Descrições de Transações TPC-C
bench-tpcc-new-order = Novo Pedido - Transação complexa com verificações de inventário e criação de pedido
bench-tpcc-payment = Pagamento - Atualizar saldo do cliente e totais de armazém/distrito
bench-tpcc-order-status = Status do Pedido - Consulta somente leitura para histórico de pedidos do cliente
bench-tpcc-delivery = Entrega - Processamento em lote de pedidos pendentes
bench-tpcc-stock-level = Nível de Estoque - Contar itens abaixo do limite em pedidos recentes

# Discussão TPC-C
bench-tpcc-disc-faster-title = { $speedup } Mais Rápido que SQLite
bench-tpcc-disc-faster = O VibeSQL alcança <strong>~79.000 transações por segundo</strong> comparado às ~1.900 TPS do SQLite, uma melhoria de 42x. Essa aceleração dramática vem da nossa arquitetura MVCC livre de locks que evita o bloqueio de granularidade grossa do SQLite em cada operação de escrita.
bench-tpcc-disc-dominates-title = Por que o VibeSQL Domina OLTP
bench-tpcc-disc-lockfree = MVCC permite que leitores e escritores procedam concorrentemente sem bloqueio
bench-tpcc-disc-optimistic = Transações só conflitam no momento do commit, não durante a execução
bench-tpcc-disc-btree = Estrutura de índice construída para propósito específico otimizada para cargas de trabalho em memória
bench-tpcc-disc-prepared = Planos de consulta são compilados uma vez e reutilizados
bench-tpcc-disc-scaling-title = Escalando Ainda Mais
bench-tpcc-disc-scaling = Os resultados atuais são single-threaded. A arquitetura do VibeSQL suporta processamento de transações multi-threaded, e esperamos escalonamento quase linear conforme adicionamos suporte a execução paralela. Nosso objetivo é alcançar 500K+ TPS em hardware multi-core moderno.

# Sysbench Embedded specific
bench-sysbench-embedded-name = Sysbench (Embutido)
bench-sysbench-embedded-title = Micro-Benchmarks Sysbench (Embutido)
bench-sysbench-embedded-description = <strong>Sysbench</strong> fornece micro-benchmarks focados que isolam operações específicas de banco de dados. Esses testes medem o desempenho bruto para operações fundamentais sem a complexidade de cargas de trabalho de transação completas.
bench-sysbench-embedded-ops-label = Operações Sysbench
bench-sysbench-embedded-note = O modo embutido executa o banco de dados em processo com zero overhead de rede, ideal para aplicações de processo único onde latência mínima é crítica.

# Descrições de Operações Sysbench
bench-sysbench-point-select = Seleção Pontual - Consulta de linha única por chave primária
bench-sysbench-insert = Inserção - Inserir novas linhas na tabela
bench-sysbench-update-index = Atualização de Índice - Atualizar coluna indexada (k = k + 1)
bench-sysbench-update-non-index = Atualização Sem Índice - Atualizar coluna não indexada
bench-sysbench-delete = Exclusão - Remover linhas por chave primária
bench-sysbench-range-queries = Consultas de Intervalo - Varreduras de intervalo simples, SUM, ORDER BY e DISTINCT

# Discussão do Sysbench Embutido
bench-sysbench-emb-disc-point-title = Consultas Pontuais: Gap de { $pointRatio }
bench-sysbench-emb-disc-point = As seleções pontuais do VibeSQL rodam em <strong>~{ $pointVibesqlUs }µs vs ~{ $pointSqliteUs }µs do SQLite</strong>. Este gap de { $pointRatio } representa nosso objetivo principal de otimização OLTP - estamos investigando layout de nós B-tree e caminhos de leitura sem bloqueio para fechar este gap.
bench-sysbench-emb-disc-index-title = Atualizações de Índice: Gap de { $indexRatio }
bench-sysbench-emb-disc-index = As atualizações indexadas do VibeSQL rodam em <strong>~{ $indexVibesqlUs }µs vs ~{ $indexSqliteUs }µs do SQLite</strong>. Esta é uma área para otimização já que nosso design MVCC adiciona overhead para manutenção de índice que estamos trabalhando para reduzir.
bench-sysbench-emb-disc-improve-title = Áreas para Melhoria
bench-sysbench-emb-disc-bulk = O caminho de inserção em lote do SQLite é altamente otimizado; estamos adicionando operações de B-tree em lote
bench-sysbench-emb-disc-nonindex = Atualizações não indexadas mostram VibeSQL em ~{ $nonIndexVibesqlUs }µs vs ~{ $nonIndexSqliteUs }µs do SQLite
bench-sysbench-emb-disc-deletes = Operações de exclusão mostram VibeSQL em ~{ $deleteVibesqlUs }µs vs ~{ $deleteSqliteUs }µs do SQLite
bench-sysbench-emb-disc-architecture-title = Compensações Arquiteturais
bench-sysbench-emb-disc-architecture = A arquitetura híbrida do VibeSQL visa tanto cargas de trabalho OLTP quanto OLAP. Nosso armazenamento B-tree fornece desempenho de consulta pontual competitivo com SQLite, enquanto a execução colunar lida com consultas analíticas de forma eficiente. Isso difere de bancos de dados OLAP puros como o DuckDB, que otimizam exclusivamente para operações em massa ao custo de latência de linha única.

# Sysbench Servidor específico
bench-sysbench-server-name = Sysbench (Servidor)
bench-sysbench-server-title = Micro-Benchmarks Sysbench (Servidor)
bench-sysbench-server-description = Os benchmarks de servidor <strong>Sysbench</strong> comparam o VibeSQL Server (protocolo wire PostgreSQL) com o MySQL, medindo o desempenho para implantações de banco de dados multi-cliente.
bench-sysbench-server-ops-label = Operações Sysbench
bench-sysbench-server-note = O modo servidor usa o protocolo wire PostgreSQL, permitindo acesso multi-cliente e compatibilidade com ferramentas e drivers PostgreSQL existentes.

# Discussão do Sysbench Servidor
bench-sysbench-srv-disc-protocol-title = Protocolo Wire PostgreSQL
bench-sysbench-srv-disc-protocol = O VibeSQL Server implementa o protocolo wire PostgreSQL, permitindo compatibilidade com drivers e ferramentas PostgreSQL existentes. Isso adiciona ~10-50µs de overhead de protocolo por consulta comparado ao modo embutido, mas permite implantações multi-cliente.
bench-sysbench-srv-disc-mysql-title = Comparação com MySQL
bench-sysbench-srv-disc-mysql = Os benchmarks de servidor comparam com o MySQL para avaliar o VibeSQL como substituto direto para bancos de dados cliente-servidor tradicionais. O VibeSQL Server supera o MySQL em todas as operações Sysbench, com acelerações de <strong>2,4x</strong> (consultas de intervalo) a <strong>12,8x</strong> (atualizações indexadas).
bench-sysbench-srv-disc-perf-title = Por que o VibeSQL Server é mais rápido
bench-sysbench-srv-disc-perf-arch = A arquitetura do VibeSQL difere fundamentalmente do design RDBMS tradicional do MySQL
bench-sysbench-srv-disc-perf-storage = O VibeSQL usa um mecanismo de armazenamento colunar em memória otimizado para cargas analíticas e OLTP, evitando a sobrecarga de gerenciamento de páginas InnoDB baseado em disco do MySQL
bench-sysbench-srv-disc-perf-locking = Sem bloqueio pesado em nível de linha nem contabilidade MVCC—o VibeSQL usa controle de concorrência leve projetado para CPUs multi-core modernas
bench-sysbench-srv-disc-perf-protocol = Implementação eficiente do protocolo PostgreSQL com sobrecarga de serialização mínima comparada ao protocolo do MySQL
bench-sysbench-srv-disc-perf-writes = Operações de escrita (inserções/atualizações) mostram os maiores ganhos (<strong>8-12x</strong>) porque o VibeSQL evita a sincronização de redo log, undo log e doublewrite buffer do MySQL
bench-sysbench-srv-disc-perf-reads = Operações de leitura mostram ganhos menores mas consistentes (<strong>2-3x</strong>) devido a padrões de acesso colunar eficientes em cache e zero E/S de disco
bench-sysbench-srv-disc-roadmap-title = Roadmap do Servidor
bench-sysbench-srv-disc-pooling = Reduzir overhead de estabelecimento de conexão para cenários de alto throughput
bench-sysbench-srv-disc-caching = Cache de planos de consulta no lado do servidor entre conexões
bench-sysbench-srv-disc-extended = Suporte completo ao protocolo de consulta estendida PostgreSQL para operações em lote

# TPC-H Server específico
bench-tpch-server-name = TPC-H (Servidor)
bench-tpch-server-title = Benchmark Analítico TPC-H (Servidor)
bench-tpch-server-description = Os <strong>benchmarks de servidor TPC-H</strong> comparam o VibeSQL Server (protocolo PostgreSQL) com o MySQL para cargas de trabalho de consultas analíticas, medindo o desempenho OLAP em implantações cliente-servidor.
bench-tpch-server-ops-label = consultas TPC-H
bench-tpch-server-note-intro = Os benchmarks de servidor testam a implementação do <strong>protocolo PostgreSQL</strong>, medindo a latência de consulta de ponta a ponta incluindo overhead de rede.
bench-tpch-server-note-queries = As consultas testam JOINs complexos, subconsultas e agregações típicas de cargas de trabalho de business intelligence.

# Discussão TPC-H Server
bench-tpch-srv-disc-protocol-title = Protocolo PostgreSQL
bench-tpch-srv-disc-protocol = O VibeSQL Server fala o protocolo PostgreSQL, permitindo o uso de drivers e ferramentas PostgreSQL padrão. Este benchmark mede a latência completa de ponta a ponta incluindo overhead do protocolo.
bench-tpch-srv-disc-comparison-title = Comparação com MySQL
bench-tpch-srv-disc-comparison = A comparação com o MySQL fornece uma linha de base para bancos de dados cliente-servidor tradicionais em cargas de trabalho analíticas. O motor de execução colunar do VibeSQL oferece vantagens para agregações e joins complexos.
bench-tpch-srv-disc-roadmap-title = Roadmap OLAP Server
bench-tpch-srv-disc-prepared = Reutilizar planos de consulta compilados entre conexões
bench-tpch-srv-disc-pooling = Tratamento eficiente de conexões para cenários de alto throughput
bench-tpch-srv-disc-scale = Testes de conjuntos de dados maiores (SF 0.1, SF 1.0) para validação em escala de produção

# TPC-C Server específico
bench-tpcc-server-name = TPC-C (Servidor)
bench-tpcc-server-title = Benchmark OLTP TPC-C (Servidor)
bench-tpcc-server-description = Os <strong>benchmarks de servidor TPC-C</strong> comparam o VibeSQL Server (protocolo PostgreSQL) com o MySQL para cargas de trabalho de transações OLTP, medindo o throughput para implantações de banco de dados multi-cliente.
bench-tpcc-server-ops-label = transações TPC-C
bench-tpcc-server-note-intro = Os benchmarks de servidor testam a implementação do <strong>protocolo PostgreSQL</strong>, medindo o throughput transacional incluindo overhead de rede.
bench-tpcc-server-note-results = Os resultados relatam transações por segundo (TPS) para o mix de transações TPC-C padrão.
bench-tpcc-mixed = Carga Mista - Mix de transações TPC-C padrão (45% Nova-Ordem, 43% Pagamento, 4% Status-Ordem, 4% Entrega, 4% Nível-Estoque)

# Discussão TPC-C Server
bench-tpcc-srv-disc-protocol-title = Protocolo PostgreSQL
bench-tpcc-srv-disc-protocol = O VibeSQL Server fala o protocolo PostgreSQL, permitindo o uso de drivers e ferramentas PostgreSQL padrão. Este benchmark mede a latência transacional completa de ponta a ponta incluindo overhead do protocolo.
bench-tpcc-srv-disc-comparison-title = Comparação com MySQL
bench-tpcc-srv-disc-comparison = A comparação com o MySQL fornece uma linha de base para bancos de dados cliente-servidor tradicionais em cargas de trabalho OLTP. O MySQL é o padrão da indústria para cargas de trabalho transacionais, e TPC-C é o ponto forte do MySQL.
bench-tpcc-srv-disc-roadmap-title = Roadmap OLTP Server
bench-tpcc-srv-disc-prepared = Reutilizar planos de consulta compilados entre conexões
bench-tpcc-srv-disc-pooling = Tratamento eficiente de conexões para cenários de alto throughput
bench-tpcc-srv-disc-parallel = Processamento concorrente de transações multi-cliente

# Footprint Embedded específico
bench-footprint-embedded-name = Footprint (Embedded)
bench-footprint-embedded-title = Native Binary Footprint
bench-footprint-embedded-description = <strong>Embedded footprint benchmarks</strong> measure the resource efficiency of native database binaries, comparing binary size, cold startup time, and peak memory usage.
bench-footprint-embedded-ops-label = databases compared
bench-footprint-embedded-note = Native binary footprint is critical for <strong>embedded and edge deployments</strong> where binary size, startup latency, and memory consumption directly impact deployment feasibility.

# Footprint Embedded Descriptions
bench-footprint-binary-size = Binary Size - Size of the compiled database binary on disk
bench-footprint-startup-time = Startup Time - Time to cold-start and execute first query
bench-footprint-peak-memory = Peak Memory - Maximum resident set size during initialization

# Footprint Embedded Discussion
bench-footprint-emb-disc-size-title = Binary Size: Middle Ground
bench-footprint-emb-disc-size = VibeSQL at <strong>~{ $vibesqlBinaryMb }MB</strong> sits between SQLite (~{ $sqliteBinaryMb }MB) and DuckDB (~{ $duckdbBinaryMb }MB). This reflects our choice to include advanced features (window functions, CTEs, columnar execution) while keeping the binary manageable for embedded deployments.
bench-footprint-emb-disc-startup-title = Startup: Fastest Cold Start
bench-footprint-emb-disc-startup = VibeSQL achieves <strong>~7.7ms cold startup</strong>, slightly faster than SQLite (~{ $sqliteStartupMs }ms) and significantly faster than DuckDB (~{ $duckdbStartupMs }ms). Our minimal initialization path loads only essential metadata structures on startup.
bench-footprint-emb-disc-memory-title = Memory Efficiency
bench-footprint-emb-disc-memory = Peak memory during startup is ~7MB for VibeSQL vs ~3MB for SQLite and ~11MB for DuckDB. The difference from SQLite reflects our more sophisticated query optimizer and columnar execution infrastructure that's allocated upfront.
bench-footprint-emb-disc-roadmap-title = Size Reduction Roadmap
bench-footprint-emb-disc-flags = Compile-time feature selection to exclude unused functionality
bench-footprint-emb-disc-lto = Whole-program link-time optimization for dead code elimination
bench-footprint-emb-disc-modular = Separate core engine from optional features (e.g., window functions)

# Footprint Server/WASM specific
bench-footprint-server-name = Footprint (Server/WASM)
bench-footprint-server-title = WASM Footprint
bench-footprint-server-description = <strong>WASM footprint benchmarks</strong> measure the WebAssembly module size for browser deployment, critical for web applications where download size impacts user experience.
bench-footprint-server-ops-label = deployment targets
bench-footprint-server-note = WASM sizes are critical for <strong>web deployments</strong> where download time directly impacts time-to-interactive. Gzip sizes are most relevant as browsers automatically decompress gzip content.
bench-footprint-server-note2 = <strong>Note:</strong> VibeSQL WASM is designed for minimal download size while maintaining full SQL:1999 compliance in the browser.

# Footprint Server Descriptions
bench-footprint-wasm-size = WASM Size - Size of the WebAssembly module for browser deployment
bench-footprint-wasm-gzip = WASM (gzip) - Compressed size for web delivery

# Footprint Server Discussion
bench-footprint-srv-disc-wasm-title = WASM: { $wasmSizeGzipMb }MB Compressed
bench-footprint-srv-disc-wasm = VibeSQL's WebAssembly module compresses to <strong>~{ $wasmSizeGzipMb }MB gzipped</strong>, enabling fast initial page loads. This is a full SQL:1999 database with window functions, CTEs, and ACID transactions running entirely in the browser.
bench-footprint-srv-disc-included-title = What's Included
bench-footprint-srv-disc-parser = Complete SQL parser and query optimizer
bench-footprint-srv-disc-btree = B-tree storage engine with MVCC
bench-footprint-srv-disc-window = Window functions and advanced aggregations
bench-footprint-srv-disc-cte = Common table expressions (WITH clause)
bench-footprint-srv-disc-acid = Full ACID transaction support
bench-footprint-srv-disc-benefits-title = Browser Deployment Benefits
bench-footprint-srv-disc-benefits = Running SQL in the browser eliminates round-trip latency to servers, enables offline-first applications, and keeps sensitive data on the user's device. VibeSQL's WASM build is designed for this use case with minimal dependencies and efficient memory usage.
bench-footprint-srv-disc-roadmap-title = WASM Roadmap
bench-footprint-srv-disc-streaming = Start executing while the module downloads
bench-footprint-srv-disc-indexeddb = Durable storage across browser sessions
bench-footprint-srv-disc-worker = Run queries off the main thread for responsive UIs

# Bullet point labels (used with descriptions)
bench-bullet-join-ordering = Join ordering
bench-bullet-hash-sizing = Hash table sizing
bench-bullet-vectorized = Vectorized joins
bench-bullet-inl-joins = Index-nested-loop joins
bench-bullet-cte-materialization = CTE materialization
bench-bullet-decorrelation = Subquery decorrelation
bench-bullet-star-optimization = Star schema optimization
bench-bullet-lock-free = Lock-free reads
bench-bullet-optimistic = Optimistic concurrency
bench-bullet-btree = In-memory B-tree
bench-bullet-prepared = Prepared statement caching
bench-bullet-bulk-inserts = Bulk inserts
bench-bullet-non-indexed = Non-indexed updates
bench-bullet-deletes = Deletes
bench-bullet-connection-pooling = Connection pooling
bench-bullet-stmt-caching = Prepared statement caching
bench-bullet-extended-protocol = Extended query protocol
bench-bullet-concurrency = Concorrência leve
bench-bullet-protocol = Eficiência do protocolo
bench-bullet-writes = Operações de escrita
bench-bullet-reads = Operações de leitura
bench-bullet-feature-flags = Feature flags
bench-bullet-lto = LTO optimization
bench-bullet-modular = Modular builds
bench-bullet-streaming = Streaming compilation
bench-bullet-indexeddb = IndexedDB persistence
bench-bullet-worker = Suporte a worker threads
bench-bullet-prepared-stmts = Prepared statements
bench-bullet-larger-scale = Fatores de escala maiores
bench-bullet-parallel-clients = Clientes paralelos

# =============================================================================
# Conformance Page
# =============================================================================

# Overview section
conformance-sql-conformance = SQL Conformance
conformance-testing-against = Testing against SQLLogicTest - the industry standard SQL test suite
conformance-full-pass-rate = 100% File Pass Rate Achieved!
conformance-tests-passing = Tests Passing
conformance-files-passing = Files Passing
conformance-loading = Loading conformance report...
conformance-error-loading = Error Loading Report
conformance-no-data = No conformance data available

# Category breakdown
conformance-category-title = Test Coverage by Category
conformance-category-header = Category
conformance-pass-rate-header = Pass Rate
conformance-progress-header = Progress
conformance-tests-header = Tests
conformance-cat-select = SELECT Queries
conformance-cat-aggregates = Aggregates
conformance-cat-joins = JOINs
conformance-cat-expressions = Expressions
conformance-cat-subqueries = Subqueries
conformance-cat-index = Index Operations
conformance-cat-ddl = DDL Statements
conformance-cat-evidence = Evidence Tests
conformance-cat-random = Random Tests
conformance-cat-other = Other Tests

# Timeline
conformance-timeline-title = Pass Rate History
conformance-timeline-desc = Conformance progress over the last 90 days
conformance-timeline-loading = Loading chart data...

# Milestones
conformance-milestones-title = Milestones

# Running tests locally
conformance-running-locally-title = Running Tests Locally
conformance-run-sqltest = # Run SQL:1999 conformance tests
conformance-run-sqllogictest = # Run SQLLogicTest suite (takes hours)
conformance-generate-coverage = # Generate coverage report
conformance-open-coverage = # Open coverage report

bench-table-query = Query
bench-tpcc-disc-duckdb = DuckDB achieves only ~{ $duckdbTps } TPS on TPC-C (~{ $duckdbVsVibesql } slower than VibeSQL, 12x slower than SQLite). This is expected: DuckDB is an <strong>analytical (OLAP) database</strong> optimized for large batch operations, not single-row transactions. Its columnar storage format excels at scanning millions of rows but adds overhead for point lookups and small updates that dominate OLTP workloads like TPC-C.
bench-tpcc-disc-duckdb-title = Why DuckDB Lags on OLTP
bench-tpcc-transactions-label = transactions executed

# Conformance page (English placeholders)
conformance-additional-features = Additional Features
conformance-bottom-line = <strong>Bottom Line:</strong> We use two complementary test suites to ensure both standards conformance (sqltest) and practical correctness (SQLLogicTest). High pass rates in both demonstrate serious SQL:1999 implementation quality, though formal Core certification would require testing against official NIST suites.
conformance-commit = Commit:
conformance-core-explanation = SQL:1999 Core is the official mandatory feature set defined in the SQL:1999 (ISO/IEC 9075:1999) standard. It consists of approximately 169 required features that any database claiming Core compliance must implement. Official Core compliance is verified through the NIST SQL Test Suite, not community test suites.
conformance-core-features = Core Features (E-Series)
conformance-coverage-point = <span class="font-medium">Coverage:</span> sqltest covers 739 standard feature tests; SQLLogicTest covers practical scenarios
conformance-e011 = Numeric data types
conformance-e021 = Character string types
conformance-e031 = Identifiers
conformance-e051 = Basic query specification
conformance-e061 = Basic predicates and search conditions
conformance-e071 = Basic query expressions
conformance-e081 = Basic privileges
conformance-e091 = Set functions
conformance-e101 = Basic data manipulation
conformance-e111 = Single row SELECT statement
conformance-e121 = Basic cursor support
conformance-e131 = Null value support
conformance-e141 = Basic integrity constraints
conformance-e151 = Transaction support
conformance-e161 = SQL comments
conformance-error-label = Error:
conformance-errors = Errors
conformance-explanation-title = Understanding Our Test Suites
conformance-f031 = Basic schema manipulation
conformance-failed = Failed
conformance-failing-tests-desc = The following tests are currently failing. Click to expand details.
conformance-failing-tests-title = Failing Tests
conformance-files-of-passing = { $passed } of { $total } test files passing
conformance-generated = Generated:
conformance-how-complement = How do they complement each other?
conformance-overall-pass-rate = Overall Pass Rate
conformance-pass-rates-mean = Our <strong>{ $sqltestRate }% sqltest pass rate</strong> ({ $sqltestPassed }/{ $sqltestTotal } tests) demonstrates strong SQL:1999 grammar conformance. { $sltInfo } Together, these results indicate comprehensive SQL:1999 compliance, though they do not constitute official Core certification.
conformance-passed = Passed
conformance-philosophy-point = <span class="font-medium">Philosophy:</span> sqltest says "can you parse this?"; SQLLogicTest says "does this work correctly?"
conformance-slt-ddl = DDL Tests
conformance-slt-desc = Results from the comprehensive <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a> suite containing ~{ $testCases } tests across { $testFiles } test files from the official SQLite corpus.
conformance-slt-evidence = Evidence Tests
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> is a comprehensive test suite originally developed for SQLite, containing ~{ $testCases } SQL test cases across { $testFiles } test files. It tests practical correctness by running real-world queries and validating results. This suite focuses on semantic correctness and edge cases rather than pure grammar conformance.
conformance-slt-index = Index Tests
conformance-slt-note = <strong>Note:</strong> SQLLogicTest provides a different perspective from sqltest. While sqltest focuses on BNF grammar conformance from the SQL:1999 specification, SQLLogicTest contains millions of real-world SQL queries testing practical correctness across a wide range of scenarios.
conformance-slt-other = Other Tests
conformance-slt-pass-info = Our <strong>{ $sltRate }% SQLLogicTest pass rate</strong> ({ $sltPassed }/{ $sltTotal } test files) shows we handle real-world queries correctly.
conformance-slt-random = Random Tests
conformance-slt-select = SELECT Tests
conformance-slt-title = SQLLogicTest Results
conformance-slt-validates = <span class="font-medium">SQLLogicTest (Result-driven):</span> Validates semantic correctness with millions of real queries
conformance-sqltest-desc = Results from <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a> - a community-maintained BNF-driven conformance test suite derived from the SQL:1999 standard, containing 739 tests covering Core and Foundation features.
conformance-sqltest-explanation = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a> is a community-maintained test suite by Elliot Chance that provides BNF-driven conformance tests derived from the SQL:1999 standard. It contains 739 tests covering Core and Foundation features across E-series and F-series test categories. This suite tests whether our implementation conforms to the SQL:1999 grammar specification.
conformance-sqltest-title = sqltest Results
conformance-sqltest-validates = <span class="font-medium">sqltest (BNF-driven):</span> Validates grammar conformance to SQL:1999 standard specifications
conformance-status = Status:
conformance-test-categories = Test Categories
conformance-test-coverage = Test Coverage
conformance-tests-of-passing = { $passed } of { $total } tests passing
conformance-view-failing = View failing test details ({ $count } tests)
conformance-what-is-core = What is SQL:1999 Core?
conformance-what-is-slt = What is SQLLogicTest?
conformance-what-is-sqltest = What is sqltest?
conformance-what-mean = What do our pass rates mean?

# PostgreSQL Regression Tests
conformance-pgsql-title = Testes de Regressão PostgreSQL
conformance-pgsql-desc = Resultados da execução da <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suíte de testes de regressão do PostgreSQL</a> - a suíte de testes canônica usada para validar a compatibilidade com PostgreSQL.
conformance-pgsql-tests-passing = testes aprovados
conformance-pgsql-tests-excluded = testes excluídos
conformance-pgsql-pass-rate = Taxa de Aprovação
conformance-pgsql-excluded-reason = Os testes excluídos usam recursos específicos do PostgreSQL não aplicáveis ao VibeSQL
conformance-pgsql-note = <strong>Nota:</strong> Os testes de regressão do PostgreSQL validam o comportamento SQL contra a implementação de referência do PostgreSQL. Os testes excluídos envolvem recursos específicos do PostgreSQL como catálogos do sistema, linguagens procedurais ou módulos de extensão.

# Seção da Suíte de Testes TCL do SQLite
conformance-tcl-title = Suíte de Testes TCL do SQLite
conformance-tcl-desc = Resultados da <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suíte de testes TCL</a> canônica do SQLite contendo { $fileCount } arquivos de teste. Esta suíte é o padrão ouro para testes de compatibilidade com SQLite.
conformance-tcl-overall-rate = Taxa de Aprovação Geral
conformance-tcl-tests-passing = { $passed } de { $total } testes aprovados
conformance-tcl-passed = Aprovados
conformance-tcl-failed = Reprovados
conformance-tcl-skipped = Ignorados
conformance-tcl-total = Total
conformance-tcl-categories-title = Categorias de Testes
conformance-tcl-category = Categoria
conformance-tcl-rate = Taxa
conformance-tcl-progress = Progresso
conformance-tcl-tests = Testes
conformance-tcl-common-failures = Falhas Comuns
conformance-tcl-failure-patterns = Top { $count } padrões de falha por contagem de ocorrências
conformance-tcl-about-title = Sobre os Testes TCL:
conformance-tcl-about-text = A suíte de testes TCL do SQLite é o teste de conformidade canônico para compatibilidade com SQLite. Ela testa comportamentos específicos do SQLite, peculiaridades e casos extremos que podem não ser cobertos por suítes de teste SQL padrão. Taxas de aprovação altas aqui indicam forte compatibilidade com SQLite para cenários de migração de aplicativos.

# =============================================================================
# Página Inicial
# =============================================================================

home-page-title = VibeSQL — Um Banco de Dados SQL Puro em Rust Feito para Velocidade

# Seção hero
home-hero-title = Um banco de dados SQL puro em Rust<br>feito para velocidade
home-hero-subtitle = O VibeSQL troca eficiência de armazenamento por desempenho. Armazenamento híbrido de linhas e colunas, execução vetorizada e zero código unsafe. Otimizado para conjuntos de dados que cabem na memória.
home-hero-subtext = Compatível com SQL:1999. Executa nativamente, em WebAssembly e como biblioteca embutida.
home-btn-demo = Experimentar no Navegador
home-btn-github = GitHub
home-btn-crates = crates.io

# Seção por que VibeSQL
home-why-title = Por Que VibeSQL?
home-hybrid-title = Armazenamento Híbrido
home-hybrid-text = Armazenamento de linhas e colunas em um único motor. Formato de linhas para buscas pontuais rápidas e OLTP. Formato colunar para varreduras analíticas com execução vetorizada. Sem necessidade de escolher.
home-speed-title = Velocidade Acima do Armazenamento
home-speed-text = Troca deliberadamente espaço em disco por desempenho de consultas. Layouts de armazenamento redundantes, cache agressivo e índices pré-computados significam que bancos de dados menores executam o mais rápido possível.
home-rust-title = Rust Puro, Zero Unsafe
home-rust-text = Escrito inteiramente em Rust seguro. Sem dependências de C, sem FFI, sem blocos unsafe. Compila para binários nativos e WebAssembly a partir da mesma base de código.

# Seção de arquitetura
home-arch-title = Arquitetura
home-pipeline-title = Pipeline de Consultas
home-pipeline-parser = <strong>Parser</strong> — Gramática SQL:1999 completa, AST alocado em arena
home-pipeline-planner = <strong>Planejador</strong> — Otimizador baseado em custo com reordenação de joins
home-pipeline-executor = <strong>Executor</strong> — Execução vetorizada com processamento em lotes
home-pipeline-storage = <strong>Armazenamento</strong> — Híbrido linhas/colunar com índices B-tree
home-features-title = Recursos Principais
home-feature-window = Funções de janela (ROW_NUMBER, RANK, LEAD/LAG, NTILE, ...)
home-feature-cte = Expressões de tabela comuns (WITH, CTEs recursivos)
home-feature-subquery = Subconsultas (correlacionadas, EXISTS, IN, escalares)
home-feature-join = Suporte completo a JOIN (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL)
home-feature-triggers = Triggers, views, chaves estrangeiras, restrições CHECK
home-feature-wasm = Alvo WASM com armazenamento persistente OPFS

# Seção de desempenho
home-perf-title = Desempenho
home-perf-full = Benchmarks completos →
home-stat-tpch-label = Consultas TPC-H Aprovadas
home-stat-tpch-sub = Benchmark de suporte à decisão
home-stat-conformance-label = Taxa de Aprovação SQLLogicTest
home-stat-conformance-sub = Mais de 6M de asserções de teste
home-stat-tpcds-label = Consultas TPC-DS Aprovadas
home-stat-tpcds-sub = Benchmark de análise complexa
home-perf-note = Comparado com SQLite, DuckDB e MySQL em cargas de trabalho equivalentes. <a href="benchmarks.html" class="text-blue-600 dark:text-blue-400 hover:underline">Ver resultados completos.</a>

# Seção começar
home-start-title = Começar
home-demo-title = Demo Interativa
home-demo-text = Execute consultas SQL no seu navegador. Motor de banco de dados completo compilado para WebAssembly com armazenamento persistente via OPFS. Sem instalação necessária.
home-install-title = Instalar
home-install-cargo = Cargo
home-install-library = Como biblioteca

# Seção explorar
home-explore-conformance-title = Relatório de Conformidade
home-explore-conformance-text = Detalhamento da conformidade com padrões SQL:1999 em 622 arquivos de teste.
home-explore-bench-title = Benchmarks de Desempenho
home-explore-bench-text = Resultados de TPC-H, TPC-DS, TPC-C e Sysbench contra SQLite, DuckDB e MySQL.

# Rodapé
home-footer = VibeSQL — Um banco de dados SQL puro em Rust feito para velocidade
