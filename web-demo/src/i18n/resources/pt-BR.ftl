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

# Section headers
bench-section-embedded = Embedded
bench-section-server = Server
bench-results-title = Benchmark Results
bench-perf-comparison = Performance Comparison
bench-methodology-title = Methodology
bench-analysis-roadmap = Analysis & Roadmap

# Summary cards
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = Operations Tested
bench-last-updated = Last Updated
bench-avg-speedup = average speedup
bench-from-main = from main branch
bench-loading = Loading...
bench-na = N/A
bench-faster = { $value }x faster
bench-slower = { $value }x slower
bench-speedup = { $value }x
bench-startup-time-label = startup time
bench-download-size = download size
bench-uncompressed = uncompressed
bench-size-metrics = size metrics
bench-failed = FAILED
bench-failed-title = Query failed (timeout or error)
bench-no-wasm-data = Nenhum dado WASM disponível
bench-no-server-data = Nenhum dado de benchmark de servidor Sysbench disponível
bench-no-server-data-hint = Os benchmarks de servidor requerem a execução do sysbench_server com a comparação MySQL habilitada.

# Table headers
bench-table-operation = Operation
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = Loading benchmark results...
bench-vibesql-server-title = VibeSQL via PostgreSQL wire protocol

# Common benchmark terms
bench-hardware = Hardware
bench-benchmark-framework = Benchmark Framework
bench-scale-factor = Scale Factor
bench-data = Data
bench-databases-tested = Databases Tested
bench-execution-mode = Execution Mode
bench-measurement = Measurement
bench-workload = Workload
bench-transaction-mix = Transaction Mix
bench-warehouses = Warehouses
bench-concurrency = Concurrency
bench-acid-compliance = ACID Compliance
bench-mode = Mode
bench-workload-types = Workload Types
bench-table-size = Table Size
bench-index-types = Index Types
bench-operations = Operations
bench-databases = Databases
bench-protocol-overhead = Protocol Overhead
bench-binary-size = Binary Size
bench-startup-time = Startup Time
bench-peak-memory = Peak Memory
bench-schema = Schema
bench-query-count = Query Count
bench-query-types = Query Types
bench-sql-features = SQL Features
bench-wasm-size = WASM Size
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H specific
bench-tpch-name = TPC-H
bench-tpch-title = TPC-H Decision Support Benchmark
bench-tpch-description = These benchmarks use the industry-standard <strong>TPC-H benchmark suite</strong>, which simulates real-world decision support workloads with complex analytical queries involving aggregations, joins, subqueries, and sorting.
bench-tpch-ops-label = TPC-H queries
bench-tpch-note-intro = All benchmarks measure end-to-end query execution time including parsing, planning, execution, and result materialization. This represents <strong>real-world SQL engine performance</strong> for analytical workloads.
bench-tpch-note-queries = <strong>Note:</strong> TPC-H queries test different aspects of SQL performance: simple aggregations (Q1, Q6), complex joins (Q2-Q5, Q7-Q10), subqueries (Q11-Q15), and advanced analytics (Q16-Q22). Hover over query names in the table above for descriptions.

# TPC-H Discussion
bench-tpch-disc-excels-title = Where VibeSQL Excels
bench-tpch-disc-excels = VibeSQL shows strong performance on <strong>scan-heavy aggregation queries</strong> (Q1, Q6, Q14, Q15, Q20) where our columnar execution engine and SIMD-accelerated aggregations shine. These queries involve filtering large tables and computing aggregates without complex join patterns.
bench-tpch-disc-targets-title = Current Optimization Targets
bench-tpch-disc-targets = Multi-way join queries (Q3, Q5, Q7-Q10, Q18, Q19, Q21) currently show SQLite ahead. The primary bottleneck is our hash join implementation, which doesn't yet employ the same level of optimization as SQLite's decades-refined B-tree joins. Specific areas under active development:
bench-tpch-disc-join-ordering = Improved cardinality estimation for better join order selection
bench-tpch-disc-hash-sizing = Adaptive hash table growth and spill-to-disk for large joins
bench-tpch-disc-vectorized = Batch processing in the join inner loop to improve cache utilization
bench-tpch-disc-inl-joins = Leveraging B-tree indexes when beneficial
bench-tpch-disc-path-title = Path to Leadership
bench-tpch-disc-path = VibeSQL's architecture is designed for modern hardware with features like columnar storage, vectorized execution, and lock-free concurrency. As these optimizations mature, we expect VibeSQL to achieve consistent leadership across all TPC-H queries. The fundamental design supports parallelism and SIMD that traditional row-store databases cannot easily retrofit.

# TPC-H Query Descriptions
bench-tpch-q1 = Pricing Summary Report - Aggregate pricing with GROUP BY and ORDER BY
bench-tpch-q2 = Minimum Cost Supplier - 3-table JOIN with ORDER BY and LIMIT
bench-tpch-q3 = Shipping Priority - 3-table JOIN with aggregation
bench-tpch-q4 = Order Priority Checking - Correlated EXISTS subquery
bench-tpch-q5 = Local Supplier Volume - 6-table JOIN with complex filtering
bench-tpch-q6 = Forecasting Revenue Change - WHERE filters with BETWEEN and SUM
bench-tpch-q7 = Volume Shipping - 6-table JOIN with SUBSTR and date filtering
bench-tpch-q8 = National Market Share - 7-table JOIN with CASE expressions
bench-tpch-q9 = Product Type Profit Measure - 4-table JOIN with aggregation
bench-tpch-q10 = Returned Item Reporting - 4-table JOIN with TOP-N LIMIT
bench-tpch-q11 = Important Stock Identification - Subquery in HAVING clause
bench-tpch-q12 = Shipping Modes Priority - CASE aggregation with date logic
bench-tpch-q13 = Customer Distribution - LEFT OUTER JOIN with subquery
bench-tpch-q14 = Promotion Effect - Conditional aggregation with CASE
bench-tpch-q15 = Top Supplier - Nested subqueries with MAX
bench-tpch-q16 = Parts/Supplier Relationship - NOT IN subquery with DISTINCT
bench-tpch-q17 = Small-Quantity-Order Revenue - Correlated subquery in WHERE
bench-tpch-q18 = Large Volume Customer - GROUP BY with HAVING
bench-tpch-q19 = Discounted Revenue - Complex OR conditions
bench-tpch-q20 = Potential Part Promotion - IN subquery with GROUP BY/HAVING
bench-tpch-q21 = Suppliers Who Kept Orders Waiting - Multi-table EXISTS
bench-tpch-q22 = Global Sales Opportunity - SUBSTR with NOT EXISTS subquery

# TPC-DS specific
bench-tpcds-name = TPC-DS
bench-tpcds-title = TPC-DS Decision Support Benchmark
bench-tpcds-description = <strong>TPC-DS</strong> is the successor to TPC-H, featuring 99 queries that model a modern decision support system with significantly more complex query patterns including multiple fact tables, snow-flake schema, and advanced SQL features.
bench-tpcds-ops-label = TPC-DS queries
bench-tpcds-note-intro = TPC-DS queries are substantially more complex than TPC-H, testing advanced SQL features like window functions, common table expressions (WITH clause), and complex join patterns across multiple fact and dimension tables.
bench-tpcds-note-remaining = <strong>Note:</strong> Remaining unsupported queries require features like INTERSECT/EXCEPT or specific date arithmetic functions not yet implemented.

# TPC-DS Discussion
bench-tpcds-disc-coverage-title = SQL:1999 Feature Coverage
bench-tpcds-disc-coverage = TPC-DS exercises the most demanding SQL features. VibeSQL passes <strong>88 of 99 queries</strong>, demonstrating broad coverage of SQL:1999 including ROLLUP, CUBE, GROUPING(), window functions with complex framing, and recursive CTEs. The remaining queries require INTERSECT/EXCEPT set operations.
bench-tpcds-disc-optimization-title = Complex Query Optimization
bench-tpcds-disc-optimization = TPC-DS queries often join 10+ tables with correlated subqueries. Current focus areas:
bench-tpcds-disc-cte = Intelligent decision between materialized and inline CTEs
bench-tpcds-disc-decorrelation = Converting correlated subqueries to joins when beneficial
bench-tpcds-disc-star = Fact-dimension join ordering for analytical patterns
bench-tpcds-disc-toward-title = Toward 99/99
bench-tpcds-disc-toward = INTERSECT and EXCEPT are planned additions that will enable the remaining queries. These set operations fit naturally into our existing query algebra and will be implemented as hash-based operators similar to our DISTINCT processing.

# TPC-C specific
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online Transaction Processing Benchmark
bench-tpcc-description = The <strong>TPC-C benchmark</strong> simulates a complete order-entry environment with a mix of complex transactions including order entry, payment processing, order status queries, delivery processing, and stock level monitoring.
bench-tpcc-ops-label = TPC-C transactions
bench-tpcc-note-intro = TPC-C measures transactions per minute (tpmC) and tests the database's ability to handle concurrent transactions with complex business logic. This benchmark is critical for evaluating <strong>transactional workload performance</strong>.
bench-tpcc-note-results = <strong>Note:</strong> Results show average transaction latency. Lower is better. TPC-C is particularly demanding for write-heavy workloads with strict consistency requirements.

# TPC-C Transaction Descriptions
bench-tpcc-new-order = New Order - Complex transaction with inventory checks and order creation
bench-tpcc-payment = Payment - Update customer balance and warehouse/district totals
bench-tpcc-order-status = Order Status - Read-only query for customer order history
bench-tpcc-delivery = Delivery - Batch processing of pending orders
bench-tpcc-stock-level = Stock Level - Count items below threshold in recent orders

# TPC-C Discussion
bench-tpcc-disc-faster-title = 42x Faster Than SQLite
bench-tpcc-disc-faster = VibeSQL achieves <strong>~79,000 transactions per second</strong> compared to SQLite's ~1,900 TPS, a 42x improvement. This dramatic speedup comes from our lock-free MVCC architecture that avoids SQLite's coarse-grained locking on every write operation.
bench-tpcc-disc-dominates-title = Why VibeSQL Dominates OLTP
bench-tpcc-disc-lockfree = MVCC allows readers and writers to proceed concurrently without blocking
bench-tpcc-disc-optimistic = Transactions only conflict at commit time, not during execution
bench-tpcc-disc-btree = Purpose-built index structure optimized for in-memory workloads
bench-tpcc-disc-prepared = Query plans are compiled once and reused
bench-tpcc-disc-scaling-title = Scaling Further
bench-tpcc-disc-scaling = Current results are single-threaded. VibeSQL's architecture supports multi-threaded transaction processing, and we expect near-linear scaling as we add parallel execution support. Our goal is to achieve 500K+ TPS on modern multi-core hardware.

# Sysbench Embedded specific
bench-sysbench-embedded-name = Sysbench (Embedded)
bench-sysbench-embedded-title = Sysbench Micro-Benchmarks (Embedded)
bench-sysbench-embedded-description = <strong>Sysbench</strong> provides focused micro-benchmarks that isolate specific database operations. These tests measure raw performance for fundamental operations without the complexity of full transaction workloads.
bench-sysbench-embedded-ops-label = Sysbench operations
bench-sysbench-embedded-note = Embedded mode runs the database in-process with zero network overhead, ideal for single-process applications where minimal latency is critical.

# Sysbench Operation Descriptions
bench-sysbench-point-select = Point Select - Single row lookup by primary key
bench-sysbench-insert = Insert - Insert new rows into table
bench-sysbench-update-index = Update Index - Update indexed column (k = k + 1)
bench-sysbench-update-non-index = Update Non-Index - Update non-indexed column
bench-sysbench-delete = Delete - Remove rows by primary key
bench-sysbench-range-queries = Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans

# Sysbench Embedded Discussion
bench-sysbench-emb-disc-point-title = Point Lookups: VibeSQL Leads
bench-sysbench-emb-disc-point = VibeSQL's direct API achieves <strong>~137ns per point select</strong>, matching SQLite and vastly outperforming DuckDB (~140µs). Our B-tree implementation is optimized for single-row lookups with minimal pointer chasing and cache-friendly node layouts.
bench-sysbench-emb-disc-index-title = Index Updates: 2x Faster
bench-sysbench-emb-disc-index = VibeSQL's indexed updates run at <strong>~740ns vs SQLite's ~1.6µs</strong>. Our MVCC design allows in-place index updates without write-ahead logging overhead for each operation.
bench-sysbench-emb-disc-improve-title = Areas for Improvement
bench-sysbench-emb-disc-bulk = SQLite's batch insert path is highly optimized; we're adding batched B-tree operations
bench-sysbench-emb-disc-nonindex = Full table scans for non-indexed columns need predicate pushdown optimization
bench-sysbench-emb-disc-deletes = Our tombstone-based deletion has cleanup overhead; compaction improvements are planned
bench-sysbench-emb-disc-duckdb-title = DuckDB Comparison
bench-sysbench-emb-disc-duckdb = DuckDB is optimized for analytical workloads, not micro-operations. Its 100-1000x slower results here reflect architectural choices (columnar storage, vectorized execution) that trade single-row latency for bulk throughput. VibeSQL targets both use cases.
bench-sysbench-emb-disc-architecture-title = Architectural Trade-offs
bench-sysbench-emb-disc-architecture = VibeSQL's hybrid architecture targets both OLTP and OLAP workloads. Our B-tree storage provides SQLite-competitive point lookup performance, while columnar execution handles analytical queries efficiently. This differs from pure OLAP databases like DuckDB that optimize exclusively for bulk operations at the cost of single-row latency.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = <strong>Sysbench</strong> server benchmarks compare VibeSQL Server (PostgreSQL wire protocol) against MySQL, measuring performance for multi-client database deployments.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = Server mode uses the PostgreSQL wire protocol, enabling multi-client access and compatibility with existing PostgreSQL tooling and drivers.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = PostgreSQL Wire Protocol
bench-sysbench-srv-disc-protocol = VibeSQL Server implements the PostgreSQL wire protocol, enabling compatibility with existing PostgreSQL drivers and tools. This adds ~10-50µs of protocol overhead per query compared to embedded mode, but enables multi-client deployments.
bench-sysbench-srv-disc-mysql-title = MySQL Comparison
bench-sysbench-srv-disc-mysql = Server benchmarks compare against MySQL to evaluate VibeSQL as a drop-in replacement for traditional client-server databases. Results vary by operation type, with VibeSQL showing advantages in read-heavy workloads.
bench-sysbench-srv-disc-roadmap-title = Server Roadmap
bench-sysbench-srv-disc-pooling = Reduce connection establishment overhead for high-throughput scenarios
bench-sysbench-srv-disc-caching = Server-side caching of query plans across connections
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
bench-footprint-emb-disc-size = VibeSQL at <strong>~17MB</strong> sits between SQLite (~5MB) and DuckDB (~45MB). This reflects our choice to include advanced features (window functions, CTEs, columnar execution) while keeping the binary manageable for embedded deployments.
bench-footprint-emb-disc-startup-title = Startup: Fastest Cold Start
bench-footprint-emb-disc-startup = VibeSQL achieves <strong>~7.7ms cold startup</strong>, slightly faster than SQLite (~8.2ms) and significantly faster than DuckDB (~14.6ms). Our minimal initialization path loads only essential metadata structures on startup.
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
bench-footprint-srv-disc-wasm-title = WASM: 2.2MB Compressed
bench-footprint-srv-disc-wasm = VibeSQL's WebAssembly module compresses to <strong>~2.2MB gzipped</strong>, enabling fast initial page loads. This is a full SQL:1999 database with window functions, CTEs, and ACID transactions running entirely in the browser.
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
bench-tpcc-disc-duckdb = DuckDB achieves only ~385 TPS on TPC-C (60x slower than VibeSQL, 12x slower than SQLite). This is expected: DuckDB is an <strong>analytical (OLAP) database</strong> optimized for large batch operations, not single-row transactions. Its columnar storage format excels at scanning millions of rows but adds overhead for point lookups and small updates that dominate OLTP workloads like TPC-C.
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
conformance-slt-desc = Results from the comprehensive <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a> suite containing ~5.9 million tests across 623 test files from the official SQLite corpus.
conformance-slt-evidence = Evidence Tests
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> is a comprehensive test suite originally developed for SQLite, containing ~5.9 million SQL test cases across 623 test files. It tests practical correctness by running real-world queries and validating results. This suite focuses on semantic correctness and edge cases rather than pure grammar conformance.
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
