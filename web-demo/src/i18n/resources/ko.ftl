# VibeSQL Web UI - 한국어

# Page titles
page-title = VibeSQL - AI 기반 SQL:1999 데이터베이스
demo-title = VibeSQL 데모
benchmarks-title = 성능 벤치마크 - VibeSQL
benchmarks-heading = VibeSQL - 성능 벤치마크
conformance-title = 적합성 보고서 - VibeSQL
conformance-heading = 적합성 보고서
conformance-subtitle = SQL:1999 표준 준수 테스트

# Navigation
nav-showcase = SQL:1999 쇼케이스
nav-conformance = sqltest 결과 보기
nav-sqllogictest = SQLLogicTest 결과 보기

# Editor section
editor-title = SQL 편집기
editor-storage = 저장소
editor-storage-init = 초기화 중...
editor-execute = 쿼리 실행

# Results section
results-title = 결과
results-empty = 결과를 보려면 쿼리를 실행하세요
results-loading = 로딩 중...
results-rows = { $count }개 행
results-rows-with-time = { $count }개 행 ({ $time }ms)
results-copy = 클립보드에 복사
results-export = CSV 내보내기
results-limit-warning = { $total }개 행 중 처음 { $limit }개를 표시합니다. LIMIT 절을 사용하여 쿼리를 세분화하세요.

# Examples sidebar
examples-title = 예제
examples-basic = 기본 쿼리
examples-advanced = 고급 쿼리

# Database selector
db-select-label = 데이터베이스

# Footer
footer-tagline = VibeSQL - WebAssembly의 SQL:1999 데이터베이스
footer-deployed = 배포일: { $date }

# Theme
theme-toggle-dark = 다크 모드로 전환
theme-toggle-light = 라이트 모드로 전환

# Locale
locale-select = 언어 선택

# Messages
msg-query-success = 쿼리가 성공적으로 실행되었습니다
msg-rows-affected = { $count }개 행이 영향을 받았습니다

# Errors
error-generic = 오류가 발생했습니다
error-query-failed = 쿼리 실패
error-no-databases = 사용 가능한 데이터베이스가 없습니다

# Loading states
loading-initializing-theme = 테마 초기화 중
loading-preparing-editor = 편집기 준비 중
loading-database-engine = 데이터베이스 엔진 로드 중
loading-setting-up-ui = 사용자 인터페이스 설정 중
loading-editor = 편집기 로드 중...
loading-compliance-data = 규정 준수 데이터 로드 중...
loading-conformance-report = 적합성 보고서 로드 중...

# Editor
editor-placeholder = SQL 쿼리를 입력하세요... (Ctrl+Enter 또는 Cmd+Enter로 실행)

# Navigation links
nav-terminal = SQL 터미널 데모
nav-compliance = SQL 테스트 적합성 보고서
nav-benchmarks = 성능 벤치마크
nav-github = GitHub 저장소
nav-home = 홈

# Results
results-success-zero = 쿼리가 성공적으로 실행되었습니다 (0개 행)
results-null = NULL

# Help Modal
help-title = 키보드 단축키 및 도움말
help-close = 닫기
help-editor-shortcuts = 편집기 단축키
help-navigation = 탐색
help-results-actions = 결과 작업
help-tips = 팁
help-shortcut-execute = 현재 쿼리 실행
help-shortcut-comment = 줄 주석 토글
help-shortcut-indent = 선택 영역 들여쓰기
help-shortcut-show-help = 이 도움말 대화 상자 표시
help-shortcut-close-help = 도움말 대화 상자 닫기
help-action-copy = 클립보드에 복사
help-action-copy-desc = 탭으로 구분된 값으로 결과 복사
help-action-export = CSV 내보내기
help-action-export-desc = CSV 파일로 결과 다운로드
help-tip-limit = 성능을 위해 결과는 1,000개 행으로 제한됩니다. LIMIT 절을 사용하여 쿼리를 세분화하세요.
help-tip-time = 실행 시간이 쿼리 결과와 함께 표시됩니다.
help-tip-syntax = 편집기는 SQL 구문 강조 및 자동 완성을 지원합니다.
help-tip-theme = 테마 버튼을 사용하여 라이트/다크 모드 간 전환하세요.
help-got-it = 알겠습니다!

# Showcase Navigation
showcase-title = SQL:1999 Core 쇼케이스
showcase-description = 구현된 SQL:1999 Core 기능을 대화형으로 탐색하세요
showcase-complete = { $percent }% 완료
showcase-categories = 기능 카테고리
showcase-legend = 상태 범례
showcase-status-implemented = 완전 구현
showcase-status-partial = 부분 구현
showcase-status-planned = 계획됨

# Showcase category labels
showcase-cat-compliance = 적합성 대시보드
showcase-cat-data-types = 데이터 타입
showcase-cat-dml = DML 작업
showcase-cat-predicates = 술어 및 연산자
showcase-cat-joins = JOIN
showcase-cat-subqueries = 서브쿼리
showcase-cat-aggregates = 집계 및 GROUP BY
showcase-cat-ddl = DDL 및 제약 조건

# Common showcase elements
showcase-interactive-examples = 대화형 예제
showcase-try-example = 이 예제 시도하기
showcase-progress = { $total }개 { $type } 중 { $implemented }개 ({ $percent }%)
showcase-table-status = 상태
showcase-table-category = 카테고리
showcase-table-description = 설명
showcase-table-syntax = 구문
showcase-table-use-case = 사용 사례

# Status labels
status-implemented = 구현됨
status-partial = 부분적
status-planned = 계획됨

# Aggregates Showcase
aggregates-title = SQL 집계 및 GROUP BY
aggregates-description = SQL:1999 Core 집계 함수 및 그룹화 기능
aggregates-reference = 집계 함수 참조
aggregates-table-function = 함수
aggregates-progress-type = 함수
aggregates-ex-basic = 기본 집계 함수
aggregates-ex-group-single = GROUP BY (단일 열)
aggregates-ex-group-multiple = GROUP BY (다중 열)
aggregates-ex-having = HAVING 절
aggregates-ex-orderby = 집계와 ORDER BY
aggregates-ex-null = 집계에서 NULL 처리

# DML Operations Showcase
dml-title = DML 작업 (데이터 조작 언어)
dml-description = 데이터 쿼리 및 수정을 위한 SQL:1999 Core 작업
dml-reference = DML 작업 참조
dml-table-operation = 작업
dml-progress-type = 작업
dml-ex-select-basic = SELECT - 기본 쿼리
dml-ex-select-ordering = SELECT - 정렬 및 제한
dml-ex-insert = INSERT 작업
dml-ex-update = UPDATE 작업
dml-ex-delete = DELETE 작업
dml-ex-combined = 통합 CRUD 워크플로우

# Data Types Showcase
datatypes-title = SQL:1999 Core 데이터 타입
datatypes-description = SQL:1999 Core 사양에 정의된 기본 데이터 타입 탐색
datatypes-reference = 데이터 타입 참조
datatypes-table-type = 타입 이름
datatypes-table-example = 예제 값
datatypes-table-spec = 사양
datatypes-progress-type = 타입
datatypes-ex-numeric = 숫자 타입 작업
datatypes-ex-null = NULL 처리 및 3값 논리
datatypes-ex-comparisons = 타입 비교 및 연산

# JOINs Showcase
joins-title = SQL JOIN
joins-description = 여러 테이블의 데이터를 결합하기 위한 SQL:1999 Core JOIN 작업
joins-reference = JOIN 타입 참조
joins-table-type = JOIN 타입
joins-progress-type = JOIN 타입
joins-category-suffix = JOIN
joins-ex-sample = 샘플 데이터 설정
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = 다중 테이블 JOIN

# Predicates Showcase
predicates-title = 술어 및 연산자
predicates-description = 필터링 및 논리 연산을 위한 SQL:1999 술어
predicates-reference = 술어 참조
predicates-table-predicate = 술어
predicates-progress-type = 술어
predicates-ex-comparison = 비교 연산자
predicates-ex-between = BETWEEN 및 범위 술어
predicates-ex-null = NULL 술어 및 3값 논리
predicates-ex-boolean = 부울 논리 (AND, OR, NOT)
predicates-ex-in = 서브쿼리와 IN 술어
predicates-ex-combined = 결합된 술어 연산

# Subqueries Showcase
subqueries-title = SQL 서브쿼리
subqueries-description = 중첩 쿼리 연산을 위한 SQL:1999 Core 서브쿼리 기능
subqueries-reference = 서브쿼리 타입 참조
subqueries-table-type = 서브쿼리 타입
subqueries-progress-type = 서브쿼리 타입
subqueries-ex-scalar-select = SELECT의 스칼라 서브쿼리
subqueries-ex-scalar-where = WHERE의 스칼라 서브쿼리
subqueries-ex-derived = 파생 테이블 (FROM의 서브쿼리)
subqueries-ex-in = 서브쿼리와 IN 술어
subqueries-ex-correlated = 상관 서브쿼리
subqueries-ex-nested = 중첩 서브쿼리

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
bench-no-wasm-data = WASM 데이터가 없습니다
bench-no-server-data = Sysbench 서버 벤치마크 데이터가 없습니다
bench-no-server-data-hint = 서버 벤치마크는 MySQL 비교가 활성화된 상태에서 sysbench_server를 실행해야 합니다.

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
bench-tpch-description = 이 벤치마크는 집계, 조인, 서브쿼리 및 정렬이 포함된 복잡한 분석 쿼리로 실제 의사결정 지원 워크로드를 시뮬레이션하는 산업 표준 <strong>TPC-H 벤치마크 스위트</strong>를 사용합니다.
bench-tpch-ops-label = TPC-H queries
bench-tpch-note-intro = 모든 벤치마크는 구문 분석, 계획, 실행 및 결과 구체화를 포함한 종단간 쿼리 실행 시간을 측정합니다. 이는 분석 워크로드에 대한 <strong>실제 SQL 엔진 성능</strong>을 나타냅니다.
bench-tpch-note-queries = <strong>참고:</strong> TPC-H 쿼리는 SQL 성능의 다양한 측면을 테스트합니다: 단순 집계(Q1, Q6), 복잡한 조인(Q2-Q5, Q7-Q10), 서브쿼리(Q11-Q15), 고급 분석(Q16-Q22). 설명을 보려면 위 표의 쿼리 이름 위에 마우스를 올려주세요.

# TPC-H Discussion
bench-tpch-disc-excels-title = VibeSQL이 뛰어난 영역
bench-tpch-disc-excels = VibeSQL은 컬럼형 실행 엔진과 SIMD 가속 집계가 빛나는 <strong>스캔 집약적 집계 쿼리</strong>(Q1, Q6, Q14, Q15, Q20)에서 강력한 성능을 보여줍니다. 이러한 쿼리는 복잡한 조인 패턴 없이 대형 테이블 필터링과 집계 계산을 포함합니다.
bench-tpch-disc-targets-title = 현재 최적화 목표
bench-tpch-disc-targets = 다중 조인 쿼리(Q3, Q5, Q7-Q10, Q18, Q19, Q21)는 현재 SQLite가 앞서 있습니다. 주요 병목 현상은 아직 SQLite의 수십 년간 개선된 B-tree 조인과 같은 수준의 최적화를 사용하지 않는 해시 조인 구현입니다. 활발히 개발 중인 특정 영역:
bench-tpch-disc-join-ordering = 더 나은 조인 순서 선택을 위한 향상된 카디널리티 추정
bench-tpch-disc-hash-sizing = 대규모 조인을 위한 적응형 해시 테이블 증가 및 디스크 스필
bench-tpch-disc-vectorized = 캐시 활용도 향상을 위한 조인 내부 루프의 배치 처리
bench-tpch-disc-inl-joins = 유리할 때 B-tree 인덱스 활용
bench-tpch-disc-path-title = 리더십으로 가는 길
bench-tpch-disc-path = VibeSQL의 아키텍처는 컬럼형 스토리지, 벡터화된 실행 및 락프리 동시성과 같은 기능으로 현대 하드웨어에 맞게 설계되었습니다. 이러한 최적화가 성숙해지면서 VibeSQL이 모든 TPC-H 쿼리에서 지속적인 리더십을 달성할 것으로 예상합니다.

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
bench-tpcds-description = <strong>TPC-DS</strong>는 TPC-H의 후속으로, 여러 팩트 테이블, 스노우플레이크 스키마 및 고급 SQL 기능을 포함한 훨씬 더 복잡한 쿼리 패턴을 모델링하는 99개의 쿼리를 제공합니다.
bench-tpcds-ops-label = TPC-DS queries
bench-tpcds-note-intro = TPC-DS 쿼리는 TPC-H보다 상당히 복잡하며, 윈도우 함수, 공통 테이블 표현식(WITH 절), 여러 팩트 및 차원 테이블에 걸친 복잡한 조인 패턴과 같은 고급 SQL 기능을 테스트합니다.
bench-tpcds-note-remaining = <strong>참고:</strong> 모든 99개의 TPC-DS 쿼리가 통과하여 INTERSECT, EXCEPT, 윈도우 함수, CTE 및 복잡한 서브쿼리를 포함한 포괄적인 SQL:1999 기능 지원을 보여줍니다.

# TPC-DS Discussion
bench-tpcds-disc-coverage-title = SQL:1999 기능 범위
bench-tpcds-disc-coverage = TPC-DS는 가장 까다로운 SQL 기능을 테스트합니다. VibeSQL은 ROLLUP, CUBE, GROUPING(), 복잡한 프레이밍이 있는 윈도우 함수, 재귀 CTE 및 INTERSECT/EXCEPT 집합 연산을 포함한 SQL:1999의 완전한 범위를 보여주며 <strong>모든 99개 쿼리</strong>를 통과합니다.
bench-tpcds-disc-optimization-title = 복잡한 쿼리 최적화
bench-tpcds-disc-optimization = TPC-DS 쿼리는 종종 상관 서브쿼리와 함께 10개 이상의 테이블을 조인합니다. 현재 집중 영역:
bench-tpcds-disc-cte = 구체화된 CTE와 인라인 CTE 사이의 지능적 결정
bench-tpcds-disc-decorrelation = 유리할 때 상관 서브쿼리를 조인으로 변환
bench-tpcds-disc-star = 분석 패턴을 위한 팩트-차원 조인 순서 지정
bench-tpcds-disc-toward-title = 완전한 TPC-DS 범위
bench-tpcds-disc-toward = 모든 99개 쿼리가 통과함으로써 VibeSQL은 복잡한 분석 워크로드에 대한 프로덕션 준비 SQL:1999 준수를 입증합니다. 최근 INTERSECT 및 EXCEPT 집합 연산 추가로 전체 TPC-DS 범위가 완료되었습니다.
bench-tpcds-disc-sqlite-title = SQLite 비교 참고
bench-tpcds-disc-sqlite = SQLite는 누락된 SQL:1999 OLAP 기능으로 인해 99개 TPC-DS 쿼리 중 12개(Q2, Q5, Q14, Q17, Q18, Q22, Q36, Q67, Q70, Q77, Q80, Q86)를 실행할 수 없습니다: <strong>ROLLUP/CUBE</strong> 그룹화 집합, <strong>GROUPING()</strong> 함수 및 <strong>STDDEV_SAMP()</strong>. 이러한 쿼리는 SQLite 벤치마크에서 건너뜁니다. VibeSQL과 DuckDB는 모든 99개 쿼리를 지원합니다.

# TPC-C specific
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online Transaction Processing Benchmark
bench-tpcc-description = <strong>TPC-C 벤치마크</strong>는 주문 입력, 결제 처리, 주문 상태 쿼리, 배송 처리 및 재고 수준 모니터링을 포함한 복잡한 트랜잭션 혼합으로 완전한 주문 입력 환경을 시뮬레이션합니다.
bench-tpcc-ops-label = TPC-C transactions
bench-tpcc-note-intro = TPC-C는 분당 트랜잭션(tpmC)을 측정하고 복잡한 비즈니스 로직으로 동시 트랜잭션을 처리하는 데이터베이스의 능력을 테스트합니다. 이 벤치마크는 <strong>트랜잭션 워크로드 성능</strong> 평가에 매우 중요합니다.
bench-tpcc-note-results = <strong>참고:</strong> 결과는 평균 트랜잭션 지연 시간을 보여줍니다. 낮을수록 좋습니다. TPC-C는 엄격한 일관성 요구 사항이 있는 쓰기 집약적 워크로드에 특히 까다롭습니다.

# TPC-C Transaction Descriptions
bench-tpcc-new-order = New Order - Complex transaction with inventory checks and order creation
bench-tpcc-payment = Payment - Update customer balance and warehouse/district totals
bench-tpcc-order-status = Order Status - Read-only query for customer order history
bench-tpcc-delivery = Delivery - Batch processing of pending orders
bench-tpcc-stock-level = Stock Level - Count items below threshold in recent orders

# TPC-C Discussion
bench-tpcc-disc-faster-title = SQLite보다 5배 빠름
bench-tpcc-disc-faster = VibeSQL은 SQLite의 ~4,500 TPS에 비해 <strong>초당 약 23,000 트랜잭션</strong>을 달성하여 5배 향상되었습니다. 이 속도 향상은 모든 쓰기 작업에서 SQLite의 조대한 잠금을 피하는 락프리 MVCC 아키텍처에서 비롯됩니다.
bench-tpcc-disc-dominates-title = VibeSQL이 OLTP를 지배하는 이유
bench-tpcc-disc-lockfree = MVCC는 리더와 라이터가 블로킹 없이 동시에 진행할 수 있게 합니다
bench-tpcc-disc-optimistic = 트랜잭션은 실행 중이 아닌 커밋 시에만 충돌합니다
bench-tpcc-disc-btree = 인메모리 워크로드에 최적화된 목적 구축 인덱스 구조
bench-tpcc-disc-prepared = 쿼리 계획은 한 번 컴파일되고 재사용됩니다
bench-tpcc-disc-scaling-title = 더 나은 확장
bench-tpcc-disc-scaling = 현재 결과는 단일 스레드입니다. VibeSQL의 아키텍처는 다중 스레드 트랜잭션 처리를 지원하며, 병렬 실행 지원을 추가하면서 향상된 확장을 기대합니다.

# Sysbench Embedded specific
bench-sysbench-embedded-name = Sysbench (Embedded)
bench-sysbench-embedded-title = Sysbench Micro-Benchmarks (Embedded)
bench-sysbench-embedded-description = <strong>Sysbench</strong>는 특정 데이터베이스 작업을 분리하는 집중된 마이크로 벤치마크를 제공합니다. 이 테스트는 전체 트랜잭션 워크로드의 복잡성 없이 기본 작업에 대한 원시 성능을 측정합니다.
bench-sysbench-embedded-ops-label = Sysbench operations
bench-sysbench-embedded-note = 임베디드 모드는 네트워크 오버헤드 없이 프로세스 내에서 데이터베이스를 실행하며, 최소 지연 시간이 중요한 단일 프로세스 애플리케이션에 이상적입니다.

# Sysbench Operation Descriptions
bench-sysbench-point-select = Point Select - Single row lookup by primary key
bench-sysbench-insert = Insert - Insert new rows into table
bench-sysbench-update-index = Update Index - Update indexed column (k = k + 1)
bench-sysbench-update-non-index = Update Non-Index - Update non-indexed column
bench-sysbench-delete = Delete - Remove rows by primary key
bench-sysbench-range-queries = Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans

# Sysbench Embedded Discussion
bench-sysbench-emb-disc-point-title = 포인트 조회: 동등
bench-sysbench-emb-disc-point = VibeSQL의 포인트 선택은 SQLite의 ~0.36µs와 일치하는 <strong>~0.37µs</strong>에서 실행됩니다. 우리의 B-tree 구현은 최소한의 포인터 추적과 캐시 친화적인 노드 레이아웃으로 단일 행 조회에 최적화되어 있습니다.
bench-sysbench-emb-disc-index-title = 인덱스 업데이트: 개선 여지
bench-sysbench-emb-disc-index = VibeSQL의 인덱스 업데이트는 <strong>SQLite의 ~1.7µs 대비 ~4.3µs</strong>에서 실행됩니다. 이는 MVCC 설계가 인덱스 유지 관리에 오버헤드를 추가하기 때문에 최적화 영역이며, 이를 줄이기 위해 작업 중입니다.
bench-sysbench-emb-disc-improve-title = 개선 영역
bench-sysbench-emb-disc-bulk = SQLite의 배치 삽입 경로는 고도로 최적화되어 있습니다; 배치 B-tree 작업을 추가 중입니다
bench-sysbench-emb-disc-nonindex = 비인덱스 업데이트는 SQLite의 ~1.4µs 대비 VibeSQL ~1.9µs로 거의 동등합니다
bench-sysbench-emb-disc-deletes = 삭제 작업이 크게 개선되었습니다: 이제 SQLite의 ~3.8µs 대비 ~5.5µs(이전 1183µs)
bench-sysbench-emb-disc-architecture-title = 아키텍처 트레이드오프
bench-sysbench-emb-disc-architecture = VibeSQL의 하이브리드 아키텍처는 OLTP와 OLAP 워크로드 모두를 대상으로 합니다. B-tree 스토리지는 SQLite 경쟁력 있는 포인트 조회 성능을 제공하고, 컬럼형 실행은 분석 쿼리를 효율적으로 처리합니다.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = <strong>Sysbench</strong> 서버 벤치마크는 VibeSQL Server(PostgreSQL 와이어 프로토콜)를 MySQL과 비교하여 다중 클라이언트 데이터베이스 배포의 성능을 측정합니다.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = 서버 모드는 PostgreSQL 와이어 프로토콜을 사용하여 다중 클라이언트 액세스와 기존 PostgreSQL 도구 및 드라이버와의 호환성을 제공합니다.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = PostgreSQL 와이어 프로토콜
bench-sysbench-srv-disc-protocol = VibeSQL Server는 PostgreSQL 와이어 프로토콜을 구현하여 기존 PostgreSQL 드라이버 및 도구와의 호환성을 제공합니다. 이는 임베디드 모드에 비해 쿼리당 ~10-50µs의 프로토콜 오버헤드를 추가하지만 다중 클라이언트 배포를 가능하게 합니다.
bench-sysbench-srv-disc-mysql-title = MySQL 비교
bench-sysbench-srv-disc-mysql = 서버 벤치마크는 전통적인 클라이언트-서버 데이터베이스의 드롭인 대체품으로 VibeSQL을 평가하기 위해 MySQL과 비교합니다. 결과는 작업 유형에 따라 다르며, VibeSQL은 읽기 집약적 워크로드에서 장점을 보여줍니다.
bench-sysbench-srv-disc-roadmap-title = 서버 로드맵
bench-sysbench-srv-disc-pooling = 고처리량 시나리오를 위한 연결 설정 오버헤드 감소
bench-sysbench-srv-disc-caching = 연결 간 서버측 쿼리 계획 캐싱
bench-sysbench-srv-disc-extended = 배치 작업을 위한 전체 PostgreSQL 확장 쿼리 프로토콜 지원

# TPC-H Server specific
bench-tpch-server-name = TPC-H (서버)
bench-tpch-server-title = TPC-H 분석 벤치마크 (서버)
bench-tpch-server-description = <strong>TPC-H 서버 벤치마크</strong>는 분석 쿼리 워크로드에 대해 VibeSQL Server(PostgreSQL 와이어 프로토콜)를 MySQL과 비교하여 클라이언트-서버 배포에서 OLAP 성능을 측정합니다.
bench-tpch-server-ops-label = TPC-H 쿼리
bench-tpch-server-note-intro = 서버 벤치마크는 네트워크 오버헤드를 포함한 종단간 쿼리 지연 시간을 측정하여 <strong>PostgreSQL 와이어 프로토콜</strong> 구현을 테스트합니다.
bench-tpch-server-note-queries = 쿼리는 비즈니스 인텔리전스 워크로드에 일반적인 복잡한 JOIN, 서브쿼리 및 집계를 테스트합니다.

# TPC-H Server Discussion
bench-tpch-srv-disc-protocol-title = PostgreSQL 와이어 프로토콜
bench-tpch-srv-disc-protocol = VibeSQL Server는 표준 PostgreSQL 드라이버와 도구를 사용할 수 있도록 PostgreSQL 와이어 프로토콜을 사용합니다. 이 벤치마크는 프로토콜 오버헤드를 포함한 전체 종단간 지연 시간을 측정합니다.
bench-tpch-srv-disc-comparison-title = MySQL 비교
bench-tpch-srv-disc-comparison = MySQL과의 비교는 분석 워크로드에서 기존 클라이언트-서버 데이터베이스의 기준선을 제공합니다. VibeSQL의 컬럼형 실행 엔진은 복잡한 집계 및 조인에 이점을 제공합니다.
bench-tpch-srv-disc-roadmap-title = 서버 OLAP 로드맵
bench-tpch-srv-disc-prepared = 연결 간 컴파일된 쿼리 계획 재사용
bench-tpch-srv-disc-pooling = 고처리량 시나리오를 위한 효율적인 연결 처리
bench-tpch-srv-disc-scale = 프로덕션 규모 검증을 위한 더 큰 데이터 세트 테스트 (SF 0.1, SF 1.0)

# TPC-C Server specific
bench-tpcc-server-name = TPC-C (서버)
bench-tpcc-server-title = TPC-C OLTP 벤치마크 (서버)
bench-tpcc-server-description = <strong>TPC-C 서버 벤치마크</strong>는 OLTP 트랜잭션 워크로드에 대해 VibeSQL Server(PostgreSQL 와이어 프로토콜)를 MySQL과 비교하여 다중 클라이언트 데이터베이스 배포의 처리량을 측정합니다.
bench-tpcc-server-ops-label = TPC-C 트랜잭션
bench-tpcc-server-note-intro = 서버 벤치마크는 네트워크 오버헤드를 포함한 트랜잭션 처리량을 측정하여 <strong>PostgreSQL 와이어 프로토콜</strong> 구현을 테스트합니다.
bench-tpcc-server-note-results = 결과는 표준 TPC-C 트랜잭션 혼합에 대한 초당 트랜잭션(TPS)을 보고합니다.
bench-tpcc-mixed = 혼합 워크로드 - 표준 TPC-C 트랜잭션 혼합 (45% 신규 주문, 43% 결제, 4% 주문 상태, 4% 배송, 4% 재고 수준)

# TPC-C Server Discussion
bench-tpcc-srv-disc-protocol-title = PostgreSQL 와이어 프로토콜
bench-tpcc-srv-disc-protocol = VibeSQL Server는 표준 PostgreSQL 드라이버와 도구를 사용할 수 있도록 PostgreSQL 와이어 프로토콜을 사용합니다. 이 벤치마크는 프로토콜 오버헤드를 포함한 전체 종단간 트랜잭션 지연 시간을 측정합니다.
bench-tpcc-srv-disc-comparison-title = MySQL 비교
bench-tpcc-srv-disc-comparison = MySQL과의 비교는 OLTP 워크로드에서 기존 클라이언트-서버 데이터베이스의 기준선을 제공합니다. MySQL은 트랜잭션 워크로드의 산업 표준이며 TPC-C는 MySQL의 강점입니다.
bench-tpcc-srv-disc-roadmap-title = 서버 OLTP 로드맵
bench-tpcc-srv-disc-prepared = 연결 간 컴파일된 쿼리 계획 재사용
bench-tpcc-srv-disc-pooling = 고처리량 시나리오를 위한 효율적인 연결 처리
bench-tpcc-srv-disc-parallel = 다중 클라이언트 동시 트랜잭션 처리

# Footprint Embedded specific
bench-footprint-embedded-name = Footprint (Embedded)
bench-footprint-embedded-title = Native Binary Footprint
bench-footprint-embedded-description = <strong>임베디드 풋프린트 벤치마크</strong>는 네이티브 데이터베이스 바이너리의 리소스 효율성을 측정하며, 바이너리 크기, 콜드 스타트업 시간 및 피크 메모리 사용량을 비교합니다.
bench-footprint-embedded-ops-label = databases compared
bench-footprint-embedded-note = 네이티브 바이너리 풋프린트는 바이너리 크기, 스타트업 지연 시간 및 메모리 소비가 배포 가능성에 직접적으로 영향을 미치는 <strong>임베디드 및 엣지 배포</strong>에 매우 중요합니다.

# Footprint Embedded Descriptions
bench-footprint-binary-size = Binary Size - Size of the compiled database binary on disk
bench-footprint-startup-time = Startup Time - Time to cold-start and execute first query
bench-footprint-peak-memory = Peak Memory - Maximum resident set size during initialization

# Footprint Embedded Discussion
bench-footprint-emb-disc-size-title = 바이너리 크기: 중간 지점
bench-footprint-emb-disc-size = VibeSQL은 <strong>~17MB</strong>로 SQLite(~5MB)와 DuckDB(~45MB) 사이에 위치합니다. 이는 임베디드 배포를 위해 바이너리를 관리 가능하게 유지하면서 고급 기능(윈도우 함수, CTE, 컬럼형 실행)을 포함하는 선택을 반영합니다.
bench-footprint-emb-disc-startup-title = 스타트업: 가장 빠른 콜드 스타트
bench-footprint-emb-disc-startup = VibeSQL은 SQLite(~6.5ms)보다 빠르고 DuckDB(~13ms)보다 훨씬 빠른 <strong>~6ms 콜드 스타트업</strong>을 달성합니다. 최소한의 초기화 경로는 스타트업 시 필수 메타데이터 구조만 로드합니다.
bench-footprint-emb-disc-memory-title = 메모리 효율성
bench-footprint-emb-disc-memory = 스타트업 중 피크 메모리는 VibeSQL ~7MB vs SQLite ~3MB 및 DuckDB ~11MB입니다. SQLite와의 차이는 더 정교한 쿼리 옵티마이저와 미리 할당된 컬럼형 실행 인프라를 반영합니다.
bench-footprint-emb-disc-roadmap-title = 크기 축소 로드맵
bench-footprint-emb-disc-flags = 사용하지 않는 기능을 제외하기 위한 컴파일 시간 기능 선택
bench-footprint-emb-disc-lto = 죽은 코드 제거를 위한 전체 프로그램 링크 시간 최적화
bench-footprint-emb-disc-modular = 핵심 엔진을 선택적 기능(예: 윈도우 함수)에서 분리

# Footprint Server/WASM specific
bench-footprint-server-name = Footprint (Server/WASM)
bench-footprint-server-title = WASM Footprint
bench-footprint-server-description = <strong>WASM 풋프린트 벤치마크</strong>는 브라우저 배포를 위한 WebAssembly 모듈 크기를 측정하며, 다운로드 크기가 사용자 경험에 영향을 미치는 웹 애플리케이션에 매우 중요합니다.
bench-footprint-server-ops-label = deployment targets
bench-footprint-server-note = WASM 크기는 다운로드 시간이 상호작용 시간에 직접적으로 영향을 미치는 <strong>웹 배포</strong>에 매우 중요합니다. 브라우저가 자동으로 gzip 콘텐츠를 압축 해제하므로 Gzip 크기가 가장 관련성이 있습니다.
bench-footprint-server-note2 = <strong>참고:</strong> VibeSQL WASM은 브라우저에서 완전한 SQL:1999 준수를 유지하면서 최소 다운로드 크기를 위해 설계되었습니다.

# Footprint Server Descriptions
bench-footprint-wasm-size = WASM Size - Size of the WebAssembly module for browser deployment
bench-footprint-wasm-gzip = WASM (gzip) - Compressed size for web delivery

# Footprint Server Discussion
bench-footprint-srv-disc-wasm-title = WASM: 1.5MB 압축
bench-footprint-srv-disc-wasm = VibeSQL의 WebAssembly 모듈은 <strong>~1.5MB gzipped</strong>로 압축되어 빠른 초기 페이지 로드를 가능하게 합니다. 이는 윈도우 함수, CTE 및 ACID 트랜잭션이 있는 완전한 SQL:1999 데이터베이스가 브라우저에서 완전히 실행됩니다.
bench-footprint-srv-disc-included-title = 포함 내용
bench-footprint-srv-disc-parser = 완전한 SQL 파서 및 쿼리 옵티마이저
bench-footprint-srv-disc-btree = MVCC가 있는 B-tree 스토리지 엔진
bench-footprint-srv-disc-window = 윈도우 함수 및 고급 집계
bench-footprint-srv-disc-cte = 공통 테이블 표현식(WITH 절)
bench-footprint-srv-disc-acid = 완전한 ACID 트랜잭션 지원
bench-footprint-srv-disc-benefits-title = 브라우저 배포 이점
bench-footprint-srv-disc-benefits = 브라우저에서 SQL을 실행하면 서버로의 왕복 지연 시간이 제거되고, 오프라인 우선 애플리케이션이 가능하며, 민감한 데이터가 사용자 장치에 유지됩니다. VibeSQL의 WASM 빌드는 최소한의 종속성과 효율적인 메모리 사용으로 이 사용 사례를 위해 설계되었습니다.
bench-footprint-srv-disc-roadmap-title = WASM 로드맵
bench-footprint-srv-disc-streaming = 모듈이 다운로드되는 동안 실행 시작
bench-footprint-srv-disc-indexeddb = 브라우저 세션 간 영구 스토리지
bench-footprint-srv-disc-worker = 반응형 UI를 위해 메인 스레드에서 쿼리 실행

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
bench-bullet-worker = Worker thread support
bench-bullet-prepared-stmts = 준비된 문장
bench-bullet-larger-scale = 더 큰 스케일 팩터
bench-bullet-parallel-clients = 병렬 클라이언트

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
bench-tpcc-disc-duckdb = DuckDB는 TPC-C에서 ~385 TPS만 달성합니다(VibeSQL보다 60배 느림, SQLite보다 12배 느림). 이는 예상된 결과입니다: DuckDB는 단일 행 트랜잭션이 아닌 대규모 배치 작업에 최적화된 <strong>분석(OLAP) 데이터베이스</strong>입니다.
bench-tpcc-disc-duckdb-title = DuckDB가 OLTP에서 뒤처지는 이유
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
conformance-pgsql-title = PostgreSQL Regression Tests
conformance-pgsql-desc = Results from running <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">PostgreSQL's regression test suite</a> - the canonical test suite used to validate PostgreSQL compatibility.
conformance-pgsql-tests-passing = tests passing
conformance-pgsql-tests-excluded = tests excluded
conformance-pgsql-pass-rate = Pass Rate
conformance-pgsql-excluded-reason = Excluded tests use PostgreSQL-specific features not applicable to VibeSQL
conformance-pgsql-note = <strong>Note:</strong> PostgreSQL regression tests validate SQL behavior against PostgreSQL's reference implementation. Excluded tests involve PostgreSQL-specific features like system catalogs, procedural languages, or extension modules.

# SQLite TCL 테스트 스위트 섹션
conformance-tcl-title = SQLite TCL 테스트 스위트
conformance-tcl-desc = { $fileCount }개의 테스트 파일을 포함하는 SQLite의 표준 <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">TCL 테스트 스위트</a> 결과입니다. 이 스위트는 SQLite 호환성 테스트의 표준입니다.
conformance-tcl-overall-rate = 전체 통과율
conformance-tcl-tests-passing = { $total }개 테스트 중 { $passed }개 통과
conformance-tcl-passed = 통과
conformance-tcl-failed = 실패
conformance-tcl-skipped = 건너뜀
conformance-tcl-total = 전체
conformance-tcl-categories-title = 테스트 카테고리
conformance-tcl-category = 카테고리
conformance-tcl-rate = 비율
conformance-tcl-progress = 진행률
conformance-tcl-tests = 테스트
conformance-tcl-common-failures = 자주 발생하는 실패
conformance-tcl-failure-patterns = 발생 횟수별 상위 { $count }개 실패 패턴
conformance-tcl-about-title = TCL 테스트 소개:
conformance-tcl-about-text = SQLite의 TCL 테스트 스위트는 SQLite 호환성을 위한 표준 적합성 테스트입니다. 표준 SQL 테스트 스위트에서 다루지 않을 수 있는 특정 SQLite 동작, 특이점 및 엣지 케이스를 테스트합니다. 여기서 높은 통과율은 애플리케이션 마이그레이션 시나리오에 대한 강력한 SQLite 호환성을 나타냅니다.
