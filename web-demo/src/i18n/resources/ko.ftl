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
