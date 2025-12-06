# VibeSQL CLI Localization - Korean (ko)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - SQL:1999 FULL 표준 준수 데이터베이스
cli-help-hint = \help를 입력하면 도움말, \quit를 입력하면 종료합니다
cli-goodbye = 안녕히 가세요!
locale-changed = 언어가 { $locale }(으)로 변경되었습니다

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - SQL:1999 FULL 표준 준수 데이터베이스

cli-long-about = VibeSQL 명령줄 인터페이스

    사용 모드:
      대화형 REPL:       vibesql (--database <FILE>)
      명령 실행:         vibesql -c "SELECT * FROM users"
      파일 실행:         vibesql -f script.sql
      stdin에서 실행:    cat data.sql | vibesql
      타입 생성:         vibesql codegen --schema schema.sql --output types.ts

    대화형 REPL:
      -c, -f 또는 파이프 입력 없이 시작하면 VibeSQL은 readline 지원,
      명령 히스토리 및 다음과 같은 메타 명령을 갖춘 대화형 REPL을 시작합니다:
        \d (table)  - 테이블 설명 또는 모든 테이블 목록
        \dt         - 테이블 목록
        \f <format> - 출력 형식 설정
        \copy       - CSV/JSON 가져오기/내보내기
        \help       - 모든 REPL 명령 표시

    하위 명령:
      codegen           데이터베이스 스키마에서 TypeScript 타입 생성

    설정:
      ~/.vibesqlrc (TOML 형식)에서 설정을 구성할 수 있습니다.
      섹션: display, database, history, query

    예제:
      # 인메모리 데이터베이스로 대화형 REPL 시작
      vibesql

      # 영구 데이터베이스 파일 사용
      vibesql --database mydata.db

      # 단일 명령 실행
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # SQL 스크립트 파일 실행
      vibesql -f schema.sql -v

      # CSV에서 데이터 가져오기
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # 쿼리 결과를 JSON으로 내보내기
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # 스키마 파일에서 TypeScript 타입 생성
      vibesql codegen --schema schema.sql --output src/types.ts

      # 실행 중인 데이터베이스에서 TypeScript 타입 생성
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = 데이터베이스 파일 경로 (지정하지 않으면 인메모리 데이터베이스 사용)
arg-file-help = 파일에서 SQL 명령 실행
arg-command-help = SQL 명령을 직접 실행하고 종료
arg-stdin-help = stdin에서 SQL 명령 읽기 (파이프 시 자동 감지)
arg-verbose-help = 파일/stdin 실행 중 상세 출력 표시
arg-format-help = 쿼리 결과 출력 형식
arg-lang-help = 표시 언어 설정 (예: en-US, es, ja, ko)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = 데이터베이스 스키마에서 TypeScript 타입 생성

codegen-long-about = VibeSQL 데이터베이스 스키마에서 TypeScript 타입 정의를 생성합니다.

    이 명령은 데이터베이스의 모든 테이블에 대한 TypeScript 인터페이스와
    런타임 타입 검사 및 IDE 지원을 위한 메타데이터 객체를 생성합니다.

    입력 소스:
      --database <FILE>  기존 데이터베이스 파일에서 생성
      --schema <FILE>    SQL 스키마 파일(CREATE TABLE 문)에서 생성

    출력:
      --output <FILE>    생성된 타입을 이 파일에 쓰기 (기본값: types.ts)

    옵션:
      --camel-case       컬럼 이름을 camelCase로 변환
      --no-metadata      tables 메타데이터 객체 생성 건너뛰기

    예제:
      # 데이터베이스 파일에서
      vibesql codegen --database mydata.db --output src/db/types.ts

      # SQL 스키마 파일에서
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # camelCase 속성 이름으로
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = CREATE TABLE 문이 포함된 SQL 스키마 파일
codegen-output-help = 생성된 TypeScript의 출력 파일 경로
codegen-camel-case-help = 컬럼 이름을 camelCase로 변환
codegen-no-metadata-help = 테이블 메타데이터 객체 생성 건너뛰기

codegen-from-schema = 스키마 파일에서 TypeScript 타입 생성 중: { $path }
codegen-from-database = 데이터베이스에서 TypeScript 타입 생성 중: { $path }
codegen-written = TypeScript 타입이 저장됨: { $path }
codegen-error-no-source = --database 또는 --schema를 지정해야 합니다.
    사용법은 'vibesql codegen --help'를 참조하세요.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = 메타 명령:
help-describe = \d (table)      - 테이블 설명 또는 모든 테이블 목록
help-tables = \dt             - 테이블 목록
help-schemas = \ds             - 스키마 목록
help-indexes = \di             - 인덱스 목록
help-roles = \du             - 역할/사용자 목록
help-format = \f <format>     - 출력 형식 설정 (table, json, csv, markdown, html)
help-timing = \timing         - 쿼리 타이밍 토글
help-copy-to = \copy <table> TO <file>   - 테이블을 CSV/JSON 파일로 내보내기
help-copy-from = \copy <table> FROM <file> - CSV 파일을 테이블로 가져오기
help-save = \save (file)    - 데이터베이스를 SQL 덤프 파일로 저장
help-errors = \errors         - 최근 오류 기록 표시
help-help = \h, \help      - 이 도움말 표시
help-quit = \q, \quit      - 종료

help-sql-title = SQL 내성 검사:
help-show-tables = SHOW TABLES                  - 모든 테이블 목록
help-show-databases = SHOW DATABASES               - 모든 스키마/데이터베이스 목록
help-show-columns = SHOW COLUMNS FROM <table>    - 테이블 컬럼 표시
help-show-index = SHOW INDEX FROM <table>      - 테이블 인덱스 표시
help-show-create = SHOW CREATE TABLE <table>    - CREATE TABLE 문 표시
help-describe-sql = DESCRIBE <table>             - SHOW COLUMNS의 별칭

help-examples-title = 예제:
help-example-create = CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
help-example-insert = INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
help-example-select = SELECT * FROM users;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM users;
help-example-describe = DESCRIBE users;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy users TO '/tmp/users.csv'
help-example-copy-from = \copy users FROM '/tmp/users.csv'
help-example-copy-json = \copy users TO '/tmp/users.json'
help-example-errors = \errors

# =============================================================================
# Status Messages
# =============================================================================

format-changed = 출력 형식이 설정됨: { $format }
database-saved = 데이터베이스가 저장됨: { $path }
no-database-file = 오류: 데이터베이스 파일이 지정되지 않았습니다. \save <filename>을 사용하거나 --database 플래그로 시작하세요

# =============================================================================
# Error Display
# =============================================================================

no-errors = 이 세션에서 오류가 없습니다.
recent-errors = 최근 오류:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = 스크립트에서 SQL 문을 찾을 수 없습니다
script-executing = 문 { $current }/{ $total } 실행 중...
script-error = 문 { $index } 실행 오류: { $error }
script-summary-title = === 스크립트 실행 요약 ===
script-total = 전체 문: { $count }
script-successful = 성공: { $count }
script-failed = 실패: { $count }
script-failed-error = { $count }개의 문이 실패했습니다

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count }개 행 ({ $time }초)
rows-count = { $count }개 행

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = 경고: 설정 파일을 로드할 수 없습니다: { $error }
warning-auto-save-failed = 경고: 데이터베이스 자동 저장 실패: { $error }
warning-save-on-exit-failed = 경고: 종료 시 데이터베이스 저장 실패: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = 파일 '{ $path }' 읽기 실패: { $error }
stdin-read-error = stdin에서 읽기 실패: { $error }
database-load-error = 데이터베이스 로드 실패: { $error }
