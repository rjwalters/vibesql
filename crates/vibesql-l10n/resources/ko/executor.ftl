# VibeSQL Executor Error Messages - Korean (ko)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = 테이블 '{ $name }'을(를) 찾을 수 없습니다
executor-table-already-exists = 테이블 '{ $name }'이(가) 이미 존재합니다

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = 테이블 '{ $table_name }'에서 컬럼 '{ $column_name }'을(를) 찾을 수 없습니다
executor-column-not-found-searched = 컬럼 '{ $column_name }'을(를) 찾을 수 없습니다 (검색한 테이블: { $searched_tables })
executor-column-not-found-with-available = 컬럼 '{ $column_name }'을(를) 찾을 수 없습니다 (검색한 테이블: { $searched_tables }). 사용 가능한 컬럼: { $available_columns }
executor-invalid-table-qualifier = 컬럼 '{ $column }'에 대한 테이블 한정자 '{ $qualifier }'이(가) 잘못되었습니다. 사용 가능한 테이블: { $available_tables }
executor-column-already-exists = 컬럼 '{ $name }'이(가) 이미 존재합니다
executor-column-index-out-of-bounds = 컬럼 인덱스 { $index }이(가) 범위를 벗어났습니다

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = 인덱스 '{ $name }'을(를) 찾을 수 없습니다
executor-index-already-exists = 인덱스 '{ $name }'이(가) 이미 존재합니다
executor-invalid-index-definition = 잘못된 인덱스 정의: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = 트리거 '{ $name }'을(를) 찾을 수 없습니다
executor-trigger-already-exists = 트리거 '{ $name }'이(가) 이미 존재합니다

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = 스키마 '{ $name }'을(를) 찾을 수 없습니다
executor-schema-already-exists = 스키마 '{ $name }'이(가) 이미 존재합니다
executor-schema-not-empty = 스키마 '{ $name }'을(를) 삭제할 수 없습니다: 스키마가 비어 있지 않습니다

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = 역할 '{ $name }'을(를) 찾을 수 없습니다
executor-permission-denied = 권한 거부: 역할 '{ $role }'에 { $object }에 대한 { $privilege } 권한이 없습니다
executor-dependent-privileges-exist = 종속 권한이 존재합니다: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = 타입 '{ $name }'을(를) 찾을 수 없습니다
executor-type-already-exists = 타입 '{ $name }'이(가) 이미 존재합니다
executor-type-in-use = 타입 '{ $name }'을(를) 삭제할 수 없습니다: 타입이 아직 사용 중입니다
executor-type-mismatch = 타입 불일치: { $left } { $op } { $right }
executor-type-error = 타입 오류: { $message }
executor-cast-error = { $from_type }을(를) { $to_type }(으)로 변환할 수 없습니다
executor-type-conversion-error = { $from }을(를) { $to }(으)로 변환할 수 없습니다

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = 0으로 나누기
executor-invalid-where-clause = 잘못된 WHERE 절: { $message }
executor-unsupported-expression = 지원되지 않는 표현식: { $message }
executor-unsupported-feature = 지원되지 않는 기능: { $message }
executor-parse-error = 구문 분석 오류: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = 스칼라 서브쿼리가 { $actual }개의 행을 반환했습니다. 예상: { $expected }개
executor-subquery-column-count-mismatch = 서브쿼리가 { $actual }개의 컬럼을 반환했습니다. 예상: { $expected }개
executor-column-count-mismatch = 파생 컬럼 목록에 { $provided }개의 컬럼이 있지만 쿼리는 { $expected }개의 컬럼을 생성합니다

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = 제약 조건 위반: { $message }
executor-multiple-primary-keys = 여러 PRIMARY KEY 제약 조건은 허용되지 않습니다
executor-cannot-drop-column = 컬럼을 삭제할 수 없습니다: { $message }
executor-constraint-not-found = 테이블 '{ $table_name }'에서 제약 조건 '{ $constraint_name }'을(를) 찾을 수 없습니다

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = 표현식 깊이 제한 초과: { $depth } > { $max_depth } (스택 오버플로우 방지)
executor-query-timeout-exceeded = 쿼리 시간 초과: { $elapsed_seconds }초 > { $max_seconds }초
executor-row-limit-exceeded = 행 처리 제한 초과: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = 메모리 제한 초과: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = 변수 '{ $variable_name }'을(를) 찾을 수 없습니다
executor-variable-not-found-with-available = 변수 '{ $variable_name }'을(를) 찾을 수 없습니다. 사용 가능한 변수: { $available_variables }
executor-label-not-found = 레이블 '{ $name }'을(를) 찾을 수 없습니다

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = 프로시저 SELECT INTO는 정확히 { $expected }개의 행을 반환해야 합니다. 실제: { $actual }개{ $plural }
executor-select-into-column-count = 프로시저 SELECT INTO 컬럼 수 불일치: { $expected }개의 변수{ $expected_plural }가 있지만 쿼리가 { $actual }개의 컬럼{ $actual_plural }을(를) 반환했습니다

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = 스키마 '{ $schema_name }'에서 프로시저 '{ $procedure_name }'을(를) 찾을 수 없습니다
executor-procedure-not-found-with-available = 스키마 '{ $schema_name }'에서 프로시저 '{ $procedure_name }'을(를) 찾을 수 없습니다
    .available = 사용 가능한 프로시저: { $available_procedures }
executor-procedure-not-found-with-suggestion = 스키마 '{ $schema_name }'에서 프로시저 '{ $procedure_name }'을(를) 찾을 수 없습니다
    .available = 사용 가능한 프로시저: { $available_procedures }
    .suggestion = '{ $suggestion }'을(를) 의미하셨습니까?

executor-function-not-found-simple = 스키마 '{ $schema_name }'에서 함수 '{ $function_name }'을(를) 찾을 수 없습니다
executor-function-not-found-with-available = 스키마 '{ $schema_name }'에서 함수 '{ $function_name }'을(를) 찾을 수 없습니다
    .available = 사용 가능한 함수: { $available_functions }
executor-function-not-found-with-suggestion = 스키마 '{ $schema_name }'에서 함수 '{ $function_name }'을(를) 찾을 수 없습니다
    .available = 사용 가능한 함수: { $available_functions }
    .suggestion = '{ $suggestion }'을(를) 의미하셨습니까?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }'은(는) { $expected }개의 매개변수{ $expected_plural }를 예상합니다 ({ $parameter_signature }), { $actual }개의 인수{ $actual_plural }가 전달됨
executor-parameter-type-mismatch = 매개변수 '{ $parameter_name }'은(는) { $expected_type } 타입을 예상합니다. 실제: { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = 인수 수 불일치: 예상 { $expected }개, 실제 { $actual }개

executor-recursion-limit-exceeded = 최대 재귀 깊이({ $max_depth }) 초과: { $message }
executor-recursion-call-stack = 호출 스택:
executor-function-must-return = 함수는 값을 반환해야 합니다
executor-invalid-control-flow = 잘못된 제어 흐름: { $message }
executor-invalid-function-body = 잘못된 함수 본문: { $message }
executor-function-read-only-violation = 함수 읽기 전용 위반: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = { $value_type } 값에서 { $field }을(를) 추출할 수 없습니다

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Arrow 배열을 { $expected_type }(으)로 다운캐스트하지 못했습니다 ({ $context })
executor-columnar-type-mismatch-binary = { $operation }에 대해 호환되지 않는 타입: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = { $operation }에 대해 호환되지 않는 타입: { $left_type }
executor-simd-operation-failed = SIMD { $operation } 실패: { $reason }
executor-columnar-column-not-found = 컬럼 인덱스 { $column_index }이(가) 범위를 벗어났습니다 (배치에 { $batch_columns }개의 컬럼이 있음)
executor-columnar-column-not-found-by-name = 컬럼을 찾을 수 없습니다: { $column_name }
executor-columnar-length-mismatch = { $context }에서 컬럼 길이 불일치: 예상 { $expected }, 실제 { $actual }
executor-unsupported-array-type = { $operation }에 대해 지원되지 않는 배열 타입: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name }은(는) { $expected }을(를) 예상합니다. 실제: { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = 커서 '{ $name }'이(가) 이미 존재합니다
executor-cursor-not-found = 커서 '{ $name }'을(를) 찾을 수 없습니다
executor-cursor-already-open = 커서 '{ $name }'이(가) 이미 열려 있습니다
executor-cursor-not-open = 커서 '{ $name }'이(가) 열려 있지 않습니다
executor-cursor-not-scrollable = 커서 '{ $name }'은(는) 스크롤할 수 없습니다 (SCROLL이 지정되지 않음)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = 스토리지 오류: { $message }
executor-other = { $message }
