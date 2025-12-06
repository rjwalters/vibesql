# VibeSQL Catalog Error Messages - Korean (ko)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = 테이블 '{ $name }'이(가) 이미 존재합니다
catalog-table-not-found = 테이블 '{ $table_name }'을(를) 찾을 수 없습니다

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = 컬럼 '{ $name }'이(가) 이미 존재합니다
catalog-column-not-found = 테이블 '{ $table_name }'에서 컬럼 '{ $column_name }'을(를) 찾을 수 없습니다

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = 스키마 '{ $name }'이(가) 이미 존재합니다
catalog-schema-not-found = 스키마 '{ $name }'을(를) 찾을 수 없습니다
catalog-schema-not-empty = 스키마 '{ $name }'이(가) 비어 있지 않습니다

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = 역할 '{ $name }'이(가) 이미 존재합니다
catalog-role-not-found = 역할 '{ $name }'을(를) 찾을 수 없습니다

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = 도메인 '{ $name }'이(가) 이미 존재합니다
catalog-domain-not-found = 도메인 '{ $name }'을(를) 찾을 수 없습니다
catalog-domain-in-use = 도메인 '{ $domain_name }'이(가) { $count }개의 컬럼에서 아직 사용 중입니다: { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = 시퀀스 '{ $name }'이(가) 이미 존재합니다
catalog-sequence-not-found = 시퀀스 '{ $name }'을(를) 찾을 수 없습니다
catalog-sequence-in-use = 시퀀스 '{ $sequence_name }'이(가) { $count }개의 컬럼에서 아직 사용 중입니다: { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = 타입 '{ $name }'이(가) 이미 존재합니다
catalog-type-not-found = 타입 '{ $name }'을(를) 찾을 수 없습니다
catalog-type-in-use = 타입 '{ $name }'이(가) 하나 이상의 테이블에서 아직 사용 중입니다

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = 콜레이션 '{ $name }'이(가) 이미 존재합니다
catalog-collation-not-found = 콜레이션 '{ $name }'을(를) 찾을 수 없습니다
catalog-character-set-already-exists = 문자 집합 '{ $name }'이(가) 이미 존재합니다
catalog-character-set-not-found = 문자 집합 '{ $name }'을(를) 찾을 수 없습니다
catalog-translation-already-exists = 번역 '{ $name }'이(가) 이미 존재합니다
catalog-translation-not-found = 번역 '{ $name }'을(를) 찾을 수 없습니다

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = 뷰 '{ $name }'이(가) 이미 존재합니다
catalog-view-not-found = 뷰 '{ $name }'을(를) 찾을 수 없습니다
catalog-view-in-use = 뷰 또는 테이블 '{ $view_name }'이(가) { $count }개의 뷰에서 아직 사용 중입니다: { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = 트리거 '{ $name }'이(가) 이미 존재합니다
catalog-trigger-not-found = 트리거 '{ $name }'을(를) 찾을 수 없습니다

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = 어설션 '{ $name }'이(가) 이미 존재합니다
catalog-assertion-not-found = 어설션 '{ $name }'을(를) 찾을 수 없습니다

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = 함수 '{ $name }'이(가) 이미 존재합니다
catalog-function-not-found = 함수 '{ $name }'을(를) 찾을 수 없습니다
catalog-procedure-already-exists = 프로시저 '{ $name }'이(가) 이미 존재합니다
catalog-procedure-not-found = 프로시저 '{ $name }'을(를) 찾을 수 없습니다

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = 제약 조건 '{ $name }'이(가) 이미 존재합니다
catalog-constraint-not-found = 제약 조건 '{ $name }'을(를) 찾을 수 없습니다

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = 테이블 '{ $table_name }'의 인덱스 '{ $index_name }'이(가) 이미 존재합니다
catalog-index-not-found = 테이블 '{ $table_name }'의 인덱스 '{ $index_name }'을(를) 찾을 수 없습니다

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = 테이블 '{ $table_name }'에서 순환 외래 키 종속성이 감지되었습니다: { $message }
