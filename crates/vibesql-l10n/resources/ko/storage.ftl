# VibeSQL Storage Error Messages - Korean (ko)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = 테이블 '{ $name }'을(를) 찾을 수 없습니다

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = 컬럼 수 불일치: 예상 { $expected }개, 실제 { $actual }개
storage-column-index-out-of-bounds = 컬럼 인덱스 { $index }이(가) 범위를 벗어났습니다
storage-column-not-found = 테이블 '{ $table_name }'에서 컬럼 '{ $column_name }'을(를) 찾을 수 없습니다

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = 인덱스 '{ $name }'이(가) 이미 존재합니다
storage-index-not-found = 인덱스 '{ $name }'을(를) 찾을 수 없습니다
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = NOT NULL 제약 조건 위반: 컬럼 '{ $column }'은(는) NULL일 수 없습니다
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = 컬럼 '{ $column }'에서 타입 불일치: 예상 { $expected }, 실제 { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = 카탈로그 오류: { $message }
storage-transaction-error = 트랜잭션 오류: { $message }
storage-row-not-found = 행을 찾을 수 없습니다

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = I/O 오류: { $message }
storage-invalid-page-size = 잘못된 페이지 크기: 예상 { $expected }, 실제 { $actual }
storage-invalid-page-id = 잘못된 페이지 ID: { $page_id }
storage-lock-error = 잠금 오류: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = 메모리 예산 초과: 사용 중 { $used } 바이트, 예산 { $budget } 바이트
storage-no-index-to-evict = 퇴거할 인덱스가 없습니다 (모든 인덱스가 이미 디스크 기반입니다)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = 구현되지 않음: { $message }
storage-other = { $message }
