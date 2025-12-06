# VibeSQL Executor Error Messages - Indonesian (id)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Tabel '{ $name }' tidak ditemukan
executor-table-already-exists = Tabel '{ $name }' sudah ada

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Kolom '{ $column_name }' tidak ditemukan dalam tabel '{ $table_name }'
executor-column-not-found-searched = Kolom '{ $column_name }' tidak ditemukan (tabel yang dicari: { $searched_tables })
executor-column-not-found-with-available = Kolom '{ $column_name }' tidak ditemukan (tabel yang dicari: { $searched_tables }). Kolom yang tersedia: { $available_columns }
executor-invalid-table-qualifier = Kualifier tabel '{ $qualifier }' tidak valid untuk kolom '{ $column }'. Tabel yang tersedia: { $available_tables }
executor-column-already-exists = Kolom '{ $name }' sudah ada
executor-column-index-out-of-bounds = Indeks kolom { $index } di luar batas

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Indeks '{ $name }' tidak ditemukan
executor-index-already-exists = Indeks '{ $name }' sudah ada
executor-invalid-index-definition = Definisi indeks tidak valid: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Trigger '{ $name }' tidak ditemukan
executor-trigger-already-exists = Trigger '{ $name }' sudah ada

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Skema '{ $name }' tidak ditemukan
executor-schema-already-exists = Skema '{ $name }' sudah ada
executor-schema-not-empty = Tidak dapat menghapus skema '{ $name }': skema tidak kosong

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Peran '{ $name }' tidak ditemukan
executor-permission-denied = Izin ditolak: peran '{ $role }' tidak memiliki hak { $privilege } pada { $object }
executor-dependent-privileges-exist = Hak dependen ada: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Tipe '{ $name }' tidak ditemukan
executor-type-already-exists = Tipe '{ $name }' sudah ada
executor-type-in-use = Tidak dapat menghapus tipe '{ $name }': tipe masih digunakan
executor-type-mismatch = Ketidakcocokan tipe: { $left } { $op } { $right }
executor-type-error = Kesalahan tipe: { $message }
executor-cast-error = Tidak dapat mengkonversi { $from_type } ke { $to_type }
executor-type-conversion-error = Tidak dapat mengkonversi { $from } ke { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Pembagian dengan nol
executor-invalid-where-clause = Klausa WHERE tidak valid: { $message }
executor-unsupported-expression = Ekspresi tidak didukung: { $message }
executor-unsupported-feature = Fitur tidak didukung: { $message }
executor-parse-error = Kesalahan parsing: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Subquery skalar mengembalikan { $actual } baris, diharapkan { $expected }
executor-subquery-column-count-mismatch = Subquery mengembalikan { $actual } kolom, diharapkan { $expected }
executor-column-count-mismatch = Daftar kolom turunan memiliki { $provided } kolom tetapi query menghasilkan { $expected } kolom

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Pelanggaran batasan: { $message }
executor-multiple-primary-keys = Batasan PRIMARY KEY ganda tidak diperbolehkan
executor-cannot-drop-column = Tidak dapat menghapus kolom: { $message }
executor-constraint-not-found = Batasan '{ $constraint_name }' tidak ditemukan dalam tabel '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Batas kedalaman ekspresi terlampaui: { $depth } > { $max_depth } (mencegah stack overflow)
executor-query-timeout-exceeded = Batas waktu query terlampaui: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Batas pemrosesan baris terlampaui: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Batas memori terlampaui: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Variabel '{ $variable_name }' tidak ditemukan
executor-variable-not-found-with-available = Variabel '{ $variable_name }' tidak ditemukan. Variabel yang tersedia: { $available_variables }
executor-label-not-found = Label '{ $name }' tidak ditemukan

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = SELECT INTO prosedural harus mengembalikan tepat { $expected } baris, mendapat { $actual } baris{ $plural }
executor-select-into-column-count = Ketidakcocokan jumlah kolom SELECT INTO prosedural: { $expected } variabel{ $expected_plural } tetapi query mengembalikan { $actual } kolom{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Prosedur '{ $procedure_name }' tidak ditemukan dalam skema '{ $schema_name }'
executor-procedure-not-found-with-available = Prosedur '{ $procedure_name }' tidak ditemukan dalam skema '{ $schema_name }'
    .available = Prosedur yang tersedia: { $available_procedures }
executor-procedure-not-found-with-suggestion = Prosedur '{ $procedure_name }' tidak ditemukan dalam skema '{ $schema_name }'
    .available = Prosedur yang tersedia: { $available_procedures }
    .suggestion = Apakah Anda maksud '{ $suggestion }'?

executor-function-not-found-simple = Fungsi '{ $function_name }' tidak ditemukan dalam skema '{ $schema_name }'
executor-function-not-found-with-available = Fungsi '{ $function_name }' tidak ditemukan dalam skema '{ $schema_name }'
    .available = Fungsi yang tersedia: { $available_functions }
executor-function-not-found-with-suggestion = Fungsi '{ $function_name }' tidak ditemukan dalam skema '{ $schema_name }'
    .available = Fungsi yang tersedia: { $available_functions }
    .suggestion = Apakah Anda maksud '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' mengharapkan { $expected } parameter{ $expected_plural } ({ $parameter_signature }), mendapat { $actual } argumen{ $actual_plural }
executor-parameter-type-mismatch = Parameter '{ $parameter_name }' mengharapkan { $expected_type }, mendapat { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Ketidakcocokan jumlah argumen: diharapkan { $expected }, mendapat { $actual }

executor-recursion-limit-exceeded = Kedalaman rekursi maksimum ({ $max_depth }) terlampaui: { $message }
executor-recursion-call-stack = Stack panggilan:
executor-function-must-return = Fungsi harus mengembalikan nilai
executor-invalid-control-flow = Alur kontrol tidak valid: { $message }
executor-invalid-function-body = Badan fungsi tidak valid: { $message }
executor-function-read-only-violation = Pelanggaran fungsi hanya-baca: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Tidak dapat mengekstrak { $field } dari nilai tipe { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Gagal menurunkan array Arrow ke { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Tipe tidak kompatibel untuk { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Tipe tidak kompatibel untuk { $operation }: { $left_type }
executor-simd-operation-failed = Operasi SIMD { $operation } gagal: { $reason }
executor-columnar-column-not-found = Indeks kolom { $column_index } di luar batas (batch memiliki { $batch_columns } kolom)
executor-columnar-column-not-found-by-name = Kolom tidak ditemukan: { $column_name }
executor-columnar-length-mismatch = Ketidakcocokan panjang kolom dalam { $context }: diharapkan { $expected }, mendapat { $actual }
executor-unsupported-array-type = Tipe array tidak didukung untuk { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } mengharapkan { $expected }, mendapat { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Kursor '{ $name }' sudah ada
executor-cursor-not-found = Kursor '{ $name }' tidak ditemukan
executor-cursor-already-open = Kursor '{ $name }' sudah terbuka
executor-cursor-not-open = Kursor '{ $name }' tidak terbuka
executor-cursor-not-scrollable = Kursor '{ $name }' tidak dapat di-scroll (SCROLL tidak ditentukan)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Kesalahan penyimpanan: { $message }
executor-other = { $message }
