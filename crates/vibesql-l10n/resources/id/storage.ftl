# VibeSQL Storage Error Messages - Indonesian (id)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = Tabel '{ $name }' tidak ditemukan

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = Ketidakcocokan jumlah kolom: diharapkan { $expected }, mendapat { $actual }
storage-column-index-out-of-bounds = Indeks kolom { $index } di luar batas
storage-column-not-found = Kolom '{ $column_name }' tidak ditemukan dalam tabel '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = Indeks '{ $name }' sudah ada
storage-index-not-found = Indeks '{ $name }' tidak ditemukan
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = Pelanggaran batasan NOT NULL: kolom '{ $column }' tidak boleh NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = Ketidakcocokan tipe dalam kolom '{ $column }': diharapkan { $expected }, mendapat { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = Kesalahan katalog: { $message }
storage-transaction-error = Kesalahan transaksi: { $message }
storage-row-not-found = Baris tidak ditemukan

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = Kesalahan I/O: { $message }
storage-invalid-page-size = Ukuran halaman tidak valid: diharapkan { $expected }, mendapat { $actual }
storage-invalid-page-id = ID halaman tidak valid: { $page_id }
storage-lock-error = Kesalahan kunci: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = Anggaran memori terlampaui: menggunakan { $used } byte, anggaran { $budget } byte
storage-no-index-to-evict = Tidak ada indeks yang tersedia untuk dievakuasi (semua indeks sudah di disk)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = Tidak diimplementasikan: { $message }
storage-other = { $message }
