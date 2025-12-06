# VibeSQL Catalog Error Messages - Indonesian (id)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = Tabel '{ $name }' sudah ada
catalog-table-not-found = Tabel '{ $table_name }' tidak ditemukan

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = Kolom '{ $name }' sudah ada
catalog-column-not-found = Kolom '{ $column_name }' tidak ditemukan dalam tabel '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = Skema '{ $name }' sudah ada
catalog-schema-not-found = Skema '{ $name }' tidak ditemukan
catalog-schema-not-empty = Skema '{ $name }' tidak kosong

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = Peran '{ $name }' sudah ada
catalog-role-not-found = Peran '{ $name }' tidak ditemukan

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = Domain '{ $name }' sudah ada
catalog-domain-not-found = Domain '{ $name }' tidak ditemukan
catalog-domain-in-use = Domain '{ $domain_name }' masih digunakan oleh { $count } kolom: { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = Urutan '{ $name }' sudah ada
catalog-sequence-not-found = Urutan '{ $name }' tidak ditemukan
catalog-sequence-in-use = Urutan '{ $sequence_name }' masih digunakan oleh { $count } kolom: { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = Tipe '{ $name }' sudah ada
catalog-type-not-found = Tipe '{ $name }' tidak ditemukan
catalog-type-in-use = Tipe '{ $name }' masih digunakan oleh satu atau lebih tabel

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = Kolasi '{ $name }' sudah ada
catalog-collation-not-found = Kolasi '{ $name }' tidak ditemukan
catalog-character-set-already-exists = Set karakter '{ $name }' sudah ada
catalog-character-set-not-found = Set karakter '{ $name }' tidak ditemukan
catalog-translation-already-exists = Terjemahan '{ $name }' sudah ada
catalog-translation-not-found = Terjemahan '{ $name }' tidak ditemukan

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = View '{ $name }' sudah ada
catalog-view-not-found = View '{ $name }' tidak ditemukan
catalog-view-in-use = View atau tabel '{ $view_name }' masih digunakan oleh { $count } view: { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = Trigger '{ $name }' sudah ada
catalog-trigger-not-found = Trigger '{ $name }' tidak ditemukan

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = Asersi '{ $name }' sudah ada
catalog-assertion-not-found = Asersi '{ $name }' tidak ditemukan

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = Fungsi '{ $name }' sudah ada
catalog-function-not-found = Fungsi '{ $name }' tidak ditemukan
catalog-procedure-already-exists = Prosedur '{ $name }' sudah ada
catalog-procedure-not-found = Prosedur '{ $name }' tidak ditemukan

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = Batasan '{ $name }' sudah ada
catalog-constraint-not-found = Batasan '{ $name }' tidak ditemukan

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = Indeks '{ $index_name }' pada tabel '{ $table_name }' sudah ada
catalog-index-not-found = Indeks '{ $index_name }' pada tabel '{ $table_name }' tidak ditemukan

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = Ketergantungan kunci asing melingkar terdeteksi untuk tabel '{ $table_name }': { $message }
