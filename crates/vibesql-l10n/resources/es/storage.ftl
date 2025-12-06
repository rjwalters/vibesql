# VibeSQL Storage Mensajes de Error - Español
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = Tabla '{ $name }' no encontrada

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = Discrepancia en el número de columnas: esperadas { $expected }, obtenidas { $actual }
storage-column-index-out-of-bounds = Índice de columna { $index } fuera de límites
storage-column-not-found = Columna '{ $column_name }' no encontrada en la tabla '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = El índice '{ $name }' ya existe
storage-index-not-found = Índice '{ $name }' no encontrado
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = Violación de restricción NOT NULL: la columna '{ $column }' no puede ser NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = Discrepancia de tipo en columna '{ $column }': esperado { $expected }, obtenido { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = Error de catálogo: { $message }
storage-transaction-error = Error de transacción: { $message }
storage-row-not-found = Fila no encontrada

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = Error de E/S: { $message }
storage-invalid-page-size = Tamaño de página inválido: esperado { $expected }, obtenido { $actual }
storage-invalid-page-id = ID de página inválido: { $page_id }
storage-lock-error = Error de bloqueo: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = Presupuesto de memoria excedido: usando { $used } bytes, presupuesto es { $budget } bytes
storage-no-index-to-evict = No hay índice disponible para desalojar (todos los índices ya tienen respaldo en disco)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = No implementado: { $message }
storage-other = { $message }
