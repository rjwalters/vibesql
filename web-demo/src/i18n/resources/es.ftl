# VibeSQL Web UI - Español

# Page titles
page-title = VibeSQL - Base de Datos SQL:1999 con IA
demo-title = Demo de VibeSQL
benchmarks-title = Pruebas de Rendimiento - VibeSQL
benchmarks-heading = VibeSQL - Pruebas de Rendimiento
conformance-title = Informe de Conformidad - VibeSQL
conformance-heading = Informe de Conformidad
conformance-subtitle = Pruebas de Cumplimiento del Estándar SQL:1999

# Navigation
nav-showcase = Demostración SQL:1999
nav-conformance = Ver resultados de sqltest
nav-sqllogictest = Ver resultados de SQLLogicTest

# Editor section
editor-title = Editor SQL
editor-storage = Almacenamiento
editor-storage-init = Inicializando...
editor-execute = Ejecutar Consulta

# Results section
results-title = Resultados
results-empty = Ejecuta una consulta para ver los resultados
results-loading = Cargando...
results-rows = { $count } { $count ->
    [one] fila
   *[other] filas
}
results-rows-with-time = { $count } { $count ->
    [one] fila
   *[other] filas
} ({ $time }ms)
results-copy = Copiar al portapapeles
results-export = Exportar CSV
results-limit-warning = Mostrando las primeras { $limit } de { $total } filas. Usa la cláusula LIMIT para refinar tu consulta.

# Examples sidebar
examples-title = Ejemplos
examples-basic = Consultas Básicas
examples-advanced = Consultas Avanzadas

# Database selector
db-select-label = Base de datos

# Footer
footer-tagline = VibeSQL - Base de Datos SQL:1999 en WebAssembly
footer-deployed = Desplegado: { $date }

# Theme
theme-toggle-dark = Cambiar a modo oscuro
theme-toggle-light = Cambiar a modo claro

# Locale
locale-select = Seleccionar idioma

# Messages
msg-query-success = Consulta ejecutada correctamente
msg-rows-affected = { $count } { $count ->
    [one] fila afectada
   *[other] filas afectadas
}

# Errors
error-generic = Ocurrió un error
error-query-failed = La consulta falló
