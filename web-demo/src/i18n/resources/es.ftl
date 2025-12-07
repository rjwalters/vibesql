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

# Editor
editor-placeholder = Ingresa tu consulta SQL aquí... (Ctrl+Enter o Cmd+Enter para ejecutar)

# Navigation links
nav-terminal = Terminal SQL Demo
nav-compliance = Informe de Cumplimiento SQL
nav-benchmarks = Pruebas de Rendimiento
nav-github = Repositorio en GitHub
nav-home = Inicio

# Results
results-success-zero = Consulta ejecutada correctamente (0 filas)
results-null = NULO

# Help Modal
help-title = Atajos de Teclado y Ayuda
help-close = Cerrar
help-editor-shortcuts = Atajos del Editor
help-navigation = Navegación
help-results-actions = Acciones de Resultados
help-tips = Consejos
help-shortcut-execute = Ejecutar consulta actual
help-shortcut-comment = Alternar comentario de línea
help-shortcut-indent = Indentar selección
help-shortcut-show-help = Mostrar este diálogo de ayuda
help-shortcut-close-help = Cerrar diálogo de ayuda
help-action-copy = Copiar al portapapeles
help-action-copy-desc = Copiar resultados como valores separados por tabulación
help-action-export = Exportar CSV
help-action-export-desc = Descargar resultados como archivo CSV
help-tip-limit = Los resultados están limitados a 1,000 filas por rendimiento. Usa la cláusula LIMIT para refinar las consultas.
help-tip-time = El tiempo de ejecución se muestra con los resultados de la consulta.
help-tip-syntax = El editor soporta resaltado de sintaxis SQL y autocompletado.
help-tip-theme = Alterna entre temas claro/oscuro usando el botón de tema.
help-got-it = ¡Entendido!

# Showcase Navigation
showcase-title = Demostración del Núcleo SQL:1999
showcase-description = Explora las características SQL:1999 Core implementadas de forma interactiva
showcase-complete = { $percent }% Completado
showcase-categories = Categorías de Características
showcase-legend = Leyenda de Estado
showcase-status-implemented = Completamente Implementado
showcase-status-partial = Parcialmente Implementado
showcase-status-planned = Planificado

# Showcase category labels
showcase-cat-compliance = Panel de Cumplimiento
showcase-cat-data-types = Tipos de Datos
showcase-cat-dml = Operaciones DML
showcase-cat-predicates = Predicados y Operadores
showcase-cat-joins = JOINs
showcase-cat-subqueries = Subconsultas
showcase-cat-aggregates = Agregados y GROUP BY
showcase-cat-ddl = DDL y Restricciones

# Common showcase elements
showcase-interactive-examples = Ejemplos Interactivos
showcase-try-example = Probar Este Ejemplo
showcase-progress = { $implemented } de { $total } { $type } ({ $percent }%)
showcase-table-status = Estado
showcase-table-category = Categoría
showcase-table-description = Descripción
showcase-table-syntax = Sintaxis
showcase-table-use-case = Caso de Uso

# Status labels
status-implemented = Implementado
status-partial = Parcial
status-planned = Planificado

# Aggregates Showcase
aggregates-title = Agregados SQL y GROUP BY
aggregates-description = Funciones de agregado del núcleo SQL:1999 y capacidades de agrupación
aggregates-reference = Referencia de Funciones de Agregado
aggregates-table-function = Función
aggregates-progress-type = funciones
aggregates-ex-basic = Funciones de Agregado Básicas
aggregates-ex-group-single = GROUP BY (Columna Única)
aggregates-ex-group-multiple = GROUP BY (Múltiples Columnas)
aggregates-ex-having = Cláusula HAVING
aggregates-ex-orderby = ORDER BY con Agregados
aggregates-ex-null = Manejo de NULL en Agregados

# DML Operations Showcase
dml-title = Operaciones DML (Lenguaje de Manipulación de Datos)
dml-description = Operaciones del núcleo SQL:1999 para consultar y modificar datos
dml-reference = Referencia de Operaciones DML
dml-table-operation = Operación
dml-progress-type = operaciones
dml-ex-select-basic = SELECT - Consultas Básicas
dml-ex-select-ordering = SELECT - Ordenamiento y Limitación
dml-ex-insert = Operaciones INSERT
dml-ex-update = Operaciones UPDATE
dml-ex-delete = Operaciones DELETE
dml-ex-combined = Flujo de Trabajo CRUD Combinado

# Data Types Showcase
datatypes-title = Tipos de Datos del Núcleo SQL:1999
datatypes-description = Explora los tipos de datos fundamentales definidos en la especificación SQL:1999 Core
datatypes-reference = Referencia de Tipos de Datos
datatypes-table-type = Nombre del Tipo
datatypes-table-example = Valores de Ejemplo
datatypes-table-spec = Especificación
datatypes-progress-type = tipos
datatypes-ex-numeric = Trabajando con Tipos Numéricos
datatypes-ex-null = Manejo de NULL y Lógica de Tres Valores
datatypes-ex-comparisons = Comparaciones y Operaciones de Tipos

# JOINs Showcase
joins-title = JOINs en SQL
joins-description = Operaciones JOIN del núcleo SQL:1999 para combinar datos de múltiples tablas
joins-reference = Referencia de Tipos de JOIN
joins-table-type = Tipo de JOIN
joins-progress-type = tipos de JOIN
joins-category-suffix = JOINs
joins-ex-sample = Configuración de Datos de Ejemplo
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN Multi-tabla

# Predicates Showcase
predicates-title = Predicados y Operadores
predicates-description = Predicados SQL:1999 para filtrado y operaciones lógicas
predicates-reference = Referencia de Predicados
predicates-table-predicate = Predicado
predicates-progress-type = predicados
predicates-ex-comparison = Operadores de Comparación
predicates-ex-between = BETWEEN y Predicados de Rango
predicates-ex-null = Predicados NULL y Lógica de Tres Valores
predicates-ex-boolean = Lógica Booleana (AND, OR, NOT)
predicates-ex-in = Predicado IN con Subconsultas
predicates-ex-combined = Operaciones de Predicados Combinadas

# Subqueries Showcase
subqueries-title = Subconsultas SQL
subqueries-description = Capacidades de subconsultas del núcleo SQL:1999 para operaciones de consultas anidadas
subqueries-reference = Referencia de Tipos de Subconsulta
subqueries-table-type = Tipo de Subconsulta
subqueries-progress-type = tipos de subconsulta
subqueries-ex-scalar-select = Subconsulta Escalar en SELECT
subqueries-ex-scalar-where = Subconsulta Escalar en WHERE
subqueries-ex-derived = Tablas Derivadas (Subconsulta en FROM)
subqueries-ex-in = Predicado IN con Subconsulta
subqueries-ex-correlated = Subconsultas Correlacionadas
subqueries-ex-nested = Subconsultas Anidadas
