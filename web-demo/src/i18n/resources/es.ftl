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
error-no-databases = No hay bases de datos disponibles

# Loading states
loading-initializing-theme = Inicializando tema
loading-preparing-editor = Preparando editor
loading-database-engine = Cargando motor de base de datos
loading-setting-up-ui = Configurando interfaz de usuario
loading-editor = Cargando editor...
loading-compliance-data = Cargando datos de cumplimiento...
loading-conformance-report = Cargando informe de conformidad...

# Editor
editor-placeholder = Ingresa tu consulta SQL aquí... (Ctrl+Enter o Cmd+Enter para ejecutar)

# Navigation links
nav-terminal = Terminal SQL Demo
nav-compliance = Informe de Cumplimiento SQL
nav-benchmarks = Pruebas de Rendimiento
nav-github = Repositorio en GitHub
nav-home = Inicio
nav-trends = Tendencias de Rendimiento

# Trends page
trends-title = Tendencias de Rendimiento - VibeSQL
trends-heading = VibeSQL - Tendencias de Rendimiento
trends-total-runs = Ejecuciones Totales
trends-across-suites = en todas las suites
trends-date-range = Rango de Fechas
trends-first-to-last = primera a última ejecución
trends-latest-commit = Último Commit
trends-most-recent = benchmark más reciente
trends-generated = Generado
trends-last-export = última exportación de datos

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

# =============================================================================
# Página de Benchmarks
# =============================================================================

# Encabezados de sección
bench-section-embedded = Embebido
bench-section-server = Servidor
bench-results-title = Resultados de Benchmark
bench-perf-comparison = Comparación de Rendimiento
bench-methodology-title = Metodología
bench-analysis-roadmap = Análisis y Hoja de Ruta

# Tarjetas de resumen
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = Operaciones Probadas
bench-last-updated = Última Actualización
bench-avg-speedup = aceleración promedio
bench-from-main = desde rama main
bench-loading = Cargando...
bench-na = N/D
bench-faster = { $value }x más rápido
bench-slower = { $value }x más lento
bench-speedup = { $value }x
bench-startup-time-label = tiempo de inicio
bench-download-size = tamaño de descarga
bench-uncompressed = sin comprimir
bench-size-metrics = métricas de tamaño
bench-failed = FALLIDO
bench-failed-title = Consulta fallida (tiempo agotado o error)
bench-no-wasm-data = No hay datos WASM disponibles
bench-no-server-data = No hay datos de benchmark de servidor Sysbench disponibles
bench-no-server-data-hint = Los benchmarks de servidor requieren ejecutar sysbench_server con la comparación de MySQL habilitada.

# Encabezados de tabla
bench-table-operation = Operación
bench-table-query = Consulta
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = Cargando resultados de benchmark...
bench-vibesql-server-title = VibeSQL vía protocolo PostgreSQL

# Términos comunes de benchmark
bench-hardware = Hardware
bench-benchmark-framework = Framework de Benchmark
bench-scale-factor = Factor de Escala
bench-data = Datos
bench-databases-tested = Bases de Datos Probadas
bench-execution-mode = Modo de Ejecución
bench-measurement = Medición
bench-workload = Carga de Trabajo
bench-transaction-mix = Mezcla de Transacciones
bench-warehouses = Almacenes
bench-concurrency = Concurrencia
bench-acid-compliance = Cumplimiento ACID
bench-mode = Modo
bench-workload-types = Tipos de Carga de Trabajo
bench-table-size = Tamaño de Tabla
bench-index-types = Tipos de Índice
bench-operations = Operaciones
bench-databases = Bases de Datos
bench-protocol-overhead = Sobrecarga de Protocolo
bench-binary-size = Tamaño del Binario
bench-startup-time = Tiempo de Inicio
bench-peak-memory = Memoria Máxima
bench-schema = Esquema
bench-query-count = Cantidad de Consultas
bench-query-types = Tipos de Consulta
bench-sql-features = Características SQL
bench-wasm-size = Tamaño WASM
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H específico
bench-tpch-name = TPC-H
bench-tpch-title = Benchmark de Soporte a Decisiones TPC-H
bench-tpch-description = Estos benchmarks utilizan el <strong>conjunto de benchmarks TPC-H</strong> estándar de la industria, que simula cargas de trabajo de soporte a decisiones del mundo real con consultas analíticas complejas que involucran agregaciones, joins, subconsultas y ordenamiento.
bench-tpch-ops-label = consultas TPC-H
bench-tpch-note-intro = Todos los benchmarks miden el tiempo de ejecución de consultas de extremo a extremo, incluyendo análisis, planificación, ejecución y materialización de resultados. Esto representa el <strong>rendimiento real del motor SQL</strong> para cargas de trabajo analíticas.
bench-tpch-note-queries = <strong>Nota:</strong> Las consultas TPC-H prueban diferentes aspectos del rendimiento SQL: agregaciones simples (Q1, Q6), joins complejos (Q2-Q5, Q7-Q10), subconsultas (Q11-Q15) y análisis avanzado (Q16-Q22). Pase el cursor sobre los nombres de consultas en la tabla para ver descripciones.

# Discusión TPC-H
bench-tpch-disc-excels-title = Donde VibeSQL Destaca
bench-tpch-disc-excels = VibeSQL muestra un rendimiento sólido en <strong>consultas de agregación intensivas en escaneo</strong> (Q1, Q6, Q14, Q15, Q20) donde nuestro motor de ejecución columnar y agregaciones aceleradas por SIMD brillan. Estas consultas involucran filtrar tablas grandes y calcular agregados sin patrones de join complejos.
bench-tpch-disc-targets-title = Objetivos de Optimización Actuales
bench-tpch-disc-targets = Las consultas de join múltiple (Q3, Q5, Q7-Q10, Q18, Q19, Q21) actualmente muestran a SQLite adelante. El cuello de botella principal es nuestra implementación de hash join, que aún no emplea el mismo nivel de optimización que los joins B-tree refinados durante décadas de SQLite. Áreas específicas en desarrollo activo:
bench-tpch-disc-join-ordering = Estimación de cardinalidad mejorada para mejor selección de orden de join
bench-tpch-disc-hash-sizing = Crecimiento adaptativo de tabla hash y derrame a disco para joins grandes
bench-tpch-disc-vectorized = Procesamiento por lotes en el bucle interno del join para mejorar utilización de caché
bench-tpch-disc-inl-joins = Aprovechamiento de índices B-tree cuando es beneficioso
bench-tpch-disc-path-title = Camino hacia el Liderazgo
bench-tpch-disc-path = La arquitectura de VibeSQL está diseñada para hardware moderno con características como almacenamiento columnar, ejecución vectorizada y concurrencia sin bloqueos. A medida que estas optimizaciones maduren, esperamos que VibeSQL logre un liderazgo consistente en todas las consultas TPC-H. El diseño fundamental soporta paralelismo y SIMD que las bases de datos tradicionales de almacenamiento por filas no pueden adaptar fácilmente.

# Descripciones de Consultas TPC-H
bench-tpch-q1 = Informe Resumen de Precios - Agregación de precios con GROUP BY y ORDER BY
bench-tpch-q2 = Proveedor de Costo Mínimo - JOIN de 3 tablas con ORDER BY y LIMIT
bench-tpch-q3 = Prioridad de Envío - JOIN de 3 tablas con agregación
bench-tpch-q4 = Verificación de Prioridad de Pedido - Subconsulta EXISTS correlacionada
bench-tpch-q5 = Volumen de Proveedor Local - JOIN de 6 tablas con filtrado complejo
bench-tpch-q6 = Pronóstico de Cambio de Ingresos - Filtros WHERE con BETWEEN y SUM
bench-tpch-q7 = Volumen de Envío - JOIN de 6 tablas con SUBSTR y filtrado de fechas
bench-tpch-q8 = Cuota de Mercado Nacional - JOIN de 7 tablas con expresiones CASE
bench-tpch-q9 = Medida de Ganancia por Tipo de Producto - JOIN de 4 tablas con agregación
bench-tpch-q10 = Informe de Artículos Devueltos - JOIN de 4 tablas con TOP-N LIMIT
bench-tpch-q11 = Identificación de Stock Importante - Subconsulta en cláusula HAVING
bench-tpch-q12 = Prioridad de Modos de Envío - Agregación CASE con lógica de fechas
bench-tpch-q13 = Distribución de Clientes - LEFT OUTER JOIN con subconsulta
bench-tpch-q14 = Efecto de Promoción - Agregación condicional con CASE
bench-tpch-q15 = Mejor Proveedor - Subconsultas anidadas con MAX
bench-tpch-q16 = Relación Partes/Proveedor - Subconsulta NOT IN con DISTINCT
bench-tpch-q17 = Ingresos por Pedidos Pequeños - Subconsulta correlacionada en WHERE
bench-tpch-q18 = Cliente de Gran Volumen - GROUP BY con HAVING
bench-tpch-q19 = Ingresos con Descuento - Condiciones OR complejas
bench-tpch-q20 = Promoción Potencial de Partes - Subconsulta IN con GROUP BY/HAVING
bench-tpch-q21 = Proveedores que Retrasaron Pedidos - EXISTS de múltiples tablas
bench-tpch-q22 = Oportunidad de Ventas Global - SUBSTR con subconsulta NOT EXISTS

# TPC-DS específico
bench-tpcds-name = TPC-DS
bench-tpcds-title = Benchmark de Soporte a Decisiones TPC-DS
bench-tpcds-description = <strong>TPC-DS</strong> es el sucesor de TPC-H, con 99 consultas que modelan un sistema moderno de soporte a decisiones con patrones de consulta significativamente más complejos incluyendo múltiples tablas de hechos, esquema copo de nieve y características SQL avanzadas.
bench-tpcds-ops-label = consultas TPC-DS
bench-tpcds-note-intro = Las consultas TPC-DS son sustancialmente más complejas que TPC-H, probando características SQL avanzadas como funciones de ventana, expresiones de tabla comunes (cláusula WITH) y patrones de join complejos a través de múltiples tablas de hechos y dimensiones.
bench-tpcds-note-remaining = <strong>Nota:</strong> Las consultas restantes no soportadas requieren características como INTERSECT/EXCEPT o funciones específicas de aritmética de fechas aún no implementadas.

# Discusión TPC-DS
bench-tpcds-disc-coverage-title = Cobertura de Características SQL:1999
bench-tpcds-disc-coverage = TPC-DS ejercita las características SQL más exigentes. VibeSQL pasa <strong>88 de 99 consultas</strong>, demostrando amplia cobertura de SQL:1999 incluyendo ROLLUP, CUBE, GROUPING(), funciones de ventana con enmarcado complejo y CTEs recursivos. Las consultas restantes requieren operaciones de conjunto INTERSECT/EXCEPT.
bench-tpcds-disc-optimization-title = Optimización de Consultas Complejas
bench-tpcds-disc-optimization = Las consultas TPC-DS frecuentemente unen más de 10 tablas con subconsultas correlacionadas. Áreas de enfoque actuales:
bench-tpcds-disc-cte = Decisión inteligente entre CTEs materializados e inline
bench-tpcds-disc-decorrelation = Conversión de subconsultas correlacionadas a joins cuando es beneficioso
bench-tpcds-disc-star = Ordenamiento de joins hecho-dimensión para patrones analíticos
bench-tpcds-disc-toward-title = Hacia 99/99
bench-tpcds-disc-toward = INTERSECT y EXCEPT son adiciones planificadas que habilitarán las consultas restantes. Estas operaciones de conjunto encajan naturalmente en nuestro álgebra de consultas existente y se implementarán como operadores basados en hash similares a nuestro procesamiento DISTINCT.

# TPC-C específico
bench-tpcc-name = TPC-C
bench-tpcc-title = Benchmark de Procesamiento de Transacciones en Línea TPC-C
bench-tpcc-description = El <strong>benchmark TPC-C</strong> simula un entorno completo de entrada de pedidos con una mezcla de transacciones complejas incluyendo entrada de pedidos, procesamiento de pagos, consultas de estado de pedidos, procesamiento de entregas y monitoreo de nivel de stock.
bench-tpcc-ops-label = transacciones TPC-C
bench-tpcc-transactions-label = transacciones ejecutadas
bench-tpcc-note-intro = TPC-C mide transacciones por minuto (tpmC) y prueba la capacidad de la base de datos para manejar transacciones concurrentes con lógica de negocio compleja. Este benchmark es crítico para evaluar el <strong>rendimiento de carga de trabajo transaccional</strong>.
bench-tpcc-note-results = <strong>Nota:</strong> Los resultados muestran la latencia promedio de transacción. Menor es mejor. TPC-C es particularmente exigente para cargas de trabajo con muchas escrituras con requisitos estrictos de consistencia.

# Descripciones de Transacciones TPC-C
bench-tpcc-new-order = Nuevo Pedido - Transacción compleja con verificaciones de inventario y creación de pedido
bench-tpcc-payment = Pago - Actualizar balance de cliente y totales de almacén/distrito
bench-tpcc-order-status = Estado de Pedido - Consulta de solo lectura para historial de pedidos del cliente
bench-tpcc-delivery = Entrega - Procesamiento por lotes de pedidos pendientes
bench-tpcc-stock-level = Nivel de Stock - Contar artículos bajo umbral en pedidos recientes

# Discusión TPC-C
bench-tpcc-disc-faster-title = 42x Más Rápido que SQLite
bench-tpcc-disc-faster = VibeSQL logra <strong>~79,000 transacciones por segundo</strong> comparado con ~1,900 TPS de SQLite, una mejora de 42x. Esta aceleración dramática proviene de nuestra arquitectura MVCC sin bloqueos que evita el bloqueo de granularidad gruesa de SQLite en cada operación de escritura.
bench-tpcc-disc-dominates-title = Por Qué VibeSQL Domina OLTP
bench-tpcc-disc-lockfree = MVCC permite que lectores y escritores procedan concurrentemente sin bloqueos
bench-tpcc-disc-optimistic = Las transacciones solo entran en conflicto en el momento del commit, no durante la ejecución
bench-tpcc-disc-btree = Estructura de índice construida a propósito optimizada para cargas de trabajo en memoria
bench-tpcc-disc-prepared = Los planes de consulta se compilan una vez y se reutilizan
bench-tpcc-disc-scaling-title = Escalando Más Allá
bench-tpcc-disc-scaling = Los resultados actuales son de un solo hilo. La arquitectura de VibeSQL soporta procesamiento de transacciones multi-hilo, y esperamos escalado casi lineal a medida que agreguemos soporte de ejecución paralela. Nuestro objetivo es lograr más de 500K TPS en hardware multi-núcleo moderno.
bench-tpcc-disc-duckdb-title = Por Qué DuckDB Queda Atrás en OLTP
bench-tpcc-disc-duckdb = DuckDB logra solo ~385 TPS en TPC-C (60x más lento que VibeSQL, 12x más lento que SQLite). Esto es esperado: DuckDB es una <strong>base de datos analítica (OLAP)</strong> optimizada para operaciones por lotes grandes, no transacciones de una sola fila. Su formato de almacenamiento columnar sobresale en escanear millones de filas pero agrega sobrecarga para búsquedas puntuales y actualizaciones pequeñas que dominan las cargas de trabajo OLTP como TPC-C.

# Sysbench Embebido específico
bench-sysbench-embedded-name = Sysbench (Embebido)
bench-sysbench-embedded-title = Micro-Benchmarks Sysbench (Embebido)
bench-sysbench-embedded-description = <strong>Sysbench</strong> proporciona micro-benchmarks enfocados que aíslan operaciones específicas de base de datos. Estas pruebas miden el rendimiento bruto para operaciones fundamentales sin la complejidad de cargas de trabajo de transacciones completas.
bench-sysbench-embedded-ops-label = operaciones Sysbench
bench-sysbench-embedded-note = El modo embebido ejecuta la base de datos en proceso con cero sobrecarga de red, ideal para aplicaciones de un solo proceso donde la latencia mínima es crítica.

# Descripciones de Operaciones Sysbench
bench-sysbench-point-select = Selección Puntual - Búsqueda de una sola fila por clave primaria
bench-sysbench-insert = Insertar - Insertar nuevas filas en la tabla
bench-sysbench-update-index = Actualizar Índice - Actualizar columna indexada (k = k + 1)
bench-sysbench-update-non-index = Actualizar Sin Índice - Actualizar columna no indexada
bench-sysbench-delete = Eliminar - Eliminar filas por clave primaria
bench-sysbench-range-queries = Consultas de Rango - Escaneos de rango Simple, SUM, ORDER BY y DISTINCT

# Discusión Sysbench Embebido
bench-sysbench-emb-disc-point-title = Búsquedas Puntuales: VibeSQL Lidera
bench-sysbench-emb-disc-point = La API directa de VibeSQL logra <strong>~137ns por selección puntual</strong>, igualando a SQLite y superando vastamente a DuckDB (~140µs). Nuestra implementación B-tree está optimizada para búsquedas de una sola fila con mínimo seguimiento de punteros y diseños de nodos amigables con la caché.
bench-sysbench-emb-disc-index-title = Actualizaciones de Índice: 2x Más Rápido
bench-sysbench-emb-disc-index = Las actualizaciones indexadas de VibeSQL ejecutan a <strong>~740ns vs ~1.6µs de SQLite</strong>. Nuestro diseño MVCC permite actualizaciones de índice in situ sin sobrecarga de registro de escritura anticipada para cada operación.
bench-sysbench-emb-disc-improve-title = Áreas de Mejora
bench-sysbench-emb-disc-bulk = La ruta de inserción por lotes de SQLite está altamente optimizada; estamos agregando operaciones B-tree por lotes
bench-sysbench-emb-disc-nonindex = Los escaneos de tabla completa para columnas no indexadas necesitan optimización de pushdown de predicados
bench-sysbench-emb-disc-deletes = Nuestra eliminación basada en tombstones tiene sobrecarga de limpieza; se planean mejoras de compactación
bench-sysbench-emb-disc-duckdb-title = Comparación con DuckDB
bench-sysbench-emb-disc-duckdb = DuckDB está optimizado para cargas de trabajo analíticas, no micro-operaciones. Sus resultados 100-1000x más lentos aquí reflejan elecciones arquitectónicas (almacenamiento columnar, ejecución vectorizada) que intercambian latencia de una sola fila por rendimiento en masa. VibeSQL apunta a ambos casos de uso.
bench-sysbench-emb-disc-architecture-title = Architectural Trade-offs
bench-sysbench-emb-disc-architecture = VibeSQL's hybrid architecture targets both OLTP and OLAP workloads. Our B-tree storage provides SQLite-competitive point lookup performance, while columnar execution handles analytical queries efficiently. This differs from pure OLAP databases like DuckDB that optimize exclusively for bulk operations at the cost of single-row latency.

# Sysbench Servidor específico
bench-sysbench-server-name = Sysbench (Servidor)
bench-sysbench-server-title = Micro-Benchmarks Sysbench (Servidor)
bench-sysbench-server-description = Los benchmarks de servidor <strong>Sysbench</strong> comparan VibeSQL Server (protocolo PostgreSQL) contra MySQL, midiendo rendimiento para despliegues de base de datos multi-cliente.
bench-sysbench-server-ops-label = operaciones Sysbench
bench-sysbench-server-note = El modo servidor usa el protocolo PostgreSQL, habilitando acceso multi-cliente y compatibilidad con herramientas y drivers PostgreSQL existentes.

# Discusión Sysbench Servidor
bench-sysbench-srv-disc-protocol-title = Protocolo PostgreSQL
bench-sysbench-srv-disc-protocol = VibeSQL Server implementa el protocolo PostgreSQL, habilitando compatibilidad con drivers y herramientas PostgreSQL existentes. Esto agrega ~10-50µs de sobrecarga de protocolo por consulta comparado con el modo embebido, pero habilita despliegues multi-cliente.
bench-sysbench-srv-disc-mysql-title = Comparación con MySQL
bench-sysbench-srv-disc-mysql = Los benchmarks de servidor comparan contra MySQL para evaluar VibeSQL como reemplazo directo para bases de datos cliente-servidor tradicionales. Los resultados varían por tipo de operación, con VibeSQL mostrando ventajas en cargas de trabajo de lectura intensiva.
bench-sysbench-srv-disc-roadmap-title = Hoja de Ruta del Servidor
bench-sysbench-srv-disc-pooling = Reducir sobrecarga de establecimiento de conexión para escenarios de alto rendimiento
bench-sysbench-srv-disc-caching = Caché de planes de consulta del lado del servidor entre conexiones
bench-sysbench-srv-disc-extended = Soporte completo del protocolo de consulta extendida PostgreSQL para operaciones por lotes

# TPC-H Servidor específico
bench-tpch-server-name = TPC-H (Servidor)
bench-tpch-server-title = Benchmark Analítico TPC-H (Servidor)
bench-tpch-server-description = Los <strong>benchmarks de servidor TPC-H</strong> comparan VibeSQL Server (protocolo PostgreSQL) contra MySQL para cargas de trabajo de consultas analíticas, midiendo el rendimiento OLAP en despliegues cliente-servidor.
bench-tpch-server-ops-label = consultas TPC-H
bench-tpch-server-note-intro = Los benchmarks de servidor prueban la implementación del <strong>protocolo PostgreSQL</strong>, midiendo la latencia de consulta de extremo a extremo incluyendo la sobrecarga de red.
bench-tpch-server-note-queries = Las consultas prueban JOINs complejos, subconsultas y agregaciones típicas de cargas de trabajo de inteligencia empresarial.

# Discusión TPC-H Servidor
bench-tpch-srv-disc-protocol-title = Protocolo PostgreSQL
bench-tpch-srv-disc-protocol = VibeSQL Server habla el protocolo PostgreSQL, permitiendo el uso de drivers y herramientas PostgreSQL estándar. Este benchmark mide la latencia completa de extremo a extremo incluyendo la sobrecarga del protocolo.
bench-tpch-srv-disc-comparison-title = Comparación con MySQL
bench-tpch-srv-disc-comparison = Comparar con MySQL proporciona una línea base para bases de datos cliente-servidor tradicionales en cargas de trabajo analíticas. El motor de ejecución columnar de VibeSQL proporciona ventajas para agregaciones y joins complejos.
bench-tpch-srv-disc-roadmap-title = Hoja de Ruta OLAP Servidor
bench-tpch-srv-disc-prepared = Reutilizar planes de consulta compilados entre conexiones
bench-tpch-srv-disc-pooling = Manejo eficiente de conexiones para escenarios de alto rendimiento
bench-tpch-srv-disc-scale = Pruebas con conjuntos de datos más grandes (SF 0.1, SF 1.0) para validación a escala de producción

# TPC-C Servidor específico
bench-tpcc-server-name = TPC-C (Servidor)
bench-tpcc-server-title = Benchmark OLTP TPC-C (Servidor)
bench-tpcc-server-description = Los <strong>benchmarks de servidor TPC-C</strong> comparan VibeSQL Server (protocolo PostgreSQL) contra MySQL para cargas de trabajo de transacciones OLTP, midiendo el rendimiento para despliegues de bases de datos multi-cliente.
bench-tpcc-server-ops-label = transacciones TPC-C
bench-tpcc-server-note-intro = Los benchmarks de servidor prueban la implementación del <strong>protocolo PostgreSQL</strong>, midiendo el rendimiento transaccional incluyendo la sobrecarga de red.
bench-tpcc-server-note-results = Los resultados reportan transacciones por segundo (TPS) para la mezcla de transacciones TPC-C estándar.
bench-tpcc-mixed = Carga Mixta - Mezcla de transacciones TPC-C estándar (45% Nueva-Orden, 43% Pago, 4% Estado-Orden, 4% Entrega, 4% Nivel-Stock)

# Discusión TPC-C Servidor
bench-tpcc-srv-disc-protocol-title = Protocolo PostgreSQL
bench-tpcc-srv-disc-protocol = VibeSQL Server habla el protocolo PostgreSQL, permitiendo el uso de drivers y herramientas PostgreSQL estándar. Este benchmark mide la latencia transaccional completa de extremo a extremo incluyendo la sobrecarga del protocolo.
bench-tpcc-srv-disc-comparison-title = Comparación con MySQL
bench-tpcc-srv-disc-comparison = Comparar con MySQL proporciona una línea base para bases de datos cliente-servidor tradicionales en cargas de trabajo OLTP. MySQL es el estándar de la industria para cargas de trabajo transaccionales, y TPC-C es la fortaleza de MySQL.
bench-tpcc-srv-disc-roadmap-title = Hoja de Ruta OLTP Servidor
bench-tpcc-srv-disc-prepared = Reutilizar planes de consulta compilados entre conexiones
bench-tpcc-srv-disc-pooling = Manejo eficiente de conexiones para escenarios de alto rendimiento
bench-tpcc-srv-disc-parallel = Procesamiento concurrente de transacciones multi-cliente

# Huella Embebida específico
bench-footprint-embedded-name = Huella (Embebido)
bench-footprint-embedded-title = Huella de Binario Nativo
bench-footprint-embedded-description = Los <strong>benchmarks de huella embebida</strong> miden la eficiencia de recursos de binarios de base de datos nativos, comparando tamaño de binario, tiempo de inicio en frío y uso máximo de memoria.
bench-footprint-embedded-ops-label = bases de datos comparadas
bench-footprint-embedded-note = La huella de binario nativo es crítica para <strong>despliegues embebidos y de borde</strong> donde el tamaño del binario, latencia de inicio y consumo de memoria impactan directamente la viabilidad del despliegue.

# Descripciones de Huella Embebida
bench-footprint-binary-size = Tamaño del Binario - Tamaño del binario de base de datos compilado en disco
bench-footprint-startup-time = Tiempo de Inicio - Tiempo para iniciar en frío y ejecutar la primera consulta
bench-footprint-peak-memory = Memoria Máxima - Tamaño máximo del conjunto residente durante la inicialización

# Discusión de Huella Embebida
bench-footprint-emb-disc-size-title = Tamaño del Binario: Punto Medio
bench-footprint-emb-disc-size = VibeSQL con <strong>~17MB</strong> se sitúa entre SQLite (~5MB) y DuckDB (~45MB). Esto refleja nuestra elección de incluir características avanzadas (funciones de ventana, CTEs, ejecución columnar) mientras mantenemos el binario manejable para despliegues embebidos.
bench-footprint-emb-disc-startup-title = Inicio: El Más Rápido en Frío
bench-footprint-emb-disc-startup = VibeSQL logra <strong>~7.7ms de inicio en frío</strong>, ligeramente más rápido que SQLite (~8.2ms) y significativamente más rápido que DuckDB (~14.6ms). Nuestra ruta de inicialización mínima carga solo estructuras de metadatos esenciales al iniciar.
bench-footprint-emb-disc-memory-title = Eficiencia de Memoria
bench-footprint-emb-disc-memory = La memoria máxima durante el inicio es ~7MB para VibeSQL vs ~3MB para SQLite y ~11MB para DuckDB. La diferencia con SQLite refleja nuestro optimizador de consultas más sofisticado e infraestructura de ejecución columnar que se asigna por adelantado.
bench-footprint-emb-disc-roadmap-title = Hoja de Ruta de Reducción de Tamaño
bench-footprint-emb-disc-flags = Selección de características en tiempo de compilación para excluir funcionalidad no utilizada
bench-footprint-emb-disc-lto = Optimización de tiempo de enlace de programa completo para eliminación de código muerto
bench-footprint-emb-disc-modular = Separar motor central de características opcionales (ej., funciones de ventana)

# Huella Servidor/WASM específico
bench-footprint-server-name = Huella (Servidor/WASM)
bench-footprint-server-title = Huella WASM
bench-footprint-server-description = Los <strong>benchmarks de huella WASM</strong> miden el tamaño del módulo WebAssembly para despliegue en navegador, crítico para aplicaciones web donde el tamaño de descarga impacta la experiencia del usuario.
bench-footprint-server-ops-label = objetivos de despliegue
bench-footprint-server-note = Los tamaños WASM son críticos para <strong>despliegues web</strong> donde el tiempo de descarga impacta directamente el tiempo hasta interactivo. Los tamaños gzip son más relevantes ya que los navegadores descomprimen automáticamente el contenido gzip.
bench-footprint-server-note2 = <strong>Nota:</strong> El WASM de VibeSQL está diseñado para tamaño de descarga mínimo mientras mantiene cumplimiento completo de SQL:1999 en el navegador.

# Descripciones de Huella Servidor
bench-footprint-wasm-size = Tamaño WASM - Tamaño del módulo WebAssembly para despliegue en navegador
bench-footprint-wasm-gzip = WASM (gzip) - Tamaño comprimido para entrega web

# Discusión de Huella Servidor
bench-footprint-srv-disc-wasm-title = WASM: 2.2MB Comprimido
bench-footprint-srv-disc-wasm = El módulo WebAssembly de VibeSQL se comprime a <strong>~2.2MB gzipped</strong>, habilitando cargas iniciales de página rápidas. Esta es una base de datos SQL:1999 completa con funciones de ventana, CTEs y transacciones ACID ejecutándose completamente en el navegador.
bench-footprint-srv-disc-included-title = Qué Está Incluido
bench-footprint-srv-disc-parser = Analizador SQL completo y optimizador de consultas
bench-footprint-srv-disc-btree = Motor de almacenamiento B-tree con MVCC
bench-footprint-srv-disc-window = Funciones de ventana y agregaciones avanzadas
bench-footprint-srv-disc-cte = Expresiones de tabla comunes (cláusula WITH)
bench-footprint-srv-disc-acid = Soporte completo de transacciones ACID
bench-footprint-srv-disc-benefits-title = Beneficios del Despliegue en Navegador
bench-footprint-srv-disc-benefits = Ejecutar SQL en el navegador elimina la latencia de ida y vuelta a los servidores, habilita aplicaciones offline-first y mantiene los datos sensibles en el dispositivo del usuario. El build WASM de VibeSQL está diseñado para este caso de uso con dependencias mínimas y uso eficiente de memoria.
bench-footprint-srv-disc-roadmap-title = Hoja de Ruta WASM
bench-footprint-srv-disc-streaming = Comenzar a ejecutar mientras el módulo se descarga
bench-footprint-srv-disc-indexeddb = Almacenamiento duradero entre sesiones de navegador
bench-footprint-srv-disc-worker = Ejecutar consultas fuera del hilo principal para UIs responsivas

# Etiquetas de viñetas (usadas con descripciones)
bench-bullet-join-ordering = Ordenamiento de joins
bench-bullet-hash-sizing = Dimensionamiento de tabla hash
bench-bullet-vectorized = Joins vectorizados
bench-bullet-inl-joins = Joins index-nested-loop
bench-bullet-cte-materialization = Materialización de CTE
bench-bullet-decorrelation = Decorrelación de subconsultas
bench-bullet-star-optimization = Optimización de esquema estrella
bench-bullet-lock-free = Lecturas sin bloqueo
bench-bullet-optimistic = Concurrencia optimista
bench-bullet-btree = B-tree en memoria
bench-bullet-prepared = Caché de sentencias preparadas
bench-bullet-bulk-inserts = Inserciones masivas
bench-bullet-non-indexed = Actualizaciones no indexadas
bench-bullet-deletes = Eliminaciones
bench-bullet-connection-pooling = Pool de conexiones
bench-bullet-stmt-caching = Caché de sentencias preparadas
bench-bullet-extended-protocol = Protocolo de consulta extendida
bench-bullet-feature-flags = Banderas de características
bench-bullet-lto = Optimización LTO
bench-bullet-modular = Builds modulares
bench-bullet-streaming = Compilación streaming
bench-bullet-indexeddb = Persistencia IndexedDB
bench-bullet-worker = Soporte de worker threads
bench-bullet-prepared-stmts = Sentencias preparadas
bench-bullet-larger-scale = Factores de escala mayores
bench-bullet-parallel-clients = Clientes paralelos

# =============================================================================
# Conformance Page
# =============================================================================

# Overview section
conformance-sql-conformance = SQL Conformance
conformance-testing-against = Testing against SQLLogicTest - the industry standard SQL test suite
conformance-full-pass-rate = 100% File Pass Rate Achieved!
conformance-tests-passing = Tests Passing
conformance-files-passing = Files Passing
conformance-loading = Loading conformance report...
conformance-error-loading = Error Loading Report
conformance-no-data = No conformance data available

# Category breakdown
conformance-category-title = Test Coverage by Category
conformance-category-header = Category
conformance-pass-rate-header = Pass Rate
conformance-progress-header = Progress
conformance-tests-header = Tests
conformance-cat-select = SELECT Queries
conformance-cat-aggregates = Aggregates
conformance-cat-joins = JOINs
conformance-cat-expressions = Expressions
conformance-cat-subqueries = Subqueries
conformance-cat-index = Index Operations
conformance-cat-ddl = DDL Statements
conformance-cat-evidence = Evidence Tests
conformance-cat-random = Random Tests
conformance-cat-other = Other Tests

# Timeline
conformance-timeline-title = Pass Rate History
conformance-timeline-desc = Conformance progress over the last 90 days
conformance-timeline-loading = Loading chart data...

# Milestones
conformance-milestones-title = Milestones

# Running tests locally
conformance-running-locally-title = Running Tests Locally
conformance-run-sqltest = # Ejecutar pruebas de conformidad SQL:1999
conformance-run-sqllogictest = # Ejecutar suite SQLLogicTest (toma horas)
conformance-generate-coverage = # Generar informe de cobertura
conformance-open-coverage = # Abrir informe de cobertura

# Sección sqltest
conformance-sqltest-title = Resultados de sqltest
conformance-sqltest-desc = Resultados de <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a> - una suite de pruebas de conformidad basada en BNF mantenida por la comunidad, derivada del estándar SQL:1999, que contiene 739 pruebas que cubren características Core y Foundation.
conformance-overall-pass-rate = Tasa de Aprobación General
conformance-tests-of-passing = { $passed } de { $total } pruebas aprobadas
conformance-passed = Aprobado
conformance-failed = Fallido
conformance-errors = Errores
conformance-test-coverage = Cobertura de Pruebas
conformance-core-features = Características Core (Serie E)
conformance-additional-features = Características Adicionales

# Códigos de características
conformance-e011 = Tipos de datos numéricos
conformance-e021 = Tipos de cadena de caracteres
conformance-e031 = Identificadores
conformance-e051 = Especificación de consulta básica
conformance-e061 = Predicados básicos y condiciones de búsqueda
conformance-e071 = Expresiones de consulta básicas
conformance-e081 = Privilegios básicos
conformance-e091 = Funciones de conjunto
conformance-e101 = Manipulación básica de datos
conformance-e111 = Sentencia SELECT de una sola fila
conformance-e121 = Soporte básico de cursor
conformance-e131 = Soporte de valores NULL
conformance-e141 = Restricciones de integridad básicas
conformance-e151 = Soporte de transacciones
conformance-e161 = Comentarios SQL
conformance-f031 = Manipulación básica de esquema

# Sección SQLLogicTest
conformance-slt-title = Resultados de SQLLogicTest
conformance-slt-desc = Resultados de la suite completa <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a> que contiene ~5.9 millones de pruebas en 623 archivos de prueba del corpus oficial de SQLite.
conformance-files-of-passing = { $passed } de { $total } archivos de prueba aprobados
conformance-test-categories = Categorías de Prueba
conformance-slt-select = Pruebas SELECT
conformance-slt-evidence = Pruebas de Evidencia
conformance-slt-index = Pruebas de Índice
conformance-slt-random = Pruebas Aleatorias
conformance-slt-ddl = Pruebas DDL
conformance-slt-other = Otras Pruebas
conformance-slt-note = <strong>Nota:</strong> SQLLogicTest proporciona una perspectiva diferente a sqltest. Mientras sqltest se enfoca en la conformidad gramatical BNF de la especificación SQL:1999, SQLLogicTest contiene millones de consultas SQL del mundo real que prueban la corrección práctica en una amplia gama de escenarios.

# Sección de explicación
conformance-explanation-title = Entendiendo Nuestras Suites de Prueba
conformance-what-is-sqltest = ¿Qué es sqltest?
conformance-sqltest-explanation = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a> es una suite de pruebas mantenida por la comunidad por Elliot Chance que proporciona pruebas de conformidad basadas en BNF derivadas del estándar SQL:1999. Contiene 739 pruebas que cubren características Core y Foundation en las categorías de prueba de la serie E y F. Esta suite prueba si nuestra implementación se ajusta a la especificación gramatical SQL:1999.
conformance-what-is-slt = ¿Qué es SQLLogicTest?
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> es una suite de pruebas completa desarrollada originalmente para SQLite, que contiene ~5.9 millones de casos de prueba SQL en 623 archivos de prueba. Prueba la corrección práctica ejecutando consultas del mundo real y validando resultados. Esta suite se enfoca en la corrección semántica y casos límite en lugar de la conformidad gramatical pura.
conformance-how-complement = ¿Cómo se complementan?
conformance-sqltest-validates = <span class="font-medium">sqltest (basado en BNF):</span> Valida la conformidad gramatical con las especificaciones del estándar SQL:1999
conformance-slt-validates = <span class="font-medium">SQLLogicTest (basado en resultados):</span> Valida la corrección semántica con millones de consultas reales
conformance-coverage-point = <span class="font-medium">Cobertura:</span> sqltest cubre 739 pruebas de características estándar; SQLLogicTest cubre escenarios prácticos
conformance-philosophy-point = <span class="font-medium">Filosofía:</span> sqltest pregunta "¿puedes analizar esto?"; SQLLogicTest pregunta "¿funciona correctamente?"
conformance-what-is-core = ¿Qué es SQL:1999 Core?
conformance-core-explanation = SQL:1999 Core es el conjunto de características obligatorias oficial definido en el estándar SQL:1999 (ISO/IEC 9075:1999). Consiste en aproximadamente 169 características requeridas que cualquier base de datos que reclame conformidad Core debe implementar. La conformidad Core oficial se verifica a través del NIST SQL Test Suite, no suites de prueba de la comunidad.
conformance-what-mean = ¿Qué significan nuestras tasas de aprobación?
conformance-pass-rates-mean = Nuestra <strong>tasa de aprobación de { $sqltestRate }% en sqltest</strong> ({ $sqltestPassed }/{ $sqltestTotal } pruebas) demuestra una fuerte conformidad gramatical SQL:1999. { $sltInfo } Juntos, estos resultados indican conformidad integral SQL:1999, aunque no constituyen certificación Core oficial.
conformance-slt-pass-info = Nuestra <strong>tasa de aprobación de { $sltRate }% en SQLLogicTest</strong> ({ $sltPassed }/{ $sltTotal } archivos de prueba) muestra que manejamos consultas del mundo real correctamente.
conformance-bottom-line = <strong>Conclusión:</strong> Usamos dos suites de prueba complementarias para asegurar tanto la conformidad con estándares (sqltest) como la corrección práctica (SQLLogicTest). Altas tasas de aprobación en ambas demuestran una seria calidad de implementación SQL:1999, aunque la certificación Core formal requeriría pruebas contra las suites oficiales NIST.

# Sección de pruebas fallidas
conformance-failing-tests-title = Pruebas Fallidas
conformance-failing-tests-desc = Las siguientes pruebas están fallando actualmente. Haz clic para expandir detalles.
conformance-view-failing = Ver detalles de pruebas fallidas ({ $count } pruebas)
conformance-error-label = Error:

# Pruebas de Regresión de PostgreSQL
conformance-pgsql-title = Pruebas de Regresión de PostgreSQL
conformance-pgsql-desc = Resultados de ejecutar la <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suite de pruebas de regresión de PostgreSQL</a> - la suite de pruebas canónica utilizada para validar la compatibilidad con PostgreSQL.
conformance-pgsql-tests-passing = pruebas aprobadas
conformance-pgsql-tests-excluded = pruebas excluidas
conformance-pgsql-pass-rate = Tasa de Aprobación
conformance-pgsql-excluded-reason = Las pruebas excluidas utilizan características específicas de PostgreSQL no aplicables a VibeSQL
conformance-pgsql-note = <strong>Nota:</strong> Las pruebas de regresión de PostgreSQL validan el comportamiento SQL contra la implementación de referencia de PostgreSQL. Las pruebas excluidas involucran características específicas de PostgreSQL como catálogos del sistema, lenguajes procedurales o módulos de extensión.

# Sección Suite de Pruebas TCL de SQLite
conformance-tcl-title = Suite de Pruebas TCL de SQLite
conformance-tcl-desc = Resultados de la <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suite de pruebas TCL</a> canónica de SQLite que contiene { $fileCount } archivos de prueba. Esta suite es el estándar de oro para pruebas de compatibilidad con SQLite.
conformance-tcl-overall-rate = Tasa de Aprobación General
conformance-tcl-tests-passing = { $passed } de { $total } pruebas aprobadas
conformance-tcl-passed = Aprobadas
conformance-tcl-failed = Fallidas
conformance-tcl-skipped = Omitidas
conformance-tcl-total = Total
conformance-tcl-categories-title = Categorías de Pruebas
conformance-tcl-category = Categoría
conformance-tcl-rate = Tasa
conformance-tcl-progress = Progreso
conformance-tcl-tests = Pruebas
conformance-tcl-common-failures = Fallos Comunes
conformance-tcl-failure-patterns = Top { $count } patrones de fallo por cantidad de ocurrencias
conformance-tcl-about-title = Sobre las Pruebas TCL:
conformance-tcl-about-text = La suite de pruebas TCL de SQLite es la prueba de conformidad canónica para compatibilidad con SQLite. Prueba comportamientos específicos de SQLite, peculiaridades y casos límite que pueden no estar cubiertos por las suites de pruebas SQL estándar. Tasas de aprobación altas aquí indican fuerte compatibilidad con SQLite para escenarios de migración de aplicaciones.

# Metadatos
conformance-generated = Generado:
conformance-commit = Commit:
conformance-status = Estado:
