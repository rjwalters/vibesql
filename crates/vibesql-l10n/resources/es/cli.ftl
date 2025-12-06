# VibeSQL CLI Localización - Español
# Este archivo contiene todas las cadenas visibles para el usuario de la interfaz de línea de comandos.

# =============================================================================
# Banner del REPL y Mensajes Básicos
# =============================================================================

cli-banner = VibeSQL v{ $version } - Base de Datos con Cumplimiento COMPLETO de SQL:1999
cli-help-hint = Escriba \help para ayuda, \quit para salir
cli-goodbye = ¡Hasta luego!
locale-changed = Idioma cambiado a { $locale }

# =============================================================================
# Texto de Ayuda de Comandos (Argumentos Clap)
# =============================================================================

cli-about = VibeSQL - Base de Datos con Cumplimiento COMPLETO de SQL:1999

cli-long-about = Interfaz de línea de comandos de VibeSQL

    MODOS DE USO:
      REPL Interactivo:    vibesql (--database <ARCHIVO>)
      Ejecutar Comando:    vibesql -c "SELECT * FROM usuarios"
      Ejecutar Archivo:    vibesql -f script.sql
      Ejecutar desde stdin: cat datos.sql | vibesql
      Generar Tipos:       vibesql codegen --schema schema.sql --output types.ts

    REPL INTERACTIVO:
      Cuando se inicia sin -c, -f o entrada por tubería, VibeSQL entra en un REPL
      interactivo con soporte de readline, historial de comandos y meta-comandos como:
        \d (tabla)   - Describir tabla o listar todas las tablas
        \dt          - Listar tablas
        \f <formato> - Establecer formato de salida
        \copy        - Importar/exportar CSV/JSON
        \help        - Mostrar todos los comandos del REPL

    SUBCOMANDOS:
      codegen           Generar tipos TypeScript desde el esquema de base de datos

    CONFIGURACIÓN:
      La configuración se puede establecer en ~/.vibesqlrc (formato TOML).
      Secciones: display, database, history, query

    EJEMPLOS:
      # Iniciar REPL interactivo con base de datos en memoria
      vibesql

      # Usar archivo de base de datos persistente
      vibesql --database misdatos.db

      # Ejecutar un solo comando
      vibesql -c "CREATE TABLE usuarios (id INT, nombre VARCHAR(100))"

      # Ejecutar archivo de script SQL
      vibesql -f schema.sql -v

      # Importar datos desde CSV
      echo "\copy usuarios FROM 'datos.csv'" | vibesql --database misdatos.db

      # Exportar resultados de consulta como JSON
      vibesql -d misdatos.db -c "SELECT * FROM usuarios" --format json

      # Generar tipos TypeScript desde un archivo de esquema
      vibesql codegen --schema schema.sql --output src/types.ts

      # Generar tipos TypeScript desde una base de datos en ejecución
      vibesql codegen --database misdatos.db --output src/types.ts

# Cadenas de ayuda de argumentos
arg-database-help = Ruta del archivo de base de datos (si no se especifica, usa base de datos en memoria)
arg-file-help = Ejecutar comandos SQL desde archivo
arg-command-help = Ejecutar comando SQL directamente y salir
arg-stdin-help = Leer comandos SQL desde stdin (auto-detectado cuando se usa tubería)
arg-verbose-help = Mostrar salida detallada durante la ejecución de archivo/stdin
arg-format-help = Formato de salida para resultados de consultas
arg-lang-help = Establecer el idioma de visualización (ej., en-US, es, ja)

# =============================================================================
# Subcomando Codegen
# =============================================================================

codegen-about = Generar tipos TypeScript desde el esquema de base de datos

codegen-long-about = Generar definiciones de tipos TypeScript desde un esquema de base de datos VibeSQL.

    Este comando crea interfaces TypeScript para todas las tablas en la base de datos,
    junto con objetos de metadatos para verificación de tipos en tiempo de ejecución y soporte IDE.

    FUENTES DE ENTRADA:
      --database <ARCHIVO>  Generar desde un archivo de base de datos existente
      --schema <ARCHIVO>    Generar desde un archivo de esquema SQL (sentencias CREATE TABLE)

    SALIDA:
      --output <ARCHIVO>    Escribir tipos generados en este archivo (predeterminado: types.ts)

    OPCIONES:
      --camel-case          Convertir nombres de columnas a camelCase
      --no-metadata         Omitir la generación del objeto de metadatos de tablas

    EJEMPLOS:
      # Desde un archivo de base de datos
      vibesql codegen --database misdatos.db --output src/db/types.ts

      # Desde un archivo de esquema SQL
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Con nombres de propiedades en camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = Archivo de esquema SQL con sentencias CREATE TABLE
codegen-output-help = Ruta del archivo de salida para TypeScript generado
codegen-camel-case-help = Convertir nombres de columnas a camelCase
codegen-no-metadata-help = Omitir la generación del objeto de metadatos de tablas

codegen-from-schema = Generando tipos TypeScript desde archivo de esquema: { $path }
codegen-from-database = Generando tipos TypeScript desde base de datos: { $path }
codegen-written = Tipos TypeScript escritos en: { $path }
codegen-error-no-source = Debe especificar --database o --schema.
    Use 'vibesql codegen --help' para información de uso.

# =============================================================================
# Ayuda de Meta-comandos (salida de \help)
# =============================================================================

help-title = Meta-comandos:
help-describe = \d (tabla)      - Describir tabla o listar todas las tablas
help-tables = \dt             - Listar tablas
help-schemas = \ds             - Listar esquemas
help-indexes = \di             - Listar índices
help-roles = \du             - Listar roles/usuarios
help-format = \f <formato>    - Establecer formato de salida (table, json, csv, markdown, html)
help-timing = \timing         - Alternar tiempo de consulta
help-copy-to = \copy <tabla> TO <archivo>   - Exportar tabla a archivo CSV/JSON
help-copy-from = \copy <tabla> FROM <archivo> - Importar archivo CSV a tabla
help-save = \save (archivo) - Guardar base de datos en archivo de volcado SQL
help-errors = \errors         - Mostrar historial de errores recientes
help-help = \h, \help      - Mostrar esta ayuda
help-quit = \q, \quit      - Salir

help-sql-title = Introspección SQL:
help-show-tables = SHOW TABLES                  - Listar todas las tablas
help-show-databases = SHOW DATABASES               - Listar todos los esquemas/bases de datos
help-show-columns = SHOW COLUMNS FROM <tabla>    - Mostrar columnas de tabla
help-show-index = SHOW INDEX FROM <tabla>      - Mostrar índices de tabla
help-show-create = SHOW CREATE TABLE <tabla>    - Mostrar sentencia CREATE TABLE
help-describe-sql = DESCRIBE <tabla>             - Alias para SHOW COLUMNS

help-examples-title = Ejemplos:
help-example-create = CREATE TABLE usuarios (id INT PRIMARY KEY, nombre VARCHAR(100));
help-example-insert = INSERT INTO usuarios VALUES (1, 'Alicia'), (2, 'Roberto');
help-example-select = SELECT * FROM usuarios;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM usuarios;
help-example-describe = DESCRIBE usuarios;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy usuarios TO '/tmp/usuarios.csv'
help-example-copy-from = \copy usuarios FROM '/tmp/usuarios.csv'
help-example-copy-json = \copy usuarios TO '/tmp/usuarios.json'
help-example-errors = \errors

# =============================================================================
# Mensajes de Estado
# =============================================================================

format-changed = Formato de salida establecido a: { $format }
database-saved = Base de datos guardada en: { $path }
no-database-file = Error: No se especificó archivo de base de datos. Use \save <nombre_archivo> o inicie con el flag --database

# =============================================================================
# Visualización de Errores
# =============================================================================

no-errors = Sin errores en esta sesión.
recent-errors = Errores recientes:

# =============================================================================
# Mensajes de Ejecución de Script
# =============================================================================

script-no-statements = No se encontraron sentencias SQL en el script
script-executing = Ejecutando sentencia { $current } de { $total }...
script-error = Error ejecutando sentencia { $index }: { $error }
script-summary-title = === Resumen de Ejecución del Script ===
script-total = Total de sentencias: { $count }
script-successful = Exitosas: { $count }
script-failed = Fallidas: { $count }
script-failed-error = { $count } sentencias fallaron

# =============================================================================
# Formato de Salida
# =============================================================================

rows-with-time = { $count } filas en el conjunto ({ $time }s)
rows-count = { $count } filas

# =============================================================================
# Advertencias
# =============================================================================

warning-config-load = Advertencia: No se pudo cargar el archivo de configuración: { $error }
warning-auto-save-failed = Advertencia: Error al auto-guardar la base de datos: { $error }
warning-save-on-exit-failed = Advertencia: Error al guardar la base de datos al salir: { $error }

# =============================================================================
# Operaciones de Archivo
# =============================================================================

file-read-error = Error al leer archivo '{ $path }': { $error }
stdin-read-error = Error al leer desde stdin: { $error }
database-load-error = Error al cargar base de datos: { $error }
