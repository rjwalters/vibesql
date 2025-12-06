/**
 * Type-safe interfaces for WASM database bindings
 */

/** Main database interface */
export interface Database {
  /** Execute DDL/DML statement (CREATE, INSERT, UPDATE, DELETE) */
  execute(sql: string): ExecuteResult

  /** Execute SELECT query and return results */
  query(sql: string): QueryResult

  /** Get list of all table names */
  list_tables(): string[]

  /** Get schema information for a table */
  describe_table(table: string): TableSchema

  /** Load the Employees example database */
  load_employees(): ExecuteResult

  /** Load the Northwind example database */
  load_northwind(): ExecuteResult

  /** Get database version string */
  version(): string
}

/** Query execution result (SELECT) */
export interface QueryResult {
  columns: string[]
  rows: string[] // JSON strings from WASM
  row_count: number
}

/** Execute result (DDL/DML) */
export interface ExecuteResult {
  rows_affected: number
  message: string
}

/** Table schema metadata */
export interface TableSchema {
  name: string
  columns: ColumnInfo[]
}

/** Column metadata */
export interface ColumnInfo {
  name: string
  data_type: string
  nullable: boolean
}

/** SQL value types */
export type SqlValue = string | number | boolean | null

/** WASM module interface */
export interface WasmModule {
  /** Initialize WASM (must be called first) */
  default(): Promise<void>

  /** Database constructor */
  Database: {
    new (): Database
    newWithPersistence(): Promise<Database>
  }

  /** Set the locale for localized error messages (optional, requires i18n feature) */
  set_locale?: (locale: string) => void

  /** Get the current locale (optional, requires i18n feature) */
  get_locale?: () => string
}
