/**
 * SQL:1999 Feature Showcase - Example Queries
 *
 * Comprehensive library of SQL examples organized by feature category.
 * All data loaded from JSON files for easy maintenance.
 */

import type {
  QueryExample,
  ExampleCategory,
  ExampleMetadata,
  CategoryMetadata,
} from './examples-metadata'

import { loadAllCategories } from './examples/loader'

// Re-export types and interfaces
export type { QueryExample, ExampleCategory, ExampleMetadata, CategoryMetadata }

// Re-export for backward compatibility
export { exampleCategoriesMetadata } from './examples-metadata'

// Re-export metadata functions
export {
  getAllExampleMetadata,
  findExampleMetadata,
  getExampleMetadataForDatabase,
} from './examples-metadata'

/**
 * Load all example categories with SQL payloads
 */
export const exampleCategories: ExampleCategory[] = loadAllCategories()

/**
 * Get all examples flattened from all categories
 */
export function getAllExamples(): QueryExample[] {
  return exampleCategories.flatMap(cat => cat.queries)
}

/**
 * Find an example by ID
 */
export function findExample(id: string): QueryExample | undefined {
  return getAllExamples().find(ex => ex.id === id)
}

/**
 * Get examples for a specific database
 */
export function getExamplesForDatabase(
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
): QueryExample[] {
  return getAllExamples().filter(ex => ex.database === database)
}
