/**
 * SQL:1999 Feature Showcase - Example Metadata
 *
 * Metadata loaded from JSON source files for easy maintenance.
 * This file provides type definitions and re-exports data from the loader.
 */

import { loadAllCategoryMetadata } from './examples/loader'

// Type definitions
export interface QueryExample {
  id: string
  title: string
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
  sql: string
  description: string
  sqlFeatures: string[]
  // Enhanced metadata
  difficulty?: 'beginner' | 'intermediate' | 'advanced'
  useCase?: 'analytics' | 'admin' | 'development' | 'reports' | 'data-quality'
  performanceNotes?: string
  executionTimeMs?: number
  relatedExamples?: string[] // IDs of related examples
  tags?: string[] // Searchable tags
  // Expected results (regenerated from the engine; see ExamplePayload in examples/types.ts)
  expectedRows?: string[][] // Expected result rows (excluding header)
  expectedCount?: number // Expected number of rows
}

export interface ExampleCategory {
  id: string
  title: string
  description: string
  queries: QueryExample[]
}

export interface ExampleMetadata {
  id: string
  title: string
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
  description: string
  sqlFeatures: string[]
  difficulty?: 'beginner' | 'intermediate' | 'advanced'
  useCase?: 'analytics' | 'admin' | 'development' | 'reports' | 'data-quality'
  performanceNotes?: string
  executionTimeMs?: number
  relatedExamples?: string[]
  tags?: string[]
}

export interface CategoryMetadata {
  id: string
  title: string
  description: string
  queries: ExampleMetadata[]
}

/**
 * Metadata for all example categories (loaded from JSON files)
 */
export const exampleCategoriesMetadata: CategoryMetadata[] = loadAllCategoryMetadata()

/**
 * Helper functions for accessing metadata
 */

/**
 * Get all example metadata across all categories
 */
export function getAllExampleMetadata(): ExampleMetadata[] {
  return exampleCategoriesMetadata.flatMap(cat => cat.queries)
}

/**
 * Find metadata for a specific example by ID
 */
export function findExampleMetadata(id: string): ExampleMetadata | undefined {
  return getAllExampleMetadata().find(ex => ex.id === id)
}

/**
 * Get example metadata for a specific database
 */
export function getExampleMetadataForDatabase(
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
): ExampleMetadata[] {
  return getAllExampleMetadata().filter(ex => ex.database === database)
}
