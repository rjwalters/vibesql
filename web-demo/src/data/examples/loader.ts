import type {
  ExampleCategory,
  CategoryMetadata,
  QueryExample,
  ExampleMetadata,
} from '../examples-metadata'

// Import all JSON category files (these will be bundled by Vite)
import advancedMultiFeature from './advanced-multi-feature.json'
import aggregates from './aggregates.json'
import basic from './basic.json'
import businessIntelligence from './business-intelligence.json'
import caseExamples from './case.json'
import company from './company.json'
import dataQuality from './data-quality.json'
import datetime from './datetime.json'
import ddl from './ddl.json'
import dml from './dml.json'
import joins from './joins.json'
import math from './math.json'
import nullHandling from './null-handling.json'
import patterns from './patterns.json'
import performancePatterns from './performance-patterns.json'
import recursive from './recursive.json'
import reportTemplates from './report-templates.json'
import set from './set.json'
import sql1999Standards from './sql1999-standards.json'
import sqllogictest from './sqllogictest.json'
import string from './string.json'
import subqueries from './subqueries.json'
import university from './university.json'
import window from './window.json'

// Type for the JSON structure
interface CategoryJSON {
  _category: {
    id: string
    title: string
    description: string
  }
  examples: {
    [exampleId: string]: {
      title: string
      database: string
      sql: string
      description: string
      sqlFeatures: string[]
      difficulty?: 'beginner' | 'intermediate' | 'advanced'
      useCase?: 'analytics' | 'admin' | 'development' | 'reports' | 'data-quality'
      performanceNotes?: string
      executionTimeMs?: number
      relatedExamples?: string[]
      tags?: string[]
      expectedRows?: string[][]
      expectedCount?: number
    }
  }
}

// All category JSON files
const categoryJSONFiles: CategoryJSON[] = [
  advancedMultiFeature,
  aggregates,
  basic,
  businessIntelligence,
  caseExamples,
  company,
  dataQuality,
  datetime,
  ddl,
  dml,
  joins,
  math,
  nullHandling,
  patterns,
  performancePatterns,
  recursive,
  reportTemplates,
  set,
  sql1999Standards,
  sqllogictest,
  string,
  subqueries,
  university,
  window,
] as CategoryJSON[]

/**
 * Load all example categories with full data from JSON files
 */
export function loadAllCategories(): ExampleCategory[] {
  return categoryJSONFiles.map(categoryJSON => {
    const queries: QueryExample[] = Object.entries(categoryJSON.examples).map(([id, example]) => ({
      id,
      title: example.title,
      database: example.database as QueryExample['database'],
      sql: example.sql,
      description: example.description,
      sqlFeatures: example.sqlFeatures,
      difficulty: example.difficulty,
      useCase: example.useCase,
      performanceNotes: example.performanceNotes,
      executionTimeMs: example.executionTimeMs,
      relatedExamples: example.relatedExamples,
      tags: example.tags,
      expectedRows: example.expectedRows,
      expectedCount: example.expectedCount,
    }))

    return {
      id: categoryJSON._category.id,
      title: categoryJSON._category.title,
      description: categoryJSON._category.description,
      queries,
    }
  })
}

/**
 * Load category metadata (without SQL payloads)
 */
export function loadAllCategoryMetadata(): CategoryMetadata[] {
  return categoryJSONFiles.map(categoryJSON => {
    const queries: ExampleMetadata[] = Object.entries(categoryJSON.examples).map(
      ([id, example]) => ({
        id,
        title: example.title,
        database: example.database as ExampleMetadata['database'],
        description: example.description,
        sqlFeatures: example.sqlFeatures,
        difficulty: example.difficulty,
        useCase: example.useCase,
        performanceNotes: example.performanceNotes,
        executionTimeMs: example.executionTimeMs,
        relatedExamples: example.relatedExamples,
        tags: example.tags,
      })
    )

    return {
      id: categoryJSON._category.id,
      title: categoryJSON._category.title,
      description: categoryJSON._category.description,
      queries,
    }
  })
}
