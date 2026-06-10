import { describe, it, expect } from 'vitest'
import {
  exampleCategories,
  getAllExamples,
  findExample,
  getExamplesForDatabase,
} from '../data/examples'

describe('Examples Integration', () => {
  it('should load all 24 categories', () => {
    expect(exampleCategories).toHaveLength(24)
  })

  it('should have queries in each category', () => {
    exampleCategories.forEach(category => {
      expect(category.queries.length).toBeGreaterThan(0)
      expect(category.id).toBeTruthy()
      expect(category.title).toBeTruthy()
      expect(category.description).toBeTruthy()
    })
  })

  it('should load SQL payloads for all queries', () => {
    const allExamples = getAllExamples()
    expect(allExamples.length).toBeGreaterThan(0)

    allExamples.forEach(example => {
      expect(example.id).toBeTruthy()
      expect(example.sql).toBeTruthy()
      expect(example.title).toBeTruthy()
      expect(example.database).toBeTruthy()
      expect(example.description).toBeTruthy()
      expect(example.sqlFeatures).toBeInstanceOf(Array)
      expect(example.sqlFeatures.length).toBeGreaterThan(0)
    })
  })

  it('should find examples by ID', () => {
    const example = findExample('basic-1')
    expect(example).toBeDefined()
    expect(example?.id).toBe('basic-1')
    expect(example?.sql).toBeTruthy()
  })

  it('should filter examples by database', () => {
    const northwindExamples = getExamplesForDatabase('northwind')
    expect(northwindExamples.length).toBeGreaterThan(0)

    northwindExamples.forEach(example => {
      expect(example.database).toBe('northwind')
    })
  })

  it('should have at least 147 total queries', () => {
    const allExamples = getAllExamples()
    expect(allExamples.length).toBeGreaterThanOrEqual(147)
  })

  it('should load specific category examples correctly', () => {
    const basicCategory = exampleCategories.find(cat => cat.id === 'basic')
    expect(basicCategory).toBeDefined()
    expect(basicCategory?.queries.length).toBeGreaterThan(0)

    const firstQuery = basicCategory?.queries[0]
    expect(firstQuery?.sql).toBeTruthy()
  })
})
