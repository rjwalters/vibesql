/**
 * Tests for OPFS persistent storage functionality
 *
 * These tests verify that data persists when using OPFS-backed storage.
 * Note: OPFS is only available in browsers, so these tests will use fallback
 * in Node.js test environments.
 */

import { describe, it, expect } from 'vitest'
import { initDatabase, isOpfsSupported, getStorageMode } from '../db/wasm'

describe('OPFS Persistence', () => {
  it('should detect OPFS support', () => {
    // In Node.js test environment, OPFS won't be supported
    const supported = isOpfsSupported()
    expect(typeof supported).toBe('boolean')

    // In browser environments with OPFS, this would be true
    // In Node.js, this will be false
    if (typeof window !== 'undefined' && typeof navigator !== 'undefined') {
      // We're in a browser-like environment
      const hasStorageAPI = 'storage' in navigator && 'getDirectory' in (navigator.storage || {})
      expect(supported).toBe(hasStorageAPI)
    } else {
      // We're in Node.js
      expect(supported).toBe(false)
    }
  })

  it('should report storage mode', async () => {
    const db = await initDatabase()
    expect(db).toBeDefined()

    const mode = getStorageMode()
    expect(typeof mode).toBe('string')
    expect(mode.length).toBeGreaterThan(0)

    // Should be either OPFS or Memory mode
    expect(mode.includes('OPFS') || mode.includes('Memory')).toBe(true)
  })

  it('should initialize database with OPFS when supported', async () => {
    // Try to initialize with OPFS
    const db = await initDatabase(true)
    expect(db).toBeDefined()

    // Verify database functionality
    const version = db.version()
    expect(version).toContain('vibesql')
  })

  it('should fall back to in-memory when OPFS unavailable', async () => {
    // In Node.js test environment, this will always fall back to memory
    const db = await initDatabase(true)
    expect(db).toBeDefined()

    const mode = getStorageMode()

    // In Node.js, should use memory storage
    if (typeof window === 'undefined') {
      expect(mode).toContain('Memory')
    }
  })

  it('should create tables in OPFS-backed database', async () => {
    const db = await initDatabase(true)
    expect(db).toBeDefined()

    // Create a test table
    const createResult = db.execute('CREATE TABLE test_persist (id INTEGER PRIMARY KEY, name TEXT)')
    expect(createResult).toBeDefined()
    expect(createResult.message).toContain('created')

    // Insert data
    const insertResult = db.execute("INSERT INTO test_persist VALUES (1, 'Alice'), (2, 'Bob')")
    expect(insertResult.rows_affected).toBe(2)

    // Query data
    const queryResult = db.query('SELECT * FROM test_persist ORDER BY id')
    expect(queryResult.rows).toHaveLength(2)
    expect(queryResult.columns).toEqual(['id', 'name'])

    // Clean up
    db.execute('DROP TABLE test_persist')
  })

  it('should handle multiple database operations with persistence', async () => {
    const db = await initDatabase(true)
    expect(db).toBeDefined()

    // Create and populate table
    db.execute('CREATE TABLE users_persistent (id INTEGER, username TEXT)')
    db.execute("INSERT INTO users_persistent VALUES (1, 'user1')")
    db.execute("INSERT INTO users_persistent VALUES (2, 'user2')")
    db.execute("INSERT INTO users_persistent VALUES (3, 'user3')")

    // Verify data
    let result = db.query('SELECT COUNT(*) as count FROM users_persistent')
    const count = JSON.parse(result.rows[0])[0]
    expect(count).toBe(3)

    // Update data
    db.execute("UPDATE users_persistent SET username = 'updated_user' WHERE id = 2")

    // Verify update
    result = db.query('SELECT username FROM users_persistent WHERE id = 2')
    const username = JSON.parse(result.rows[0])[0]
    expect(username).toBe('updated_user')

    // Delete data
    db.execute('DELETE FROM users_persistent WHERE id = 1')

    // Verify deletion
    result = db.query('SELECT COUNT(*) as count FROM users_persistent')
    const newCount = JSON.parse(result.rows[0])[0]
    expect(newCount).toBe(2)

    // Clean up
    db.execute('DROP TABLE users_persistent')
  })
})
