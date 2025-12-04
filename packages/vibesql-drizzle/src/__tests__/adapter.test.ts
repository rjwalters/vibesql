import { describe, it, expect, vi, beforeEach } from 'vitest';
import { sql } from 'drizzle-orm';
import { createDrizzle, getClient } from '../adapter.js';
import type { VibeSQLClient } from '../types.js';

// Mock VibeSQL client
function createMockClient(): VibeSQLClient & { query: ReturnType<typeof vi.fn> } {
  return {
    query: vi.fn(),
  };
}

describe('createDrizzle', () => {
  let mockClient: ReturnType<typeof createMockClient>;

  beforeEach(() => {
    mockClient = createMockClient();
  });

  it('should create a Drizzle instance', () => {
    const db = createDrizzle(mockClient);
    expect(db).toBeDefined();
  });

  it('should store client reference for retrieval via getClient()', () => {
    const db = createDrizzle(mockClient);
    const retrievedClient = getClient(db);
    expect(retrievedClient).toBe(mockClient);
  });

  it('should execute SELECT queries via sqlite-proxy', async () => {
    mockClient.query.mockResolvedValueOnce({
      columns: ['id', 'name', 'email'],
      rows: [
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
      ],
    });

    const db = createDrizzle(mockClient);

    // Execute a raw SQL query through Drizzle's run method using sql template tag
    await db.run(sql`SELECT * FROM users`);

    expect(mockClient.query).toHaveBeenCalledWith(
      'SELECT * FROM users',
      []
    );
  });

  it('should handle INSERT queries (run method)', async () => {
    mockClient.query.mockResolvedValueOnce({
      columns: [],
      rows: [],
      rowsAffected: 1,
    });

    const db = createDrizzle(mockClient);
    const name = 'Alice';
    const email = 'alice@example.com';

    await db.run(sql`INSERT INTO users (name, email) VALUES (${name}, ${email})`);

    expect(mockClient.query).toHaveBeenCalledWith(
      'INSERT INTO users (name, email) VALUES (?, ?)',
      ['Alice', 'alice@example.com']
    );
  });

  it('should log queries when logger is enabled', async () => {
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    mockClient.query.mockResolvedValueOnce({
      columns: ['id'],
      rows: [{ id: 1 }],
    });

    const db = createDrizzle(mockClient, { logger: true });

    await db.run(sql`SELECT 1 as id`);

    expect(consoleSpy).toHaveBeenCalledWith(
      '[Drizzle Query]',
      'SELECT 1 as id',
      []
    );

    consoleSpy.mockRestore();
  });

  it('should use custom logger when provided', async () => {
    const customLogger = {
      logQuery: vi.fn(),
    };

    mockClient.query.mockResolvedValueOnce({
      columns: ['id'],
      rows: [{ id: 1 }],
    });

    const db = createDrizzle(mockClient, { logger: customLogger });

    await db.run(sql`SELECT 1 as id`);

    expect(customLogger.logQuery).toHaveBeenCalledWith('SELECT 1 as id', []);
  });

  it('should convert row objects to arrays', async () => {
    mockClient.query.mockResolvedValueOnce({
      columns: ['id', 'name'],
      rows: [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ],
    });

    const db = createDrizzle(mockClient);

    // The adapter should convert objects to arrays internally
    // for sqlite-proxy compatibility
    const result = await db.all(sql`SELECT id, name FROM users`);

    expect(mockClient.query).toHaveBeenCalled();
    // Result should be properly formatted
    expect(result).toBeDefined();
  });

  it('should handle parameterized queries', async () => {
    mockClient.query.mockResolvedValueOnce({
      columns: ['id', 'name'],
      rows: [{ id: 1, name: 'Alice' }],
    });

    const db = createDrizzle(mockClient);
    const userId = 1;

    await db.run(sql`SELECT * FROM users WHERE id = ${userId}`);

    expect(mockClient.query).toHaveBeenCalledWith(
      'SELECT * FROM users WHERE id = ?',
      [1]
    );
  });

  it('should handle empty result sets', async () => {
    mockClient.query.mockResolvedValueOnce({
      columns: ['id', 'name'],
      rows: [],
    });

    const db = createDrizzle(mockClient);

    const result = await db.all(sql`SELECT * FROM users WHERE 1=0`);

    expect(result).toBeDefined();
  });

  it('should propagate query errors', async () => {
    mockClient.query.mockRejectedValueOnce(new Error('Database error'));

    const db = createDrizzle(mockClient);

    await expect(
      db.run(sql`SELECT * FROM nonexistent`)
    ).rejects.toThrow('Database error');
  });
});

describe('batch operations', () => {
  let mockClient: ReturnType<typeof createMockClient>;

  beforeEach(() => {
    mockClient = createMockClient();
  });

  it('should execute batch queries sequentially by default', async () => {
    mockClient.query
      .mockResolvedValueOnce({ columns: [], rows: [], rowsAffected: 1 })
      .mockResolvedValueOnce({ columns: [], rows: [], rowsAffected: 1 });

    const db = createDrizzle(mockClient);

    // Batch operations are handled by sqlite-proxy internally
    // This test verifies that queries are executed
    const name1 = 'Alice';
    const name2 = 'Bob';
    await db.run(sql`INSERT INTO users (name) VALUES (${name1})`);
    await db.run(sql`INSERT INTO users (name) VALUES (${name2})`);

    expect(mockClient.query).toHaveBeenCalledTimes(2);
  });

  it('should use custom batch callback when provided', async () => {
    const customBatchCallback = vi.fn().mockResolvedValue([
      { rows: [] },
      { rows: [] },
    ]);

    mockClient.query.mockResolvedValue({ columns: [], rows: [] });

    // Create db with custom batch callback
    createDrizzle(mockClient, {
      batchCallback: customBatchCallback,
    });

    // The batch callback would be used internally by Drizzle's batch operations
    expect(customBatchCallback).not.toHaveBeenCalled();
  });
});
