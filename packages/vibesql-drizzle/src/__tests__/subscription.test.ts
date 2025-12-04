import { describe, it, expect, vi } from 'vitest';
import { extractQuery, subscribeToDrizzleQuery } from '../subscription.js';

describe('extractQuery', () => {
  it('should extract SQL and params from query object', () => {
    const mockQuery = {
      toSQL: () => ({
        sql: 'SELECT * FROM users WHERE id = ?',
        params: [1],
      }),
    };

    const { sql, params } = extractQuery(mockQuery);

    expect(sql).toBe('SELECT * FROM users WHERE id = ?');
    expect(params).toEqual([1]);
  });

  it('should handle query with no params', () => {
    const mockQuery = {
      toSQL: () => ({
        sql: 'SELECT * FROM users',
        params: [],
      }),
    };

    const { sql, params } = extractQuery(mockQuery);

    expect(sql).toBe('SELECT * FROM users');
    expect(params).toEqual([]);
  });

  it('should handle query with multiple params', () => {
    const mockQuery = {
      toSQL: () => ({
        sql: 'SELECT * FROM users WHERE name = ? AND age > ?',
        params: ['Alice', 18],
      }),
    };

    const { sql, params } = extractQuery(mockQuery);

    expect(sql).toBe('SELECT * FROM users WHERE name = ? AND age > ?');
    expect(params).toEqual(['Alice', 18]);
  });
});

describe('subscribeToDrizzleQuery', () => {
  // Create a mock VibeSQL client
  function createMockVibeSQL() {
    const subscriptionCallbacks: {
      data?: (update: { rows: unknown[]; operation: string }) => void;
      error?: (error: { message: string }) => void;
    } = {};

    const mockSubscription = {
      onData: vi.fn((callback) => {
        subscriptionCallbacks.data = callback;
      }),
      onError: vi.fn((callback) => {
        subscriptionCallbacks.error = callback;
      }),
      getId: vi.fn(() => 'mock-subscription-id'),
    };

    const mockClient = {
      subscribe: vi.fn(() => mockSubscription),
      unsubscribe: vi.fn(() => Promise.resolve()),
      _triggerData: (data: { rows: unknown[]; operation: string }) => {
        subscriptionCallbacks.data?.(data);
      },
      _triggerError: (error: { message: string }) => {
        subscriptionCallbacks.error?.(error);
      },
    };

    return { mockClient, mockSubscription };
  }

  it('should create subscription from Drizzle query', () => {
    const { mockClient } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({
        sql: 'SELECT * FROM users WHERE active = ?',
        params: [true],
      }),
    };

    const sub = subscribeToDrizzleQuery(mockClient as never, mockQuery);

    expect(mockClient.subscribe).toHaveBeenCalledWith(
      'SELECT * FROM users WHERE active = ?',
      [true]
    );
    expect(sub.loading).toBe(true);
    expect(sub.data).toEqual([]);
    expect(sub.error).toBeNull();
  });

  it('should update data on full-sync operation', () => {
    const { mockClient } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({ sql: 'SELECT * FROM users', params: [] }),
    };

    const onData = vi.fn();
    const sub = subscribeToDrizzleQuery(mockClient as never, mockQuery, { onData });

    // Simulate data arriving
    mockClient._triggerData({
      rows: [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ],
      operation: 'full-sync',
    });

    expect(sub.loading).toBe(false);
    expect(sub.data).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
    expect(onData).toHaveBeenCalledWith([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
  });

  it('should handle insert operations', () => {
    const { mockClient } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({ sql: 'SELECT * FROM users', params: [] }),
    };

    const sub = subscribeToDrizzleQuery(mockClient as never, mockQuery);

    // Initial data
    mockClient._triggerData({
      rows: [{ id: 1, name: 'Alice' }],
      operation: 'full-sync',
    });

    // Insert new row
    mockClient._triggerData({
      rows: [{ id: 2, name: 'Bob' }],
      operation: 'insert',
    });

    expect(sub.data).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
  });

  it('should handle delete operations', () => {
    const { mockClient } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({ sql: 'SELECT * FROM users', params: [] }),
    };

    const sub = subscribeToDrizzleQuery(mockClient as never, mockQuery);

    // Initial data
    mockClient._triggerData({
      rows: [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ],
      operation: 'full-sync',
    });

    // Delete a row
    mockClient._triggerData({
      rows: [{ id: 1 }],
      operation: 'delete',
    });

    expect(sub.data).toEqual([{ id: 2, name: 'Bob' }]);
  });

  it('should handle update operations', () => {
    const { mockClient } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({ sql: 'SELECT * FROM users', params: [] }),
    };

    const sub = subscribeToDrizzleQuery(mockClient as never, mockQuery);

    // Initial data
    mockClient._triggerData({
      rows: [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ],
      operation: 'full-sync',
    });

    // Update a row
    mockClient._triggerData({
      rows: [{ id: 1, name: 'Alicia' }],
      operation: 'update',
    });

    expect(sub.data).toEqual([
      { id: 1, name: 'Alicia' },
      { id: 2, name: 'Bob' },
    ]);
  });

  it('should handle errors', () => {
    const { mockClient } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({ sql: 'SELECT * FROM users', params: [] }),
    };

    const onError = vi.fn();
    const sub = subscribeToDrizzleQuery(mockClient as never, mockQuery, { onError });

    mockClient._triggerError({ message: 'Query failed' });

    expect(sub.loading).toBe(false);
    expect(sub.error).toBeInstanceOf(Error);
    expect(sub.error?.message).toBe('Query failed');
    expect(onError).toHaveBeenCalledWith(expect.any(Error));
  });

  it('should apply transform function', () => {
    const { mockClient } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({ sql: 'SELECT * FROM users', params: [] }),
    };

    interface User {
      id: number;
      name: string;
      displayName: string;
    }

    const transform = (rows: unknown[]): User[] =>
      rows.map((row) => ({
        ...(row as { id: number; name: string }),
        displayName: `User: ${(row as { name: string }).name}`,
      }));

    const sub = subscribeToDrizzleQuery<User>(mockClient as never, mockQuery, { transform });

    mockClient._triggerData({
      rows: [{ id: 1, name: 'Alice' }],
      operation: 'full-sync',
    });

    expect(sub.data).toEqual([
      { id: 1, name: 'Alice', displayName: 'User: Alice' },
    ]);
  });

  it('should unsubscribe when called', async () => {
    const { mockClient, mockSubscription } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({ sql: 'SELECT * FROM users', params: [] }),
    };

    const sub = subscribeToDrizzleQuery(mockClient as never, mockQuery);

    await sub.unsubscribe();

    expect(mockClient.unsubscribe).toHaveBeenCalledWith(mockSubscription);
  });

  it('should provide access to underlying subscription', () => {
    const { mockClient, mockSubscription } = createMockVibeSQL();

    const mockQuery = {
      toSQL: () => ({ sql: 'SELECT * FROM users', params: [] }),
    };

    const sub = subscribeToDrizzleQuery(mockClient as never, mockQuery);

    expect(sub.subscription).toBe(mockSubscription);
  });
});
