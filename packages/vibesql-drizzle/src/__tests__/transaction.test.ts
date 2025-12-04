import { describe, it, expect, vi, beforeEach } from 'vitest';
import { withTransaction, createTransactionHelper } from '../transaction.js';
import type { VibeSQLClient } from '../types.js';

// Mock VibeSQL client
function createMockClient(): VibeSQLClient & { query: ReturnType<typeof vi.fn> } {
  return {
    query: vi.fn(),
  };
}

describe('withTransaction', () => {
  let mockClient: ReturnType<typeof createMockClient>;

  beforeEach(() => {
    mockClient = createMockClient();
    mockClient.query.mockResolvedValue({ columns: [], rows: [] });
  });

  it('should begin and commit transaction on success', async () => {
    const callback = vi.fn().mockResolvedValue('result');

    const result = await withTransaction(mockClient, callback);

    expect(result).toBe('result');
    expect(mockClient.query).toHaveBeenCalledWith('BEGIN DEFERRED TRANSACTION');
    expect(mockClient.query).toHaveBeenCalledWith('COMMIT');
    expect(mockClient.query).not.toHaveBeenCalledWith('ROLLBACK');
  });

  it('should rollback transaction on error', async () => {
    const callback = vi.fn().mockRejectedValue(new Error('Test error'));

    await expect(withTransaction(mockClient, callback)).rejects.toThrow('Test error');

    expect(mockClient.query).toHaveBeenCalledWith('BEGIN DEFERRED TRANSACTION');
    expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
    expect(mockClient.query).not.toHaveBeenCalledWith('COMMIT');
  });

  it('should use IMMEDIATE mode when specified', async () => {
    const callback = vi.fn().mockResolvedValue('result');

    await withTransaction(mockClient, callback, { mode: 'IMMEDIATE' });

    expect(mockClient.query).toHaveBeenCalledWith('BEGIN IMMEDIATE TRANSACTION');
  });

  it('should use EXCLUSIVE mode when specified', async () => {
    const callback = vi.fn().mockResolvedValue('result');

    await withTransaction(mockClient, callback, { mode: 'EXCLUSIVE' });

    expect(mockClient.query).toHaveBeenCalledWith('BEGIN EXCLUSIVE TRANSACTION');
  });

  it('should provide transaction context to callback', async () => {
    mockClient.query.mockResolvedValue({
      columns: ['id'],
      rows: [{ id: 1 }],
    });

    await withTransaction(mockClient, async (tx) => {
      const result = await tx.query('SELECT 1 as id');
      expect(result.rows).toEqual([{ id: 1 }]);
    });

    expect(mockClient.query).toHaveBeenCalledWith('SELECT 1 as id', undefined);
  });

  it('should pass parameters to queries in transaction', async () => {
    await withTransaction(mockClient, async (tx) => {
      await tx.query('INSERT INTO users (name) VALUES (?)', ['Alice']);
    });

    expect(mockClient.query).toHaveBeenCalledWith(
      'INSERT INTO users (name) VALUES (?)',
      ['Alice']
    );
  });
});

describe('createTransactionHelper', () => {
  let mockClient: ReturnType<typeof createMockClient>;

  beforeEach(() => {
    mockClient = createMockClient();
    mockClient.query.mockResolvedValue({ columns: [], rows: [] });
  });

  it('should create a transaction helper function', () => {
    const transaction = createTransactionHelper(mockClient);
    expect(typeof transaction).toBe('function');
  });

  it('should execute callback in transaction', async () => {
    const transaction = createTransactionHelper(mockClient);
    const callback = vi.fn().mockResolvedValue('result');

    const result = await transaction(callback);

    expect(result).toBe('result');
    expect(mockClient.query).toHaveBeenCalledWith('BEGIN DEFERRED TRANSACTION');
    expect(mockClient.query).toHaveBeenCalledWith('COMMIT');
  });

  it('should rollback on error', async () => {
    const transaction = createTransactionHelper(mockClient);
    const callback = vi.fn().mockRejectedValue(new Error('Failed'));

    await expect(transaction(callback)).rejects.toThrow('Failed');

    expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
  });

  it('should accept transaction options', async () => {
    const transaction = createTransactionHelper(mockClient);
    const callback = vi.fn().mockResolvedValue('result');

    await transaction(callback, { mode: 'IMMEDIATE' });

    expect(mockClient.query).toHaveBeenCalledWith('BEGIN IMMEDIATE TRANSACTION');
  });
});
