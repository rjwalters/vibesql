# @vibesql/drizzle

Drizzle ORM adapter for VibeSQL using the sqlite-proxy driver. This package enables type-safe queries with minimal effort by leveraging Drizzle's existing ecosystem.

## Installation

```bash
npm install @vibesql/drizzle drizzle-orm @vibesql/client
# or
pnpm add @vibesql/drizzle drizzle-orm @vibesql/client
```

## Quick Start

```typescript
import { VibeSQL } from '@vibesql/client';
import { createDrizzle } from '@vibesql/drizzle';
import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';

// Define your schema (standard Drizzle)
const users = sqliteTable('users', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  name: text('name').notNull(),
  email: text('email').notNull(),
});

// Create VibeSQL client
const vibesql = new VibeSQL();
await vibesql.connect();

// Create Drizzle instance
const db = createDrizzle(vibesql);

// Type-safe queries!
const allUsers = await db.select().from(users);
// Type: { id: number; name: string; email: string }[]

// Inserts
const newUser = await db.insert(users)
  .values({ name: 'Alice', email: 'alice@example.com' })
  .returning();

// Updates
await db.update(users)
  .set({ name: 'Bob' })
  .where(eq(users.id, 1));

// Deletes
await db.delete(users)
  .where(eq(users.id, 1));
```

## Features

### Type-Safe Queries

Drizzle provides full TypeScript support with type inference:

```typescript
// Type-safe select with conditions
const activeUsers = await db
  .select({
    id: users.id,
    name: users.name,
  })
  .from(users)
  .where(eq(users.active, true));
// Type: { id: number; name: string }[]
```

### Real-Time Subscriptions

Bridge Drizzle queries to VibeSQL's real-time subscription system:

```typescript
import { extractQuery, subscribeToDrizzleQuery } from '@vibesql/drizzle';

// Create a Drizzle query
const query = db.select().from(users).where(eq(users.active, true));

// Subscribe to changes
const sub = subscribeToDrizzleQuery(vibesql, query, {
  onData: (users) => console.log('Active users:', users),
  onError: (err) => console.error('Error:', err),
});

// Later: unsubscribe
await sub.unsubscribe();
```

### React Integration

Use the `useDrizzleQuery` hook for reactive subscriptions in React:

```tsx
import { useDrizzleQuery } from '@vibesql/drizzle/react';
import { useVibeSQL } from '@vibesql/client/react';
import { eq } from 'drizzle-orm';

function ActiveUsers() {
  const vibesql = useVibeSQL();
  const query = db.select().from(users).where(eq(users.active, true));

  const { data, loading, error } = useDrizzleQuery(vibesql, query);

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error.message}</div>;

  return (
    <ul>
      {data.map((user) => (
        <li key={user.id}>{user.name}</li>
      ))}
    </ul>
  );
}
```

### Transactions

Execute multiple operations atomically:

```typescript
import { withTransaction, createTransactionHelper } from '@vibesql/drizzle';

// Option 1: withTransaction helper
await withTransaction(vibesql, async (tx) => {
  await tx.query('INSERT INTO users (name) VALUES (?)', ['Alice']);
  await tx.query('INSERT INTO accounts (user_id) VALUES (last_insert_rowid())');
});

// Option 2: Transaction helper for use with Drizzle db
const transaction = createTransactionHelper(vibesql);

await transaction(async () => {
  await db.insert(users).values({ name: 'Alice', email: 'alice@example.com' });
  await db.insert(accounts).values({ userId: 1, balance: 0 });
});

// With transaction mode
await transaction(
  async () => { /* ... */ },
  { mode: 'IMMEDIATE' } // or 'EXCLUSIVE'
);
```

### Migrations

Apply Drizzle Kit migrations to your VibeSQL database:

```typescript
import { migrate } from '@vibesql/drizzle/migrator';

// First, generate migrations with Drizzle Kit:
// npx drizzle-kit generate:sqlite --schema=./src/schema.ts

// Then apply them:
await migrate(vibesql, {
  migrationsFolder: './drizzle',
});
```

Check migration status:

```typescript
import { getMigrationStatus } from '@vibesql/drizzle/migrator';

const status = await getMigrationStatus(vibesql, {
  migrationsFolder: './drizzle',
});

console.log('Applied:', status.applied);
console.log('Pending:', status.pending);
```

## API Reference

### `createDrizzle(client, options?)`

Creates a Drizzle database instance connected to VibeSQL.

**Parameters:**
- `client: VibeSQLClient` - VibeSQL client instance
- `options?: DrizzleAdapterOptions`
  - `logger?: boolean | { logQuery: Function }` - Enable query logging
  - `batchCallback?: AsyncBatchRemoteCallback` - Custom batch operation handler

**Returns:** `SqliteRemoteDatabase` - Drizzle database instance

### `extractQuery(query)`

Extracts SQL string and parameters from a Drizzle query.

**Parameters:**
- `query` - Drizzle query with `toSQL()` method

**Returns:** `{ sql: string; params: unknown[] }`

### `subscribeToDrizzleQuery(client, query, config?)`

Creates a managed subscription from a Drizzle query.

**Parameters:**
- `client: VibeSQL` - VibeSQL client instance
- `query` - Drizzle query to subscribe to
- `config?: DrizzleSubscriptionConfig`
  - `onData?: (data: T[]) => void` - Data callback
  - `onError?: (error: Error) => void` - Error callback
  - `transform?: (rows: unknown[]) => T[]` - Row transformer

**Returns:** `DrizzleSubscription<T>` - Managed subscription

### `useDrizzleQuery(client, query, options?)` (React)

React hook for subscribing to Drizzle query results.

**Parameters:**
- `client: VibeSQL | null` - VibeSQL client instance
- `query` - Drizzle query
- `options?: UseDrizzleQueryOptions`
  - `enabled?: boolean` - Enable/disable subscription (default: true)
  - `transform?: Function` - Row transformer
  - `onData?: Function` - Data callback
  - `onError?: Function` - Error callback

**Returns:** `UseDrizzleQueryResult<T>`
- `data: T[]` - Current data
- `loading: boolean` - Loading state
- `error: Error | null` - Error state
- `subscription: Subscription | null` - Underlying subscription
- `refetch: () => void` - Manual refetch trigger

### `withTransaction(client, callback, options?)`

Execute a callback within a database transaction.

**Parameters:**
- `client: VibeSQLClient` - VibeSQL client instance
- `callback: TransactionCallback<T>` - Transaction callback
- `options?: TransactionOptions`
  - `mode?: 'DEFERRED' | 'IMMEDIATE' | 'EXCLUSIVE'`

**Returns:** `Promise<T>` - Callback result

### `migrate(client, config)`

Run Drizzle Kit migrations.

**Parameters:**
- `client: VibeSQLClient` - VibeSQL client instance
- `config: MigrationConfig`
  - `migrationsFolder: string` - Path to migrations
  - `migrationsTable?: string` - Metadata table name (default: '__drizzle_migrations')
  - `useTransactions?: boolean` - Use transactions (default: true)

**Returns:** `Promise<number>` - Number of migrations applied

## Why Drizzle?

| Approach | Effort | Ecosystem | Maintenance |
|----------|--------|-----------|-------------|
| Custom query builder | High | None | Us |
| Drizzle adapter | Low | Drizzle's | Drizzle team |

The sqlite-proxy approach requires ~100 lines of adapter code vs thousands for a custom query builder implementation.

## License

MIT OR Apache-2.0
