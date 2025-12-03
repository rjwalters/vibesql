# @vibesql/client

TypeScript SDK for VibeSQL with real-time subscription support.

## Installation

```bash
npm install @vibesql/client
# or
pnpm add @vibesql/client
```

## Quick Start

### Direct Usage

```typescript
import { VibeSQL } from '@vibesql/client';

// Create client
const client = new VibeSQL({
  host: 'localhost',
  port: 5432,
  database: 'mydb',
});

// Connect to server
await client.connect();

// One-time query
const users = await client.query('SELECT * FROM users');
console.log(users.rows);

// Subscribe to changes
const subscription = client.subscribe(
  'SELECT * FROM users WHERE active = true'
);

subscription.on('data', (update) => {
  console.log('Users updated:', update.rows);
  console.log('Operation:', update.operation); // 'insert', 'update', 'delete', or 'full-sync'
});

subscription.on('error', (error) => {
  console.error('Subscription error:', error.message);
});

// Later: unsubscribe
await client.unsubscribe(subscription);

// Disconnect
await client.disconnect();
```

### React Integration

```typescript
import React from 'react';
import { VibeSQLProvider, useSubscription, useQuery } from '@vibesql/client/react';

function App() {
  return (
    <VibeSQLProvider config={{ host: 'localhost', port: 5432 }}>
      <UserList />
    </VibeSQLProvider>
  );
}

function UserList() {
  const { data, loading, error } = useSubscription(
    'SELECT * FROM users WHERE active = $1',
    [true]
  );

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

## API Reference

### Client

#### Constructor

```typescript
const client = new VibeSQL(config);
```

**Config Options:**
- `host` (string, default: 'localhost') - Server hostname
- `port` (number, default: 5432) - Server port
- `database` (string) - Database name
- `username` (string) - Username
- `password` (string) - Password
- `ssl` (boolean, default: false) - Use SSL
- `reconnectInterval` (number, default: 1000) - Milliseconds between reconnect attempts
- `maxReconnectAttempts` (number, default: 5) - Maximum reconnection attempts
- `queryTimeout` (number, default: 30000) - Query timeout in milliseconds

#### Methods

##### `connect(): Promise<void>`

Establish connection to VibeSQL server.

##### `disconnect(): Promise<void>`

Disconnect and cleanup all subscriptions.

##### `query(sql: string, params?: any[]): Promise<QueryResult>`

Execute a one-time query.

```typescript
const result = await client.query('SELECT * FROM users WHERE id = $1', [1]);
// result.columns: ['id', 'name', 'email']
// result.rows: [{ id: 1, name: 'John', email: 'john@example.com' }]
```

##### `subscribe(sql: string, params?: any[]): Subscription`

Subscribe to real-time query updates. Returns a `Subscription` object.

```typescript
const subscription = client.subscribe('SELECT * FROM events ORDER BY timestamp DESC LIMIT 100');
```

##### `unsubscribe(subscription: Subscription): Promise<void>`

Stop receiving updates for a subscription.

#### Events

- `connect` - Connected to server
- `disconnect` - Disconnected from server
- `stateChange` - Connection state changed
- `error` - Error occurred
- `subscriptionData` - Data received on any subscription
- `subscriptionError` - Error on any subscription
- `maxReconnectAttemptsReached` - Reconnection failed after max attempts

### Subscription

A subscription represents a real-time query.

#### Methods

##### `on(event: string, listener: Function): Subscription`

Attach event listener. Chainable.

```typescript
subscription
  .on('data', (update) => { /* ... */ })
  .on('error', (error) => { /* ... */ });
```

##### `onData(callback: (update: SubscriptionUpdate) => void): Subscription`

Attach data listener. Shorthand for `.on('data', ...)`.

##### `onError(callback: (error: SubscriptionError) => void): Subscription`

Attach error listener. Shorthand for `.on('error', ...)`.

##### `onClose(callback: () => void): Subscription`

Attach close listener.

##### `unsubscribe(): void`

Local unsubscription. Use `client.unsubscribe(subscription)` to notify server.

##### `getId(): string`

Get subscription ID.

##### `getSql(): string`

Get subscription query.

##### `getParams(): any[]`

Get subscription parameters.

##### `getState(): SubscriptionState`

Get current subscription state.

#### Events

- `data` - Data update received
- `error` - Error occurred
- `close` - Subscription closed
- `stateChange` - State changed

### React Hooks

#### `useQuery(sql: string, params?: any[]) => { data, loading, error }`

Execute a one-time query.

```typescript
const { data, loading, error } = useQuery('SELECT * FROM users');

if (loading) return <div>Loading...</div>;
if (error) return <div>Error: {error.message}</div>;
return <pre>{JSON.stringify(data.rows)}</pre>;
```

#### `useSubscription(sql: string, params?: any[]) => { data, loading, error, subscription }`

Subscribe to real-time updates. Returns the latest rows.

```typescript
const { data, loading, error } = useSubscription('SELECT * FROM users');

// data: array of rows
// loading: true while initial load
// error: SubscriptionError or null
```

Automatic updates:
- `insert` - Appends new rows
- `update` - Updates matching rows
- `delete` - Removes deleted rows
- `full-sync` - Replaces all rows

#### `VibeSQLProvider`

Wrap your app to provide client to hooks.

```typescript
<VibeSQLProvider config={{ host: 'localhost', port: 5432 }}>
  <YourApp />
</VibeSQLProvider>
```

## Types

### QueryResult

```typescript
interface QueryResult {
  columns: string[];
  rows: any[];
  rowsAffected?: number;
}
```

### SubscriptionUpdate

```typescript
interface SubscriptionUpdate {
  subscriptionId: string;
  columns: string[];
  rows: any[];
  operation: 'insert' | 'update' | 'delete' | 'full-sync';
  timestamp: number;
}
```

### SubscriptionError

```typescript
interface SubscriptionError {
  subscriptionId: string;
  code: string;
  message: string;
  timestamp: number;
}
```

## Error Handling

```typescript
// Connection errors
client.on('error', (error) => {
  console.error('Connection error:', error);
});

// Query errors
try {
  const result = await client.query('SELECT * FROM invalid');
} catch (error) {
  console.error('Query failed:', error);
}

// Subscription errors
subscription.on('error', (error) => {
  console.error(`Subscription ${error.subscriptionId} failed:`, error.message);
});
```

## Automatic Reconnection

The client automatically reconnects on connection loss:

```typescript
// Configure reconnection
const client = new VibeSQL({
  host: 'localhost',
  port: 5432,
  reconnectInterval: 1000,      // Start at 1s
  maxReconnectAttempts: 5,       // Exponential backoff
});

// Subscriptions are automatically restored on reconnect
// (no action needed from your code)

// Listen for failed reconnections
client.on('maxReconnectAttemptsReached', () => {
  console.error('Failed to reconnect after max attempts');
  // Handle gracefully
});
```

## Connection States

- `disconnected` - Not connected
- `connecting` - Attempting to connect
- `connected` - Connected and ready
- `reconnecting` - Attempting to reconnect after disconnect
- `error` - Connection error

## License

MIT OR Apache-2.0
