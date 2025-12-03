# VibeSQL Wire Protocol Extensions

VibeSQL extends the PostgreSQL wire protocol with additional message types for real-time query subscriptions. This document describes these extensions and their compatibility with standard PostgreSQL clients.

## Compatibility

Standard PostgreSQL clients (psql, libpq, etc.) work normally with VibeSQL servers. The subscription features require a VibeSQL-aware client that can recognize and handle the new message types.

**Key Principle**: If a client doesn't understand a message type, it should ignore it. This allows for forward/backward compatibility.

## Message Overview

VibeSQL adds four new message types in the custom range (0xF0-0xF3), chosen to avoid collision with PostgreSQL protocol messages:

| Code | Direction | Name | Description |
|------|-----------|------|-------------|
| `0xF0` (240) | Frontend | Subscribe | Subscribe to query updates |
| `0xF1` (241) | Frontend | Unsubscribe | Cancel subscription |
| `0xF2` (242) | Backend | SubscriptionData | Query result update |
| `0xF3` (243) | Backend | SubscriptionError | Subscription error |

## Protocol Messages

### Subscribe (0xF0) - Frontend Message

Subscribe to receive push notifications when query results change.

**Format:**
```
Byte1   'ð' (0xF0)
Int32   Message length (including self, excluding type byte)
String  Query string (null-terminated C string)
Int16   Parameter count
[For each parameter:]
  Int32   Parameter length (-1 for NULL)
  Byte[]  Parameter value
```

**Example** (Subscribe to "SELECT * FROM users"):
```
F0           - Message type
00 00 00 21  - Length (33 bytes)
53 45 4c 45 43 54 20 2a 20 46 52 4f 4d 20 75 73 65 72 73 00  - "SELECT * FROM users\0"
00 00        - 0 parameters
```

**Response**: Server sends SubscriptionData (0xF2) with the current query results.

### Unsubscribe (0xF1) - Frontend Message

Cancel an active subscription.

**Format:**
```
Byte1   'ñ' (0xF1)
Int32   Message length (including self, excluding type byte) = 20
Byte[16] Subscription ID (UUID in big-endian byte order)
```

**Example**:
```
F1                          - Message type
00 00 00 14                 - Length (20 bytes)
[16 bytes subscription ID]  - UUID
```

**Response**: No response required. Server removes the subscription and stops sending updates.

### SubscriptionData (0xF2) - Backend Message

Sends query result updates to the client.

**Format:**
```
Byte1   'ò' (0xF2)
Int32   Message length (including self, excluding type byte)
Byte[16] Subscription ID (UUID)
Int8    Update type (0=Full, 1=DeltaInsert, 2=DeltaUpdate, 3=DeltaDelete)
Int32   Row count
[For each row:]
  Int16   Column count
  [For each column:]
    Int32   Column length (-1 for NULL)
    Byte[]  Column value
```

**Update Types:**
- **0 (Full)**: Complete result set (initial or major change)
- **1 (DeltaInsert)**: New rows inserted
- **2 (DeltaUpdate)**: Existing rows updated
- **3 (DeltaDelete)**: Rows deleted

**Example** (Full update with 1 row, 2 columns):
```
F2                          - Message type
00 00 00 29                 - Length (41 bytes)
[16 bytes subscription ID]  - UUID
00                          - Update type: Full
00 00 00 01                 - 1 row
00 02                       - 2 columns
00 00 00 05                 - Column 1 length: 5
41 42 43 44 45              - "ABCDE"
00 00 00 03                 - Column 2 length: 3
58 59 5a                    - "XYZ"
```

### SubscriptionError (0xF3) - Backend Message

Notifies the client of a subscription error (e.g., invalid query, permission denied).

**Format:**
```
Byte1   'ó' (0xF3)
Int32   Message length (including self, excluding type byte)
Byte[16] Subscription ID (UUID)
String  Error message (null-terminated C string)
```

**Example**:
```
F3                          - Message type
00 00 00 19                 - Length (25 bytes)
[16 bytes subscription ID]  - UUID
51 75 65 72 79 20 65 72 72 6f 72 00  - "Query error\0"
```

## Subscription Lifecycle

1. **Subscribe**: Client sends Subscribe message with query
   - Server validates query and extracts table dependencies
   - Server creates SubscriptionId (UUID)
   - Server executes query and sends SubscriptionData with initial results

2. **Listen**: Client receives SubscriptionData messages when results change
   - Server monitors for changes to dependent tables
   - When change detected, re-executes query
   - If results differ from last version, sends SubscriptionData
   - Only sends update if actual data changed (not on every storage change)

3. **Unsubscribe**: Client sends Unsubscribe message
   - Server removes subscription
   - Stops monitoring and sending updates

4. **Error Handling**: At any point, server can send SubscriptionError
   - Query execution fails
   - Permission revoked
   - Subscription limit exceeded
   - Other server errors

## Integration with Standard Protocol

Subscription messages are sent/received alongside standard PostgreSQL messages:

- Client can send queries (Query) and subscriptions (Subscribe) in the same session
- Server responds with standard messages (DataRow, CommandComplete) and subscription messages
- Transaction handling is normal: subscriptions are per-session

**Example Session**:
```
Client: Query("SELECT 1")
Server: DataRow, CommandComplete, ReadyForQuery
Client: Subscribe("SELECT * FROM users")
Server: SubscriptionData (initial), ReadyForQuery
[Table changes]
Server: SubscriptionData (update)
Client: Unsubscribe(subscription_id)
Server: ReadyForQuery
Client: Terminate
```

## Implementation Notes

### Message Parsing

Clients must:
1. Read message type byte (0xF0-0xF3 are subscription messages)
2. Read 4-byte message length (excluding type byte)
3. Parse message-specific data
4. Be prepared to ignore unknown message types (for forward compatibility)

### Message Encoding

Servers must:
1. Write message type byte
2. Calculate total message length (excluding type byte)
3. Write length as 4-byte big-endian integer
4. Write message-specific data

All multi-byte integers are in big-endian (network byte order).

### UUID Format

Subscription IDs are 16-byte UUIDs in big-endian byte order:
- Bytes 0-3: Time low (4 bytes)
- Bytes 4-5: Time mid (2 bytes)
- Bytes 6-7: Time high and version (2 bytes)
- Bytes 8-15: Clock sequence and node (8 bytes)

### Error Handling

If a subscription request fails:
- Server sends SubscriptionError instead of SubscriptionData
- Client should treat this as subscription creation failure
- No subsequent updates will be sent for that subscription_id

## Security Considerations

1. **Query Validation**: Subscriptions go through normal query validation and permission checks
2. **Resource Limits**: Servers should limit:
   - Number of active subscriptions per session/client
   - Total number of subscriptions
   - Maximum rows per subscription
3. **DoS Prevention**: Rapid subscribe/unsubscribe could be used for DoS. Clients should rate-limit.
4. **Data Privacy**: Subscriptions follow the same permission model as queries

## Future Extensions

Potential future enhancements:
- `SubscriptionAck` (0xF4): Acknowledge subscription creation
- `SubscriptionPause` (0xF5): Temporarily pause updates
- `SubscriptionResume` (0xF6): Resume paused subscription
- Filtering expressions for deltas
- Selective column updates (don't send unchanged columns)
