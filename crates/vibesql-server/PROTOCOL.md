# VibeSQL Wire Protocol Extensions

VibeSQL extends the PostgreSQL wire protocol with additional message types for real-time query subscriptions. This document describes these extensions and their compatibility with standard PostgreSQL clients.

## Overview & Motivation

Traditional database connections require polling to detect data changes, which introduces:
- **Latency**: Clients only see changes at poll intervals
- **Overhead**: Frequent polling wastes resources; infrequent polling misses updates
- **Complexity**: Applications must implement their own refresh logic

VibeSQL's subscription protocol solves these problems by:
- **Push-based updates**: Clients receive notifications immediately when data changes
- **Efficient resource use**: No polling overhead; server only sends when data changes
- **Simplified clients**: Applications subscribe once and react to updates

## Compatibility

Standard PostgreSQL clients (psql, libpq, etc.) work normally with VibeSQL servers. The subscription features require a VibeSQL-aware client that can recognize and handle the new message types.

**Key Principle**: If a client doesn't understand a message type, it should ignore it. This allows for forward/backward compatibility.

## Message Overview

VibeSQL adds subscription message types in the custom range (0xF0-0xF7), chosen to avoid collision with PostgreSQL protocol messages (which use ASCII letters):

| Code | Direction | Name | Description |
|------|-----------|------|-------------|
| `0xF0` (240) | Frontend | Subscribe | Subscribe to query updates |
| `0xF1` (241) | Frontend | Unsubscribe | Cancel subscription |
| `0xF2` (242) | Backend | SubscriptionData | Query result update (full rows) |
| `0xF3` (243) | Backend | SubscriptionError | Subscription error |
| `0xF4` (244) | Backend | SubscriptionAck | Acknowledge subscription creation |
| `0xF5` (245) | Frontend | SubscriptionPause | Temporarily pause updates |
| `0xF6` (246) | Frontend | SubscriptionResume | Resume paused subscription |
| `0xF7` (247) | Backend | SubscriptionPartialData | Selective column update |

## Protocol Messages

All messages follow the standard PostgreSQL wire protocol structure:
- 1 byte message type identifier
- 4 byte length (big-endian, includes itself but NOT the type byte)
- Variable message body

### Subscribe (0xF0) - Frontend Message

Subscribe to receive push notifications when query results change.

**Byte-Level Format:**
```
┌──────────┬──────────────┬─────────────────────┬─────────────┬────────────────────┬─────────────┬────────────────────┐
│  Type    │   Length     │   Query (C-string)  │ Param Count │   Parameters...    │Filter Length│ Filter (optional)  │
│   1B     │   4B (BE)    │   variable + NUL    │   2B (BE)   │     variable       │   2B (BE)   │     variable       │
└──────────┴──────────────┴─────────────────────┴─────────────┴────────────────────┴─────────────┴────────────────────┘
     ↓            ↓                  ↓                 ↓                ↓                 ↓                ↓
   0xF0     Total length      SQL query text      Number of       Each param:       Filter length    Filter expression
            after type        null-terminated      params          len (4B) + data  (0 = no filter)  (SQL WHERE clause)
```

**Field Details:**

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | Message Type | `u8` | `0xF0` (Subscribe) |
| 1-4 | Length | `i32` BE | Total length after type byte |
| 5-N | Query | C-string | SQL query, null-terminated |
| N+1 | Param Count | `i16` BE | Number of query parameters |
| ... | Parameters | Array | Parameter values (see below) |
| ... | Filter Length | `i16` BE | Length of filter expression (0 = none, optional) |
| ... | Filter | bytes | Filter expression (if length > 0) |

**Parameter Encoding:**
```
┌───────────────┬─────────────────┐
│ Param Length  │   Param Value   │
│   4B (BE)     │    variable     │
└───────────────┴─────────────────┘
```
- If `Param Length` is `-1` (0xFFFFFFFF), the parameter is NULL
- Otherwise, `Param Value` contains exactly `Param Length` bytes

**Filter Expression:**

The optional filter expression is a SQL WHERE clause body that is applied to subscription results before sending to the client. This allows clients to receive only rows that match their criteria, reducing network traffic and client-side processing.

Supported filter syntax:
- Comparison operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- Logical operators: `AND`, `OR`, `NOT`
- NULL checks: `IS NULL`, `IS NOT NULL`
- List membership: `IN (value1, value2, ...)`
- Range checks: `BETWEEN low AND high`
- Pattern matching: `LIKE 'pattern'` (supports `%` and `_` wildcards)

Examples:
- `status = 'active'`
- `amount > 100 AND status = 'pending'`
- `category IN ('electronics', 'books')`
- `name LIKE 'A%'`
- `deleted_at IS NULL`

**Example 1** - Subscribe to `SELECT * FROM users` (no parameters, no filter):
```
Byte:  00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F 10 11 12 13 14 15 16 17 18 19 1A
       ─────────────────────────────────────────────────────────────────────────────────
Hex:   F0 00 00 00 1A 53 45 4C 45 43 54 20 2A 20 46 52 4F 4D 20 75 73 65 72 73 00 00 00
       ↑  └────┬────┘ └──────────────────────────────────┬────────────────────────┘ └─┬─┘
       │       │                                         │                           │
      Type   Length                                   Query                      Params
      0xF0    26                            "SELECT * FROM users\0"               count=0

Breakdown:
  F0                                     - Message type: Subscribe
  00 00 00 1A                            - Length: 26 bytes (0x1A)
  53 45 4C 45 43 54 20 2A 20 46 52       - "SELECT * FR"
  4F 4D 20 75 73 65 72 73 00             - "OM users\0"
  00 00                                  - Parameter count: 0
```

**Example 2** - Subscribe to `SELECT * FROM users WHERE id = $1` with param "42":
```
Hex:   F0 00 00 00 2C 53 45 4C 45 43 54 20 2A 20 46 52 4F 4D 20 75 73 65 72 73
       20 57 48 45 52 45 20 69 64 20 3D 20 24 31 00 00 01 00 00 00 02 34 32

Breakdown:
  F0                                     - Message type: Subscribe
  00 00 00 2C                            - Length: 44 bytes
  53 45 4C ... 24 31 00                  - "SELECT * FROM users WHERE id = $1\0"
  00 01                                  - Parameter count: 1
  00 00 00 02                            - Param 1 length: 2 bytes
  34 32                                  - Param 1 value: "42"
```

**Example 3** - Subscribe with filter expression `status = 'active'`:
```
Hex:   F0 00 00 00 2D 53 45 4C 45 43 54 20 2A 20 46 52 4F 4D 20 75 73 65 72 73
       00 00 00 00 11 73 74 61 74 75 73 20 3D 20 27 61 63 74 69 76 65 27

Breakdown:
  F0                                     - Message type: Subscribe
  00 00 00 2D                            - Length: 45 bytes
  53 45 4C ... 73 00                     - "SELECT * FROM users\0"
  00 00                                  - Parameter count: 0
  00 11                                  - Filter length: 17 bytes
  73 74 61 74 75 73 20 3D 20 27         - "status = '"
  61 63 74 69 76 65 27                   - "active'"
```

**Response**: Server sends SubscriptionData (0xF2) with the initial query results (filtered if filter provided), or SubscriptionError (0xF3) on failure.

### Unsubscribe (0xF1) - Frontend Message

Cancel an active subscription. The server will stop monitoring and sending updates.

**Byte-Level Format:**
```
┌──────────┬──────────────┬──────────────────────────────────────┐
│  Type    │   Length     │         Subscription ID              │
│   1B     │   4B (BE)    │            16B (UUID)                │
└──────────┴──────────────┴──────────────────────────────────────┘
     ↓            ↓                       ↓
   0xF1       Always 20            UUID from initial
                                   SubscriptionData
```

**Field Details:**

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | Message Type | `u8` | `0xF1` (Unsubscribe) |
| 1-4 | Length | `i32` BE | Always `20` (4 + 16) |
| 5-20 | Subscription ID | `[u8; 16]` | UUID from SubscriptionData message |

**Example** - Unsubscribe from a subscription:
```
Byte:  00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F 10 11 12 13 14
       ────────────────────────────────────────────────────────────────
Hex:   F1 00 00 00 14 a1 b2 c3 d4 e5 f6 07 18 29 3a 4b 5c 6d 7e 8f 90
       ↑  └────┬────┘ └──────────────────────────┬─────────────────────┘
       │       │                                 │
      Type   Length                     Subscription ID
      0xF1    20                         (16-byte UUID)

Breakdown:
  F1                                     - Message type: Unsubscribe
  00 00 00 14                            - Length: 20 bytes
  a1 b2 c3 d4 e5 f6 07 18                - Subscription ID bytes 0-7
  29 3a 4b 5c 6d 7e 8f 90                - Subscription ID bytes 8-15
```

**Response**: No response message is sent. The server silently removes the subscription and stops sending updates.

### SubscriptionData (0xF2) - Backend Message

Sends query result updates to the client. Sent immediately after successful Subscribe (initial results) and whenever underlying data changes.

**Byte-Level Format:**
```
┌──────────┬──────────────┬─────────────────┬─────────────┬───────────┬───────────┐
│  Type    │   Length     │ Subscription ID │ Update Type │ Row Count │  Rows...  │
│   1B     │   4B (BE)    │    16B (UUID)   │    1B       │  4B (BE)  │ variable  │
└──────────┴──────────────┴─────────────────┴─────────────┴───────────┴───────────┘
     ↓            ↓               ↓               ↓             ↓           ↓
   0xF2     Total length     Identifies      Full/Delta      Number     Row data
            after type       subscription    type flag       of rows    (see below)
```

**Field Details:**

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | Message Type | `u8` | `0xF2` (SubscriptionData) |
| 1-4 | Length | `i32` BE | Total length after type byte |
| 5-20 | Subscription ID | `[u8; 16]` | UUID identifying this subscription |
| 21 | Update Type | `u8` | Type of update (see below) |
| 22-25 | Row Count | `i32` BE | Number of rows in this message |
| 26+ | Rows | Array | Row data (see below) |

**Update Types:**

| Value | Name | Description |
|-------|------|-------------|
| `0` | Full | Complete result set (initial subscription or full refresh) |
| `1` | DeltaInsert | New rows added to result set |
| `2` | DeltaUpdate | Existing rows modified |
| `3` | DeltaDelete | Rows removed from result set |

**Row Encoding:**
```
┌──────────────┬───────────────────────────────────────────────────────────┐
│ Column Count │                      Column Values...                     │
│   2B (BE)    │                        variable                           │
└──────────────┴───────────────────────────────────────────────────────────┘
```

**Column Value Encoding:**
```
┌───────────────┬─────────────────┐
│ Value Length  │   Value Data    │
│   4B (BE)     │    variable     │
└───────────────┴─────────────────┘
```
- If `Value Length` is `-1` (0xFFFFFFFF), the value is NULL
- Otherwise, `Value Data` contains exactly `Value Length` bytes in text format

**Example** - Initial subscription result with 1 row: `(1, "Alice")`:
```
Byte:  00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F 10 11 12 13 14 15 16
       ─────────────────────────────────────────────────────────────────────
Hex:   F2 00 00 00 26 a1 b2 c3 d4 e5 f6 07 18 29 3a 4b 5c 6d 7e 8f 90 00 00
       ↑  └────┬────┘ └───────────────────────────┬────────────────┘  ↑  └┬─
       │       │                                  │                   │   │
      Type   Length                        Subscription ID         Update Row
      0xF2    38                             (16 bytes)             Type  Count
                                                                   Full   1

Hex (continued):
       17 18 19 1A 1B 1C 1D 1E 1F 20 21 22 23 24 25
       ───────────────────────────────────────────────
       00 00 01 00 02 00 00 00 01 31 00 00 00 05 41 6C 69 63 65
       └──┬──┘ └─┬─┘ └────┬────┘ ↑  └────┬────┘ └──────┬──────┘
          │     │        │      │       │             │
       Row Cnt Cols   Val1 Len "1"   Val2 Len      "Alice"
          1     2        1            5

Full Breakdown:
  F2                                     - Message type: SubscriptionData
  00 00 00 26                            - Length: 38 bytes
  a1 b2 c3 d4 e5 f6 07 18                - Subscription ID bytes 0-7
  29 3a 4b 5c 6d 7e 8f 90                - Subscription ID bytes 8-15
  00                                     - Update type: Full (0)
  00 00 00 01                            - Row count: 1
  00 02                                  - Column count: 2
  00 00 00 01                            - Column 1 length: 1 byte
  31                                     - Column 1 value: "1"
  00 00 00 05                            - Column 2 length: 5 bytes
  41 6C 69 63 65                         - Column 2 value: "Alice"
```

**Example** - Row with NULL value:
```
Breakdown:
  ...
  00 02                                  - Column count: 2
  00 00 00 01                            - Column 1 length: 1 byte
  31                                     - Column 1 value: "1"
  FF FF FF FF                            - Column 2 length: -1 (NULL)
                                         - (no data follows)
```

### SubscriptionError (0xF3) - Backend Message

Notifies the client of a subscription error. This can be sent:
- Immediately after Subscribe if the query fails to parse or execute
- At any time if an existing subscription encounters an error (e.g., schema change)

**Byte-Level Format:**
```
┌──────────┬──────────────┬─────────────────┬───────────────────────────┐
│  Type    │   Length     │ Subscription ID │   Message (C-string)      │
│   1B     │   4B (BE)    │    16B (UUID)   │   variable + NUL          │
└──────────┴──────────────┴─────────────────┴───────────────────────────┘
     ↓            ↓               ↓                     ↓
   0xF3     Total length     Identifies         Error description
            after type       subscription       null-terminated
```

**Field Details:**

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | Message Type | `u8` | `0xF3` (SubscriptionError) |
| 1-4 | Length | `i32` BE | Total length after type byte |
| 5-20 | Subscription ID | `[u8; 16]` | UUID of the affected subscription |
| 21+ | Message | C-string | Error description, null-terminated |

**Special Case - Parse Errors:**
If a Subscribe message fails to parse before a subscription ID is allocated, the server sends a SubscriptionError with a **zeroed subscription ID** (`[0x00; 16]`).

**Example** - Subscription error with message "Query error":
```
Byte:  00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F 10 11 12 13 14 15 16
       ─────────────────────────────────────────────────────────────────────
Hex:   F3 00 00 00 21 a1 b2 c3 d4 e5 f6 07 18 29 3a 4b 5c 6d 7e 8f 90 51 75
       ↑  └────┬────┘ └───────────────────────────┬────────────────┘  └──┬──
       │       │                                  │                      │
      Type   Length                        Subscription ID            Message
      0xF3    33                             (16 bytes)               start

Hex (continued):
       17 18 19 1A 1B 1C 1D 1E 1F 20
       ───────────────────────────────
       65 72 79 20 65 72 72 6F 72 00
       └───────────────┬────────────┘
                       │
                "Query error\0"

Full Breakdown:
  F3                                     - Message type: SubscriptionError
  00 00 00 21                            - Length: 33 bytes
  a1 b2 c3 d4 e5 f6 07 18                - Subscription ID bytes 0-7
  29 3a 4b 5c 6d 7e 8f 90                - Subscription ID bytes 8-15
  51 75 65 72 79 20 65 72 72 6F 72 00    - "Query error\0"
```

**Example** - Parse error (zeroed subscription ID):
```
Hex:   F3 00 00 00 21 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 50 61
       72 73 65 20 65 72 72 6F 72 00

Breakdown:
  F3                                     - Message type: SubscriptionError
  00 00 00 21                            - Length: 33 bytes
  00 00 00 00 00 00 00 00                - Subscription ID: all zeros (parse failed)
  00 00 00 00 00 00 00 00                - (before ID was assigned)
  50 61 72 73 65 20 65 72 72 6F 72 00    - "Parse error\0"
```

### SubscriptionAck (0xF4) - Backend Message

Acknowledges successful subscription registration. Sent immediately after a subscription is registered but before the initial data is sent. This allows clients to:
- Know the subscription was accepted before potentially slow query execution
- Correlate the subscription ID with their request
- Handle registration failures separately from query execution failures

**Byte-Level Format:**
```
┌──────────┬──────────────┬─────────────────┬─────────────────────┐
│  Type    │   Length     │ Subscription ID │   Table Count       │
│   1B     │   4B (BE)    │    16B (UUID)   │     2B (BE)         │
└──────────┴──────────────┴─────────────────┴─────────────────────┘
     ↓            ↓               ↓                   ↓
   0xF4       Always 22      UUID assigned       Number of tables
                             to subscription     being monitored
```

**Field Details:**

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | Message Type | `u8` | `0xF4` (SubscriptionAck) |
| 1-4 | Length | `i32` BE | Always `22` (4 + 16 + 2) |
| 5-20 | Subscription ID | `[u8; 16]` | UUID assigned to this subscription |
| 21-22 | Table Count | `u16` BE | Number of tables being monitored |

**Example** - Subscription acknowledged with 2 table dependencies:
```
Hex:   F4 00 00 00 16 a1 b2 c3 d4 e5 f6 07 18 29 3a 4b 5c 6d 7e 8f 90 00 02

Breakdown:
  F4                                     - Message type: SubscriptionAck
  00 00 00 16                            - Length: 22 bytes
  a1 b2 c3 d4 e5 f6 07 18                - Subscription ID bytes 0-7
  29 3a 4b 5c 6d 7e 8f 90                - Subscription ID bytes 8-15
  00 02                                  - Table count: 2
```

### SubscriptionPause (0xF5) - Frontend Message

Temporarily pause updates for a subscription. While paused, the subscription remains registered but no SubscriptionData messages are sent when underlying data changes. This is useful for:
- Clients processing a backlog of updates
- Mobile apps going to background
- Reducing server load during periods of disinterest

**Byte-Level Format:**
```
┌──────────┬──────────────┬──────────────────────────────────────┐
│  Type    │   Length     │         Subscription ID              │
│   1B     │   4B (BE)    │            16B (UUID)                │
└──────────┴──────────────┴──────────────────────────────────────┘
     ↓            ↓                       ↓
   0xF5       Always 20            UUID of subscription
                                   to pause
```

**Field Details:**

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | Message Type | `u8` | `0xF5` (SubscriptionPause) |
| 1-4 | Length | `i32` BE | Always `20` (4 + 16) |
| 5-20 | Subscription ID | `[u8; 16]` | UUID of the subscription to pause |

**Response**: No response message is sent. The server silently pauses the subscription.

**Example** - Pause a subscription:
```
Hex:   F5 00 00 00 14 a1 b2 c3 d4 e5 f6 07 18 29 3a 4b 5c 6d 7e 8f 90

Breakdown:
  F5                                     - Message type: SubscriptionPause
  00 00 00 14                            - Length: 20 bytes
  a1 b2 c3 d4 e5 f6 07 18                - Subscription ID bytes 0-7
  29 3a 4b 5c 6d 7e 8f 90                - Subscription ID bytes 8-15
```

### SubscriptionResume (0xF6) - Frontend Message

Resume a previously paused subscription. After resuming, the client will again receive SubscriptionData messages when underlying data changes. Note: The client does NOT automatically receive the current state upon resume; it only receives updates going forward.

**Byte-Level Format:**
```
┌──────────┬──────────────┬──────────────────────────────────────┐
│  Type    │   Length     │         Subscription ID              │
│   1B     │   4B (BE)    │            16B (UUID)                │
└──────────┴──────────────┴──────────────────────────────────────┘
     ↓            ↓                       ↓
   0xF6       Always 20            UUID of subscription
                                   to resume
```

**Field Details:**

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | Message Type | `u8` | `0xF6` (SubscriptionResume) |
| 1-4 | Length | `i32` BE | Always `20` (4 + 16) |
| 5-20 | Subscription ID | `[u8; 16]` | UUID of the subscription to resume |

**Response**: No response message is sent. The server silently resumes the subscription.

**Example** - Resume a subscription:
```
Hex:   F6 00 00 00 14 a1 b2 c3 d4 e5 f6 07 18 29 3a 4b 5c 6d 7e 8f 90

Breakdown:
  F6                                     - Message type: SubscriptionResume
  00 00 00 14                            - Length: 20 bytes
  a1 b2 c3 d4 e5 f6 07 18                - Subscription ID bytes 0-7
  29 3a 4b 5c 6d 7e 8f 90                - Subscription ID bytes 8-15
```

### SubscriptionPartialData (0xF7) - Backend Message

Sends selective column updates to the client. This is an optimization for wide tables where only a few columns change frequently. Instead of sending all columns, only changed columns (plus primary key columns for row identification) are included.

**When to Use:**
- Wide tables with many columns
- Updates that typically affect only a few columns
- Bandwidth-constrained environments

**Byte-Level Format:**
```
┌──────────┬──────────────┬─────────────────┬─────────────┬───────────┬───────────────────┐
│  Type    │   Length     │ Subscription ID │ Update Type │ Row Count │ Partial Rows...   │
│   1B     │   4B (BE)    │    16B (UUID)   │    1B       │  4B (BE)  │    variable       │
└──────────┴──────────────┴─────────────────┴─────────────┴───────────┴───────────────────┘
     ↓            ↓               ↓               ↓             ↓              ↓
   0xF7     Total length     Identifies     SelectiveUpdate  Number      Partial row
            after type       subscription     (always 4)     of rows     data (below)
```

**Field Details:**

| Offset | Field | Type | Description |
|--------|-------|------|-------------|
| 0 | Message Type | `u8` | `0xF7` (SubscriptionPartialData) |
| 1-4 | Length | `i32` BE | Total length after type byte |
| 5-20 | Subscription ID | `[u8; 16]` | UUID identifying this subscription |
| 21 | Update Type | `u8` | Always `4` (SelectiveUpdate) |
| 22-25 | Row Count | `i32` BE | Number of partial row updates |
| 26+ | Partial Rows | Array | Partial row data (see below) |

**Partial Row Encoding:**

Each partial row contains a column presence bitmap followed by values for only the present columns:

```
┌──────────────────┬─────────────────────────┬──────────────────────────────┐
│  Total Columns   │    Column Bitmap        │     Present Column Values    │
│    2B (BE)       │  ceil(columns/8) bytes  │          variable            │
└──────────────────┴─────────────────────────┴──────────────────────────────┘
```

- **Total Columns**: The total number of columns in the full schema (for bitmap sizing)
- **Column Bitmap**: One bit per column. Bit 0 = column 0, Bit 1 = column 1, etc.
  - Bit = 0: Column not present (unchanged)
  - Bit = 1: Column present in this update
- **Present Column Values**: Values for columns with bit=1, in column order
  - Encoded same as regular column values (4-byte length + data, or -1 for NULL)

**Distinguishing Unchanged from NULL:**
- Bit = 0: Column unchanged (not sent)
- Bit = 1, Length = -1: Column changed to NULL
- Bit = 1, Length >= 0: Column has new value

**Primary Key Handling:**
Server implementations should always include primary key columns (even if unchanged) to allow clients to identify which row is being updated.

**Example** - Partial update for a 5-column table, updating columns 0 (PK) and 3:
```
Byte:  00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F 10 11 12 13 14 15 16
       ─────────────────────────────────────────────────────────────────────
Hex:   F7 00 00 00 2A a1 b2 c3 d4 e5 f6 07 18 29 3a 4b 5c 6d 7e 8f 90 04 00
       ↑  └────┬────┘ └───────────────────────────┬────────────────┘  ↑  └┬─
       │       │                                  │                   │   │
      Type   Length                        Subscription ID         Update Row
      0xF7    42                             (16 bytes)             Type  Count
                                                                   4=Sel   1

Hex (continued):
       17 18 19 1A 1B 1C 1D 1E 1F 20 21 22 23 24 25 26 27 28 29
       ────────────────────────────────────────────────────────────
       00 01 00 05 09 00 00 00 01 31 00 00 00 05 76 61 6C 75 65
       └──┬──┘ ↑  └─┬─┘ └────┬────┘ ↑  └────┬────┘ └──────┬──────┘
          │   │    │        │      │       │             │
       Total Bitmap ID Len  "1"  Val Len  "value"
       Cols=5 09=0b1001     1      5
             (cols 0,3)

Full Breakdown:
  F7                                     - Message type: SubscriptionPartialData
  00 00 00 2A                            - Length: 42 bytes
  a1 b2 c3 d4 e5 f6 07 18                - Subscription ID bytes 0-7
  29 3a 4b 5c 6d 7e 8f 90                - Subscription ID bytes 8-15
  04                                     - Update type: SelectiveUpdate (4)
  00 00 00 01                            - Row count: 1
  00 05                                  - Total columns: 5
  09                                     - Bitmap: 0b00001001 (columns 0 and 3 present)
  00 00 00 01                            - Column 0 length: 1 byte
  31                                     - Column 0 value: "1" (PK)
  00 00 00 05                            - Column 3 length: 5 bytes
  76 61 6C 75 65                         - Column 3 value: "value"
```

**Example** - Partial update with NULL:
```
Hex:   ... 00 03 05 00 00 00 01 31 FF FF FF FF
            └┬─┘ ↑  └────┬────┘ ↑  └────┬────┘
             │   │       │      │       │
          Total Bitmap  ID Len "1"   NULL (-1)
          Cols=3 05=0b101       1    (column 2 is NULL)
                (cols 0,2)
```

## Subscription Lifecycle

### Successful Subscription Flow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│  Client                                                         Server     │
│    │                                                              │        │
│    │  ────────── Subscribe (0xF0) ──────────────────────────────► │        │
│    │              Query: SELECT * FROM users                      │        │
│    │                                                              │        │
│    │                                            ┌─────────────────┴───┐    │
│    │                                            │ 1. Parse query      │    │
│    │                                            │ 2. Extract tables   │    │
│    │                                            │ 3. Generate UUID    │    │
│    │                                            │ 4. Register for     │    │
│    │                                            │    table changes    │    │
│    │                                            └─────────────────┬───┘    │
│    │                                                              │        │
│    │  ◄───────── SubscriptionAck (0xF4) ──────────────────────── │        │
│    │              ID: <uuid>, Tables: 1                           │        │
│    │                                                              │        │
│    │                                            ┌─────────────────┴───┐    │
│    │                                            │ 5. Execute query    │    │
│    │                                            └─────────────────┬───┘    │
│    │                                                              │        │
│    │  ◄───────── SubscriptionData (0xF2) ─────────────────────── │        │
│    │              ID: <uuid>, Type: Full                          │        │
│    │              Rows: [initial result set]                      │        │
│    │                                                              │        │
│    │                        ... time passes ...                   │        │
│    │                                                              │        │
│    │                                            ┌─────────────────┴───┐    │
│    │                                            │ INSERT INTO users   │    │
│    │                                            │ (from other client) │    │
│    │                                            │                     │    │
│    │                                            │ → Detects change    │    │
│    │                                            │ → Re-executes query │    │
│    │                                            └─────────────────┬───┘    │
│    │                                                              │        │
│    │  ◄───────── SubscriptionData (0xF2) ─────────────────────── │        │
│    │              ID: <uuid>, Type: Full                          │        │
│    │              Rows: [updated result set]                      │        │
│    │                                                              │        │
│    │  ────────── SubscriptionPause (0xF5) ────────────────────── │        │
│    │              ID: <uuid>                                      │        │
│    │                              (no response, updates paused)   │        │
│    │                                                              │        │
│    │  ────────── SubscriptionResume (0xF6) ───────────────────── │        │
│    │              ID: <uuid>                                      │        │
│    │                              (no response, updates resumed)  │        │
│    │                                                              │        │
│    │  ────────── Unsubscribe (0xF1) ────────────────────────────► │        │
│    │              ID: <uuid>                                      │        │
│    │                                                              │        │
│    │                              (server removes subscription,   │        │
│    │                               no response sent)              │        │
│    │                                                              │        │
└────────────────────────────────────────────────────────────────────────────┘
```

### Lifecycle Phases

1. **Subscribe**: Client sends Subscribe message with query
   - Server parses and validates the SQL query
   - Server extracts table dependencies from AST
   - Server generates a unique subscription ID (UUID v4)
   - Server registers subscription for change notifications
   - Server sends **SubscriptionAck** with ID and table count
   - Server executes query to get initial results
   - Server sends SubscriptionData with initial results

2. **Listen**: Client receives SubscriptionData messages when results change
   - Server monitors for INSERT/UPDATE/DELETE on dependent tables
   - When change detected, server re-executes the query
   - Server compares results to last sent results (via hash)
   - If results differ, server sends SubscriptionData update
   - Currently uses Full update type; delta types reserved for future
   - Paused subscriptions do not receive updates

3. **Pause/Resume** (Optional): Client can pause and resume updates
   - Client sends **SubscriptionPause** to temporarily stop updates
   - While paused, subscription remains registered but no updates sent
   - Client sends **SubscriptionResume** to restart receiving updates
   - On resume, client does NOT receive missed updates (only future changes)
   - Useful for background apps, rate limiting, or processing backlogs

4. **Unsubscribe**: Client sends Unsubscribe message
   - Server removes subscription from internal tracking
   - Server stops monitoring dependent tables for this subscription
   - No response message is sent

5. **Connection Close**: Implicit unsubscribe
   - When connection terminates, all subscriptions are automatically cleaned up
   - No explicit Unsubscribe needed for each subscription

### Error Flow - Parse Error

```
  Client                                                         Server
    │                                                              │
    │  ────────── Subscribe (0xF0) ──────────────────────────────► │
    │              Query: SELEKT * FORM users  (syntax error)      │
    │                                                              │
    │                                            ┌─────────────────┴───┐
    │                                            │ Parse fails         │
    │                                            │ No ID assigned      │
    │                                            └─────────────────┬───┘
    │                                                              │
    │  ◄───────── SubscriptionError (0xF3) ─────────────────────── │
    │              ID: [00 00 00 ... 00]  (zeroed)                  │
    │              Message: "Parse error: unexpected SELEKT..."     │
    │                                                              │
```

### Error Flow - Execution Error

```
  Client                                                         Server
    │                                                              │
    │  ────────── Subscribe (0xF0) ──────────────────────────────► │
    │              Query: SELECT * FROM nonexistent_table          │
    │                                                              │
    │                                            ┌─────────────────┴───┐
    │                                            │ Parse succeeds      │
    │                                            │ ID assigned: <uuid> │
    │                                            │ Execute fails       │
    │                                            │ Remove subscription │
    │                                            └─────────────────┬───┘
    │                                                              │
    │  ◄───────── SubscriptionError (0xF3) ─────────────────────── │
    │              ID: <uuid>                                       │
    │              Message: "Execution error: table not found"      │
    │                                                              │
```

### Error Flow - Non-SELECT Query

```
  Client                                                         Server
    │                                                              │
    │  ────────── Subscribe (0xF0) ──────────────────────────────► │
    │              Query: UPDATE users SET name = 'Bob'            │
    │                                                              │
    │                                            ┌─────────────────┴───┐
    │                                            │ Parse succeeds      │
    │                                            │ ID assigned: <uuid> │
    │                                            │ Not a SELECT        │
    │                                            │ Remove subscription │
    │                                            └─────────────────┬───┘
    │                                                              │
    │  ◄───────── SubscriptionError (0xF3) ─────────────────────── │
    │              ID: <uuid>                                       │
    │              Message: "Only SELECT queries can be subscribed" │
    │                                                              │
```

## Integration with Standard Protocol

Subscription messages are sent/received alongside standard PostgreSQL messages:

- Client can send queries (Query) and subscriptions (Subscribe) in the same session
- Server responds with standard messages (DataRow, CommandComplete) and subscription messages
- Transaction handling is normal: subscriptions are per-session
- Subscriptions operate independently of query execution

**Example Session**:
```
Client: Query("SELECT 1")
Server: RowDescription, DataRow, CommandComplete, ReadyForQuery
Client: Subscribe("SELECT * FROM users")
Server: SubscriptionData (initial)
Client: Query("INSERT INTO logs VALUES (...)")
Server: CommandComplete, ReadyForQuery
[Background: data changes on 'users' table]
Server: SubscriptionData (update pushed asynchronously)
Client: Unsubscribe(subscription_id)
Client: Terminate
```

**Key Behaviors:**
- Subscribe does NOT return ReadyForQuery (it's a persistent operation)
- SubscriptionData can arrive between any other messages
- Multiple subscriptions can be active simultaneously
- Unsubscribe has no response

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

**Subscription Creation Errors:**
- Server sends SubscriptionError instead of SubscriptionData
- Client should treat this as subscription creation failure
- No subsequent updates will be sent for that subscription_id

**Error Types and Expected Messages:**

| Error Type | Subscription ID | Example Message |
|------------|-----------------|-----------------|
| Parse error | Zeroed (`[0x00; 16]`) | "Parse error: unexpected token at position 7" |
| Filter parse error | Zeroed (`[0x00; 16]`) | "Filter parse error: unexpected token" |
| Table not found | Valid UUID | "Execution error: table 'users' does not exist" |
| Permission denied | Valid UUID | "Execution error: permission denied for table 'users'" |
| Non-SELECT query | Valid UUID | "Only SELECT queries can be subscribed to" |
| Schema change | Valid UUID | "Subscription invalidated: table 'users' was dropped" |

**Client Error Handling Recommendations:**
1. Check for zeroed subscription ID to distinguish parse errors from execution errors
2. Implement reconnection logic for transient errors
3. Re-subscribe after schema changes if the query is still valid
4. Log errors for debugging but avoid exposing internal details to end users

## Compatibility with Standard PostgreSQL Clients

### Why This Works

The subscription message types (`0xF0-0xF3`) are chosen specifically because:

1. **Outside Standard Range**: PostgreSQL message types use ASCII letters (`A`-`Z` = `0x41-0x5A`, `a`-`z` = `0x61-0x7A`). The `0xF0-0xFF` range is completely unused.

2. **No Collision**: The closest standard messages are:
   - `E` (`0x45`) - ErrorResponse
   - `N` (`0x4E`) - NoticeResponse
   - `Z` (`0x5A`) - ReadyForQuery

   All far from our `0xF0-0xF3` range.

3. **Forward Compatibility**: Well-behaved clients skip unknown message types, so adding new subscription messages won't break existing clients.

### Standard Client Behavior

| Client | Behavior with VibeSQL Server |
|--------|------------------------------|
| `psql` | Works normally; cannot use subscriptions |
| `libpq` | Works normally; cannot use subscriptions |
| Node.js `pg` | Works normally; unknown messages ignored |
| Python `psycopg2` | Works normally; unknown messages may raise warning |
| Java JDBC | Works normally; cannot use subscriptions |

**Important**: Standard PostgreSQL clients will never send Subscribe/Unsubscribe messages because their protocol implementation doesn't include these message types. VibeSQL subscriptions are only usable with VibeSQL-aware clients.

### Building a VibeSQL-Aware Client

To use subscriptions, a client must:

1. **Understand message types `0xF0-0xF6`** in addition to standard PostgreSQL messages
2. **Handle asynchronous SubscriptionData** - these can arrive at any time, not just after requests
3. **Track subscription IDs** returned in SubscriptionData to correlate updates
4. **Handle SubscriptionAck** - confirms subscription registration before data arrives
5. **Implement Unsubscribe** to clean up subscriptions
6. **Optionally implement Pause/Resume** - for flow control of subscription updates

**Minimal Client Requirements:**
```
- Parse message type byte
- If type in [0xF0, 0xF1, 0xF2, 0xF3, 0xF4]:
    Handle subscription message
- If type in [0xF5, 0xF6]:
    Send subscription control message (pause/resume)
- Else:
    Handle as standard PostgreSQL message
```

## Security Considerations

1. **Query Validation**: Subscriptions go through normal query validation and permission checks
2. **Resource Limits**: Servers should limit:
   - Number of active subscriptions per session/client
   - Total number of subscriptions
   - Maximum rows per subscription
3. **DoS Prevention**: Rapid subscribe/unsubscribe could be used for DoS. Servers should rate-limit.
4. **Data Privacy**: Subscriptions follow the same permission model as queries
5. **Change Detection**: Subscriptions may reveal when data changes even if the client doesn't have permission to see the actual changes (timing side-channel)

## Future Extensions

Potential future enhancements:
- Delta compression (send only changed rows instead of full result set)
- Subscription batching (combine multiple subscriptions into single updates)
