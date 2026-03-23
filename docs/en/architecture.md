# Architecture Design

This document introduces the internal architecture and design principles of FoxKV.

## Overall Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        FoxKV Architecture                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                      Client Layer                            ││
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐         ││
│  │  │ redis-cli│  │ Python  │  │ Node.js │  │   Go    │         ││
│  │  │  Client │  │ Client  │  │ Client  │  │ Client  │         ││
│  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘         ││
│  │       └─────────────┴─────────────┴─────────────┘            ││
│  └──────────────────────────────┬───────────────────────────────┘│
│                                 │                                │
│  ┌──────────────────────────────┴───────────────────────────────┐│
│  │                    Network Layer (Tokio)                      ││
│  │  ┌─────────────────────────────────────────────────────────┐ ││
│  │  │              TCP Server (tokio::net::TcpListener)        │ ││
│  │  │                    Port: 6379                            │ ││
│  │  └─────────────────────────────────────────────────────────┘ ││
│  │                         │                                     ││
│  │  ┌──────────────────────┴──────────────────────┐             ││
│  │  │         Connection Handler (Per Client)      │             ││
│  │  │  ┌─────────────────────────────────────────┐ │             ││
│  │  │  │      Framed Read/Write (RESP)          │ │             ││
│  │  │  │         tokio_util::codec              │ │             ││
│  │  │  └─────────────────────────────────────────┘ │             ││
│  │  └──────────────────────────────────────────────┘             ││
│  └──────────────────────────────┬───────────────────────────────┘│
│                                 │                                │
│  ┌──────────────────────────────┴───────────────────────────────┐│
│  │                  Protocol Layer (RESP)                        ││
│  │  ┌─────────────────────────────────────────────────────────┐ ││
│  │  │              RESP Parser / Encoder                       │ ││
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │ ││
│  │  │  │ Simple  │ │ Error   │ │ Integer │ │ Bulk    │       │ ││
│  │  │  │ String  │ │         │ │         │ │ String  │       │ ││
│  │  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘       │ ││
│  │  │  ┌─────────┐ ┌─────────┐                               │ ││
│  │  │  │  Array  │ │  Null   │                               │ ││
│  │  │  └─────────┘ └─────────┘                               │ ││
│  │  └─────────────────────────────────────────────────────────┘ ││
│  └──────────────────────────────┬───────────────────────────────┘│
│                                 │                                │
│  ┌──────────────────────────────┴───────────────────────────────┐│
│  │                 Command Layer                                 ││
│  │  ┌─────────────────────────────────────────────────────────┐ ││
│  │  │              Command Router / Dispatcher                 │ ││
│  │  │                                                          │ ││
│  │  │   SET ──→ StringCommandHandler                          │ ││
│  │  │   HSET ─→ HashCommandHandler                            │ ││
│  │  │   LPUSH ─→ ListCommandHandler                           │ ││
│  │  │   SADD ──→ SetCommandHandler                            │ ││
│  │  │   ZADD ──→ ZSetCommandHandler                           │ ││
│  │  │   ...                                                     │ ││
│  │  └─────────────────────────────────────────────────────────┘ ││
│  └──────────────────────────────┬───────────────────────────────┘│
│                                 │                                │
│  ┌──────────────────────────────┴───────────────────────────────┐│
│  │                 Storage Layer                                 ││
│  │  ┌─────────────────────────────────────────────────────────┐ ││
│  │  │              DashMap (Concurrent HashMap)                │ ││
│  │  │                                                          │ ││
│  │  │   Key ──→ Value (TypedValue)                             │ ││
│  │  │                                                          │ ││
│  │  │   TypedValue:                                            │ ││
│  │  │   ├─ String(String)                                      │ ││
│  │  │   ├─ Hash(HashMap)                                       │ ││
│  │  │   ├─ List(LinkedList)                                    │ ││
│  │  │   ├─ Set(HashSet)                                        │ ││
│  │  │   └─ ZSet(SkipList)                                      │ ││
│  │  └─────────────────────────────────────────────────────────┘ ││
│  └──────────────────────────────┬───────────────────────────────┘│
│                                 │                                │
│  ┌──────────────────────────────┴───────────────────────────────┐│
│  │              Persistence Layer                                ││
│  │  ┌─────────────────────┐  ┌─────────────────────┐            ││
│  │  │        AOF          │  │        RDB          │            ││
│  │  │  (Append-Only File) │  │    (Snapshot)       │            ││
│  │  │                     │  │                     │            ││
│  │  │  ┌───────────────┐  │  │  ┌───────────────┐  │            ││
│  │  │  │ Command Log   │  │  │  │ Binary Dump   │  │            ││
│  │  │  │ fsync policy  │  │  │  │ Periodic save │  │            ││
│  │  │  │ Rewrite       │  │  │  │               │  │            ││
│  │  │  └───────────────┘  │  │  └───────────────┘  │            ││
│  │  └─────────────────────┘  └─────────────────────┘            ││
│  └──────────────────────────────────────────────────────────────┘│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Network Layer

Asynchronous network processing based on Tokio:

```rust
// Simplified server startup code
let listener = TcpListener::bind("0.0.0.0:6379").await?;

loop {
    let (socket, addr) = listener.accept().await?;
    tokio::spawn(handle_connection(socket, addr));
}
```

**Features:**
- One Tokio task per connection
- Uses `Framed` for RESP encoding/decoding
- Supports high concurrent connections

### 2. RESP Protocol Layer

Redis Serialization Protocol implementation:

```
Simple Strings: +OK\r\n
Errors:         -ERR message\r\n
Integers:       :1000\r\n
Bulk Strings:   $6\r\nfoobar\r\n
Arrays:         *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
```

**Parsing Flow:**
1. Read first byte to determine type
2. Read subsequent data based on type
3. Parse CRLF delimiter
4. Recursively parse nested arrays

### 3. Command Layer

Command processing architecture:

```rust
// Command dispatch
match command_name {
    "GET" | "SET" | "INCR" => string::handle(args),
    "HGET" | "HSET" => hash::handle(args),
    "LPUSH" | "RPOP" => list::handle(args),
    "SADD" | "SISMEMBER" => set::handle(args),
    "ZADD" | "ZRANGE" => zset::handle(args),
    _ => Err("Unknown command"),
}
```

### 4. Storage Layer

Uses DashMap for concurrent safety:

```rust
pub struct Storage {
    data: DashMap<String, TypedValue>,
}

pub enum TypedValue {
    String(String),
    Hash(HashMap<String, String>),
    List(VecDeque<String>),
    Set(HashSet<String>),
    ZSet(SkipList<String, f64>),
}
```

**Why DashMap?**
- Lock-free concurrent reads
- Fine-grained locking for writes
- Better performance than RwLock<HashMap>

### 5. Persistence Layer

#### AOF (Append-Only File)

```
┌─────────────────────────────────────────┐
│           AOF Write Flow                 │
├─────────────────────────────────────────┤
│                                         │
│  1. Command Execute ──→ 2. Write to AOF Buffer    │
│       │                    │            │
│       ↓                    ↓            │
│  3. Response Client ←── 4. fsync per policy       │
│       │                                 │
│       ↓                                 │
│  5. Background Rewrite (AOF Rewrite)    │
│                                         │
└─────────────────────────────────────────┘
```

**Sync Policies:**
- `always`: fsync every command
- `everysec`: fsync every second (default)
- `no`: Let OS decide

#### RDB (Snapshot)

```
┌─────────────────────────────────────────┐
│           RDB Save Flow                  │
├─────────────────────────────────────────┤
│                                         │
│  1. fork() child process                 │
│       │                                 │
│       ↓                                 │
│  2. Child process iterates database      │
│       │                                 │
│       ↓                                 │
│  3. Compress and write RDB file          │
│       │                                 │
│       ↓                                 │
│  4. Atomically replace old file          │
│                                         │
└─────────────────────────────────────────┘
```

### 6. Replication Layer

Master-replica replication architecture:

```
┌──────────────┐                      ┌──────────────┐
│    Master    │                      │   Replica    │
│              │                      │              │
│  ┌────────┐  │     SYNC/PSYNC      │  ┌────────┐  │
│  │  Data  │  │◄───────────────────►│  │  Data  │  │
│  └────────┘  │                      │  └────────┘  │
│       │      │                      │       │      │
│       ↓      │     Command Prop    │       ↓      │
│  ┌────────┐  │────────────────────►│  ┌────────┐  │
│  │  AOF   │  │   (Async Replica)   │  │ Apply  │  │
│  └────────┘  │                      │  └────────┘  │
└──────────────┘                      └──────────────┘
```

**Replication Flow:**
1. Replica sends `PSYNC` command
2. Master creates RDB snapshot
3. Master sends RDB to replica
4. Master continuously sends write commands

## Data Flow

### Read Operation Flow

```
Client ──→ TCP ──→ RESP Decode ──→ Command Parse ──→ Storage Read ──→ Response
```

### Write Operation Flow

```
Client ──→ TCP ──→ RESP Decode ──→ Command Parse ──→ Storage Write ──→ AOF Append ──→ Response
```

## Performance Optimizations

### 1. Zero Copy

- Uses `Bytes` type to avoid memory copying
- RESP parser operates directly on raw bytes

### 2. Batching

- Command pipeline support
- Batch writes to AOF

### 3. Memory Optimization

- `smallvec` reduces small array allocations
- String interning (future optimization)

### 4. Lock Optimization

- DashMap instead of global locks
- Command-level concurrency control

## Extensibility Design

### 1. Command Extension

```rust
// Steps to add a new command:
// 1. Add handler function in command/types/
// 2. Register in router
// 3. Add tests
```

### 2. Storage Engine Extension

```rust
pub trait StorageEngine {
    fn get(&self, key: &str) -> Option<Value>;
    fn set(&self, key: String, value: Value);
    fn delete(&self, key: &str) -> bool;
    // ...
}
```

### 3. Protocol Extension

Supports future RESP3 protocol extension.

## Fault Handling

### 1. Connection Disconnection

- Automatic cleanup of connection state
- Rollback of incomplete commands

### 2. Persistence Failure

- `stop-writes-on-bgsave-error` configuration
- Error logging

### 3. Master-Replica Replication Failure

- Automatic reconnection mechanism
- Partial resynchronization (PSYNC)

## Monitoring and Debugging

### Built-in Commands

- `INFO`: Server statistics
- `CLIENT LIST`: Connection list

### Log Levels

- `debug`: Detailed debug information
- `verbose`: General information
- `notice`: Important information (default)
- `warning`: Warnings and errors

## Future Architecture Evolution

### Planned Features

1. **Cluster Mode**
   - Data sharding
   - Failover

2. **Multi-threaded Storage**
   - Partitioned storage
   - Reduced lock contention

3. **New Data Types**
   - Streams
   - Geospatial

4. **Module System**
   - Dynamic loading of extensions
