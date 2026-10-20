# Redis Clone in Rust

A Redis-inspired TCP server written in Rust with Tokio. It implements a subset of Redis commands over the **RESP2** wire protocol and can be used with clients such as `redis-py` and `redis-cli`. Data is held in memory and persisted through an append-only file (AOF). The Python end-to-end suite exercises both execution modes using `redis-py` and raw TCP sockets.

| **Language** | Rust (async, Tokio) |
| **Protocol** | RESP2 |
| **Default address** | `127.0.0.1:6379` (configurable) |
| **Execution modes** | `single` (one thread, serialized commands) or `multi` (default, multi-threaded) |
| **Storage** | In-memory, 64-shard keyspace |
| **Persistence** | Append-only file (`appendonly.aof`), replayed on startup |
| **Test suite** | Python 3 + `redis` package (`test.py`) |

---

## Table of Contents

1.  [Features](#1-features)
2.  [Quick Start](#2-quick-start)
3.  [Project Layout](#3-project-layout)
4.  [Architecture](#4-architecture)
5.  [Request Lifecycle](#5-request-lifecycle)
6.  [Module Reference](#6-module-reference)
7.  [Supported Commands](#7-supported-commands)
8.  [Concurrency and Atomicity](#8-concurrency-and-atomicity)
9.  [Key Expiry (TTL)](#9-key-expiry-ttl)
10.  [Persistence (AOF)](#10-persistence-aof)
11.  [Pub/Sub](#11-pubsub)
12.  [Graceful Shutdown](#12-graceful-shutdown)
13.  [Testing](#13-testing)
14.  [Known Limitations](#14-known-limitations)
15.  [Future Work](#15-future-work)

---

## 1\. Features

*   **TCP server** handling many concurrent clients, one Tokio task per connection.
*   **RESP2 parser** that handles pipelined, fragmented, and malformed input with hard safety limits.
*   **Typed store** with strings, hashes, lists, sets, and sorted sets (floating-point scores, rank-indexed AVL tree).
*   **Sharded keyspace** (64 shards) with an operation gate for multi-key commands.
*   **Key expiration**: lazy eviction on access plus a background sweeper that runs every second.
*   **Command options**: `SET` with `EX`, `PX`, `NX`, `XX`; variadic `DEL`, `EXISTS`, `LPUSH`, `RPUSH`, `SADD`, `MGET`; `PEXPIRE`, `PTTL`, `PERSIST`.
*   **Pub/Sub** with multi-channel subscriptions, glob patterns (`PSUBSCRIBE`), and unsubscribe commands.
*   **AOF persistence** with configurable fsync policy, startup replay, incomplete-tail recovery, and `BGREWRITEAOF` compaction.
*   **Two execution modes** selectable at launch: `single` and `multi`.
*   **Graceful shutdown implementation** on Ctrl+C: stops accepting, signals active handlers, waits for connection tasks, and syncs the AOF. The current Python suite does not include a graceful-shutdown test.

---

## 2\. Quick Start

### Prerequisites

*   Rust toolchain with Cargo.
*   Python 3 and the `redis` package to run the end-to-end tests.
*   Port `6379` available when running the default test suite.

### Build and run

Build the server:

```
cargo build --release
```

Start with the defaults (`127.0.0.1:6379`, `multi` mode, `everysec` AOF syncing):

```
cargo run --release
```

Choose a host, port, execution mode, and AOF fsync policy:

```
cargo run --release -- --host 127.0.0.1 --port 6380 --mode single --appendfsync always
```

### Command-line options

| Flag | Default | Description |
| --- | --- | --- |
| `--host HOST` | `127.0.0.1` | Interface/address to bind |
| `--port PORT` | `6379` | TCP port (`0`–`65535`) |
| \`--mode single | multi\` | `multi` |
| \`--appendfsync always | everysec | no\` |
| `--help`, `-h` | — | Print usage and exit |

The server reads and writes `appendonly.aof` in its **current working directory**. Use a dedicated working directory if you want to isolate its data file.

### Try it

With the server running, connect using a RESP2 client:

```
redis-cli -2 -p 6379
> PING
PONG
> SET greeting hello EX 60
OK
> GET greeting
"hello"
> TTL greeting
(integer) 59
```

## 3\. Project Layout

```
.
├── Cargo.toml
├── README.md
├── test.py              # Single-file end-to-end suite (builds and launches the server)
└── src
    ├── main.rs          # CLI parsing, runtime selection, AOF replay, server start
    ├── server.rs        # Accept loop, TTL sweeper, graceful shutdown
    ├── handler.rs       # Per-connection loop, command execution, Pub/Sub delivery
    ├── command.rs       # Commands enum, RESP parser/validator, RESP serializer
    ├── store.rs         # Sharded store, data types, expiry, Pub/Sub channels
    └── persistence.rs   # Append-only file: append, replay, recovery
```

`test.py` must sit next to `Cargo.toml`; it runs `cargo build` from its own directory.

---

## 4\. Architecture

### 4.1 High-level view

```
 +-------------+     TCP / RESP2      +-----------------------------------------------+
 | redis-py    | <------------------> |                 Rust server                   |
 | redis-cli   |                      |                                               |
 | raw sockets |                      |  Server (accept loop, Ctrl+C, TTL sweeper)    |
 +-------------+                      |    |                                          |
                                      |    +-- spawn --> Handler (client 1) --+       |
                                      |    +-- spawn --> Handler (client 2) --+       |
                                      |    +-- spawn --> Handler (client N) --+       |
                                      |                                       v       |
                                      |   SharedDb (Arc<Db>)            Aof (shared)  |
                                      |     shards[64]: RwLock<HashMap>  appendonly   |
                                      |     operation_gate: RwLock<()>   .aof file    |
                                      |     pubsub / pattern_pubsub                   |
                                      +-----------------------------------------------+
```

### 4.2 Layers

| Layer | File | Responsibility |
| --- | --- | --- |
| Bootstrap | `main.rs` | Parse flags, build runtime, replay AOF, open AOF, start server |
| Networking | `server.rs` | Accept connections, spawn tasks, run TTL sweeper, coordinate shutdown |
| Session | `handler.rs` | Buffer bytes, parse frames, execute commands, log to AOF, push Pub/Sub messages |
| Protocol | `command.rs` | Validate and parse RESP into `Commands`; serialize replies |
| Storage | `store.rs` | Typed data, sharding, expiry, sorted-set index, Pub/Sub channels |
| Durability | `persistence.rs` | Append commands, replay and repair the log |

### 4.3 Design choices

*   **One Tokio task per connection.** Cheap, scales to many clients, keeps each client's parsing state isolated.
*   **Shared state behind** `**Arc**`**.** `SharedDb` and `Aof` are cheap clone-able handles.
*   **Two-phase parsing.** `validate_resp_frame` decides whether a complete, well-formed frame exists and its exact length. Only then does `parse_complete_command` build a `Commands` value. This cleanly separates "need more bytes" from "invalid input".
*   **Argument validation before execution.** `validate_command_args` checks arity; bad numbers and options produce an error reply instead of a silent default.
*   **Typed values via an enum.** `Value` is `String | Hash | List | Set | SortedSet`, so each key has exactly one type.
*   **Log what was executed.** The AOF stores the original RESP bytes of each successful write command, so replay reuses the same parser.

---

## 5\. Request Lifecycle

### 5.1 Startup

1.  `main` parses flags and builds a current-thread (`single`) or multi-thread (`multi`) Tokio runtime.
2.  A fresh `SharedDb` is created.
3.  `Aof::replay("appendonly.aof", &db)` restores state before any client connects.
4.  `Aof::open` opens the file for appending.
5.  `Server::new` binds the listener; `Server::run` starts the TTL sweeper and the accept loop.

### 5.2 Per-connection loop

```
accept() -> clone SharedDb + Aof -> spawn Handler::run

Handler::run_loop (tokio::select!):
  shutdown signal received      -> return Ok
  socket read (1024 bytes)      -> 0 bytes: client gone, return
                                   else append to buffer, process_buffered_commands
  Pub/Sub event from a task     -> write message / pmessage to socket

process_buffered_commands (inner loop):
  Commands::parse(&buffer)
    Complete(cmd, n) -> drain n bytes
                        (single mode: take global command lock)
                        execute_command
                        if write succeeded: Aof::append(raw bytes)
                        write reply
    Incomplete       -> break, wait for more bytes
    Invalid(msg)     -> write "-ERR Protocol error: ...", close connection
```

Because the inner loop keeps parsing until the buffer yields `Incomplete`, **pipelined** commands are answered in order and **fragmented** commands are reassembled automatically.

### 5.3 Example: `SET name Bob EX 10`

1.  Client sends `*5\r\n$3\r\nSET\r\n$4\r\nname\r\n$3\r\nBob\r\n$2\r\nEX\r\n$2\r\n10\r\n`.
2.  `validate_resp_frame` confirms the frame is complete and returns its length.
3.  `validate_command_args` checks arity; `parse_complete_command` builds `Commands::Set { key, value, expiry: Some(10s), nx: false, xx: false }`.
4.  `execute_command` calls `db.set_with_options(...)`, which locks one shard.
5.  The server replies `+OK\r\n`.
6.  Because the reply is not an error or a null, the raw command bytes are appended to the AOF.

---

## 6\. Module Reference

### 6.1 `main.rs`

Parses `--host`, `--port`, `--mode`, and `--appendfsync` (plus help flags). Selects a current-thread runtime for `single` or a multi-thread runtime for `multi`, replays the AOF, opens it for appending using the selected fsync policy, then runs the server.

### 6.2 `server.rs`

`Server { db, aof, listener, mode, command_lock }`.

*   Owns the accept loop and a `JoinSet` of connection tasks.
*   In `single` mode creates a global `Arc<Mutex<()>>` command lock shared by all handlers.
*   Spawns the TTL cleanup task (1-second interval) that calls `db.remove_expired()`.
*   On Ctrl+C: stops accepting, broadcasts shutdown through a `watch` channel, aborts the sweeper, and waits for every connection task. See [Graceful Shutdown](#12-graceful-shutdown).

### 6.3 `handler.rs`

`Handler` owns one client connection: its socket, read buffer, AOF handle, optional command lock, and its Pub/Sub subscriptions.

*   `run_loop()` is the `select!` loop described above.
*   `process_buffered_commands()` parses, executes, logs, and replies.
*   `execute_command()` is a large `match` over `Commands` that calls the store and builds a RESP reply.
*   Helpers: `handle_incrby` (INCR/DECR/INCRBY/DECRBY), `handle_pop_list`, `handle_zrange` (ZRANGE/ZREVRANGE).
*   Pub/Sub: `subscribe_channels`, `unsubscribe_channels`, `subscribe_patterns`, `unsubscribe_patterns`, `cleanup_subscriptions`, `write_pubsub_event`.

### 6.4 `command.rs`

1.  `**Commands**` **enum**: one variant per supported command, plus `Error(String)` and `Unknown(String)`. `should_persist()` marks which variants are written to the AOF.
2.  `**RespSerializer**`: `simple_string` (`+`), `error` (`-`), `integer` (`:`), `bulk_string` (`$`), `null_bulk_string` (`$-1`), `array` (`*`).
3.  **Parser**
    *   `ParseOutcome { Complete(cmd, consumed), Incomplete, Invalid(msg) }`
    *   `validate_resp_frame(src)`: `Ok(None)` when more bytes are needed, `Ok(Some(len))` for a complete frame, `Err(msg)` when malformed. Safety limits:
        *   at most **10,000** elements per array
        *   at most **16 MiB** per bulk string
        *   overflow-checked length arithmetic
        *   every bulk string must end in `\r\n`
    *   `validate_command_args(cmd, argc)`: arity check; failure yields `-ERR wrong number of arguments for '<cmd>'`.
    *   `parse_complete_command(src)`: builds a `Commands` value. Invalid numbers or options yield `-ERR invalid command arguments`. A bare non-array line becomes `Commands::Unknown`.

### 6.5 `store.rs`

```
pub enum Value {
    String(Bytes),
    Hash(HashMap<String, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    SortedSet(SortedSet),
}

pub struct DbEntry { pub value: Value, pub expires_at: Option<Instant> }

pub struct Db {
    pub shards: Vec<RwLock<HashMap<String, DbEntry>>>,   // 64 shards
    pub operation_gate: RwLock<()>,
    pub pubsub: RwLock<HashMap<String, broadcast::Sender<Bytes>>>,
    pub pattern_pubsub: RwLock<HashMap<String, broadcast::Sender<(String, Bytes)>>>,
}

pub struct SharedDb(pub Arc<Db>);
```

*   **Sharding**: a key is hashed with `DefaultHasher` and mapped to one of 64 shards.
*   **Sorted sets** combine a `HashMap<Bytes, f64>` (member to score) with a size-augmented **AVL tree** ordered by `(score, member)`. Rank lookups are O(log n) and `ZRANGE` only visits the requested ranks. Scores are `f64`; NaN is rejected by the parser.
*   **Strings/values** use `bytes::Bytes` (cheap clones, binary-safe).
*   **Pub/Sub** uses one `tokio::sync::broadcast` channel (capacity 1024) per channel and per pattern. Patterns support `*`, `?`, `[...]`, `[^...]`, and `\` escapes.

### 6.6 `persistence.rs`

*   `Aof::open(path, policy)` opens the file in create+append mode and starts a periodic sync task for `everysec`.
*   `Aof::append(bytes)` writes and flushes one command record, then applies the configured sync policy.
*   `Aof::replay(path, db)` validates the complete log before applying records in order; it truncates an incomplete final record and rejects malformed or unsupported records.
*   `Aof::rewrite(db)` writes a compact snapshot to a temporary file and replaces the AOF.

---

## 7\. Supported Commands

Command names are case-insensitive. This is a Redis-inspired subset, not full Redis compatibility.

| Category | Commands |
| --- | --- |
| Connection | `HELLO`, `PING [message]` |
| Keys and expiry | `EXISTS`, `DEL`, `KEYS pattern`, `SCAN cursor [MATCH pattern] [COUNT count]`, `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `FLUSHALL`, `BGREWRITEAOF` |
| Strings and counters | \`SET key value \[EX seconds |
| Hashes | `HSET` (multiple field/value pairs), `HGET`, `HGETALL`, `HDEL` (multiple fields), `HKEYS` |
| Lists | `LPUSH`, `RPUSH` (multiple values), `LPOP`, `RPOP`, `LRANGE`, `LLEN` |
| Sets | `SADD`, `SREM` (multiple members), `SMEMBERS`, `SISMEMBER`, `SINTER`, `SUNION` |
| Sorted sets | `ZADD` (multiple score/member pairs), `ZREM` (multiple members), `ZCARD`, `ZSCORE`, `ZRANK`, `ZRANGE [WITHSCORES]`, `ZREVRANGE [WITHSCORES]` |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE` |

### Error replies

| Reply | When |
| --- | --- |
| `-ERR wrong number of arguments for '<cmd>'` | A command's argument count is invalid |
| `-ERR invalid command arguments` | A numeric value or command option cannot be parsed |
| `-ERR value is not an integer or out of range` | Counter command receives a non-integer value |
| `-WRONGTYPE ...` | An operation is incompatible with the key's stored type |
| Unknown-command error | The command is not recognized |
| `-ERR Protocol error: ...` | The RESP frame is malformed; the connection is closed |

Invalid command arguments generally return an error without closing the connection; malformed protocol frames close the connection. Exact wrong-type behavior is not fully consistent across every command.

---

## 8\. Concurrency and Atomicity

### 8.1 Execution modes

| Mode | Runtime | Command execution |
| --- | --- | --- |
| `multi` (default) | Multi-threaded Tokio | Commands from different clients run in parallel, limited only by store locks |
| `single` | Current-thread Tokio | Additionally, a global `Mutex` serializes every command across all clients |

### 8.2 Store locking

The store uses two lock levels:

*   **Shard lock** (`RwLock<HashMap>`, 64 of them): a single-key command locks only the shard its key hashes to, so commands on different shards proceed in parallel.
*   **Operation gate** (`RwLock<()>`):
    *   Single-key commands take the gate as a **reader**, so they run concurrently.
    *   Commands that span shards or the whole keyspace take it as a **writer**, which excludes every other command: `EXISTS`, `DEL`, `SCAN`, `FLUSHALL`, `SINTER`, and the TTL sweeper.

### 8.3 Guarantees

*   Each single-key command holds its shard lock for the full operation, so it is atomic with respect to other commands.
*   Multi-value writes (`HSET` with many fields, `SADD` with many members, `ZADD` with many members, `LPUSH`/`RPUSH` with many values) apply all items under one shard lock. Other clients never see a half-applied write.
*   `INCR`/`INCRBY` read-modify-write happens inside one shard lock, so concurrent increments never lose updates.
*   `SET ... NX/XX` and `SETNX` check and write under the same lock.

> The test suite checks these properties under load, but zero observed partial snapshots is evidence, not proof. The guarantee comes from the locking design above.

---

## 9\. Key Expiry (TTL)

Expiry uses two mechanisms:

*   **Lazy eviction.** Every command that touches a key checks `expires_at` first and removes the key if it has passed.
*   **Active sweeper.** A background task calls `remove_expired()` every second and logs `[TTL] Removed N expired keys`. This reclaims keys that are never read again.

Commands:

*   `EXPIRE key seconds` / `PEXPIRE key ms` set `expires_at`; both return `1` if the key exists, else `0`.
*   `SET key value EX seconds | PX ms` stores the value and expiry together. A zero or non-numeric amount is rejected.
*   `TTL` returns remaining whole seconds; `PTTL` returns remaining milliseconds. Both return `-1` for a key without expiry and `-2` for a missing or expired key.
*   `PERSIST` removes the expiry (`1` if one was removed, else `0`).
*   A plain `SET` replaces the entry and clears any previous expiry.

---

## 10\. Persistence (AOF)

### 10.1 Writing

After a write command executes successfully, the handler appends its **original RESP bytes** to `appendonly.aof` and flushes the file handle. A command is skipped if:

*   it is not a write (reads, Pub/Sub, `SCAN`, `HELLO`, ...),
*   the reply is an error,
*   the reply is a null (failed `SET NX/XX`), or
*   `SETNX` returned `0`.

Persisted commands include `SET`, `SETNX`, `DEL`, `FLUSHALL`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `MSET`, `HSET`, `HDEL`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `SADD`, `SREM`, `ZADD`, and `ZREM`.

### 10.2 Recovery on startup

`Aof::replay` runs before the server accepts clients:

| File state | Behaviour |
| --- | --- |
| Missing | Treated as first start; nothing replayed |
| All records complete | Every command is applied in order |
| Incomplete final record | The partial record is **truncated from the file**, complete records are kept, log line `[AOF] Discarded incomplete final record at byte N` |
| Malformed record | Startup **fails** with `Invalid AOF record at byte N: ...`; the file is left untouched |
| Unsupported command in file | Startup fails with `InvalidData` |

All records are parsed and validated before any is applied.

### 10.3 Fsync policy and caveats

The `--appendfsync` option controls explicit syncing:

| Policy | Behaviour |
| --- | --- |
| `always` | Calls `sync_data()` after each appended command |
| `everysec` (default) | Marks the file dirty after a write; a background task checks about once per second and calls `sync_data()` when needed |
| `no` | Does not explicitly sync after appends |

`BGREWRITEAOF` starts a background rewrite. The rewrite takes a database snapshot, writes a compact representation to a temporary file, syncs that file, then replaces the active AOF.

Caveats:

*   `everysec` and `no` can lose recent writes after a crash or power loss; `always` has a higher write cost.
*   Relative expiries (`EXPIRE`, `PEXPIRE`, `SET ... EX/PX`) are replayed as logged commands, so expiry timing is not preserved as an absolute wall-clock deadline across restarts.
*   The handler executes a write and appends its record after execution. Concurrent clients may therefore produce AOF ordering that differs from the exact order in which commands acquired and released store locks.
*   AOF rewrite and appends are coordinated through a rewrite gate, but this is a learning implementation rather than a full Redis persistence implementation.

---

## 11\. Pub/Sub

*   `PUBLISH channel message` delivers to direct subscribers and to every matching pattern subscriber, and returns the total number of receivers.
*   `SUBSCRIBE ch [ch ...]` and `PSUBSCRIBE pattern [pattern ...]` reply with one confirmation array per argument: `["subscribe" | "psubscribe", name, count]`. The count is the connection's total of channel plus pattern subscriptions.
*   `UNSUBSCRIBE [ch ...]` and `PUNSUBSCRIBE [pattern ...]` remove the named subscriptions, or all of them when called without arguments, and reply with the remaining count.
*   Messages arrive as `["message", channel, payload]` or `["pmessage", pattern, channel, payload]`.

How it works: each subscription is a small Tokio task that forwards messages from a `broadcast` channel into the connection's internal queue; the handler's `select!` loop writes them to the socket. A subscriber that lags simply skips the lost messages.

Cleanup: unsubscribing, disconnecting, or shutting down aborts the forwarding tasks and removes any channel or pattern that has no receivers left.

---

## 12\. Graceful Shutdown

On Ctrl+C (SIGINT):

1.  The accept loop exits and the listener is dropped.
2.  A shutdown flag is broadcast on a `watch` channel; every handler leaves its loop, aborts its Pub/Sub tasks, and closes its socket.
3.  The TTL sweeper is aborted.
4.  The server waits for all connection tasks and logs `All connection tasks finished.`

---

## 13\. Testing

`test.py` is a single, ordered end-to-end suite. It builds the Rust server once per script run, starts it in a temporary directory (so the project's normal `appendonly.aof` is not touched), and runs integration and AOF persistence/recovery tests for each selected execution mode. The current suite does not run a graceful-shutdown test.

### 13.1 Run

```
pip install redis
python test.py                  # test single and multi modes
python test.py --mode multi     # test multi mode only
python test.py --mode single    # test single mode only
```

Requirements: `cargo` on `PATH`, `test.py` next to `Cargo.toml`, binary named `redis-clone`, and port 6379 free (**stop any running server first**). The script exits with status `1` if any check fails.

### 13.2 Suite structure

| Part | Section | What it checks |
| --- | --- | --- |
| **Integration** (one live server) | 5.1 Strings and counters | `PING`, `SET`/`GET`, `INCRBY`, `SET` with `NX`/`XX`/`EX`/`PX` |
|   | 5.2 Keys | Variadic `EXISTS` and `DEL` |
|   | 5.3 Expiry | `TTL`, `EXPIRE`, expiry after 1s, concurrent reads of an expired key, `PEXPIRE`, `PERSIST` |
|   | 5.4 Hashes | Multi-field `HSET`, `HGET`, `HGETALL`, `HDEL` |
|   | 5.5 Lists | `LPUSH`, `RPUSH` (single and variadic), `LLEN`, `LPOP`, `RPOP`, `LRANGE` |
|   | 5.6 Sets | Multi-member `SADD`, `SISMEMBER`, `SMEMBERS` |
|   | 5.7 Sorted sets | Float scores, ordering, negative ranks, updates, `ZREM`, ties, 4000-member rank lookup |
|   | 5.8 Pub/Sub | Basic delivery; multi-channel, `UNSUBSCRIBE`, `PSUBSCRIBE` with `*` and `?`, `PUNSUBSCRIBE`, disconnect cleanup |
|   | 5.9 Invalid arguments | Error reply returned, connection stays open |
|   | 5.10 Concurrency | 8 threads x 100 `INCR` lose no updates; concurrent `SADD` inserts once; 16 clients on separate keys |
|   | 5.11 Atomicity | 1000-field `HSET` / 1000-member `SADD` while 8 readers poll: only empty or complete snapshots |
|   | 5.12 Protocol robustness | Pipelining, fragmented request, incomplete request waits, malformed array length, server survives disconnects |
| **Lifecycle** (restarts server) | AOF persistence and recovery | Write supported data types, restart, verify recovery; incomplete tail is truncated; malformed record blocks startup; recovery after restoring a valid file |

Helper functions `encode_command`, `read_line`, and `read_resp` implement a minimal RESP client for raw-socket tests. Integration tests are registered in `INTEGRATION_TESTS`; persistence and recovery checks are orchestrated by `test_persistence()`.

---

## 14\. Known Limitations

This is a learning/portfolio implementation. It implements a subset of Redis behaviour and is not a drop-in replacement for production Redis.

**Protocol and commands**

*   RESP2 only. `HELLO` returns a fixed server-info array; it does not negotiate RESP3.
*   `SCAN` supports cursor, `MATCH`, and `COUNT`, but its behaviour is simplified compared with Redis and should not be assumed to provide full Redis cursor semantics.
*   `SET` supports `EX`, `PX`, `NX`, and `XX`; options such as `GET`, `KEEPTTL`, and absolute-expiry variants are not implemented.
*   Some command semantics and error handling, especially wrong-type handling, are not identical to Redis.
*   Collection results such as `SMEMBERS`, `HGETALL`, `HKEYS`, and key scans may be returned in arbitrary order.
*   The implementation does not provide all Redis commands, flags, or edge-case semantics.

**Persistence**

*   AOF replay uses the server's supported command parser; an unsupported or malformed record prevents startup.
*   Expiry commands are persisted as commands rather than as absolute expiry timestamps, so restart timing may differ from a production Redis server.
*   Durability depends on the selected `--appendfsync` policy.
*   AOF rewrite is implemented, but this is not a complete replacement for Redis's production persistence and recovery machinery.

**Performance and concurrency**

*   Multi-key/global operations such as `EXISTS`, `DEL`, `SCAN`, `KEYS`, `FLUSHALL`, set operations, and the TTL sweeper can require broad locking and limit parallelism.
*   The TTL sweeper scans the keyspace once per second.
*   Sorted-set rank lookup uses a size-augmented AVL tree; returned members are still cloned into replies.
*   The `single` mode serializes command execution across connections; `multi` mode allows commands to execute concurrently subject to store locks.

**Pub/Sub and security**

*   Pub/Sub uses bounded broadcast channels; a slow subscriber can miss messages when its buffer overflows.
*   No replication, transactions (`MULTI`/`EXEC`), authentication, TLS, or Redis configuration-file support is provided.
*   The server should not be exposed to untrusted networks without adding authentication and transport security.

---

## 15\. Future Work

*   Expand command compatibility and close remaining semantic gaps with Redis.
*   Make wrong-type errors and integer-overflow handling consistent across all commands.
*   Add broader Rust unit tests and CI coverage alongside the Python end-to-end suite.
*   Improve `SCAN` compatibility and measure lock contention for multi-key operations.
*   Add authentication, TLS, and configuration-file support if this is developed beyond a local learning project.
