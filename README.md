# Redis Clone in Rust

A small, in-memory, Redis-compatible server written in Rust on top of [Tokio](https://tokio.rs). It speaks the **RESP2** wire protocol, so standard Redis clients (such as `redis-py` or `redis-cli`) can connect to it directly. A Python test suite exercises it using the official `redis` client plus raw TCP sockets.

*   **Language:** Rust (async, Tokio)
*   **Protocol:** RESP2
*   **Address:** `127.0.0.1:6379`
*   **Storage:** in-memory only (no persistence)
*   **Test client:** Python `redis` package

---

## Table of Contents

1.  [Overview](#1-overview)
2.  [Project Layout](#2-project-layout)
3.  [Architecture](#3-architecture)
4.  [Execution Flow](#4-execution-flow)
5.  [Module Deep Dive](#5-module-deep-dive)
6.  [Supported Commands](#6-supported-commands)
7.  [Concurrency and Atomicity Model](#7-concurrency-and-atomicity-model)
8.  [Key Expiry (TTL)](#8-key-expiry-ttl)
9.  [Pub/Sub](#9-pubsub)
10.  [Testing with the Python Client](#10-testing-with-the-python-client)
11.  [Build and Run](#11-build-and-run)
12.  [Known Limitations and Differences from Real Redis](#12-known-limitations-and-differences-from-real-redis)
13.  [Future Work](#13-future-work)

---

## 1\. Overview

The goal of this project is to implement the core ideas behind Redis from scratch:

*   A **TCP server** that accepts many concurrent clients.
*   A **RESP parser** that handles pipelined, fragmented, and malformed input.
*   A **typed key-value store** supporting strings, hashes, lists, sets, and sorted sets.
*   **Key expiration** with lazy eviction.
*   **Publish/Subscribe** messaging.
*   A **Python test harness** that validates behaviour through a real Redis client library, proving wire-level compatibility.

---

## 2\. Project Layout

```
.
├── main.rs            # Entry point: creates the DB, binds the server, runs it
├── server.rs          # TCP accept loop; spawns one task per connection
├── handler.rs         # Per-connection loop: read -> parse -> execute -> respond
├── command.rs         # Command enum, RESP parser/validator, RESP serializer
├── store.rs           # In-memory database, data types, TTL, pub/sub channels
├── test.py            # Full Python test suite (functional, concurrency, TTL, protocol)
└── debug_atomicity.py # Focused debug script for large-HSET atomicity
```

> `main.rs` declares the four modules (`command`, `handler`, `server`, `store`). In a standard Cargo project these live under `src/`.

---

## 3\. Architecture

### 3.1 High-level view

```
 +-------------+      TCP / RESP2       +--------------------------------------+
 | redis-py    | <--------------------> |            Rust server               |
 | redis-cli   |                        |                                      |
 | raw sockets |                        |  Server (accept loop)                |
 +-------------+                        |     |                                |
                                        |     +-- spawn --> Handler (client 1) |
                                        |     +-- spawn --> Handler (client 2) |
                                        |     +-- spawn --> Handler (client N) |
                                        |                      |               |
                                        |                      v               |
                                        |            SharedDb (Arc<Db>)        |
                                        |   entries: RwLock<HashMap<..>>       |
                                        |   pubsub : RwLock<HashMap<..>>       |
                                        +--------------------------------------+
```

### 3.2 Layers

| Layer | File | Responsibility |
| --- | --- | --- |
| Bootstrap | `main.rs` | Build shared state, bind socket, start loop |
| Networking | `server.rs` | Accept connections, spawn a Tokio task per client |
| Session | `handler.rs` | Buffer bytes, parse frames, dispatch commands, write replies |
| Protocol | `command.rs` | Validate/parse RESP into `Commands`; serialize replies |
| Storage | `store.rs` | Typed data, expiry, concurrency control, pub/sub broadcast |

### 3.3 Design choices

*   **One Tokio task per connection.** Cheap, scales to many clients, and keeps each client's parsing state isolated.
*   **One shared database behind** `**Arc**`**.** `SharedDb` is a thin clone-able handle; cloning only bumps a reference count.
*   **Two-phase parsing.** `validate_resp_frame` first determines whether a _complete, well-formed_ frame exists (and its exact length). Only then does `parse_complete_command` build a `Commands` value. This cleanly separates "need more bytes" from "invalid input".
*   **Typed values via an enum.** `Value` is `String | Hash | List | Set | SortedSet`, so each key has exactly one type and commands check it.

---

## 4\. Execution Flow

### 4.1 Startup

1.  `main` creates `SharedDb::new()`.
2.  `Server::new("127.0.0.1:6379", db)` binds a `TcpListener`.
3.  `Server::run()` loops forever on `accept()`.

### 4.2 Per-connection lifecycle

```
accept() -> clone SharedDb -> tokio::spawn(Handler::run)

Handler::run loop:
  read up to 1024 bytes from socket
    0 bytes? -> client disconnected, return
  append to self.buffer
  inner loop:
    Commands::parse(&buffer)
      Complete(cmd, n) -> drain n bytes, execute_command, write reply
      Incomplete       -> break, wait for more bytes
      Invalid(msg)     -> write "-ERR Protocol error: ...", close connection
```

Because the inner loop keeps parsing until the buffer yields `Incomplete`, **pipelined commands** (many commands in one TCP read) are answered in order, and **fragmented commands** (one command split across reads) are reassembled automatically.

### 4.3 Example: `SET name Bob`

1.  Client sends `*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$3\r\nBob\r\n`.
2.  `validate_resp_frame` confirms the frame is complete and returns its length.
3.  `parse_complete_command` produces `Commands::Set { key: "name", value: "Bob" }`.
4.  `execute_command` calls `db.set(...)` under the write lock.
5.  Server replies `+OK\r\n`.

---

## 5\. Module Deep Dive

### 5.1 `main.rs`

Creates the shared database, binds the server to `127.0.0.1:6379`, prints a startup message, and runs the accept loop. Uses `#[tokio::main]`.

### 5.2 `server.rs`

`Server { db, listener }`. `run()` accepts sockets forever; for each one it clones the `SharedDb`, spawns a task running `Handler`, and logs (without crashing the server) if a session ends with an error.

### 5.3 `handler.rs`

`Handler { db, stream, buffer }` owns one client connection.

*   `run()` — the read/parse/execute/respond loop described above.
*   `execute_command()` — a big `match` over `Commands` that calls the store and builds a RESP reply.
*   Helpers: `simple_ok`, `handle_incrby` (shared by INCR/DECR/INCRBY/DECRBY), `handle_pop_list`, `handle_zrange` (shared by ZRANGE/ZREVRANGE).
*   `handle_subscribe_stream_only()` — after `SUBSCRIBE`, the connection enters a one-way push loop (see [Pub/Sub](#9-pubsub)).

### 5.4 `command.rs`

Contains three things:

1.  `**Commands**` **enum** — one variant per supported command, plus `Unknown(String)`.
2.  `**RespSerializer**` — builders for RESP replies: `simple_string` (`+`), `error` (`-`), `integer` (`:`), `bulk_string` (`$`), `null_bulk_string` (`$-1`), and `array` (`*`).
3.  **Parser**
    *   `ParseOutcome { Complete(cmd, consumed), Incomplete, Invalid(msg) }`
    *   `validate_resp_frame(src)` — returns `Ok(None)` if more bytes are needed, `Ok(Some(len))` for a complete frame, or `Err(msg)` if malformed. It enforces safety limits:
        *   max **10,000** elements per array
        *   max **16 MiB** per bulk string
        *   overflow-checked length arithmetic
        *   every bulk string must end in `\r\n`
    *   `parse_complete_command(src)` — turns a validated frame into a `Commands` value. Non-array input (a bare line) becomes `Commands::Unknown`.

### 5.5 `store.rs`

```
pub enum Value {
    String(Bytes),
    Hash(HashMap<String, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    SortedSet(BTreeSet<ZSetMember>),
}

pub struct DbEntry { pub value: Value, pub expires_at: Option<Instant> }

pub struct Db {
    pub entries: RwLock<HashMap<String, DbEntry>>,
    pub pubsub:  RwLock<HashMap<String, broadcast::Sender<Bytes>>>,
}

pub struct SharedDb(pub Arc<Db>);
```

*   **Sorted sets** use `BTreeSet<ZSetMember>`, ordered by `(score, value)`, so iteration is naturally sorted.
*   **Strings/values** use `bytes::Bytes` (cheap clones, binary-safe).
*   **Pub/Sub** uses one `tokio::sync::broadcast` channel (capacity 1024) per channel name.

---

## 6\. Supported Commands

| Category | Commands |
| --- | --- |
| Connection | `HELLO`, `PING [message]` |
| Keys | `EXISTS`, `DEL`, `EXPIRE`, `TTL`, `SCAN`, `FLUSHALL` |
| Strings / counters | `SET`, `GET`, `SETNX`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `MSET`, `MGET` |
| Hashes | `HSET` (multi-field), `HGET`, `HGETALL`, `HDEL`, `HKEYS` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN` |
| Sets | `SADD` (multi-member), `SREM`, `SMEMBERS`, `SISMEMBER`, `SINTER` (two keys) |
| Sorted sets | `ZADD`, `ZREM`, `ZRANGE [WITHSCORES]`, `ZREVRANGE [WITHSCORES]` |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE` |

Command names are case-insensitive. Unrecognised commands reply `-Unknown command: <NAME>`.

Error replies currently used by the store:

*   `ERR value is not an integer or out of range`
*   `WRONGTYPE Operation against a key holding the wrong kind of value`

---

## 7\. Concurrency and Atomicity Model

*   Each data operation takes the `entries` lock **once** and holds it for the full operation. This makes each command atomic with respect to other commands.
*   Multi-value writes (`HSET` with many fields, `SADD` with many members) are implemented as `hset_many` / `sadd_many`, which apply **all** items under a **single** write-lock acquisition. Other clients can never observe a half-applied `HSET` or `SADD`.
*   Read-only commands that don't modify state (`hget`, `hgetall`, `lrange`, `smembers`, etc.) use the read lock, allowing parallel readers.
*   Commands that may evict expired keys (`get`, `exists`, `ttl`, `scan`, `expire`) take the write lock.
*   `INCR`/`INCRBY` read-modify-write happens inside one write-lock section, so concurrent increments don't lose updates (verified by the concurrency test).

---

## 8\. Key Expiry (TTL)

Expiry is **lazy**: there is no background sweeper.

*   `EXPIRE key seconds` sets `expires_at = now + seconds`.
*   `GET`, `EXISTS`, `TTL`, `SCAN`, `EXPIRE`, and `SETNX` check `expires_at` and remove the key if it has passed.
*   `TTL` returns the remaining seconds, `-1` if the key has no expiry, or `-2` if the key does not exist or has expired.
*   `SET` replaces the entry and clears any previous expiry.

---

## 9\. Pub/Sub

*   `PUBLISH channel message` sends to the channel's broadcast sender and returns the number of receivers.
*   `SUBSCRIBE channel`:
    1.  Immediately writes the confirmation array `["subscribe", channel, 1]`.
    2.  Enters `handle_subscribe_stream_only`, a `tokio::select!` loop that either:
        *   forwards each broadcast message as `["message", channel, payload]`, or
        *   detects client disconnect (a zero-length read) and exits.
    3.  If a subscriber falls behind and the channel lags, lagged messages are skipped and the loop continues.

The connection is dedicated to receiving messages once subscribed; it does not process further commands.

---

## 10\. Testing with the Python Client

Tests use the official `redis` Python package (RESP2 mode, `protocol=2`) plus raw TCP sockets to verify protocol behaviour that a client library would hide.

### 10.1 Setup

```
pip install redis
```

Start the Rust server first, then run:

```
python test.py
```

### 10.2 `test.py` sections

| # | Section | What it checks |
| --- | --- | --- |
| 1 | Basic commands | `PING`, `SET`/`GET`, `INCRBY` |
| 2 | Lists | `LPUSH`, `RPUSH`, `LLEN`, `LPOP`, `RPOP` |
| 3 | Hashes | Multi-field `HSET`, `HGET`, `HGETALL`, `HDEL` |
| 4 | Sets | Multi-member `SADD`, `SISMEMBER`, `SMEMBERS` |
| 5 | Pub/Sub | Subscribe acknowledgement, `PUBLISH` receiver count, message delivery |
| 6 | Concurrency | 8 threads x 100 `INCR` = no lost updates; concurrent `SADD` of same member inserts exactly once |
| 7 | Atomicity | 1000-field `HSET` / 1000-member `SADD` while 8 reader threads poll; asserts readers only see empty or complete snapshots |
| 8 | TTL | `TTL` = -1 without expiry, `EXPIRE`, expiry after 1s, `TTL` = -2, concurrent reads of an expired key |
| 9 | Protocol robustness | Pipelining, fragmented request, incomplete request waits, malformed array length, server survives disconnects |

Helper functions `encode_command`, `read_line`, and `read_resp` implement a minimal RESP client for the raw-socket tests in section 9.

The script exits with status `1` if any check fails and prints a summary of failures and warnings.

> **Note on the atomicity test:** zero observed partial snapshots is evidence, not proof, of atomicity. The script prints this caveat itself. The real guarantee comes from the single-lock design described in section 7.

### 10.3 `debug_atomicity.py`

A small standalone script that sends one `HSET` with 1000 fields, then reads it back with `HGETALL` and checks that every field matches. Useful for isolating large-frame parsing/write problems without running the whole suite.

```
python debug_atomicity.py
```

### 10.4 Manual testing with `redis-cli`

```
redis-cli -p 6379
> PING
PONG
> SET greeting hello
OK
> GET greeting
"hello"
```

(Use `redis-cli -2` if your CLI defaults to RESP3 and you want to match the RESP2 server.)

---

## 11\. Build and Run

### Prerequisites

*   Rust toolchain (stable)
*   Python 3 with `redis` (for tests)

### Cargo dependencies

The code uses `tokio` (with the `full` feature set, or at least `net`, `io-util`, `sync`, `time`, `macros`, `rt-multi-thread`) and `bytes`:

```
[dependencies]
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

### Run

```
cargo run --release
# Redis Clone active! Local host connection port: 127.0.0.1:6379
```

In a second terminal:

```
python test.py
```

---

## 12\. Known Limitations and Differences from Real Redis

This is a learning/portfolio implementation. Behaviours that differ from production Redis:

**Protocol and commands**

*   Only RESP2. `HELLO` returns a fixed server-info array and does not negotiate RESP3.
*   `SET` has no options (`EX`, `NX`, `XX`, etc.).
*   `EXISTS`, `DEL`, `HDEL`, `LPUSH`, `RPUSH`, `SREM`, `ZADD`, `ZREM` accept a **single** key/field/value, not variadic forms.
*   `SINTER` supports exactly two keys.
*   `SCAN` ignores cursor, `MATCH`, and `COUNT`, and returns a bare key array rather than `[cursor, keys]`.
*   Several numeric arguments are parsed with `unwrap_or(0)`, so invalid numbers become `0` instead of returning an error.
*   `MSET` with an odd number of arguments silently pads with an empty value instead of erroring.
*   Sorted-set scores are `i64` (real Redis uses floating point).
*   `ZADD` returns `1` even when it updates an existing member's score (real Redis returns `0` for updates).

**Expiry**

*   Lazy expiry only; expired keys that are never touched stay in memory.
*   Expiry checks are not applied in every command (for example hash, list, set, and sorted-set operations and `DEL` do not check TTL first).

**Performance**

*   A single global `RwLock` over the whole keyspace serializes all writers.
*   `ZRANGE`/`ZREVRANGE` copy the full sorted set to a `Vec` per call (O(n)).
*   `SMEMBERS`, `HGETALL`, and `SCAN` return unordered results (hash-map iteration order).

**Pub/Sub**

*   `SUBSCRIBE` handles one channel; no `UNSUBSCRIBE`, `PSUBSCRIBE`, or multi-channel subscriptions. The confirmation count is always `1`.
*   Channels are never cleaned up once created.

**Other**

*   No persistence (RDB/AOF), replication, transactions (`MULTI`/`EXEC`), authentication, or TLS.
*   Binds to localhost only, with a fixed port.
*   Wrong-type handling is inconsistent: `INCR` returns `WRONGTYPE`, but most other commands return an empty result or `0` instead.

---

## 13\. Future Work

*   Variadic arguments for `DEL`, `EXISTS`, `LPUSH`, `RPUSH`, etc.
*   Proper argument validation and error replies instead of `unwrap_or(0)`.
*   `SET` options (`EX`, `PX`, `NX`, `XX`) and `PEXPIRE`/`PERSIST`.
*   Background expiry sweeper.
*   Sharded or per-key locking to reduce write contention.
*   Efficient sorted-set ranges (rank-indexed structure) and float scores.
*   `UNSUBSCRIBE`, `PSUBSCRIBE`, multi-channel subscriptions.
*   Configurable bind address/port and graceful shutdown.
*   Persistence (snapshot or append-only log).
*   Rust unit/integration tests alongside the Python suite.
