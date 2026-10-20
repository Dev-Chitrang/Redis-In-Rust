#!/usr/bin/env python3
"""
Redis Clone - end-to-end test suite
===================================

Single-file suite that builds the Rust server, starts it in an isolated
temporary directory, and exercises it through the official ``redis`` client
and raw TCP sockets. The whole suite runs once per execution mode
(``single`` and ``multi``).

Usage
-----
    pip install redis
    python test.py                  # both modes
    python test.py --mode multi     # one mode only

Requirements
------------
* ``cargo`` on PATH, and this file placed next to ``Cargo.toml``.
* The Cargo binary must be named ``redis-clone``.
* Port 6379 must be free (the suite starts and stops its own server).

File layout
-----------
    1. Configuration and shared state
    2. Assertion and client helpers
    3. Raw RESP helpers
    4. Server lifecycle helpers
    5. Integration tests (run against one live server)
         5.1  Strings and counters
         5.2  Keys (EXISTS / DEL)
         5.3  Expiry (TTL / EXPIRE / PEXPIRE / PERSIST)
         5.4  Hashes
         5.5  Lists
         5.6  Sets
         5.7  Sorted sets
         5.8  Pub/Sub
         5.9  Invalid arguments
         5.10 Concurrency
         5.11 Multi-value atomicity
         5.12 Protocol robustness
    6. Lifecycle tests (restart the server)
         6.1  AOF persistence and recovery
         6.2  Graceful shutdown
    7. Test runner
"""

import argparse
import os
import pathlib
import signal
import socket
import subprocess
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import redis


# ==============================================================================
# 1. CONFIGURATION AND SHARED STATE
# ==============================================================================

HOST = "127.0.0.1"
PORT = 6379
MODES = ("single", "multi")

# Set once the Rust server has been compiled.
BUILD_DONE = False

# Shared client and result lists used by the integration tests.
r = None
failures = []
warnings = []


# ==============================================================================
# 2. ASSERTION AND CLIENT HELPERS
# ==============================================================================

def check(name, condition, details=""):
    """Record and print a single pass/fail result."""
    if condition:
        print(f"[PASS] {name}")
    else:
        print(f"[FAIL] {name} {details}")
        failures.append(name)


def section(title):
    print(f"\n=== {title} ===")


def make_client(timeout=5):
    """Create a new RESP2 client with its own connection."""
    return redis.Redis(
        host=HOST,
        port=PORT,
        decode_responses=True,
        protocol=2,
        socket_timeout=timeout,
    )


def wait_for_messages(pubsub, message_type, count=1, timeout=3.0):
    """Collect up to `count` Pub/Sub messages of one type, skipping others."""
    found = []
    deadline = time.monotonic() + timeout

    while len(found) < count and time.monotonic() < deadline:
        message = pubsub.get_message(timeout=0.2)

        if message and message["type"] == message_type:
            found.append(message)

    return found


# ==============================================================================
# 3. RAW RESP HELPERS (used by tests that a client library would hide)
# ==============================================================================

def encode_command(*args):
    """Encode a command as a RESP array of bulk strings."""
    packet = bytearray(f"*{len(args)}\r\n".encode())

    for arg in args:
        data = arg if isinstance(arg, bytes) else str(arg).encode()
        packet.extend(f"${len(data)}\r\n".encode())
        packet.extend(data)
        packet.extend(b"\r\n")

    return bytes(packet)


def read_line(sock_file):
    line = sock_file.readline()

    if not line or not line.endswith(b"\r\n"):
        raise ConnectionError("Incomplete RESP line")

    return line[:-2].decode("utf-8")


def read_resp(sock_file):
    """Read one RESP2 value. Errors are returned as ("ERROR", message)."""
    prefix = sock_file.read(1)

    if not prefix:
        raise ConnectionError("Server disconnected")

    if prefix == b"+":
        return read_line(sock_file)

    if prefix == b"-":
        return ("ERROR", read_line(sock_file))

    if prefix == b":":
        return int(read_line(sock_file))

    if prefix == b"$":
        length = int(read_line(sock_file))

        if length == -1:
            return None

        data = sock_file.read(length)

        if len(data) != length:
            raise ConnectionError("Incomplete bulk string")

        if sock_file.read(2) != b"\r\n":
            raise ValueError("Invalid bulk string terminator")

        return data.decode("utf-8")

    if prefix == b"*":
        count = int(read_line(sock_file))

        if count == -1:
            return None

        return [read_resp(sock_file) for _ in range(count)]

    raise ValueError(f"Unknown RESP prefix: {prefix!r}")


# ==============================================================================
# 4. SERVER LIFECYCLE HELPERS
# ==============================================================================

def start_server(mode, working_dir=None):
    """Build (once) and launch the server, then wait until it answers PING."""
    global BUILD_DONE

    project_dir = pathlib.Path(__file__).resolve().parent

    if not BUILD_DONE:
        subprocess.run(["cargo", "build"], cwd=project_dir, check=True)
        BUILD_DONE = True

    executable_name = "redis-clone.exe" if os.name == "nt" else "redis-clone"
    executable = project_dir / "target" / "debug" / executable_name

    if not executable.exists():
        raise FileNotFoundError(f"Server executable not found: {executable}")

    # The server writes appendonly.aof into its working directory, so each
    # test run gets an isolated directory.
    working_dir = pathlib.Path(working_dir or project_dir).resolve()
    working_dir.mkdir(parents=True, exist_ok=True)

    log_path = working_dir / f"server-{mode}-{time.time_ns()}.log"
    log_handle = open(log_path, "w", encoding="utf-8", buffering=1)

    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0

    process = subprocess.Popen(
        [str(executable), "--host", HOST, "--port", str(PORT), "--mode", mode],
        cwd=working_dir,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        creationflags=creationflags,
    )

    # Keep log information reachable for later assertions.
    process.test_log_path = log_path
    process.test_log_handle = log_handle

    deadline = time.monotonic() + 15

    while time.monotonic() < deadline:
        if process.poll() is not None:
            log_handle.close()
            log_text = log_path.read_text(encoding="utf-8", errors="replace")
            raise RuntimeError(
                f"Server exited unexpectedly in {mode} mode.\n"
                f"Server log:\n{log_text}"
            )

        client = redis.Redis(
            host=HOST,
            port=PORT,
            decode_responses=True,
            protocol=2,
            socket_connect_timeout=1,
            socket_timeout=1,
        )

        try:
            if client.ping():
                print(f"[INFO] Server ready in {mode.upper()} mode")
                return process
        except redis.exceptions.RedisError:
            time.sleep(0.25)
        finally:
            client.close()

    log_text = log_path.read_text(encoding="utf-8", errors="replace")
    stop_server(process)

    raise TimeoutError(
        f"Server did not become ready in {mode} mode.\n"
        f"Server log:\n{log_text}"
    )



def stop_server(process, graceful=False):
    """Stop the server gracefully when requested."""
    if process is None:
        return

    if process.poll() is None:
        try:
            if graceful:
                if os.name == "nt":
                    # Windows: send Ctrl+Break to the process group.
                    process.send_signal(signal.CTRL_C_EVENT)
                else:
                    # Linux/macOS: send SIGINT.
                    process.send_signal(signal.SIGINT)
            else:
                process.terminate()

            process.wait(timeout=10)

        except subprocess.TimeoutExpired:
            print(
                "[ERROR] Graceful shutdown timed out. "
                f"PID={process.pid}, returncode={process.poll()}"
            )
            process.kill()
            process.wait(timeout=5)

        except OSError as exc:
            print(f"[ERROR] Failed to signal server: {exc}")
            process.kill()
            process.wait(timeout=5)

    log_handle = getattr(process, "test_log_handle", None)

    if log_handle and not log_handle.closed:
        log_handle.close()

    print("[INFO] Server stopped.")




# ==============================================================================
# 5. INTEGRATION TESTS
# ------------------------------------------------------------------------------
# Every test below uses the shared client `r` and talks to a live server.
# ==============================================================================

# --- 5.1 Strings and counters -------------------------------------------------

def test_strings():
    section("5.1 STRINGS AND COUNTERS")

    r.delete("test:framework", "test:counter")

    check("PING", r.ping() is True)

    r.set("test:framework", "Tokio")
    check("SET / GET", r.get("test:framework") == "Tokio")

    r.set("test:counter", 10)
    check("INCRBY", r.incrby("test:counter", 5) == 15)

    # DECR and DECRBY.
    r.set("test:decr", 10)

    check("DECR", r.decr("test:decr") == 9)
    check("DECRBY", r.decrby("test:decr", 4) == 5)

    # DECR on a missing key should initialize it to -1.
    r.delete("test:decr:missing")
    check("DECR missing key", r.decr("test:decr:missing") == -1)

    # DECRBY on a missing key.
    r.delete("test:decrby:missing")
    check("DECRBY missing key", r.decrby("test:decrby:missing", 5) == -5)

    # Negative values.
    r.set("test:decr:negative", -10)
    check("DECR negative value", r.decr("test:decr:negative") == -11)

    # SET options: NX, XX, EX, PX.
    r.delete("p1:set")

    check("SET NX on missing key", r.set("p1:set", "first", nx=True) is True)
    check("SET NX on existing key", r.set("p1:set", "second", nx=True) is None)
    check("SET XX on existing key", r.set("p1:set", "updated", xx=True) is True)
    check("SET XX on missing key", r.set("p1:missing", "x", xx=True) is None)
    check("SET EX", r.set("p1:ex", "value", ex=10) is True)
    check("SET PX", r.set("p1:px", "value", px=5000) is True)
    check("SET EX creates TTL", 0 < r.ttl("p1:ex") <= 10)
    check("SET PX creates PTTL", 0 < r.pttl("p1:px") <= 5000)


# --- 5.2 Keys -----------------------------------------------------------------

def test_keys():
    section("5.2 KEYS (EXISTS / DEL)")

    r.delete("p1:a", "p1:b", "p1:c")
    r.set("p1:a", "1")
    r.set("p1:b", "2")

    check("EXISTS multiple keys", r.exists("p1:a", "p1:b", "p1:c") == 2)
    check("DEL multiple keys", r.delete("p1:a", "p1:b", "p1:c") == 2)
    check("DEL nonexistent keys", r.delete("p1:a", "p1:b") == 0)


# --- 5.3 Expiry ---------------------------------------------------------------

def test_expiry():
    section("5.3 EXPIRY (TTL / EXPIRE / PEXPIRE / PERSIST)")

    key = "test:ttl:key"
    r.set(key, "temporary")

    check("TTL without expiration is -1", r.ttl(key) == -1)
    check("EXPIRE succeeds", r.expire(key, 1) is True)
    check("TTL reports remaining time", 0 <= r.ttl(key) <= 1)

    time.sleep(1.2)

    check("Expired key returns None", r.get(key) is None)
    check("Expired key has TTL -2", r.ttl(key) == -2)

    # Several clients reading an expired key at the same time.
    concurrent_key = "test:ttl:concurrent"
    r.set(concurrent_key, "temporary")
    r.expire(concurrent_key, 1)
    time.sleep(1.2)

    def read_expired(_):
        client = make_client()
        try:
            return client.get(concurrent_key)
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(read_expired, range(8)))

    check(
        "Concurrent reads never return expired value",
        all(value is None for value in values),
    )

    # PEXPIRE and PERSIST.
    r.set("p1:expire", "value")

    check("PEXPIRE succeeds", r.pexpire("p1:expire", 5000) is True)
    check("PEXPIRE creates PTTL", 0 < r.pttl("p1:expire") <= 5000)
    check("PERSIST succeeds", r.persist("p1:expire") is True)
    check("PERSIST removes TTL", r.ttl("p1:expire") == -1)
    check("PERSIST without TTL", r.persist("p1:expire") is False)


# --- 5.4 Hashes ---------------------------------------------------------------

def test_hashes():
    section("5.4 HASHES")

    r.delete("test:user")

    added = r.hset("test:user", mapping={"name": "Bob", "role": "Admin"})

    check("HSET multiple fields", added == 2)
    check("HGET name", r.hget("test:user", "name") == "Bob")
    check("HGET role", r.hget("test:user", "role") == "Admin")
    check(
        "HGETALL",
        r.hgetall("test:user") == {"name": "Bob", "role": "Admin"},
    )

    r.hdel("test:user", "role")
    check("HDEL", r.hget("test:user", "role") is None)


# --- 5.5 Lists ----------------------------------------------------------------

def test_lists():
    section("5.5 LISTS")

    r.delete("test:tasks")
    r.lpush("test:tasks", "compile")
    r.rpush("test:tasks", "test")

    check("LLEN", r.llen("test:tasks") == 2)
    check("LPOP", r.lpop("test:tasks") == "compile")
    check("RPOP", r.rpop("test:tasks") == "test")

    # Variadic LPUSH / RPUSH.
    r.delete("p1:list")
    r.lpush("p1:list", "a", "b", "c")
    check(
        "LPUSH multiple values",
        r.lrange("p1:list", 0, -1) == ["c", "b", "a"],
    )

    r.delete("p1:list")
    r.rpush("p1:list", "a", "b", "c")
    check(
        "RPUSH multiple values",
        r.lrange("p1:list", 0, -1) == ["a", "b", "c"],
    )


# --- 5.6 Sets -----------------------------------------------------------------

def test_sets():
    section("5.6 SETS")

    r.delete("test:skills")
    added = r.sadd("test:skills", "Rust", "Python", "Go")

    check("SADD multiple members", added == 3)
    check("SISMEMBER", r.sismember("test:skills", "Python"))
    check(
        "SMEMBERS",
        r.smembers("test:skills") == {"Rust", "Python", "Go"},
    )


# --- 5.7 Sorted sets ----------------------------------------------------------

def test_sorted_sets():
    section("5.7 SORTED SETS")

    key = "phase5:leaderboard"
    r.delete(key)

    # Floating-point scores and initial insertion.
    added = r.zadd(
        key,
        {"alice": 10.5, "bob": 20.75, "charlie": 10.5, "david": 5.25},
    )
    check("ZADD inserts all members", added == 4)

    # Ascending by score, then lexicographically for equal scores.
    check(
        "Score ordering",
        r.zrange(key, 0, -1) == ["david", "alice", "charlie", "bob"],
    )
    check(
        "Floating-point scores",
        r.zrange(key, 0, -1, withscores=True)
        == [("david", 5.25), ("alice", 10.5), ("charlie", 10.5), ("bob", 20.75)],
    )
    check(
        "Reverse score ordering",
        r.zrevrange(key, 0, -1) == ["bob", "charlie", "alice", "david"],
    )

    # ZRANK: zero-based rank by ascending score.
    check("ZRANK first member", r.zrank(key, "david") == 0)
    check("ZRANK second member", r.zrank(key, "alice") == 1)
    check("ZRANK third member", r.zrank(key, "charlie") == 2)
    check("ZRANK last member", r.zrank(key, "bob") == 3)

    # A member that doesn't exist should return None.
    check("ZRANK missing member", r.zrank(key, "unknown") is None)

    # Negative indexes and rank boundaries.
    check("Negative rank boundaries", r.zrange(key, -2, -1) == ["charlie", "bob"])
    check("Out-of-range ranks", r.zrange(key, 10, 20) == [])
    check("Start rank greater than end rank", r.zrange(key, 3, 1) == [])
    check(
        "Negative start below range",
        r.zrange(key, -20, 1) == ["david", "alice"],
    )

    # Updating a score must not create a duplicate member.
    updated = r.zadd(key, {"alice": 30.125})
    check(
        "Score update does not increase member count",
        updated == 0 and len(r.zrange(key, 0, -1)) == 4,
    )
    check(
        "Updated member moves to correct position",
        r.zrange(key, -1, -1, withscores=True) == [("alice", 30.125)],
    )

    # Existing and new member in one call.
    added = r.zadd(key, {"alice": 31.5, "eve": 15.125})
    check(
        "Mixed update and insertion count",
        added == 1 and len(r.zrange(key, 0, -1)) == 5,
    )
    check(
        "Updated score changes ordering",
        r.zrange(key, 0, -1) == ["david", "charlie", "eve", "bob", "alice"],
    )

    # Removal.
    removed = r.zrem(key, "charlie")
    check(
        "ZREM removes member",
        removed == 1 and "charlie" not in r.zrange(key, 0, -1),
    )
    check("ZREM missing member", r.zrem(key, "missing-member") == 0)

    # Repeated updates of the same member.
    r.zadd(key, {"bob": 40.5})
    r.zadd(key, {"bob": 41.5})
    check(
        "Repeated score updates",
        len(r.zrange(key, 0, -1)) == 4
        and ("bob", 41.5) in r.zrange(key, 0, -1, withscores=True),
    )

    # Deterministic ordering for equal scores.
    r.delete(key)
    r.zadd(key, {"zebra": 1.5, "apple": 1.5, "mango": 1.5})
    check(
        "Equal-score ordering",
        r.zrange(key, 0, -1) == ["apple", "mango", "zebra"],
    )
    check(
        "Equal-score reverse ordering",
        r.zrevrange(key, 0, -1) == ["zebra", "mango", "apple"],
    )

    r.delete(key)


def test_sorted_set_high_rank():
    section("5.7b SORTED SETS: HIGH-RANK LOOKUP")

    key = "phase5:large-rank"
    r.delete(key)

    total = 4000
    batch_size = 500

    for start in range(0, total, batch_size):
        members = {
            f"member:{i:05d}": float(i)
            for i in range(start, min(start + batch_size, total))
        }
        r.zadd(key, members)

    last = (f"member:{total - 1:05d}", float(total - 1))

    check("Large sorted-set insertion", len(r.zrange(key, 0, -1)) == total)
    check(
        "High-rank lookup",
        r.zrange(key, total - 1, total - 1, withscores=True) == [last],
    )
    check(
        "High-rank reverse lookup",
        r.zrevrange(key, 0, 0, withscores=True) == [last],
    )
    check(
        "ZRANK high-rank member",
        r.zrank(key, f"member:{total - 1:05d}") == total - 1,
    )

    check(
        "ZRANK first member in large sorted set",
        r.zrank(key, "member:00000") == 0,
    )

    r.delete(key)


# --- 5.8 Pub/Sub --------------------------------------------------------------

def test_pubsub_basic():
    section("5.8 PUB/SUB: BASIC")

    pubsub = r.pubsub()
    pubsub.subscribe("test:alerts")

    try:
        acknowledgement = pubsub.get_message(timeout=2)
        check(
            "Subscription acknowledged",
            acknowledgement is not None
            and acknowledgement.get("type") == "subscribe",
        )

        receivers = r.publish("test:alerts", "System online!")
        message = pubsub.get_message(timeout=2)

        check("PUBLISH receiver count", receivers >= 1)
        check(
            "Subscriber receives message",
            message is not None
            and message.get("type") == "message"
            and message.get("data") == "System online!",
        )
    finally:
        pubsub.close()


def test_pubsub_advanced():
    section("5.8b PUB/SUB: MULTI-CHANNEL, PATTERNS, CLEANUP")

    client = make_client()
    pubsub = client.pubsub()

    try:
        # Multiple channel subscriptions.
        pubsub.subscribe("phase3:channel:one", "phase3:channel:two")
        acks = wait_for_messages(pubsub, "subscribe", count=2)

        check(
            "SUBSCRIBE supports multiple channels",
            len(acks) == 2
            and {m["channel"] for m in acks}
            == {"phase3:channel:one", "phase3:channel:two"},
        )
        check(
            "SUBSCRIBE acknowledgement counts",
            sorted(m["data"] for m in acks) == [1, 2],
        )

        # Direct channel delivery.
        receivers = client.publish("phase3:channel:one", "direct-message")
        found = wait_for_messages(pubsub, "message")
        message = found[0] if found else None

        check(
            "Direct channel delivery",
            receivers == 1
            and message is not None
            and message["channel"] == "phase3:channel:one"
            and message["data"] == "direct-message",
        )

        # Unsubscribe from one channel.
        pubsub.unsubscribe("phase3:channel:one")
        found = wait_for_messages(pubsub, "unsubscribe")
        acknowledgement = found[0] if found else None

        check(
            "UNSUBSCRIBE removes one channel",
            acknowledgement is not None
            and acknowledgement["channel"] == "phase3:channel:one"
            and acknowledgement["data"] == 1,
        )
        check(
            "Unsubscribed channel no longer receives messages",
            client.publish("phase3:channel:one", "should-not-be-delivered") == 0,
        )

        # Multiple pattern subscriptions.
        pubsub.psubscribe("phase3:orders:*", "phase3:users:?")
        pattern_acks = wait_for_messages(pubsub, "psubscribe", count=2)

        check(
            "PSUBSCRIBE supports multiple patterns",
            len(pattern_acks) == 2
            and {m["channel"] for m in pattern_acks}
            == {"phase3:orders:*", "phase3:users:?"},
        )
        check(
            "Pattern subscription acknowledgement counts",
            sorted(m["data"] for m in pattern_acks) == [2, 3],
        )

        # Wildcard (*) delivery.
        receivers = client.publish("phase3:orders:123", "order-created")
        found = wait_for_messages(pubsub, "pmessage")
        message = found[0] if found else None

        check(
            "PSUBSCRIBE wildcard delivery",
            receivers == 1
            and message is not None
            and message["pattern"] == "phase3:orders:*"
            and message["channel"] == "phase3:orders:123"
            and message["data"] == "order-created",
        )

        # Single-character (?) matching.
        receivers = client.publish("phase3:users:A", "user-created")
        found = wait_for_messages(pubsub, "pmessage")
        message = found[0] if found else None

        check(
            "Pattern question-mark matching",
            receivers == 1
            and message is not None
            and message["pattern"] == "phase3:users:?"
            and message["channel"] == "phase3:users:A",
        )

        # Unsubscribe from all patterns.
        pubsub.punsubscribe()
        pattern_unsub_acks = wait_for_messages(pubsub, "punsubscribe", count=2)

        check(
            "PUNSUBSCRIBE removes all patterns",
            len(pattern_unsub_acks) == 2
            and sorted(m["data"] for m in pattern_unsub_acks) == [1, 2],
        )

        # Remove the remaining direct subscription.
        pubsub.unsubscribe()
        found = wait_for_messages(pubsub, "unsubscribe")
        final_ack = found[-1] if found else None

        check(
            "UNSUBSCRIBE removes all remaining channels",
            final_ack is not None and final_ack["data"] == 0,
        )

    finally:
        pubsub.close()
        client.close()

    # A subscriber that disconnects must be cleaned up.
    disconnect_client = make_client()
    disconnect_pubsub = disconnect_client.pubsub()

    try:
        disconnect_pubsub.subscribe("phase3:disconnect")
        acknowledgement = disconnect_pubsub.get_message(timeout=3)

        check(
            "Disconnect cleanup setup",
            acknowledgement is not None and acknowledgement["type"] == "subscribe",
        )

        disconnect_pubsub.close()

        deadline = time.monotonic() + 3
        receivers = 1

        while time.monotonic() < deadline:
            receivers = disconnect_client.publish("phase3:disconnect", "cleanup-check")

            if receivers == 0:
                break

            time.sleep(0.05)

        check("Disconnected subscriber is cleaned up", receivers == 0)

    finally:
        disconnect_pubsub.close()
        disconnect_client.close()


# --- 5.9 Invalid arguments ----------------------------------------------------

def test_invalid_arguments():
    section("5.9 INVALID ARGUMENTS")

    with socket.create_connection((HOST, PORT), timeout=3) as sock:
        sock.settimeout(3)

        with sock.makefile("rb") as sock_file:
            sock.sendall(
                encode_command("INCRBY", "p1:number", "not-a-number")
                + encode_command("PING")
            )

            invalid_result = read_resp(sock_file)
            ping_result = read_resp(sock_file)

            check(
                "Invalid numeric argument returns error",
                isinstance(invalid_result, tuple) and invalid_result[0] == "ERROR",
            )
            check(
                "Connection survives invalid command arguments",
                ping_result == "PONG",
            )


# --- 5.10 Concurrency ---------------------------------------------------------

def test_concurrency():
    section("5.10 CONCURRENCY")

    workers = 8
    increments_per_worker = 100
    expected = workers * increments_per_worker

    # Many clients incrementing one key: no lost updates.
    key = "test:concurrent:counter"
    r.set(key, 0)

    def increment_many(_):
        client = make_client(timeout=10)
        try:
            for _ in range(increments_per_worker):
                client.incr(key)
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(increment_many, range(workers)))

    actual = int(r.get(key))
    check(
        "Concurrent INCR has no lost updates",
        actual == expected,
        f"(expected {expected}, got {actual})",
    )

    # Many clients adding the same set member: inserted exactly once.
    set_key = "test:concurrent:set"
    r.delete(set_key)

    def add_same_member(_):
        return r.sadd(set_key, "shared")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(add_same_member, range(workers)))

    check(
        "Concurrent SADD inserts one unique member",
        r.smembers(set_key) == {"shared"} and sum(results) == 1,
    )

    # Many clients on separate keys (exercises keyspace sharding).
    sharded_workers = 16
    operations_per_worker = 100

    def sharded_worker(worker_id):
        client = make_client(timeout=10)
        shard_key = f"test:sharded:{worker_id}"

        try:
            client.set(shard_key, 0)

            for _ in range(operations_per_worker):
                client.incr(shard_key)

            return int(client.get(shard_key))
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=sharded_workers) as pool:
        results = list(pool.map(sharded_worker, range(sharded_workers)))

    check(
        "Concurrent operations on separate keys",
        all(value == operations_per_worker for value in results),
        f"(expected {operations_per_worker} per key, got {results})",
    )


# --- 5.11 Multi-value atomicity -----------------------------------------------

def observe_during_write(key, expected, read_snapshot, write, reader_count):
    """
    Poll `key` from several reader threads while `write()` runs.

    Returns (partial_snapshot_count, total_observations). A snapshot is
    "partial" if it is non-empty and differs from `expected`.
    """
    start = threading.Barrier(reader_count + 1)
    stop = threading.Event()
    partial = []
    errors = []
    observations = [0] * reader_count
    lock = threading.Lock()

    def reader(reader_id):
        client = make_client(timeout=10)

        try:
            start.wait(timeout=5)

            while not stop.is_set():
                snapshot = read_snapshot(client, key)

                with lock:
                    observations[reader_id] += 1

                    # Empty or complete snapshots are valid.
                    if snapshot and snapshot != expected:
                        partial.append(snapshot)
                        stop.set()
                        break

        except Exception as exc:
            with lock:
                message = f"Reader {reader_id} ({key}) {type(exc).__name__}: {exc}"
                errors.append(message)
                print(f"[ATOMICITY DEBUG] {message}")

        finally:
            client.close()

    threads = [
        threading.Thread(target=reader, args=(i,)) for i in range(reader_count)
    ]

    for thread in threads:
        thread.start()

    try:
        # Release all readers together, then write while they poll.
        start.wait(timeout=5)
        write()

    except Exception as exc:
        message = f"Writer ({key}) {type(exc).__name__}: {exc}"

        with lock:
            errors.append(message)

        print(f"[ATOMICITY DEBUG] {message}")

    finally:
        stop.set()

    for thread in threads:
        thread.join(timeout=15)

    alive = [i for i, thread in enumerate(threads) if thread.is_alive()]

    if alive:
        errors.append(f"Reader threads timed out: {alive}")

    if errors:
        raise RuntimeError("; ".join(errors))

    return len(partial), sum(observations)


def test_atomicity():
    section("5.11 MULTI-VALUE ATOMICITY")

    item_count = 1000
    rounds = 10
    reader_count = 8

    partial_hashes = 0
    partial_sets = 0

    # HSET with many fields.
    for round_no in range(rounds):
        key = f"test:atomic:hash:{round_no}"
        fields = {f"field{i}": f"value{i}" for i in range(item_count)}

        r.delete(key)

        partial, observations = observe_during_write(
            key,
            fields,
            lambda client, k: client.hgetall(k),
            lambda: r.hset(key, mapping=fields),
            reader_count,
        )

        partial_hashes += partial

        check(
            f"HSET round {round_no + 1}: no partial snapshots",
            partial == 0,
            f"(partial snapshots: {partial})",
        )
        check(f"HSET round {round_no + 1}: final state", r.hgetall(key) == fields)
        print(f"  Reader observations: {observations}")

    # SADD with many members.
    for round_no in range(rounds):
        key = f"test:atomic:set:{round_no}"
        members = {f"member{i}" for i in range(item_count)}

        r.delete(key)

        partial, observations = observe_during_write(
            key,
            members,
            lambda client, k: client.smembers(k),
            lambda: r.sadd(key, *members),
            reader_count,
        )

        partial_sets += partial

        check(
            f"SADD round {round_no + 1}: no partial snapshots",
            partial == 0,
            f"(partial snapshots: {partial})",
        )
        check(f"SADD round {round_no + 1}: final state", r.smembers(key) == members)
        print(f"  Reader observations: {observations}")

    print(f"Partial HSET snapshots observed: {partial_hashes}")
    print(f"Partial SADD snapshots observed: {partial_sets}")
    print(
        "[INFO] Stress test completed. "
        "Zero partial snapshots do not prove atomicity."
    )


# --- 5.12 Protocol robustness -------------------------------------------------

def test_protocol():
    section("5.12 PROTOCOL ROBUSTNESS")

    # Pipelining: several commands in one TCP write.
    try:
        with socket.create_connection((HOST, PORT), timeout=3) as sock:
            sock.settimeout(3)

            with sock.makefile("rb") as sock_file:
                sock.sendall(
                    encode_command("PING")
                    + encode_command("SET", "test:pipeline", "hello")
                    + encode_command("GET", "test:pipeline")
                )

                responses = [read_resp(sock_file) for _ in range(3)]

                check(
                    "Pipelined commands",
                    responses == ["PONG", "OK", "hello"],
                    f"(responses: {responses})",
                )
    except Exception as exc:
        check("Pipelined commands", False, str(exc))

    # Fragmentation: one valid request split across TCP writes.
    try:
        with socket.create_connection((HOST, PORT), timeout=3) as sock:
            sock.settimeout(3)

            with sock.makefile("rb") as sock_file:
                sock.sendall(b"*1\r\n$4\r\nPI")
                time.sleep(0.1)
                sock.sendall(b"NG\r\n")

                check("Fragmented request", read_resp(sock_file) == "PONG")
    except Exception as exc:
        check("Fragmented request", False, str(exc))

    # Incomplete request: the server must wait for the rest.
    try:
        with socket.create_connection((HOST, PORT), timeout=3) as sock:
            sock.settimeout(0.3)
            sock.sendall(b"*1\r\n$4\r\nPI")

            try:
                data = sock.recv(1024)
                check(
                    "Incomplete request waits for remaining bytes",
                    len(data) == 0,
                    f"(unexpected response: {data!r})",
                )
            except socket.timeout:
                check("Incomplete request gets no premature reply", True)

    except Exception as exc:
        check("Incomplete request handling", False, str(exc))

    # Malformed array length: informational (server should reply and close).
    try:
        with socket.create_connection((HOST, PORT), timeout=3) as sock:
            sock.settimeout(0.3)
            sock.sendall(b"*not-a-number\r\n")

            try:
                data = sock.recv(1024)

                if data:
                    print(f"[INFO] Malformed request response: {data!r}")
                else:
                    print("[INFO] Server closed malformed-request connection")
            except socket.timeout:
                warnings.append(
                    "Malformed RESP received no error response before timeout"
                )
                print(
                    "[WARNING] Malformed RESP received no error response. "
                    "Parser may leave invalid input buffered."
                )

    except Exception as exc:
        print(f"[INFO] Malformed request connection result: {exc}")

    # Closing an idle client must not bring the server down.
    try:
        with socket.create_connection((HOST, PORT), timeout=3):
            pass

        check("Server survives client disconnect", r.ping() is True)
    except Exception as exc:
        check("Server survives client disconnect", False, str(exc))


# ==============================================================================
# 6. LIFECYCLE TESTS (these start and stop the server themselves)
# ==============================================================================

# --- 6.1 AOF persistence and recovery ----------------------------------------

def test_persistence(mode, working_dir):
    print("\n" + "=" * 60)
    print(f"6.1 PERSISTENCE TESTS: {mode.upper()} MODE")
    print("=" * 60)

    failures_before = len(failures)

    aof_path = pathlib.Path(working_dir) / "appendonly.aof"
    process = None

    # Dedicated keys keep recovery checks deterministic.
    string_key = f"e2e:{mode}:string"
    counter_key = f"e2e:{mode}:counter"
    hash_key = f"e2e:{mode}:hash"
    list_key = f"e2e:{mode}:list"
    set_key = f"e2e:{mode}:set"
    zset_key = f"e2e:{mode}:zset"

    expected_hash = {"role": "engineer", "language": "Rust"}
    expected_list = ["build", "test"]
    expected_set = {"Rust", "Go"}
    expected_zset = [("alice", 10.0), ("bob", 20.0)]

    def verify_recovered_data(client, label):
        check(f"{label}: string recovery", client.get(string_key) == "survives-restart")
        check(f"{label}: counter recovery", client.get(counter_key) == "40")
        check(f"{label}: hash recovery", client.hgetall(hash_key) == expected_hash)
        check(
            f"{label}: list recovery",
            client.lrange(list_key, 0, -1) == expected_list,
        )
        check(f"{label}: set recovery", client.smembers(set_key) == expected_set)
        check(
            f"{label}: sorted-set recovery",
            client.zrange(zset_key, 0, -1, withscores=True) == expected_zset,
        )

    try:
        # Step 1: write known data.
        process = start_server(mode, working_dir)
        client = make_client()

        try:
            client.set(string_key, "survives-restart")
            client.set(counter_key, 40)
            client.incr(counter_key)
            client.decr(counter_key)
            client.hset(hash_key, mapping=expected_hash)
            client.rpush(list_key, *expected_list)
            client.sadd(set_key, *expected_set)
            client.zadd(zset_key, dict(expected_zset))

            check(
                "AOF created after writes",
                aof_path.exists() and aof_path.stat().st_size > 0,
            )
        finally:
            client.close()

        # Step 2: restart and verify recovery.
        stop_server(process)
        process = start_server(mode, working_dir)
        client = make_client()

        try:
            verify_recovered_data(client, "Restart")
        finally:
            client.close()

        stop_server(process)
        process = None

        # Save the valid AOF before the corruption tests.
        valid_aof = aof_path.read_bytes()

        # Step 3: incomplete final record is discarded and truncated.
        print("\n[INFO] Testing incomplete final AOF record...")

        with aof_path.open("ab") as file:
            file.write(b"*3\r\n$3\r\nSET\r\n$15\r\npartial_record")

        process = start_server(mode, working_dir)
        client = make_client()

        try:
            check(
                "Data survives incomplete final record",
                client.get(string_key) == "survives-restart",
            )
        finally:
            client.close()

        partial_log_path = process.test_log_path
        stop_server(process)
        process = None

        partial_log = partial_log_path.read_text(encoding="utf-8", errors="replace")

        check(
            "Incomplete record detected",
            "[AOF] Discarded incomplete final record" in partial_log,
        )
        check("Incomplete record truncated", aof_path.read_bytes() == valid_aof)

        # Step 4: malformed record must block startup.
        print("\n[INFO] Testing malformed AOF record...")

        with aof_path.open("ab") as file:
            file.write(b"*not-a-number\r\n")

        corruption_rejected = False

        try:
            process = start_server(mode, working_dir)

        except (RuntimeError, TimeoutError) as exc:
            error_text = str(exc)
            print(f"[INFO] Malformed AOF startup result:\n{error_text}")

            corruption_rejected = (
                "Invalid AOF record" in error_text or "InvalidData" in error_text
            )

        finally:
            if process is not None:
                stop_server(process)
                process = None

        check("Malformed AOF prevents startup", corruption_rejected)

        # Step 5: restore the valid AOF and recover again.
        aof_path.write_bytes(valid_aof)

        process = start_server(mode, working_dir)
        client = make_client()

        try:
            verify_recovered_data(client, "Recovery after AOF restoration")
        finally:
            client.close()

        stop_server(process)
        process = None

    finally:
        if process is not None:
            stop_server(process)

    passed = len(failures) == failures_before
    print(f"\nPERSISTENCE TESTS: {'PASSED' if passed else 'FAILED'}")
    return passed


# ==============================================================================
# 7. TEST RUNNER
# ==============================================================================

# Ordered list of integration tests. Edit here to add or reorder tests.
INTEGRATION_TESTS = [
    test_strings,
    test_keys,
    test_expiry,
    test_hashes,
    test_lists,
    test_sets,
    test_sorted_sets,
    test_sorted_set_high_rank,
    test_pubsub_basic,
    test_pubsub_advanced,
    test_invalid_arguments,
    test_concurrency,
    test_atomicity,
    test_protocol,
]


def run_integration_tests(mode):
    """Run every integration test against the already-running server."""
    global r, failures, warnings

    failures = []
    warnings = []
    r = make_client()

    print("\n" + "=" * 60)
    print(f"5. INTEGRATION TESTS: {mode.upper()} MODE")
    print("=" * 60)

    try:
        for test in INTEGRATION_TESTS:
            test()

    except Exception as exc:
        failures.append(f"Unexpected test exception: {exc}")
        print(f"\n[ERROR] Test suite stopped: {exc}")
        traceback.print_exc()

    finally:
        r.close()

    print(f"\n=== {mode.upper()} MODE INTEGRATION SUMMARY ===")
    print(f"Failures: {len(failures)}")
    print(f"Warnings: {len(warnings)}")

    for failure in failures:
        print(f"FAIL: {failure}")

    for warning in warnings:
        print(f"WARN: {warning}")

    passed = not failures
    print(f"\n{mode.upper()} MODE INTEGRATION: {'PASSED' if passed else 'FAILED'}")
    return passed


def run_mode(mode):
    """Run the full suite (integration + lifecycle) for one execution mode."""
    print("\n" + "#" * 60)
    print(f"STARTING COMPLETE E2E SUITE: {mode.upper()} MODE")
    print("#" * 60)

    mode_results = {}

    # Isolate the AOF and logs from the real project directory.
    with tempfile.TemporaryDirectory(prefix=f"rust-redis-{mode}-") as temp_dir:
        process = None

        try:
            # 5. Integration tests against one live server.
            process = start_server(mode, temp_dir)
            mode_results["integration"] = run_integration_tests(mode)
            stop_server(process)
            process = None

            # 6.1 Persistence and recovery.
            mode_results["persistence"] = test_persistence(mode, temp_dir)

        except Exception as exc:
            mode_results["unexpected_error"] = False
            print(f"\n[ERROR] {mode.upper()} MODE: {type(exc).__name__}: {exc}")
            traceback.print_exc()

        finally:
            if process is not None:
                stop_server(process)

    print(f"\n{mode.upper()} MODE RESULTS:")

    for test_name, passed in mode_results.items():
        print(f"  {test_name}: {'PASSED' if passed else 'FAILED'}")

    return bool(mode_results) and all(mode_results.values())


def main():
    parser = argparse.ArgumentParser(description="Redis Clone end-to-end tests")
    parser.add_argument(
        "--mode",
        choices=("single", "multi", "both"),
        default="both",
        help="execution mode(s) to test (default: both)",
    )
    args = parser.parse_args()

    modes = MODES if args.mode == "both" else (args.mode,)
    results = {mode: run_mode(mode) for mode in modes}

    print("\n" + "=" * 60)
    print("COMPLETE REDIS CLONE E2E RESULTS")
    print("=" * 60)

    for mode, passed in results.items():
        print(f"{mode.upper()} MODE: {'PASSED' if passed else 'FAILED'}")

    if not all(results.values()):
        print("\nE2E SUITE FAILED.")
        raise SystemExit(1)

    print("\nALL E2E TESTS PASSED.")


if __name__ == "__main__":
    main()
