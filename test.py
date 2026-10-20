import redis
import socket
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import traceback
import subprocess
import pathlib

HOST = "127.0.0.1"
PORT = 6379

r = None

failures = []
warnings = []


def check(name, condition, details=""):
    if condition:
        print(f"[PASS] {name}")
    else:
        print(f"[FAIL] {name} {details}")
        failures.append(name)


# --------------------------------------------------
# RESP helpers for raw TCP protocol tests
# --------------------------------------------------

def encode_command(*args):
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


# --------------------------------------------------
# 1. Original functional tests
# --------------------------------------------------

def test_functional():
    print("\n=== 1. BASIC COMMANDS ===")

    r.delete("test:framework", "test:counter")
    check("PING", r.ping() is True)

    r.set("test:framework", "Tokio")
    check("SET / GET", r.get("test:framework") == "Tokio")

    r.set("test:counter", 10)
    check("INCRBY", r.incrby("test:counter", 5) == 15)

    print("\n=== 2. LISTS ===")

    r.delete("test:tasks")
    r.lpush("test:tasks", "compile")
    r.rpush("test:tasks", "test")

    check("LLEN", r.llen("test:tasks") == 2)
    check("LPOP", r.lpop("test:tasks") == "compile")
    check("RPOP", r.rpop("test:tasks") == "test")

    print("\n=== 3. HASHES ===")

    r.delete("test:user")
    added = r.hset(
        "test:user",
        mapping={"name": "Bob", "role": "Admin"},
    )

    check("HSET multiple fields", added == 2)
    check("HGET name", r.hget("test:user", "name") == "Bob")
    check("HGET role", r.hget("test:user", "role") == "Admin")
    check(
        "HGETALL",
        r.hgetall("test:user") == {"name": "Bob", "role": "Admin"},
    )

    r.hdel("test:user", "role")
    check("HDEL", r.hget("test:user", "role") is None)

    print("\n=== 4. SETS ===")

    r.delete("test:skills")
    added = r.sadd("test:skills", "Rust", "Python", "Go")

    check("SADD multiple members", added == 3)
    check("SISMEMBER", r.sismember("test:skills", "Python"))
    check(
        "SMEMBERS",
        r.smembers("test:skills") == {"Rust", "Python", "Go"},
    )

    print("\n=== 5. PUB/SUB ===")

    pubsub = r.pubsub()
    pubsub.subscribe("test:alerts")

    try:
        # Wait for the subscription acknowledgement.
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


# --------------------------------------------------
# 2. Concurrency
# --------------------------------------------------

def test_concurrency():
    print("\n=== 6. CONCURRENCY ===")

    key = "test:concurrent:counter"
    r.set(key, 0)

    workers = 8
    increments_per_worker = 100
    expected = workers * increments_per_worker

    def increment_many(_):
        # Each worker gets its own client connection.
        client = redis.Redis(
            host=HOST,
            port=PORT,
            decode_responses=True,
            protocol=2,
            socket_timeout=10,
        )

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

    # Concurrent attempts to insert the same set member.
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


# --------------------------------------------------
# 3. Atomicity stress checks
# --------------------------------------------------


def test_atomicity():
    print("\n=== 7. MULTI-VALUE ATOMICITY TEST ===")

    item_count = 1000
    rounds = 10
    reader_count = 8

    partial_hashes = 0
    partial_sets = 0

    def make_client():
        return redis.Redis(
            host=HOST,
            port=PORT,
            decode_responses=True,
            protocol=2,
            socket_timeout=10,
        )

    def observe_during_write(key, expected, read_snapshot, write):
        start = threading.Barrier(reader_count + 1)
        stop = threading.Event()
        partial = []
        errors = []
        observations = [0] * reader_count
        lock = threading.Lock()

        def reader(reader_id):
            client = make_client()

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
                    message = (
                        f"Reader {reader_id} ({key}) "
                        f"{type(exc).__name__}: {exc}"
                    )
                    errors.append(message)
                    print(f"[ATOMICITY DEBUG] {message}")

            finally:
                client.close()

        threads = [
            threading.Thread(target=reader, args=(i,))
            for i in range(reader_count)
        ]

        for thread in threads:
            thread.start()

        try:
            # Release all readers together.
            start.wait(timeout=5)

            # Execute the write while readers are polling.
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

        alive_threads = [
            i for i, thread in enumerate(threads)
            if thread.is_alive()
        ]

        if alive_threads:
            errors.append(
                f"Reader threads timed out: {alive_threads}"
            )

        if errors:
            raise RuntimeError("; ".join(errors))

        return len(partial), sum(observations)

    # HSET atomicity
    for round_no in range(rounds):
        key = f"test:atomic:hash:{round_no}"

        fields = {
            f"field{i}": f"value{i}"
            for i in range(item_count)
        }

        r.delete(key)

        partial, observations = observe_during_write(
            key,
            fields,
            lambda client, k: client.hgetall(k),
            lambda: r.hset(key, mapping=fields),
        )

        partial_hashes += partial

        check(
            f"HSET round {round_no + 1}: no partial snapshots",
            partial == 0,
            f"(partial snapshots: {partial})",
        )

        check(
            f"HSET round {round_no + 1}: final state",
            r.hgetall(key) == fields,
        )

        print(f"  Reader observations: {observations}")

    # SADD atomicity
    for round_no in range(rounds):
        key = f"test:atomic:set:{round_no}"

        members = {
            f"member{i}"
            for i in range(item_count)
        }

        r.delete(key)

        partial, observations = observe_during_write(
            key,
            members,
            lambda client, k: client.smembers(k),
            lambda: r.sadd(key, *members),
        )

        partial_sets += partial

        check(
            f"SADD round {round_no + 1}: no partial snapshots",
            partial == 0,
            f"(partial snapshots: {partial})",
        )

        check(
            f"SADD round {round_no + 1}: final state",
            r.smembers(key) == members,
        )

        print(f"  Reader observations: {observations}")

    print(f"Partial HSET snapshots observed: {partial_hashes}")
    print(f"Partial SADD snapshots observed: {partial_sets}")

    print(
        "[INFO] Stress test completed. "
        "Zero partial snapshots do not prove atomicity."
    )



# --------------------------------------------------
# 4. TTL
# --------------------------------------------------

def test_ttl():
    print("\n=== 8. TTL AND EXPIRATION ===")

    key = "test:ttl:key"
    r.set(key, "temporary")

    check("TTL without expiration is -1", r.ttl(key) == -1)
    check("EXPIRE succeeds", r.expire(key, 1) is True)

    remaining = r.ttl(key)
    check("TTL reports remaining time", 0 <= remaining <= 1)

    time.sleep(1.2)

    check("Expired key returns None", r.get(key) is None)
    check("Expired key has TTL -2", r.ttl(key) == -2)

    # Check expired-key behavior from several clients.
    concurrent_key = "test:ttl:concurrent"
    r.set(concurrent_key, "temporary")
    r.expire(concurrent_key, 1)
    time.sleep(1.2)

    def read_expired(_):
        client = redis.Redis(
            host=HOST,
            port=PORT,
            decode_responses=True,
            protocol=2,
            socket_timeout=5,
        )

        try:
            return client.get(concurrent_key)
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(read_expired, range(8)))

    check("Concurrent reads never return expired value", all(
        value is None for value in values
    ))


# --------------------------------------------------
# 5. RESP protocol and connection robustness
# --------------------------------------------------

def test_protocol():
    print("\n=== 9. PROTOCOL ROBUSTNESS ===")

    # Multiple commands sent together over one connection.
    try:
        with socket.create_connection((HOST, PORT), timeout=3) as sock:
            sock.settimeout(3)

            with sock.makefile("rb") as sock_file:
                packet = (
                    encode_command("PING")
                    + encode_command("SET", "test:pipeline", "hello")
                    + encode_command("GET", "test:pipeline")
                )

                sock.sendall(packet)

                responses = [
                    read_resp(sock_file),
                    read_resp(sock_file),
                    read_resp(sock_file),
                ]

                check(
                    "Pipelined commands",
                    responses == ["PONG", "OK", "hello"],
                    f"(responses: {responses})",
                )
    except Exception as exc:
        check("Pipelined commands", False, str(exc))

    # A valid request split across multiple TCP writes.
    try:
        with socket.create_connection((HOST, PORT), timeout=3) as sock:
            sock.settimeout(3)

            with sock.makefile("rb") as sock_file:
                sock.sendall(b"*1\r\n$4\r\nPI")
                time.sleep(0.1)
                sock.sendall(b"NG\r\n")

                response = read_resp(sock_file)
                check("Fragmented request", response == "PONG")
    except Exception as exc:
        check("Fragmented request", False, str(exc))

    # Incomplete request: the server should wait for the rest.
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
                print("[PASS] Incomplete request did not get a premature reply")

    except Exception as exc:
        check("Incomplete request handling", False, str(exc))

    # Malformed RESP array length.
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

    # Closing an idle client should not bring down the server.
    try:
        with socket.create_connection((HOST, PORT), timeout=3):
            pass

        check("Server survives client disconnect", r.ping() is True)
    except Exception as exc:
        check("Server survives client disconnect", False, str(exc))

def test_phase1():
    print("\n=== PHASE 1: COMMAND SEMANTICS ===")

    # Variadic DEL and EXISTS
    r.delete("p1:a", "p1:b", "p1:c")
    r.set("p1:a", "1")
    r.set("p1:b", "2")

    check("EXISTS multiple keys", r.exists("p1:a", "p1:b", "p1:c") == 2)
    check("DEL multiple keys", r.delete("p1:a", "p1:b", "p1:c") == 2)
    check("DEL nonexistent keys", r.delete("p1:a", "p1:b") == 0)

    # Variadic LPUSH and RPUSH
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

    # SET options
    r.delete("p1:set")

    check("SET NX on missing key", r.set("p1:set", "first", nx=True) is True)
    check("SET NX on existing key", r.set("p1:set", "second", nx=True) is None)
    check("SET XX on existing key", r.set("p1:set", "updated", xx=True) is True)
    check("SET XX on missing key", r.set("p1:missing", "x", xx=True) is None)
    check("SET EX", r.set("p1:ex", "value", ex=10) is True)
    check("SET PX", r.set("p1:px", "value", px=5000) is True)

    check("SET EX creates TTL", 0 < r.ttl("p1:ex") <= 10)
    check("SET PX creates PTTL", 0 < r.pttl("p1:px") <= 5000)

    # PEXPIRE and PERSIST
    r.set("p1:expire", "value")

    check("PEXPIRE succeeds", r.pexpire("p1:expire", 5000) is True)
    check("PEXPIRE creates PTTL", 0 < r.pttl("p1:expire") <= 5000)
    check("PERSIST succeeds", r.persist("p1:expire") is True)
    check("PERSIST removes TTL", r.ttl("p1:expire") == -1)
    check("PERSIST without TTL", r.persist("p1:expire") is False)

    # Invalid arguments should return errors without disconnecting.
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
                isinstance(invalid_result, tuple)
                and invalid_result[0] == "ERROR",
            )
            check(
                "Connection survives invalid command arguments",
                ping_result == "PONG",
            )

def test_sharded_key_operations():
    print("\n=== SHARDED KEY OPERATIONS ===")

    workers = 16
    operations_per_worker = 100

    def worker(worker_id):
        client = redis.Redis(
            host=HOST,
            port=PORT,
            decode_responses=True,
            protocol=2,
            socket_timeout=10,
        )

        key = f"test:sharded:{worker_id}"

        try:
            client.set(key, 0)

            for _ in range(operations_per_worker):
                client.incr(key)

            return int(client.get(key))
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(worker, range(workers)))

    expected = operations_per_worker

    check(
        "Concurrent operations on separate keys",
        all(value == expected for value in results),
        f"(expected {expected} per key, got {results})",
    )

def test_pubsub_phase3():
    print("\n=== PHASE 3: PUB/SUB ===")

    client = redis.Redis(
        host=HOST,
        port=PORT,
        decode_responses=True,
        protocol=2,
        socket_timeout=5,
    )

    pubsub = client.pubsub()

    try:
        # Multiple channel subscriptions.
        pubsub.subscribe(
            "phase3:channel:one",
            "phase3:channel:two",
        )

        acknowledgements = []

        deadline = time.monotonic() + 3

        while len(acknowledgements) < 2 and time.monotonic() < deadline:
            message = pubsub.get_message(timeout=0.2)

            if message and message["type"] == "subscribe":
                acknowledgements.append(message)

        check(
            "SUBSCRIBE supports multiple channels",
            len(acknowledgements) == 2
            and {m["channel"] for m in acknowledgements}
                == {"phase3:channel:one", "phase3:channel:two"},
        )

        check(
            "SUBSCRIBE acknowledgement counts",
            sorted(m["data"] for m in acknowledgements) == [1, 2],
        )

        # Direct-channel message.
        receivers = client.publish(
            "phase3:channel:one",
            "direct-message",
        )

        message = None
        deadline = time.monotonic() + 3

        while time.monotonic() < deadline:
            candidate = pubsub.get_message(timeout=0.2)

            if candidate and candidate["type"] == "message":
                message = candidate
                break

        check(
            "Direct channel delivery",
            receivers == 1
            and message is not None
            and message["channel"] == "phase3:channel:one"
            and message["data"] == "direct-message",
        )

        # Unsubscribe from one channel.
        pubsub.unsubscribe("phase3:channel:one")

        acknowledgement = None
        deadline = time.monotonic() + 3

        while time.monotonic() < deadline:
            candidate = pubsub.get_message(timeout=0.2)

            if candidate and candidate["type"] == "unsubscribe":
                acknowledgement = candidate
                break

        check(
            "UNSUBSCRIBE removes one channel",
            acknowledgement is not None
            and acknowledgement["channel"] == "phase3:channel:one"
            and acknowledgement["data"] == 1,
        )

        check(
            "Unsubscribed channel no longer receives messages",
            client.publish(
                "phase3:channel:one",
                "should-not-be-delivered",
            ) == 0,
        )

        # Multiple pattern subscriptions.
        pubsub.psubscribe(
            "phase3:orders:*",
            "phase3:users:?",
        )

        pattern_acks = []
        deadline = time.monotonic() + 3

        while len(pattern_acks) < 2 and time.monotonic() < deadline:
            candidate = pubsub.get_message(timeout=0.2)

            if candidate and candidate["type"] == "psubscribe":
                pattern_acks.append(candidate)

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

        # Publish a matching pattern message.
        receivers = client.publish(
            "phase3:orders:123",
            "order-created",
        )

        message = None
        deadline = time.monotonic() + 3

        while time.monotonic() < deadline:
            candidate = pubsub.get_message(timeout=0.2)

            if candidate and candidate["type"] == "pmessage":
                message = candidate
                break

        check(
            "PSUBSCRIBE wildcard delivery",
            receivers == 1
            and message is not None
            and message["pattern"] == "phase3:orders:*"
            and message["channel"] == "phase3:orders:123"
            and message["data"] == "order-created",
        )

        # Test question-mark matching.
        receivers = client.publish(
            "phase3:users:A",
            "user-created",
        )

        message = None
        deadline = time.monotonic() + 3

        while time.monotonic() < deadline:
            candidate = pubsub.get_message(timeout=0.2)

            if candidate and candidate["type"] == "pmessage":
                message = candidate
                break

        check(
            "Pattern question-mark matching",
            receivers == 1
            and message is not None
            and message["pattern"] == "phase3:users:?"
            and message["channel"] == "phase3:users:A",
        )

        # Unsubscribe from all patterns.
        pubsub.punsubscribe()

        pattern_unsub_acks = []
        deadline = time.monotonic() + 3

        while (
            len(pattern_unsub_acks) < 2
            and time.monotonic() < deadline
        ):
            candidate = pubsub.get_message(timeout=0.2)

            if candidate and candidate["type"] == "punsubscribe":
                pattern_unsub_acks.append(candidate)

        check(
            "PUNSUBSCRIBE removes all patterns",
            len(pattern_unsub_acks) == 2
            and sorted(m["data"] for m in pattern_unsub_acks) == [1, 2],
        )

        # Remove the remaining direct subscription.
        pubsub.unsubscribe()

        final_ack = None
        deadline = time.monotonic() + 3

        while time.monotonic() < deadline:
            candidate = pubsub.get_message(timeout=0.2)

            if candidate and candidate["type"] == "unsubscribe":
                final_ack = candidate

        check(
            "UNSUBSCRIBE removes all remaining channels",
            final_ack is not None
            and final_ack["data"] == 0,
        )

    finally:
        pubsub.close()
        client.close()

    # Disconnect cleanup.
    disconnect_client = redis.Redis(
        host=HOST,
        port=PORT,
        decode_responses=True,
        protocol=2,
        socket_timeout=5,
    )

    disconnect_pubsub = disconnect_client.pubsub()

    try:
        disconnect_pubsub.subscribe("phase3:disconnect")
        acknowledgement = disconnect_pubsub.get_message(timeout=3)

        check(
            "Disconnect cleanup setup",
            acknowledgement is not None
            and acknowledgement["type"] == "subscribe",
        )

        disconnect_pubsub.close()

        deadline = time.monotonic() + 3
        receivers = 1

        while time.monotonic() < deadline:
            receivers = disconnect_client.publish(
                "phase3:disconnect",
                "cleanup-check",
            )

            if receivers == 0:
                break

            time.sleep(0.05)

        check(
            "Disconnected subscriber is cleaned up",
            receivers == 0,
        )

    finally:
        disconnect_pubsub.close()
        disconnect_client.close()

def test_sorted_sets():
    print("\n=== PHASE 5: SORTED SETS ===")

    key = "phase5:leaderboard"
    r.delete(key)

    # Floating-point scores and initial insertion.
    added = r.zadd(
        key,
        {
            "alice": 10.5,
            "bob": 20.75,
            "charlie": 10.5,
            "david": 5.25,
        },
    )

    check("ZADD inserts all members", added == 4)

    # Ascending score order, with lexicographical ordering
    # when scores are equal.
    check(
        "Score ordering",
        r.zrange(key, 0, -1) == [
            "david",
            "alice",
            "charlie",
            "bob",
        ],
    )

    check(
        "Floating-point scores",
        r.zrange(key, 0, -1, withscores=True) == [
            ("david", 5.25),
            ("alice", 10.5),
            ("charlie", 10.5),
            ("bob", 20.75),
        ],
    )

    # Reverse ordering.
    check(
        "Reverse score ordering",
        r.zrevrange(key, 0, -1) == [
            "bob",
            "charlie",
            "alice",
            "david",
        ],
    )

    # Negative indexes and rank boundaries.
    check(
        "Negative rank boundaries",
        r.zrange(key, -2, -1) == ["charlie", "bob"],
    )

    check(
        "Out-of-range ranks",
        r.zrange(key, 10, 20) == [],
    )

    check(
        "Start rank greater than end rank",
        r.zrange(key, 3, 1) == [],
    )

    check(
        "Negative start below range",
        r.zrange(key, -20, 1) == ["david", "alice"],
    )

    # Updating a score should not create a duplicate member.
    updated = r.zadd(key, {"alice": 30.125})

    check(
        "Score update does not increase member count",
        updated == 0 and len(r.zrange(key, 0, -1)) == 4,
    )

    check(
        "Updated member moves to correct position",
        r.zrange(key, -1, -1, withscores=True)
        == [("alice", 30.125)],
    )

    # Adding an existing member and a new member together.
    added = r.zadd(
        key,
        {
            "alice": 31.5,
            "eve": 15.125,
        },
    )

    check(
        "Mixed update and insertion count",
        added == 1 and len(r.zrange(key, 0, -1)) == 5,
    )

    check(
        "Updated score changes ordering",
        r.zrange(key, 0, -1)
        == ["david", "charlie", "eve", "bob", "alice"],
    )

    # Remove and verify.
    removed = r.zrem(key, "charlie")

    check(
        "ZREM removes member",
        removed == 1 and "charlie" not in r.zrange(key, 0, -1),
    )

    check(
        "ZREM missing member",
        r.zrem(key, "missing-member") == 0,
    )

    # Updating the same member repeatedly must not duplicate it.
    r.zadd(key, {"bob": 40.5})
    r.zadd(key, {"bob": 41.5})

    check(
        "Repeated score updates",
        len(r.zrange(key, 0, -1)) == 4
        and ("bob", 41.5) in r.zrange(key, 0, -1, withscores=True),
    )

    # Equal-score ordering should remain deterministic.
    r.delete(key)
    r.zadd(key, {"zebra": 1.5, "apple": 1.5, "mango": 1.5})

    check(
        "Equal-score ordering",
        r.zrange(key, 0, -1)
        == ["apple", "mango", "zebra"],
    )

    check(
        "Equal-score reverse ordering",
        r.zrevrange(key, 0, -1)
        == ["zebra", "mango", "apple"],
    )

    r.delete(key)

def test_sorted_set_high_rank():
    print("\n=== PHASE 5: HIGH-RANK LOOKUP ===")

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

    check(
        "Large sorted-set insertion",
        len(r.zrange(key, 0, -1)) == total,
    )

    check(
        "High-rank lookup",
        r.zrange(key, total - 1, total - 1, withscores=True)
        == [(f"member:{total - 1:05d}", float(total - 1))],
    )

    check(
        "High-rank reverse lookup",
        r.zrevrange(key, 0, 0, withscores=True)
        == [(f"member:{total - 1:05d}", float(total - 1))],
    )

    r.delete(key)

# --------------------------------------------------
# Main
# --------------------------------------------------

def run_tests(mode):
    global r, failures, warnings

    failures = []
    warnings = []

    r = redis.Redis(
        host=HOST,
        port=PORT,
        decode_responses=True,
        protocol=2,
        socket_timeout=5,
    )

    print("\n" + "=" * 60)
    print(f"RUNNING INTEGRATION TESTS: {mode.upper()} MODE")
    print("=" * 60)

    try:
        test_functional()
        test_sorted_sets()
        test_sorted_sets()
        test_sorted_set_high_rank()
        test_pubsub_phase3()
        test_phase1()
        test_concurrency()
        test_atomicity()
        test_ttl()
        test_protocol()
        test_sharded_key_operations()

    except Exception as exc:
        failures.append(f"Unexpected test exception: {exc}")
        print(f"\n[ERROR] Test suite stopped: {exc}")
        traceback.print_exc()

    finally:
        r.close()

    print(f"\n=== {mode.upper()} MODE TEST SUMMARY ===")
    print(f"Failures: {len(failures)}")
    print(f"Warnings: {len(warnings)}")

    for failure in failures:
        print(f"FAIL: {failure}")

    for warning in warnings:
        print(f"WARN: {warning}")

    if failures:
        print(f"\n{mode.upper()} MODE: FAILED")
        return False

    print(f"\n{mode.upper()} MODE: PASSED")
    return True

def start_server(mode):
    project_dir = pathlib.Path(__file__).resolve().parent

    # Build the Rust server.
    subprocess.run(
        ["cargo", "build"],
        cwd=project_dir,
        check=True,
    )

    # Locate the compiled executable on Windows or Linux.
    executable_name = (
        "redis-clone.exe"
        if __import__("os").name == "nt"
        else "redis-clone"
    )

    executable = project_dir / "target" / "debug" / executable_name

    if not executable.exists():
        raise FileNotFoundError(
            f"Server executable not found: {executable}"
        )

    # Launch the server in the requested execution mode.
    process = subprocess.Popen(
        [
            str(executable),
            "--host", HOST,
            "--port", str(PORT),
            "--mode", mode,
        ],
        cwd=project_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )

    # Wait until the server responds to PING.
    deadline = time.monotonic() + 15

    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"Server exited unexpectedly in {mode} mode. "
                "Check whether the port is already occupied."
            )

        try:
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
                    print(f"[INFO] Server ready in {mode} mode")
                    return process
            finally:
                client.close()

        except redis.exceptions.RedisError:
            time.sleep(0.25)

    process.terminate()
    process.wait(timeout=5)

    raise TimeoutError(
        f"Server did not become ready in {mode} mode."
    )


def stop_server(process):
    if process.poll() is not None:
        return

    process.terminate()

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


if __name__ == "__main__":
    results = {}

    for mode in ("single", "multi"):
        process = None

        try:
            print(f"\nStarting server in {mode.upper()} mode...")

            process = start_server(mode)
            results[mode] = run_tests(mode)

        except Exception as exc:
            results[mode] = False
            print(f"\n[ERROR] {mode.upper()} MODE: {exc}")
            traceback.print_exc()

        finally:
            if process is not None:
                stop_server(process)

                print(
                    f"[INFO] Server stopped after "
                    f"{mode.upper()} mode tests."
                )

    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)

    for mode, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        print(f"{mode.upper()} MODE: {status}")

    if not all(results.values()):
        raise SystemExit(1)

    print("\nAll tests passed in both execution modes!")
