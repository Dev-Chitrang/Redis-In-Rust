import redis
import socket
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import traceback

HOST = "127.0.0.1"
PORT = 6379

r = redis.Redis(
    host=HOST,
    port=PORT,
    decode_responses=True,
    protocol=2,
    socket_timeout=5,
)

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


# --------------------------------------------------
# Main
# --------------------------------------------------

def run_tests():
    try:
        test_functional()
        test_concurrency()
        test_atomicity()
        test_ttl()
        test_protocol()

    except Exception as exc:
        failures.append(f"Unexpected test exception: {exc}")
        print(f"\n[ERROR] Test suite stopped: {exc}")
        traceback.print_exc()

    finally:
        r.close()

    print("\n=== TEST SUMMARY ===")
    print(f"Failures: {len(failures)}")
    print(f"Warnings: {len(warnings)}")

    for failure in failures:
        print(f"FAIL: {failure}")

    for warning in warnings:
        print(f"WARN: {warning}")

    if failures:
        raise SystemExit(1)

    print("\nCompleted all test sections.")


if __name__ == "__main__":
    run_tests()
