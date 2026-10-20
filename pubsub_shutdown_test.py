import redis
import time

client = redis.Redis(
    host="127.0.0.1",
    port=6379,
    decode_responses=True,
    protocol=2
)

pubsub = client.pubsub()
pubsub.subscribe("shutdown-test")

print("Subscribed. Keeping connection open...")
print("Now press Ctrl+C in the server terminal.")

try:
    while True:
        message = pubsub.get_message(timeout=1)
        if message:
            print(message)
        time.sleep(0.1)
except KeyboardInterrupt:
    print("Test client stopped.")
finally:
    pubsub.close()
    client.close()
