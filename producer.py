from kafka import KafkaProducer
import json
import time
import uuid
import random

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producing 1000 fraud transactions...")

for i in range(1000):
    transaction = {
        "id": str(uuid.uuid4()),
        "amount": random.randint(10001, 20000),
        "location": f"City_{random.randint(1, 100)}",
        "timestamp": time.time()
    }
    producer.send("transactions", value=transaction)

    if (i + 1) % 100 == 0:
        print(f"Sent {i + 1}/1000")

producer.flush()
print("Done. 1000 fraud transactions sent.")