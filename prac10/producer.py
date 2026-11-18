import json, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

cities = ["Mumbai", "Pune", "Nashik", "Nagpur"]
print("Air Quality Producer Started", flush=True)

while True:
    reading = {
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "city": random.choice(cities),
        "pm25": round(random.uniform(10, 180), 2),
        "pm10": round(random.uniform(20, 250), 2),
        "temperature": round(random.uniform(24, 38), 2),
    }
    producer.send("air_quality", value=reading)   # send to Kafka
    print("Sent:", reading, flush=True)
    time.sleep(2)                                 # one reading every 2 seconds
