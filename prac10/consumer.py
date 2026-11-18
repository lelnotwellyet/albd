import json
from datetime import datetime, timezone
import clickhouse_connect
from kafka import KafkaConsumer

# 1. Connect to ClickHouse and create the table if it is not there
client = clickhouse_connect.get_client(
    host="clickhouse", port=8123, username="default", password="")
client.command("""
    CREATE TABLE IF NOT EXISTS default.air_quality (
        event_time DateTime,
        city String,
        pm25 Float64,
        pm10 Float64,
        temperature Float64
    ) ENGINE = MergeTree ORDER BY (city, event_time)
""")

# 2. Connect to the Kafka topic
consumer = KafkaConsumer(
    "air_quality",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    group_id="air-quality-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")))
print("Kafka Consumer Started", flush=True)

# 3. Every message becomes one row in ClickHouse
for message in consumer:
    r = message.value
    # ClickHouse needs a real datetime, not text
    t = datetime.strptime(r["event_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    client.insert(
        "default.air_quality",
        [[t, r["city"], r["pm25"], r["pm10"], r["temperature"]]],
        column_names=["event_time", "city", "pm25", "pm10", "temperature"])
    print("Inserted:", r, flush=True)
