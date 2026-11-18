from kafka import KafkaProducer
import json, time

# Connect to all 3 brokers. json.dumps turns a dict into text, encode turns text into bytes
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092", "localhost:39092", "localhost:49092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

for i in range(10):
    message = {"student_id": i + 1,
               "name": f"Student-{i + 1}",
               "course": "Data Science",
               "message": f"Hello from student {i + 1}"}
    producer.send("student-events", value=message)   # send to the topic
    print("Produced:", message)
    time.sleep(1)

producer.flush()   # make sure everything is really sent
producer.close()
