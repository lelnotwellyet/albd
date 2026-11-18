import json, requests

URL = "http://localhost:8081"
SUBJECT = "patient-data-value"
HEAD = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

def patient(fields):
    """Build an Avro schema and wrap it the way Schema Registry wants it."""
    schema = {"type": "record", "name": "Patient", "fields": fields}
    return {"schema": json.dumps(schema)}

# V1: the first schema
v1 = [{"name": "patient_id", "type": "int"},
      {"name": "patient_name", "type": "string"},
      {"name": "age", "type": "int"},
      {"name": "heart_rate", "type": "int"}]

# V2: one NEW field WITH a default value  -> backward compatible
v2 = v1 + [{"name": "blood_pressure", "type": "string", "default": "unknown"}]

# V3: one NEW field WITHOUT a default value -> NOT backward compatible
v3 = v2 + [{"name": "blood_group", "type": "string"}]

print("1. Register V1")
print(requests.post(f"{URL}/subjects/{SUBJECT}/versions", headers=HEAD, json=patient(v1)).json())

print("2. Set the rule to BACKWARD")
print(requests.put(f"{URL}/config/{SUBJECT}", headers=HEAD, json={"compatibility": "BACKWARD"}).json())

print("3. Is V2 compatible?")
print(requests.post(f"{URL}/compatibility/subjects/{SUBJECT}/versions/latest", headers=HEAD, json=patient(v2)).json())

print("4. Register V2")
print(requests.post(f"{URL}/subjects/{SUBJECT}/versions", headers=HEAD, json=patient(v2)).json())

print("5. Is V3 compatible?")
print(requests.post(f"{URL}/compatibility/subjects/{SUBJECT}/versions/latest", headers=HEAD, json=patient(v3)).json())

print("6. Try to register V3 (must be rejected)")
r = requests.post(f"{URL}/subjects/{SUBJECT}/versions", headers=HEAD, json=patient(v3))
print(r.status_code, r.json().get("message", "")[:80])

print("7. Versions stored in the registry")
print(requests.get(f"{URL}/subjects/{SUBJECT}/versions").json())
