import json
import time
import requests
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'opensky'

# OpenSky API URL
OPENSKY_URL = "https://opensky-network.org/api/states/all"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Producer started. Sending data to Kafka...")

while True:
    try:
        # Fetch data from OpenSky
        response = requests.get(OPENSKY_URL)
        data = response.json()  # dictionary with 'time' and 'states'

        # Send to Kafka topic
        producer.send(TOPIC, data)
        producer.flush()  # ensure message is sent
        print(f"✅ Sent data with {len(data.get('states', []))} flights")

    except Exception as e:
        print("❌ Error fetching/sending data:", e)

    time.sleep(10)  # fetch every 10 seconds



# import os, time, json, requests
# from kafka import KafkaProducer

# KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
# TOPIC = os.getenv('TOPIC', 'airdata_raw')
# INTERVAL = int(os.getenv('FETCH_INTERVAL', '25'))

# producer = KafkaProducer(
#     bootstrap_servers=[KAFKA_BOOTSTRAP],
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     retries=5
# )

# URL = "https://opensky-network.org/api/states/all"

# def fetch_opensky():
#     try:
#         resp = requests.get(URL, timeout=10)
#         resp.raise_for_status()
#         return resp.json()
#     except Exception as e:
#         print("Fetch error:", e)
#         return None

# if __name__ == "__main__":
#     print("Producer started, sending to", KAFKA_BOOTSTRAP, "topic:", TOPIC)
#     while True:
#         data = fetch_opensky()
#         if data:
#             try:
#                 producer.send(TOPIC, data)
#                 producer.flush()
#                 print("Sent batch -", len(data.get('states', [])) if isinstance(data, dict) else "unknown")
#             except Exception as e:
#                 print("Kafka send error:", e)
#         time.sleep(INTERVAL)
