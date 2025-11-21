import json
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

# Kafka configuration
KAFKA_BROKER = 'kafka:9092' #'localhost:9092'
TOPIC = 'opensky'

# HDFS configuration
HDFS_URL = 'http://namenode:9870'  # Container name of NameNode'http://localhost:50070'
#'http://namenode:50070'  # Use container name "namenode" 'http://localhost:9870'  # NameNode web UI port
HDFS_USER = 'root'
HDFS_RAW_DIR = '/opensky/raw'
HDFS_PROCESSED_DIR = '/opensky/processed'

# Connect to HDFS
client = InsecureClient(HDFS_URL, user=HDFS_USER)

# Ensure directories exist
for dir_path in [HDFS_RAW_DIR, HDFS_PROCESSED_DIR]:
    try:
        client.status(dir_path)
    except Exception:
        client.makedirs(dir_path)

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',  #'earliest',
    group_id='opensky-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("🚀 Consumer started. Listening to Kafka...")

for message in consumer:
    data = message.value
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')

    # --- Store raw data ---
    raw_path = f"{HDFS_RAW_DIR}/opensky_raw_{timestamp}.json"
    with client.write(raw_path, encoding='utf-8') as writer:
        json.dump(data, writer)
    print(f"✅ Raw data saved to HDFS: {raw_path}")

    # --- Processed data (example: only callsign, lat, lon, velocity) ---
    states = data.get('states', [])
    processed = []
    for s in states:
        processed.append({
            'icao24': s[0],
            'callsign': s[1].strip() if s[1] else None,
            'origin_country': s[2],
            'time_position': s[3],
            'last_contact': s[4],
            'longitude': s[5],
            'latitude': s[6],
            'velocity': s[9],
            'heading': s[10],
            'vertical_rate': s[11]
        })
    df = pd.DataFrame(processed)

    # Save processed CSV to HDFS
    processed_path = f"{HDFS_PROCESSED_DIR}/opensky_processed_{timestamp}.csv"
    with client.write(processed_path, encoding='utf-8') as writer:
        df.to_csv(writer, index=False)
    print(f"✅ Processed data saved to HDFS: {processed_path}")


# import os, json, datetime, io
# from kafka import KafkaConsumer
# from hdfs import InsecureClient
# import pandas as pd

# KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
# TOPIC = os.getenv('TOPIC', 'airdata_raw')
# HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
# HDFS_USER = os.getenv('HDFS_USER', 'root')
# RAW_DIR = os.getenv('RAW_DIR', '/opensky/raw')
# PROCESSED_DIR = os.getenv('PROCESSED_DIR', '/opensky/processed')

# # HDFS client (WebHDFS through namenode UI endpoint)
# client = InsecureClient(HDFS_URL, user=HDFS_USER)

# consumer = KafkaConsumer(
#     TOPIC,
#     bootstrap_servers=[KAFKA_BOOTSTRAP],
#     value_deserializer=lambda v: json.loads(v.decode('utf-8')),
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='opensky-group'
# )

# def store_raw(timestamp, raw_json):
#     path = f"{RAW_DIR}/{timestamp}.json"
#     client.write(path, json.dumps(raw_json), overwrite=True)
#     return path

# def process_and_store(timestamp, raw_json):
#     # Simple processing: flatten states into dataframe with key metrics
#     states = raw_json.get('states') or []
#     rows = []
#     # STATES format: [icao24, callsign, origin_country, time_position, last_contact,
#     # longitude, latitude, baro_altitude, on_ground, velocity, heading, vertical_rate, sensors, geo_altitude, squawk, spi, position_source]
#     for s in states:
#         rows.append({
#             'icao24': s[0],
#             'callsign': (s[1] or '').strip(),
#             'origin_country': s[2],
#             'time_position': s[3],
#             'last_contact': s[4],
#             'longitude': s[5],
#             'latitude': s[6],
#             'baro_altitude': s[7],
#             'on_ground': s[8],
#             'velocity': s[9],
#             'heading': s[10],
#             'vertical_rate': s[11],
#             'geo_altitude': s[13]
#         })
#     df = pd.DataFrame(rows)
#     if df.empty:
#         # write minimal processed info
#         processed = {'timestamp': timestamp, 'flight_count': 0}
#         client.write(f"{PROCESSED_DIR}/{timestamp}.json", json.dumps(processed), overwrite=True)
#         return

#     # compute summary metrics
#     flight_count = len(df)
#     avg_speed = float(df['velocity'].dropna().mean()) if not df['velocity'].dropna().empty else None
#     avg_alt = float(df['geo_altitude'].dropna().mean()) if not df['geo_altitude'].dropna().empty else None

#     summary = {
#         'timestamp': timestamp,
#         'flight_count': int(flight_count),
#         'avg_speed': avg_speed,
#         'avg_altitude': avg_alt
#     }
#     # store summary json
#     client.write(f"{PROCESSED_DIR}/{timestamp}.json", json.dumps(summary), overwrite=True)
#     # also store full table as CSV for dashboard deeper insights
#     csv_buffer = io.StringIO()
#     df.to_csv(csv_buffer, index=False)
#     client.write(f"{PROCESSED_DIR}/{timestamp}_table.csv", csv_buffer.getvalue(), overwrite=True)

# if __name__ == "__main__":
#     print("Consumer started, listening to", TOPIC)
#     for msg in consumer:
#         try:
#             raw = msg.value
#             ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
#             print("Message received, storing:", ts)
#             store_raw(ts, raw)
#             process_and_store(ts, raw)
#             print("Stored raw & processed:", ts)
#         except Exception as e:
#             print("Processing error:", e)
