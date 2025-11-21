from kafka import KafkaProducer

try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    print("✔ Kafka connected successfully!")
except Exception as e:
    print("❌ Kafka connection failed:", e)
