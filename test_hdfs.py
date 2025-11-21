from hdfs import InsecureClient

try:
    client = InsecureClient('http://localhost:50070', user='root')
    print("Root directory:", client.list('/'))
    print("✔ HDFS connected successfully!")
except Exception as e:
    print("❌ HDFS connection failed:", e)
    