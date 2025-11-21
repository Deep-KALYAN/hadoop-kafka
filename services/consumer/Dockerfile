FROM python:3.10-slim

WORKDIR /app
COPY consumer.py /app/consumer.py
RUN pip install --no-cache-dir kafka-python hdfs pandas requests

ENV PYTHONUNBUFFERED=1

CMD ["python", "-c", "import time; time.sleep(10**9)"]
