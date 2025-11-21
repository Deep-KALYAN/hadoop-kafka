FROM python:3.10-slim

WORKDIR /app
COPY dashboard.py /app/dashboard.py
RUN pip install --no-cache-dir streamlit hdfs pandas
EXPOSE 8501
ENV PYTHONUNBUFFERED=1
CMD ["python", "-c", "import time; time.sleep(10**9)"]
