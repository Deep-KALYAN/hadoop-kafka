import os, json
import streamlit as st
from hdfs import InsecureClient
import pandas as pd
import io

HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
HDFS_USER = os.getenv('HDFS_USER', 'root')
PROCESSED_DIR = os.getenv('PROCESSED_DIR', '/opensky/processed')

client = InsecureClient(HDFS_URL, user=HDFS_USER)

st.set_page_config(page_title="OpenSky Dashboard", layout="wide")
st.title("✈ OpenSky Insights")

# get list of processed files
try:
    files = client.list(PROCESSED_DIR)
except Exception as e:
    st.error(f"Could not list HDFS processed dir: {e}")
    files = []

# filter only JSON summaries (not CSV)
json_files = sorted([f for f in files if f.endswith('.json')])
if not json_files:
    st.info("No processed files yet. Wait for consumer to process messages.")
else:
    latest = json_files[-1]
    data = json.loads(client.read(f"{PROCESSED_DIR}/{latest}", encoding='utf-8'))
    st.metric("Latest processed timestamp", latest.replace('.json',''))
    st.metric("Active flights", data.get('flight_count', 'N/A'))
    st.write("Summary:", data)

    # quick historical table by reading recent summaries
    recent = json_files[-20:]  # last 20
    rows = []
    for f in recent:
        try:
            d = json.loads(client.read(f"{PROCESSED_DIR}/{f}", encoding='utf-8'))
            rows.append(d)
        except:
            pass
    if rows:
        df = pd.DataFrame(rows)
        st.line_chart(df[['flight_count','avg_speed']].fillna(0))
        st.dataframe(df.sort_values('timestamp', ascending=False).reset_index(drop=True))
