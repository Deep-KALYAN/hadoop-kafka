import os
import json
import streamlit as st
import pandas as pd
from hdfs import InsecureClient
import pydeck as pdk

# ---------------------------------------------------------
# Streamlit Page Config
# ---------------------------------------------------------
st.set_page_config(
    page_title="OpenSky Global Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("🌍 ✈ Global OpenSky Intelligence Dashboard")
st.markdown("Real-time analytics based on aircraft ADS-B telemetry")

# ---------------------------------------------------------
# HDFS Configuration
# ---------------------------------------------------------
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/opensky/processed")

client = InsecureClient(HDFS_URL, user=HDFS_USER)

# ---------------------------------------------------------
# Load Data (cached)
# ---------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_data():
    try:
        files = sorted(
            [f for f in client.list(PROCESSED_DIR) if f.endswith(".csv")]
        )
    except:
        st.error("❌ Unable to read processed directory in HDFS")
        return pd.DataFrame()

    # Limit to last N files for performance
    files = files[-200:]

    dfs = []
    for f in files:
        try:
            with client.read(f"{PROCESSED_DIR}/{f}", encoding="utf-8") as reader:
                dfs.append(pd.read_csv(reader))
        except:
            continue

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    return df


df = load_data()

if df.empty:
    st.warning("⚠ No processed flight data found in HDFS yet.")
    st.stop()

# ---------------------------------------------------------
# Data Cleaning & Enrichment
# ---------------------------------------------------------

df["callsign"] = df["callsign"].fillna("").astype(str)
df["origin_country"] = df["origin_country"].fillna("Unknown")

# Convert timestamp columns
df["last_contact_dt"] = pd.to_datetime(df["last_contact"], unit="s", errors="coerce")
df["hour"] = df["last_contact_dt"].dt.hour

continent_map = {
    "United States": "North America", "Canada": "North America",
    "Brazil": "South America", "Argentina": "South America",
    "France": "Europe", "Germany": "Europe", "Switzerland": "Europe",
    "United Kingdom": "Europe", "Russia": "Europe",
    "China": "Asia", "India": "Asia", "Japan": "Asia",
    "Australia": "Oceania", "New Zealand": "Oceania",
    "South Africa": "Africa"
}
df["continent"] = df["origin_country"].map(continent_map).fillna("Other")

# ---------------------------------------------------------
# Data Sampling Helpers
# ---------------------------------------------------------

def sample_for_map(dataframe, max_points=5000):
    """Reduce dataset size safely to avoid Streamlit message overflow."""
    if len(dataframe) > max_points:
        return dataframe.sample(max_points)
    return dataframe


# ---------------------------------------------------------
# 🌐 GLOBAL AIR TRAFFIC HEATMAP
# ---------------------------------------------------------
st.header("🌐 Global Air Traffic Heat Map")
st.caption("Real-time visualization of global aircraft density")

heat_df = df.dropna(subset=["latitude", "longitude"])
heat_df = sample_for_map(heat_df, 5000)

st.pydeck_chart(
    pdk.Deck(
        map_style="mapbox://styles/mapbox/dark-v10",
        initial_view_state=pdk.ViewState(
            latitude=20, longitude=0, zoom=1.2, pitch=30
        ),
        layers=[
            pdk.Layer(
                "HeatmapLayer",
                data=heat_df,
                get_position='[longitude, latitude]',
                radiusPixels=30,
                intensity=1,
            )
        ],
    )
)

# ---------------------------------------------------------
# 🎯 AIRCRAFT POSITION SCATTER MAP
# ---------------------------------------------------------
st.header("🛰️ Live Aircraft Positions")
st.caption("Scatter map of sampled aircraft positions")

scatter_df = sample_for_map(df.dropna(subset=["latitude", "longitude"]), 4000)

st.pydeck_chart(
    pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v10",
        initial_view_state=pdk.ViewState(
            latitude=20, longitude=0, zoom=1.3
        ),
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=scatter_df,
                get_position='[longitude, latitude]',
                get_radius=20000,
                get_color='[255, 70, 30, 160]',
                pickable=True,
            ),
        ],
        tooltip={"text": "{callsign}\n{origin_country}"},
    )
)

# ---------------------------------------------------------
# 📈 Busiest Air Routes
# ---------------------------------------------------------
st.header("📈 Busiest Air Routes (Top Callsigns)")
route_counts = df["callsign"].value_counts().head(20)
st.bar_chart(route_counts)

# ---------------------------------------------------------
# 🌍 Countries with Highest Air Traffic
# ---------------------------------------------------------
st.header("🌍 Countries With Highest Air Traffic Volume")
country_counts = df["origin_country"].value_counts().head(15)
st.bar_chart(country_counts)

# ---------------------------------------------------------
# 🗺️ Continents with Most Inbound Flights
# ---------------------------------------------------------
st.header("🗺️ Regions With Most Inbound Flights")
continent_counts = df["continent"].value_counts()
st.bar_chart(continent_counts)

# ---------------------------------------------------------
# ⏱️ Time-of-day Traffic Load per Continent
# ---------------------------------------------------------
st.header("⏱️ Time-of-Day Air Traffic Load by Continent")

traffic_by_hour = (
    df.groupby(["continent", "hour"])["icao24"]
    .count()
    .reset_index(name="flight_count")
)

for continent in sorted(df["continent"].unique()):
    st.subheader(f"🌏 {continent}")
    sub = traffic_by_hour[traffic_by_hour["continent"] == continent]
    sub = sub.sort_values("hour")
    st.line_chart(sub.set_index("hour")["flight_count"])

# ---------------------------------------------------------
# 🔍 Sample Raw Data Table
# ---------------------------------------------------------
with st.expander("🔍 View Sample Flight Records (First 200 rows)"):
    st.dataframe(df.head(200))


# import os
# import json
# import streamlit as st
# import pandas as pd
# from hdfs import InsecureClient
# import pydeck as pdk

# # --- Configuration ---
# HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
# HDFS_USER = os.getenv("HDFS_USER", "root")
# PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/opensky/processed")

# client = InsecureClient(HDFS_URL, user=HDFS_USER)

# # Streamlit Page Config
# st.set_page_config(
#     page_title="Global OpenSky Flight Dashboard",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )

# st.title("🌍 ✈ Global OpenSky Intelligence Dashboard")
# st.markdown("Real-time analytics based on aircraft telemetry from Kafka + HDFS")

# # ---- Load Data ----
# def load_data():
#     try:
#         files = sorted([f for f in client.list(PROCESSED_DIR) if f.endswith(".csv")])
#     except:
#         st.error("Cannot read processed directory in HDFS")
#         return pd.DataFrame()

#     # Limit to last 200 files for performance
#     files = files[-200:]

#     dfs = []
#     for f in files:
#         try:
#             with client.read(f"{PROCESSED_DIR}/{f}", encoding="utf-8") as reader:
#                 dfs.append(pd.read_csv(reader))
#         except:
#             continue

#     if not dfs:
#         return pd.DataFrame()

#     df = pd.concat(dfs, ignore_index=True)
#     return df


# df = load_data()

# if df.empty:
#     st.warning("No data found. Wait until consumer writes processed CSV files.")
#     st.stop()

# # Clean + enrich data
# df["callsign"] = df["callsign"].fillna("")
# df["origin_country"] = df["origin_country"].fillna("Unknown")

# # Time-of-day
# df["last_contact_hour"] = pd.to_datetime(df["last_contact"], unit="s").dt.hour

# # Continent lookup (basic)
# continent_map = {
#     "United States": "North America",
#     "Canada": "North America",
#     "Brazil": "South America",
#     "Argentina": "South America",
#     "France": "Europe",
#     "Germany": "Europe",
#     "Switzerland": "Europe",
#     "United Kingdom": "Europe",
#     "Russia": "Europe",
#     "China": "Asia",
#     "India": "Asia",
#     "Japan": "Asia",
#     "Australia": "Oceania",
#     "New Zealand": "Oceania",
#     "South Africa": "Africa",
# }

# df["continent"] = df["origin_country"].map(continent_map).fillna("Other")

# # ----------------------------
# #   GLOBAL AIRSPACE HEATMAP
# # ----------------------------
# st.header("🌐 Global Air Traffic Heat Map")
# st.caption("Shows real-time aircraft density")

# heat_df = df.dropna(subset=["latitude", "longitude"])

# # Limit dataset to avoid Streamlit 200MB error
# MAX_HEATMAP_POINTS = 5000
# if len(heat_df) > MAX_HEATMAP_POINTS:
#     heat_df = heat_df.sample(MAX_HEATMAP_POINTS)

# st.pydeck_chart(
#     pdk.Deck(
#         map_style="mapbox://styles/mapbox/dark-v10",
#         initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1.2),
#         layers=[
#             pdk.Layer(
#                 "HeatmapLayer",
#                 data=heat_df,
#                 get_position='[longitude, latitude]',
#                 radiusPixels=30,
#                 intensity=1
#             )
#         ],
#     )
# )

# # ----------------------------
# #  BUSIEST AIR ROUTES (by callsign frequency)
# # ----------------------------
# st.header("📈 Busiest Air Routes")
# st.caption("Top 20 callsigns appearing most frequently")

# route_counts = df["callsign"].value_counts().head(20)

# st.bar_chart(route_counts)

# # ----------------------------
# # COUNTRIES WITH MOST AIR TRAFFIC
# # ----------------------------
# st.header("🌍 Countries With Highest Air Traffic Volume")
# st.caption("Based on count of aircraft last seen per origin country")

# country_counts = df["origin_country"].value_counts().head(15)

# st.bar_chart(country_counts)

# # Pie chart
# st.subheader("Share of Global Flights by Country")
# st.write(country_counts)

# # ----------------------------
# # REGIONS WITH MOST INBOUND FLIGHTS
# # ----------------------------
# st.header("🗺️ Regions With Most Inbound Flights (By Continent)")
# continent_counts = df["continent"].value_counts()
# st.bar_chart(continent_counts)

# # ----------------------------
# # AIR TRAFFIC BY TIME OF DAY PER CONTINENT
# # ----------------------------
# st.header("⏱️ Time-of-Day Traffic Load per Continent")

# continent_hour = (
#     df.groupby(["continent", "last_contact_hour"])["icao24"]
#     .count()
#     .reset_index(name="flight_count")
# )

# for continent in sorted(df["continent"].unique()):
#     st.subheader(f"📌 {continent}")
#     sub = continent_hour[continent_hour["continent"] == continent]
#     sub = sub.sort_values("last_contact_hour")
#     st.line_chart(sub.set_index("last_contact_hour")["flight_count"])

# # ----------------------------
# # RAW TABLE INSPECTION
# # ----------------------------
# with st.expander("🔍 View Sample Flight Records"):
#     st.dataframe(df.head(200))






##-------------------
##---------------------

# import os, json
# import streamlit as st
# from hdfs import InsecureClient
# import pandas as pd
# import io

# HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
# HDFS_USER = os.getenv('HDFS_USER', 'root')
# PROCESSED_DIR = os.getenv('PROCESSED_DIR', '/opensky/processed')

# client = InsecureClient(HDFS_URL, user=HDFS_USER)

# st.set_page_config(page_title="OpenSky Dashboard", layout="wide")
# st.title("✈ OpenSky Insights")

# # get list of processed files
# try:
#     files = client.list(PROCESSED_DIR)
# except Exception as e:
#     st.error(f"Could not list HDFS processed dir: {e}")
#     files = []

# # # filter only JSON summaries (not CSV)
# # json_files = sorted([f for f in files if f.endswith('.json')])
# # if not json_files:
# #     st.info("No processed files yet. Wait for consumer to process messages.")
# # else:
# #     latest = json_files[-1]
# #     data = json.loads(client.read(f"{PROCESSED_DIR}/{latest}", encoding='utf-8'))
# #     st.metric("Latest processed timestamp", latest.replace('.json',''))
# #     st.metric("Active flights", data.get('flight_count', 'N/A'))
# #     st.write("Summary:", data)

# #     # quick historical table by reading recent summaries
# #     recent = json_files[-20:]  # last 20
# # filter only CSV processed files

# csv_files = sorted([f for f in files if f.endswith('.csv')])
# if csv_files:
#     st.success(f"Found {len(csv_files)} processed CSV files:")
#     st.write(csv_files)
# else:
#     st.warning("No processed CSV files found!")


# if not csv_files:
#     st.info("No processed files yet. Wait for consumer to process messages_.")
# else:
#     latest = csv_files[-1]
#     with client.read(f"{PROCESSED_DIR}/{latest}", encoding='utf-8') as reader:
#         df = pd.read_csv(reader)

#     st.metric("Latest processed timestamp", latest.replace('.csv',''))
#     st.metric("Active flights", len(df))
#     st.dataframe(df.head(20))  # show first 20 rows

#     recent_csv = csv_files[-20:]  # last 20

#     dfs = []
#     for f in recent_csv:
#         try:
#             with client.read(f"{PROCESSED_DIR}/{f}", encoding='utf-8') as reader:
#                 dfs.append(pd.read_csv(reader))
#         except:
#             continue

#     if dfs:
#         all_df = pd.concat(dfs, ignore_index=True)
#         # Example: average speed trend
#         if 'velocity_ms' in all_df.columns:
#             st.line_chart(all_df['velocity_ms'].fillna(0))


#     # rows = []
#     # for f in recent:
#     #     try:
#     #         d = json.loads(client.read(f"{PROCESSED_DIR}/{f}", encoding='utf-8'))
#     #         rows.append(d)
#     #     except:
#     #         pass
#     # if rows:
#     #     df = pd.DataFrame(rows)
#     #     st.line_chart(df[['flight_count','avg_speed']].fillna(0))
#     #     st.dataframe(df.sort_values('timestamp', ascending=False).reset_index(drop=True))
