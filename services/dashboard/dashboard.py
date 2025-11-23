import os
import json
import streamlit as st
from hdfs import InsecureClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/opensky/processed")

# ---------------------------------------------------------------------
# STREAMLIT SETUP
# ---------------------------------------------------------------------
st.set_page_config(page_title="OpenSky Offline Dashboard", layout="wide")
st.title("🌍 ✈ OpenSky Global Intelligence Dashboard")
st.caption("All analytics and maps rendered using Plotly")

# ---------------------------------------------------------------------
# HDFS CONNECTION
# ---------------------------------------------------------------------
client = InsecureClient(HDFS_URL, user=HDFS_USER)

try:
    files = sorted([f for f in client.list(PROCESSED_DIR) if f.endswith(".csv")])
except Exception as e:
    st.error(f"❌ Cannot read HDFS folder: {e}")
    st.stop()

if not files:
    st.warning("No processed CSV files found.")
    st.stop()

# ---------------------------------------------------------------------
# LOAD RECENT FILES
# ---------------------------------------------------------------------
MAX_FILES = 30
recent_files = files[-MAX_FILES:]

dfs = []
timestamps = []

for fname in recent_files:
    try:
        with client.read(f"{PROCESSED_DIR}/{fname}", encoding="utf-8") as reader:
            df = pd.read_csv(reader)
        dfs.append(df)

        # extract 20251123_193501 from opensky_processed_...
        ts = fname.split("processed_")[-1].replace(".csv", "")
        timestamps.append(ts)

    except Exception as e:
        print("Error loading:", fname, e)

if not dfs:
    st.error("❌ No CSVs could be loaded.")
    st.stop()

df = pd.concat(dfs, ignore_index=True)

# Keep only rows with valid coords
df = df.dropna(subset=["longitude", "latitude"])

# ---------------------------------------------------------------------
# PARSE TIMESTAMP RANGE
# ---------------------------------------------------------------------
def parse_ts(ts):
    try:
        return datetime.strptime(ts, "%Y%m%d_%H%M%S")
    except:
        return None

min_dt = parse_ts(min(timestamps))
max_dt = parse_ts(max(timestamps))

if min_dt and max_dt:
    st.info(f"📅 **Data range:** {min_dt.strftime('%Y-%m-%d %H:%M:%S')} → {max_dt.strftime('%Y-%m-%d %H:%M:%S')}")
else:
    st.info(f"📅 Data range: {min(timestamps)} → {max(timestamps)}")

# ---------------------------------------------------------------------
# EXTRACT UTC HOUR
# ---------------------------------------------------------------------
df["hour"] = pd.to_datetime(df["last_contact"], unit="s").dt.hour

# ======================================================

def infer_continent(lat, lon):
    """
    Infer continent/region based on latitude and longitude.
    Returns one of: 'North America', 'South America', 'Europe', 'Africa', 'Asia', 'Oceania'
    """

    if -170 <= lon <= -30 and 5 <= lat <= 85:
        return "North America"
    elif -85 <= lon <= -30 and -60 <= lat <= 15:
        return "South America"
    elif -10 <= lon <= 60 and 35 <= lat <= 72:
        return "Europe"
    elif -20 <= lon <= 55 and -35 <= lat <= 35:
        return "Africa"
    elif 60 <= lon <= 180 and -10 <= lat <= 55:
        return "Asia"
    elif 110 <= lon <= 180 and -50 <= lat <= 0:
        return "Oceania"
    else:
        return "Other"
df["continent"] = df.apply(lambda row: infer_continent(row["latitude"], row["longitude"]), axis=1)


# ---------------------------------------------------------------------
# 🌍 OFFLINE GLOBAL AIR TRAFFIC HEATMAP (Plotly Density Map)
# ---------------------------------------------------------------------
st.subheader("🌐 Global Air Traffic Heatmap")

heat_df = df.sample(min(len(df), 10000))  # limit for speed

fig_heat = px.density_mapbox(
    heat_df,
    lat="latitude",
    lon="longitude",
    radius=7,
    zoom=1,
    center=dict(lat=20, lon=0),
    mapbox_style="open-street-map",  # rendered offline by Plotly
    hover_name="callsign",
    hover_data={
        "origin_country": True,       # show country
        "continent": True,            # show region
        "latitude": False,            # hide lat/lon if desired
        "longitude": False
    }
)

# Forces offline rendering (no internet call)
fig_heat.update_layout(mapbox_accesstoken=None)

st.plotly_chart(fig_heat, use_container_width=True)


# ---------------------------------------------------------------------
# 🛰 LIVE AIRCRAFT POSITIONS (Offline World Map)
# ---------------------------------------------------------------------
st.subheader("🛰 Live Aircraft Positions (Offline)")

scatter_df = df.sample(min(len(df), 7000))

fig_scatter = px.scatter_geo(
    scatter_df,
    lat="latitude",
    lon="longitude",
    hover_name="callsign",
    hover_data=["origin_country"],
    projection="natural earth",
)

fig_scatter.update_traces(marker=dict(size=3, color="orange"))

st.plotly_chart(fig_scatter, use_container_width=True)


# ---------------------------------------------------------------------
# 📈 BUSIEST AIR ROUTES (TOP CALLSIGNS)
# ---------------------------------------------------------------------
st.subheader("📈 Busiest Air Routes (Top Callsigns)")

route_df = (
    df["callsign"]
    .fillna("UNKNOWN")
    .value_counts()
    .head(15)
    .reset_index()
)
route_df.columns = ["callsign", "count"]

fig_routes = px.bar(route_df, x="callsign", y="count", title="Top Callsigns")
st.plotly_chart(fig_routes, use_container_width=True)


# ---------------------------------------------------------------------
# 🌍 COUNTRIES WITH MOST FLIGHTS
# ---------------------------------------------------------------------
st.subheader("🌍 Countries With Highest Flight Volume")

country_df = (
    df["origin_country"]
    .value_counts()
    .head(15)
    .reset_index()
)
country_df.columns = ["country", "count"]

fig_countries = px.bar(country_df, x="country", y="count", title="Top Origin Countries")
st.plotly_chart(fig_countries, use_container_width=True)


# ---------------------------------------------------------------------
# 🌎 INBOUND TRAFFIC BY REGION (Offline)
# ---------------------------------------------------------------------
st.subheader("🌎 Inbound Flights by Region")

# def infer_continent(lon):
#     if lon < -30:
#         return "Americas"
#     if -30 <= lon < 60:
#         return "Europe/Africa"
#     return "Asia/Oceania"

# df["continent"] = df["longitude"].apply(infer_continent)

continent_df = df["continent"].value_counts().reset_index()
continent_df.columns = ["continent", "count"]

fig_continent = px.bar(continent_df, x="continent", y="count")
st.plotly_chart(fig_continent, use_container_width=True)


# ---------------------------------------------------------------------
# ⏱️ TIME-OF-DAY TRAFFIC LOAD PER CONTINENT
# ---------------------------------------------------------------------
st.subheader("⏱️ Time-of-Day Traffic Load by Continent (Offline)")

traffic_by_hour = (
    df.groupby(["continent", "hour"])["icao24"]
    .count()
    .reset_index(name="flight_count")
)

for continent in sorted(df["continent"].unique()):
    st.markdown(f"### 🌎 {continent}")
    sub = traffic_by_hour[traffic_by_hour["continent"] == continent]

    fig_hour = px.line(sub, x="hour", y="flight_count")
    st.plotly_chart(fig_hour, use_container_width=True)


# ---------------------------------------------------------------------
# ⏰ GLOBAL TRAFFIC BY HOUR
# ---------------------------------------------------------------------
st.subheader("⏰ Global Traffic By Hour (UTC)")

hour_df = df["hour"].value_counts().sort_index().reset_index()
hour_df.columns = ["hour", "count"]

fig_hour_global = px.line(hour_df, x="hour", y="count")
st.plotly_chart(fig_hour_global, use_container_width=True)


# ---------------------------------------------------------------------
# RAW DATA SAMPLE
# ---------------------------------------------------------------------
with st.expander("📄 View Sample Data (First 200 Rows)"):
    st.dataframe(df.head(200))

st.success("Dashboard loaded successfully (offline mode).")

# import os
# import json
# import streamlit as st
# from hdfs import InsecureClient
# import pandas as pd
# import pydeck as pdk
# from datetime import datetime

# # -------------------------------
# # CONFIG
# # -------------------------------
# HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
# HDFS_USER = os.getenv('HDFS_USER', 'root')
# PROCESSED_DIR = os.getenv('PROCESSED_DIR', '/opensky/processed')

# # Mapbox token (REQUIRED for map background)
# MAPBOX_KEY = os.getenv(
#     "MAPBOX_API_KEY",
#     "pk.eyJ1IjoiZGthbHlhbjEiLCJhIjoiY21pYzI3Z3Y3MWJkaDJpcXd1OXQ3ZWN5ciJ9.z5O4pHlBJZVyMSc-IGHcMw"
# )
# os.environ["MAPBOX_API_KEY"] = MAPBOX_KEY
# pdk.settings.mapbox_api_key = MAPBOX_KEY 


# # -------------------------------
# # STREAMLIT PAGE CONFIG
# # -------------------------------
# st.set_page_config(page_title="OpenSky Global Dashboard", layout="wide")
# st.title("🌍 ✈ Global OpenSky Intelligence Dashboard")
# st.caption("Real-time analytics based on aircraft telemetry from Kafka → HDFS")


# # -------------------------------
# # HDFS CONNECTION
# # -------------------------------
# client = InsecureClient(HDFS_URL, user=HDFS_USER)

# try:
#     files = sorted([f for f in client.list(PROCESSED_DIR) if f.endswith(".csv")])
# except Exception as e:
#     st.error(f"❌ Cannot list HDFS directory: {e}")
#     files = []

# if not files:
#     st.warning("No processed CSV files found yet.")
#     st.stop()

# # -------------------------------
# # LOAD RECENT FILES (MERGED)
# # -------------------------------
# MAX_FILES = 30   # load only last 30 files for speed
# recent_files = files[-MAX_FILES:]

# dfs = []
# timestamps = []

# for f in recent_files:
#     try:
#         with client.read(f"{PROCESSED_DIR}/{f}", encoding="utf-8") as reader:
#             df = pd.read_csv(reader)
#             dfs.append(df)
#             timestamps.append(f.split("processed_")[-1].replace(".csv", ""))
#     except Exception:
#         continue

# if not dfs:
#     st.error("❌ Failed to load any CSV files.")
#     st.stop()

# df = pd.concat(dfs, ignore_index=True)

# # -------------------------------
# # TIME RANGE INFORMATION
# # -------------------------------
# min_time = min(timestamps)
# max_time = max(timestamps)
# def parse_ts(ts):
#     """Convert filename timestamp to readable datetime."""
#     try:
#         # input like 20251123_190235
#         return datetime.strptime(ts, "%Y%m%d_%H%M%S")
#     except:
#         return None

# min_dt = parse_ts(min_time)
# max_dt = parse_ts(max_time)

# if min_dt and max_dt:
#     st.info(f"📅 **Data range:** {min_dt.strftime('%Y-%m-%d %H:%M:%S')} → {max_dt.strftime('%Y-%m-%d %H:%M:%S')}")
# else:
#     st.info(f"📅 Data range: {min_time} → {max_time}")
# # min_time = min(timestamps)
# # max_time = max(timestamps)

# # st.info(f"📅 **Data range:** `{min_time}` → `{max_time}`")

# # # Keep only useful columns
# # df = df.dropna(subset=["longitude", "latitude"])


# # ======================================================
# # 🌐 GLOBAL AIR TRAFFIC HEAT MAP
# # ======================================================
# st.subheader("🌐 Global Air Traffic Heat Map")
# st.caption("Real-time visualization of aircraft density")

# # Sample at most 10,000 points to avoid Streamlit memory limit
# heat_df = df.sample(min(len(df), 10000))

# heat_layer = pdk.Layer(
#     "HeatmapLayer",
#     data=heat_df,
#     get_position='[longitude, latitude]',
#     radiusPixels=30,
#     intensity=1,
# )

# heatmap = pdk.Deck(
#     map_style="https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",#"mapbox://styles/mapbox/dark-v10",
#     initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1.5, pitch=30),
#     layers=[heat_layer],
# )

# st.pydeck_chart(heatmap)


# # ======================================================
# # 🛰️ LIVE AIRCRAFT POSITIONS
# # ======================================================
# st.subheader("🛰️ Live Aircraft Positions")

# scatter_df = df.sample(min(len(df), 8000))

# scatter_layer = pdk.Layer(
#     "ScatterplotLayer",
#     data=scatter_df,
#     get_position='[longitude, latitude]',
#     get_color='[255, 165, 0, 180]',
#     get_radius=2000,
#     pickable=True,
# )

# scatter = pdk.Deck(
#     map_style="https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",#"mapbox://styles/mapbox/light-v10",
#     initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1.5, pitch=0),
#     layers=[scatter_layer],
#     tooltip={"text": "Callsign: {callsign}\nCountry: {origin_country}"}
# )

# st.pydeck_chart(scatter)


# # ======================================================
# # 📈 BUSIEST AIR ROUTES (Top Callsigns)
# # ======================================================
# st.subheader("📈 Busiest Air Routes (Top Callsigns)")

# route_counts = df["callsign"].fillna("UNKNOWN").value_counts().head(20)

# route_df = route_counts.reset_index()
# route_df.columns = ["callsign", "count"]

# st.bar_chart(route_df.set_index("callsign"))


# # ======================================================
# # 🌍 WHICH COUNTRIES HAVE THE MOST FLIGHTS?
# # ======================================================
# st.subheader("🌍 Countries With Highest Volume of Flights")

# country_counts = df["origin_country"].value_counts().head(20)

# country_df = country_counts.reset_index()
# country_df.columns = ["country", "count"]

# st.bar_chart(country_df.set_index("country"))


# # # ======================================================
# # # 🌎 INBOUND TRAFFIC BY CONTINENT
# # # ======================================================
# st.subheader("🌎 Inbound Flights by Continent")

# # Assign continent by longitude
# def infer_continent(lon):
#     if lon < -30: return "Americas"
#     if -30 <= lon < 60: return "Europe/Africa"
#     return "Asia/Oceania"

# df["continent"] = df["longitude"].apply(infer_continent)

# # Must be BEFORE any hour-based charts
# df["hour"] = pd.to_datetime(df["last_contact"], unit="s", errors="coerce").dt.hour
# df = df.dropna(subset=["hour"])
# df["hour"] = df["hour"].astype(int)

# continent_counts = df["continent"].value_counts()

# continent_df = continent_counts.reset_index()
# continent_df.columns = ["continent", "count"]

# st.bar_chart(continent_df.set_index("continent"))



# # ---------------------------------------------------------
# # 🗺️ Continents with Most Inbound Flights
# # ---------------------------------------------------------
# # st.header("🗺️ Regions With Most Inbound Flights")
# # continent_counts = df["continent"].value_counts()
# # st.bar_chart(continent_counts)

# # ---------------------------------------------------------
# # ⏱️ Time-of-day Traffic Load per Continent
# # ---------------------------------------------------------
# st.header("⏱️ Time-of-Day Air Traffic Load by Continent")
# # df["hour"] = pd.to_datetime(df["last_contact"], unit="s").dt.hour
# traffic_by_hour = (
#     df.groupby(["continent", "hour"])["icao24"]
#     .count()
#     .reset_index(name="flight_count")
# )

# for continent in sorted(df["continent"].unique()):
#     st.subheader(f"🌏 {continent}")
#     sub = traffic_by_hour[traffic_by_hour["continent"] == continent]
#     sub = sub.sort_values("hour")
#     st.line_chart(sub.set_index("hour")["flight_count"])



# # ======================================================
# # ⏰ TRAFFIC BY HOUR OF DAY (UTC)
# # ======================================================
# st.subheader("⏰ Traffic Intensity by Hour (UTC)")

# df["hour"] = pd.to_datetime(df["last_contact"], unit="s").dt.hour
# hour_counts = df["hour"].value_counts().sort_index()

# hour_df = hour_counts.reset_index()
# hour_df.columns = ["hour", "count"]

# st.line_chart(hour_df.set_index("hour"))


# # ---------------------------------------------------------
# # 🔍 Sample Raw Data Table
# # ---------------------------------------------------------
# with st.expander("🔍 View Sample Flight Records (First 200 rows)"):
#     st.dataframe(df.head(200))

# # -------------------------------
# # END
# # -------------------------------
# st.success("Dashboard refreshed successfully.")
