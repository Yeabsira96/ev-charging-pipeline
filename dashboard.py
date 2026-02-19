# ===========================================
# dashboard.py
# This file creates an interactive web dashboard
# ===========================================

import streamlit as st  # Streamlit makes web apps easy
import pandas as pd
import sqlite3
import plotly.express as px  # For beautiful charts
from database import read_from_db
import time


# ===========================================
# PAGE CONFIGURATION
# ===========================================
st.set_page_config(
    page_title="🔋 EV Charging Network Dashboard",
    page_icon="🔋",
    layout="wide"  # Use full width of the screen
)

# Add a title
st.title("🔋 China EV Charging Network Monitor")
st.markdown("### Real-time monitoring of electric vehicle charging infrastructure")


# ===========================================
# LOAD DATA FROM DATABASE (NO CACHING - ALWAYS FRESH)
# ===========================================
def load_data():
    """Load stations from database - ALWAYS fresh, no caching"""
    try:
        conn = sqlite3.connect("charging_stations.db")
        df = read_from_db(conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not load data: {e}")
        return pd.DataFrame()

def get_stats():
    """Get statistics from database"""
    try:
        conn = sqlite3.connect("charging_stations.db")
        from database import get_stats
        stats = get_stats(conn)
        conn.close()
        return stats
    except Exception as e:
        return {}

# Load fresh data every time
df = load_data()


# ===========================================
# DISPLAY KEY METRICS WITH AUTO-REFRESH
# ===========================================
st.markdown("## 📊 Key Metrics")

# Add auto-refresh controls
col_refresh1, col_refresh2 = st.columns([3, 1])
with col_refresh1:
    st.info("💡 Dashboard auto-refreshes every 10 seconds while data is being scraped")
with col_refresh2:
    if st.button("🔄 Refresh Now"):
        st.rerun()

# Create 4 columns for metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_stations = len(df)
    st.metric(
        label="Total Stations",
        value=f"{total_stations:,}",
        help="Total number of charging stations in the database"
    )

with col2:
    if len(df) > 0:
        offline_count = df["is_offline"].sum()
        offline_pct = (offline_count/len(df))*100
        st.metric(
            label="Offline Stations",
            value=offline_count,
            delta=f"{offline_pct:.1f}% of network",
            delta_color="inverse",  # Red if high
            help="Stations that haven't been updated in 90+ days"
        )
    else:
        st.metric(label="Offline Stations", value="—")

with col3:
    if len(df) > 0:
        tesla_count = (df["operator_clean"] == "Tesla").sum()
        st.metric(
            label="Tesla Stations",
            value=f"{tesla_count:,}",
            help="Charging stations operated by Tesla"
        )
    else:
        st.metric(label="Tesla Stations", value="—")

with col4:
    if len(df) > 0:
        avg_days = df["days_since_update"].mean()
        st.metric(
            label="Avg. Days Since Update",
            value=f"{avg_days:.1f} days",
            help="Average age of station information"
        )
    else:
        st.metric(label="Avg. Days Since Update", value="—")


# ===========================================
# PROGRESS INDICATOR
# ===========================================
if len(df) > 0:
    st.success(f"✅ **{len(df):,} stations loaded!** Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
else:
    st.warning("⏳ **No data yet.** ETL pipeline is still scraping... Refresh this page in a moment!")


# ===========================================
# DISPLAY MAP
# ===========================================
if len(df) > 0:
    st.markdown("## 🗺️ Station Locations")

    # Create a map using Plotly
    fig = px.scatter_mapbox(
        df,
        lat="latitude",           # Column with latitude
        lon="longitude",          # Column with longitude
        color="is_offline",       # Color by offline status
        color_discrete_map={True: "red", False: "green"},  # Red = offline, Green = online
        hover_name="name",        # Show name on hover
        hover_data=["city", "operator_clean", "days_since_update"],  # Show extra info on hover
        zoom=3,                   # Zoom level (3 = China view)
        height=500,               # Height of the map
        title="Charging Station Distribution"
    )

    # Set map style
    fig.update_layout(
        mapbox_style="open-street-map",  # Free map style
        margin={"r":0,"t":30,"l":0,"b":0}  # Remove margins
    )

    # Display the map
    st.plotly_chart(fig, use_container_width=True)


    # ===========================================
    # DISPLAY OFFLINE STATIONS TABLE
    # ===========================================
    st.markdown("## ⚠️ Offline Stations")

    offline_df = df[df["is_offline"] == True]

    if len(offline_df) > 0:
        # Show a data table
        st.dataframe(
            offline_df[["name", "city", "operator_clean", "days_since_update", "last_updated"]],
            use_container_width=True
        )
    else:
        st.success("✅ No offline stations detected!")


    # ===========================================
    # DISPLAY OPERATOR BREAKDOWN
    # ===========================================
    st.markdown("## 🏢 Operator Breakdown")

    # Count stations by operator
    operator_counts = df["operator_clean"].value_counts().reset_index()
    operator_counts.columns = ["Operator", "Count"]

    # Create a bar chart
    fig2 = px.bar(
        operator_counts,
        x="Operator",
        y="Count",
        color="Operator",
        title="Stations by Operator"
    )

    st.plotly_chart(fig2, use_container_width=True)

    # Show data preview
    st.markdown("## 📋 Data Preview")
    st.dataframe(
        df[["name", "city", "operator_clean", "days_since_update"]].head(20),
        use_container_width=True
    )

else:
    st.markdown("---")
    st.info("⏳ **Waiting for data...** The ETL pipeline is currently scraping charging stations from the API. This may take several minutes.")
    st.info("📌 **Tip:** Keep this page open and it will auto-refresh every 10 seconds to show new data as it arrives.")

# Auto-refresh every 10 seconds
import streamlit.components.v1 as components
components.html(
    """
    <script>
    setTimeout(function() {
        window.location.reload();
    }, 10000);
    </script>
    """,
    height=0
)