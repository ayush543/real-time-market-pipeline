import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.db import get_connection


st.set_page_config(page_title="Real-Time Market Pipeline", layout="wide")

st.title("Real-Time Market Pipeline Dashboard")
st.write("Live market snapshots and anomaly monitoring")

def run_query(query):
    conn = get_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


latest_prices_query = """
WITH ranked_data AS (
    SELECT
        asset_id,
        symbol,
        name,
        price_usd,
        market_cap,
        total_volume,
        source_last_updated,
        event_ts,
        ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY event_ts DESC) AS rn
    FROM raw_market_data
)
SELECT
    asset_id,
    symbol,
    name,
    price_usd,
    market_cap,
    total_volume,
    source_last_updated,
    event_ts
FROM ranked_data
WHERE rn = 1
ORDER BY asset_id;
"""


anomalies_query = """
SELECT
    asset_id,
    symbol,
    name,
    price_usd,
    price_change_pct_24h,
    is_anomaly,
    source_last_updated,
    event_ts
FROM raw_market_data
WHERE is_anomaly = TRUE
ORDER BY event_ts DESC;
"""


latest_df = run_query(latest_prices_query)
anomalies_df = run_query(anomalies_query)

st.subheader("Latest Prices")
if latest_df.empty:
    st.warning("No market data found.")
else:
    st.dataframe(latest_df, use_container_width=True)

st.subheader("Anomalies")
if anomalies_df.empty:
    st.info("No anomalies detected yet.")
else:
    st.dataframe(anomalies_df, use_container_width=True)
