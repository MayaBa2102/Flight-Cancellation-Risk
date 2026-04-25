import os
from datetime import datetime

import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(
    page_title="Flight Risk Dashboard",
    layout="wide",
)

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "flightdb"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ.get("POSTGRES_PASSWORD", "pipeline123"),
    )


@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT
            flight_iata                             AS "Flight",
            airline                                 AS "Airline",
            dep_iata || ' → ' || arr_iata           AS "Route",
            TO_CHAR(scheduled_dep AT TIME ZONE 'UTC',
                    'YYYY-MM-DD HH24:MI')            AS "Departure",
            TO_CHAR(scheduled_arr AT TIME ZONE 'UTC',
                    'YYYY-MM-DD HH24:MI')            AS "Arrival",
            dep_wind_kmh                            AS "Dep Wind (km/h)",
            dep_rain_mm_h                           AS "Dep Rain (mm/h)",
            arr_wind_kmh                            AS "Arr Wind (km/h)",
            arr_rain_mm_h                           AS "Arr Rain (mm/h)",
            risk_level                              AS "Risk",
            risk_reason                             AS "Reason",
            risk_airport                            AS "Risk At",
            TO_CHAR(last_updated AT TIME ZONE 'UTC',
                    'HH24:MI')                       AS "Updated"
        FROM flight_risk
        ORDER BY
            CASE risk_level WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
            scheduled_dep;
    """
    return pd.read_sql(query, conn)


def colour_risk(val: str) -> str:
    colours = {"high": "background-color: #ff4b4b; color: white; font-weight: bold",
               "medium": "background-color: #ffa500; color: white",
               "low": "background-color: #21c354; color: white"}
    return colours.get(val.lower(), "")


st.title("Flight Cancellation Risk Dashboard")
st.caption("Predicts flight cancellation risk based on weather forecasts at departure and arrival airports. "
           "Data refreshes every 30 minutes.")

try:
    df = load_data()
except Exception as e:
    st.error(f"Could not connect to the database: {e}")
    st.info("Make sure the pipeline has run at least once and PostgreSQL is healthy.")
    st.stop()

if df.empty:
    st.warning("No flight data yet. Run the schedule_dag and risk_dag at least once.")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
risk_counts = df["Risk"].value_counts()
col1.metric("Total Flights", len(df))
col2.metric("🔴 High Risk",   risk_counts.get("high",   0))
col3.metric("🟠 Medium Risk", risk_counts.get("medium", 0))
col4.metric("🟢 Low Risk",    risk_counts.get("low",    0))

st.divider()

col_a, col_b, col_c = st.columns(3)
selected_risk = col_a.multiselect(
    "Filter by Risk Level",
    options=["high", "medium", "low"],
    default=["high", "medium", "low"],
)
all_airports = sorted(df["Route"].str.split(" → ").str[0].unique())
selected_dep = col_b.multiselect("Filter by Departure Airport", options=all_airports, default=all_airports)
search = col_c.text_input("Search flight / airline", "")

filtered = df[df["Risk"].isin(selected_risk)]
filtered = filtered[filtered["Route"].str.split(" → ").str[0].isin(selected_dep)]
if search:
    mask = (filtered["Flight"].str.contains(search, case=False, na=False) |
            filtered["Airline"].str.contains(search, case=False, na=False))
    filtered = filtered[mask]

st.caption(f"Showing {len(filtered)} of {len(df)} flights  |  Last data update: {df['Updated'].iloc[0]}")

st.dataframe(
    filtered.style.map(colour_risk, subset=["Risk"]),
    use_container_width=True,
    hide_index=True,
)

st.subheader("Risk Distribution by Departure Airport")
chart_data = (
    filtered.groupby(["Route", "Risk"])
    .size()
    .reset_index(name="Count")
)
chart_data["Dep Airport"] = chart_data["Route"].str.split(" → ").str[0]
pivot = chart_data.groupby(["Dep Airport", "Risk"])["Count"].sum().unstack(fill_value=0)
for level in ["high", "medium", "low"]:
    if level not in pivot.columns:
        pivot[level] = 0
st.bar_chart(
    pivot[["high", "medium", "low"]],
    color=["#ff4b4b", "#ffa500", "#21c354"],
)

st.caption(f"Page cached for 60 seconds. Last loaded at {datetime.utcnow().strftime('%H:%M:%S')}. "
           "Rerun the page to force a refresh.")
