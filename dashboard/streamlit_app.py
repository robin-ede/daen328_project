import os
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# Database connection
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

conn = get_connection()

# ETL Check
def is_etl_complete(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM restaurants")
        restaurants_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM inspections")
        inspections_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM violations")
        violations_count = cur.fetchone()[0]
    return all([restaurants_count > 0, inspections_count > 0, violations_count > 0])

# Show message if ETL hasn’t run
if not is_etl_complete(conn):
    st.title("📊 Restaurant Inspections Dashboard")
    st.warning("ETL pipeline hasn't run yet. No data available.")
    st.stop()  # Stops the app from rendering further
else:
    # Load data
    @st.cache_data
    def load_data():
        restaurants = pd.read_sql("SELECT * FROM restaurants LIMIT 100", conn)
        inspections = pd.read_sql("SELECT * FROM inspections LIMIT 1000", conn)
        violations = pd.read_sql("SELECT * FROM violations LIMIT 1000", conn)
        return restaurants, inspections, violations

    restaurants, inspections, violations = load_data()

    st.title("📊 Restaurant Inspections Dashboard")
    st.write("This dashboard provides insights into restaurant inspections, violations, and risk levels.")

    # --- Inspection Count by Result ---
    st.subheader("Inspection Results Distribution")
    result_counts = inspections["results"].value_counts().reset_index()
    result_counts.columns = ["result", "count"]
    fig = px.bar(result_counts, x="result", y="count", labels={"result": "Result", "count": "Count"})
    st.plotly_chart(fig)

    # --- Violations Over Time ---
    st.subheader("Violations Over Time")
    violation_dates = inspections.merge(violations, on="inspection_id")
    violation_dates_grouped = (
        violation_dates.groupby("inspection_date")
        .size()
        .reset_index(name="violation_count")
    )
    fig2 = px.line(violation_dates_grouped, x="inspection_date", y="violation_count", title="Violations Over Time")
    st.plotly_chart(fig2)

    # --- Restaurant Risk Levels ---
    st.subheader("Restaurant Risk Levels")
    risk_counts = restaurants["risk"].value_counts().reset_index()
    risk_counts.columns = ["risk", "count"]
    fig3 = px.pie(risk_counts, names="risk", values="count", title="Risk Distribution")
    st.plotly_chart(fig3)
