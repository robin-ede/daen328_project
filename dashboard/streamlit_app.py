import os
import json
import requests
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv


def running_in_docker():
    return os.path.exists('/.dockerenv')

@st.cache_resource
def get_connection():
    if running_in_docker():
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
    else:
        load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
        return psycopg2.connect(
            host="localhost",
            port=5433,
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
        restaurants = pd.read_sql("SELECT * FROM restaurants", conn)
        inspections = pd.read_sql("SELECT * FROM inspections", conn)
        violations = pd.read_sql("SELECT * FROM violations", conn)
        return restaurants, inspections, violations

    restaurants, inspections, violations = load_data()
    # Add to session state
    if 'restaurants' not in st.session_state:
        st.session_state.restaurants = restaurants
    if 'inspections' not in st.session_state:
        st.session_state.inspections = inspections
    if 'violations' not in st.session_state:
        st.session_state.violations = violations

    def main_page():
        st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQKqedymLmT2hxvrC6rlIC42IIyGP7wU0A-UQ&s")
        st.title("📊 Restaurant Inspections Dashboard")
        st.subheader("This dashboard provides insights into restaurant inspections, violations, and risk levels.")
        st.write("Use the sidebar to navigate visualizations")

        # --- Inspection Count by Result ---
    def inspection_count_page():
        st.title("Inspection Results Distribution")
        result_counts = st.session_state.inspections["results"].value_counts().reset_index()
        result_counts.columns = ["result", "count"]
        fig = px.bar(result_counts, x="result", y="count", labels={"result": "Result", "count": "Count"})
        st.plotly_chart(fig)

    # --- Violations Over Time ---
    def violations_overTime_page():
        st.title("Violations Over Time")
        violation_dates = st.session_state.inspections.merge(violations, on="inspection_id")
        violation_dates_grouped = (
            violation_dates.groupby("inspection_date")
            .size()
            .reset_index(name="violation_count")
        )
        fig2 = px.line(violation_dates_grouped, x="inspection_date", y="violation_count", title="Violations Over Time")
        st.plotly_chart(fig2)
    
    # --- Map of All Restaurants ---
    def map_page():
        st.title("Restaurant Locations Map")

        # Drop rows with missing coordinates
        restaurants_clean = st.session_state.restaurants.dropna(subset=["latitude", "longitude"])

        fig4 = px.scatter_map(
            restaurants_clean,
            lat="latitude",
            lon="longitude",
            hover_name="dba_name",
            hover_data={
                "address": True,
                "city": True,
                "state": True,
                "zip": True,
                "risk": True,
                "latitude": False,
                "longitude": False
            },
            color="risk",
            zoom=10,
            height=600
        )

        fig4.update_layout(
            mapbox_style="open-street-map",
            margin={"r":0,"t":0,"l":0,"b":0}
        )

        st.plotly_chart(fig4)
    
    # --- Choropleth: Violations Per ZIP Code ---
    def choropleth_page():
        st.subheader("Violations per ZIP Code")

        # Merge to associate each violation with a zip code
        violations_zip = violations.merge(inspections, on="inspection_id")
        violations_zip = violations_zip.merge(restaurants[["restaurant_id", "zip"]], on="restaurant_id")

        # Aggregate count of violations per ZIP
        zip_counts = violations_zip.groupby("zip").size().reset_index(name="violation_count")

        # Drop missing ZIP codes
        zip_counts = zip_counts[zip_counts["zip"].notna()]

        @st.cache_data
        def load_zip_geojson():
            url = "https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/refs/heads/master/il_illinois_zip_codes_geo.min.json"
            response = requests.get(url)
            return response.json()

        geojson = load_zip_geojson()
        
        fig5 = px.choropleth_map(
            zip_counts,
            geojson=geojson,
            locations="zip",
            color="violation_count",
            color_continuous_scale="Reds",
            range_color=(0, zip_counts["violation_count"].max()),
            # mapbox_style="carto-positron",
            zoom=9,
            center={"lat": restaurants["latitude"].mean(), "lon": restaurants["longitude"].mean()},
            opacity=0.6,
            labels={"violation_count": "Violations"},
            featureidkey="properties.ZCTA5CE10"
        )

        fig5.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig5)

    # --- Restaurant Risk Levels ---
    def risk_page():
        st.subheader("Restaurant Risk Levels")
        risk_counts = restaurants["risk"].value_counts().reset_index()
        risk_counts.columns = ["risk", "count"]
        fig3 = px.pie(risk_counts, names="risk", values="count", title="Risk Distribution")
        st.plotly_chart(fig3)

    def test_page():
        st.write("This is a test")

    # Define pages
    pages = {
        'Main Page': [
            st.Page(main_page, title="Main Page"),
        ],
        'Visualizations': [
            st.Page(inspection_count_page, title='Inspection Count by Result'),
            st.Page(violations_overTime_page, title='Violations Over Time'),
            st.Page(map_page, title='Map of All Restaurants'),
            st.Page(choropleth_page, title='Choropleth'),
            st.Page(risk_page, title='Restaurant Risk Levels')
            ]
    }

    pg = st.navigation(pages)
    pg.run()