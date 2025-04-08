import os
from dotenv import load_dotenv
import psycopg2
import json

def load_into_postgres(inspections, restaurants, violations):
    print("Loading data into PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    with conn.cursor() as cur:
        # Insert into restaurants
        for _, row in restaurants.iterrows():
            values = (
                row['license_'], row['dba_name'], row['aka_name'], row['facility_type'], row['risk'],
                row['address'], row['city'], row['state'], row['zip'],
                row['latitude'], row['longitude']
            )
            cur.execute("""
                INSERT INTO restaurants (license_, dba_name, aka_name, facility_type, risk,
                    address, city, state, zip, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (license_) DO NOTHING;
            """, values)

        # Insert into inspections
        for _, row in inspections.iterrows():
            values = (
                row['inspection_id'], row['license_'], row['inspection_date'],
                row['inspection_type'], row['results']
            )
            cur.execute("""
                INSERT INTO inspections (inspection_id, license_, inspection_date,
                    inspection_type, results, location)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (inspection_id) DO NOTHING;
            """, values)

        # Insert into violations
        for _, row in violations.iterrows():
            values = (
                row['inspection_id'],
                row['violation_number'],
                row['violation_description'],
                row['violation_comments']
            )
            cur.execute("""
                INSERT INTO violations (inspection_id, violation_number, violation_description, violation_comments)
                VALUES (%s, %s, %s, %s);
            """, values)

    conn.commit()
    conn.close()
    
    print("Data loaded successfully into PostgreSQL.")
