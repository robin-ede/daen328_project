import os
import psycopg2
import json

def load_into_postgres(inspections, restaurants):
    print("Loading data into PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    with conn.cursor() as cur:
        for _, row in restaurants.iterrows():
            cur.execute("""
                INSERT INTO restaurants (license_, dba_name, aka_name, facility_type, risk,
                    address, city, state, zip, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (license_) DO NOTHING;
            """, (
                row['license_'], row['dba_name'], row['aka_name'], row['facility_type'], row['risk'],
                row['address'], row['city'], row['state'], row['zip'],
                row['latitude'], row['longitude']
            ))

        for _, row in inspections.iterrows():
            cur.execute("""
                INSERT INTO inspections (inspection_id, license_, inspection_date,
                    inspection_type, results, violations, location)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (inspection_id) DO NOTHING;
            """, (
                row['inspection_id'], row['license_'], row['inspection_date'],
                row['inspection_type'], row['results'], row['violations'],
                json.dumps(row['location']) if isinstance(row['location'], dict) else None
            ))

    conn.commit()
    conn.close()
