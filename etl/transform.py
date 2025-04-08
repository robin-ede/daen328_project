import pandas as pd
import re
import pgeocode
from geopy.geocoders import Nominatim
import time

def drop_unnecessary_columns(df):
  return df.drop(columns=[col for col in df.columns if col=='location' or col.startswith(":@computed_region")])

def parse_dates(df):
  df["inspection_date"] = pd.to_datetime(df["inspection_date"])
  return df

# Initialize the nominatim lookup for US
nomi = pgeocode.Nominatim('us')

# Apply ZIP lookup to get city and state
def fill_from_zip(row):
    if pd.notnull(row['zip']) and (pd.isnull(row['city']) or pd.isnull(row['state'])):
        location = nomi.query_postal_code(row['zip'])
        if pd.isnull(row['city']):
            row['city'] = location.place_name
        if pd.isnull(row['state']):
            row['state'] = location.state_code
    return row

geolocator = Nominatim(user_agent="daen328_project_2025")

def fill_from_latlon(row):
    if (pd.notnull(row['latitude']) and pd.notnull(row['longitude'])) and \
       (pd.isnull(row['city']) or pd.isnull(row['state']) or pd.isnull(row['zip'])):
        try:
            location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", timeout=10)
            if location and location.raw.get('address'):
                address = location.raw['address']
                if pd.isnull(row['city']):
                    row['city'] = address.get('city') or address.get('town') or address.get('village')
                if pd.isnull(row['state']):
                    row['state'] = address.get('state')
                if pd.isnull(row['zip']):
                    row['zip'] = address.get('postcode')
        except Exception as e:
            print(f"Geocoding error: {e}")
        time.sleep(1)  # To avoid rate limits
    return row

def extract_violations(df):
    all_rows = []

    for _, row in df.iterrows():
        if pd.isna(row["violations"]):
            continue

        violations = [v.strip() for v in row["violations"].split('|') if v.strip()]
        inspection_id = row.get("inspection_id")

        for v in violations:
            match = re.match(
                r'(?P<number>\d{1,3})\.\s(?P<description>.*?)(?:\s*-\s*Comments:\s*(?P<comments>.*))?$',
                v.strip()
            )
            if match:
                all_rows.append({
                    "inspection_id": inspection_id,
                    "violation_number": int(match.group("number")),
                    "violation_description": match.group("description").strip(),
                    "violation_comments": match.group("comments").strip() if match.group("comments") else None
                })

    return pd.DataFrame(all_rows)

def clean_data(df):
    print("Cleaning data...")
    print("Dropping unnecessary columns...")
    df = drop_unnecessary_columns(df)
    print("Parsing dates...")
    df = parse_dates(df)
    print("Filling missing city/state/zip from ZIP code...")
    df = df.apply(fill_from_zip, axis=1)
    print("Filling missing city/state/zip from latitude/longitude...")
    df = df.apply(fill_from_latlon, axis=1)

    print("Extracting inspections...")
    inspections_df = df[[
        'inspection_id', 'license_', 'inspection_date', 'inspection_type', 'results',
    ]].drop_duplicates(subset=['inspection_id'])
    
    print("Extracting restaurants...")
    restaurants_df = df[[
        'license_', 'dba_name', 'aka_name', 'facility_type', 'risk',
        'address', 'city', 'state', 'zip', 'latitude', 'longitude'
    ]].drop_duplicates(subset=['license_'])
    
    print("Extracting violations...")
    violations_df = extract_violations(df)

    return inspections_df, restaurants_df, violations_df