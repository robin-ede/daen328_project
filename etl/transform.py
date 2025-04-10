import pandas as pd
import re
import pgeocode
from geopy.geocoders import Nominatim
import time
import hashlib
import logging
from rapidfuzz import process

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s"
)
logger = logging.getLogger(__name__)

def drop_unnecessary_columns(df):
  return df.drop(columns=[col for col in df.columns if col=='location' or col.startswith(":@computed_region")])

def parse_dates(df):
  df["inspection_date"] = pd.to_datetime(df["inspection_date"])
  return df

def preprocess_city(city):
  if not isinstance(city, str):
        return city
  city = re.sub(r'[^A-Z\s]', '', city)  # Remove non-letters
  city = re.sub(r'\s+', ' ', city)      # Normalize whitespace
  return city.strip()

trusted_cities = [
    'CHICAGO', 'EVANSTON', 'SCHAUMBURG', 'MAYWOOD', 'ELK GROVE VILLAGE',
    'SKOKIE', 'OAK PARK', 'CICERO', 'BERWYN', 'ELMHURST', 'NILES',
    'MERRILLVILLE', 'CALUMET CITY', 'WORTH', 'SUMMIT', 'PLAINFIELD',
    'HIGHLAND PARK', 'NAPERVILLE', 'ALSIP', 'BRIDGEVIEW', 'ROSEMONT',
    'SCHILLER PARK', 'EAST HAZEL CREST', 'STREAMWOOD', 'BLUE ISLAND',
    'CHICAGO HEIGHTS', 'OAK LAWN', 'BURNHAM', 'LAKE ZURICH', 'BURBANK',
    'EVERGREEN PARK', 'MATTESON', 'BROOKFIELD', 'GRAYSLAKE', 'HAMMOND',
    'WHEATON', 'WILMETTE', 'WADSWORTH', 'LANSING', 'NEW HOLSTEIN',
    'ALGONQUIN', 'GRIFFITH', 'MORTON GROVE', 'WESTERN SPRINGS',
    'TORRANCE', 'WHITING', 'GLEN ELLYN', 'LOS ANGELES', 'WESTMONT',
    'OLYMPIA FIELDS', 'NORRIDGE', 'BLOOMINGDALE', 'PALOS PARK',
    'LAKE BLUFF', 'LOMBARD', 'JUSTICE', 'BOLINGBROOK',
    'COUNTRY CLUB HILLS', 'TINLEY PARK', 'DES PLAINES', 'GLENCOE',
    'FRANKFORT', 'BROADVIEW'
]

def fuzzy_correct(city, choices, threshold=85):
    if not city or not isinstance(city, str):
        return city
    result = process.extractOne(city, choices)
    if result is None:
        return city
    match, score, _ = result
    return match if score >= threshold else city

def generate_restaurant_id(row):
    string = f"{row['license_']}_{row['address']}_{row['zip']}_{row['city']}"
    return hashlib.md5(string.encode()).hexdigest()

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
            logger.info(f"Geocoding error: {e}")
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
    logger.info("Cleaning data...")
    logger.info("Dropping unnecessary columns...")
    df = drop_unnecessary_columns(df)
    logger.info("Parsing dates...")
    df = parse_dates(df)
    logger.info("Filling missing city/state/zip from ZIP code...")
    df = df.apply(fill_from_zip, axis=1)
    logger.info("Filling missing city/state/zip from latitude/longitude...")
    df = df.apply(fill_from_latlon, axis=1)
    logger.info("Processing city names...")
    df['city'] = df['city'].apply(preprocess_city)
    df['city'] = df['city'].apply(lambda x: fuzzy_correct(x, trusted_cities))
    
    for city, count in df['city'].value_counts().items():
        logger.info(f"City: {city}, Count: {count}")
    
    # logger.info(df[df[['city', 'state', 'zip']].isnull().any(axis=1)])
    
    logger.info("Generating restaurant IDs...")
    df['restaurant_id'] = df.apply(generate_restaurant_id, axis=1)

    logger.info("Extracting inspections...")
    inspections_df = df[[
        'inspection_id', 'restaurant_id', 'inspection_date', 'inspection_type', 'results',
    ]].drop_duplicates(subset=['inspection_id'])
    
    logger.info("Extracting restaurants...")
    restaurants_df = df[[
        'restaurant_id', 'license_', 'dba_name', 'aka_name', 'facility_type', 'risk',
        'address', 'city', 'state', 'zip', 'latitude', 'longitude'
    ]].drop_duplicates(subset=['restaurant_id'])
    
    logger.info("Extracting violations...")
    violations_df = extract_violations(df)

    return inspections_df, restaurants_df, violations_df