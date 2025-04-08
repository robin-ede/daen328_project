import pandas as pd
import re

def drop_unnecessary_columns(df):
  return df.drop(columns=[col for col in df.columns if col=='location' or col.startswith(":@computed_region")])

def parse_dates(df):
  df["inspection_date"] = pd.to_datetime(df["inspection_date"])
  return df

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