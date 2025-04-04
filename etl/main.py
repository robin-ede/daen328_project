from extract import fetch_data
from transform import clean_data
from load import load_into_postgres

if __name__ == "__main__":
    print("Starting ETL pipeline...")
    raw_df = fetch_data()
    clean_df, restaurant_df = clean_data(raw_df)
    load_into_postgres(clean_df, restaurant_df)
    print("ETL pipeline completed.")
