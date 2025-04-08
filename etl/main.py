from extract import fetch_data
from transform import clean_data
from load import load_into_postgres
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s"
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting ETL pipeline...")
    raw_df = fetch_data()
    inspections_df, restaurants_df, violations_df = clean_data(raw_df)
    load_into_postgres(inspections_df, restaurants_df, violations_df)
    logger.info("ETL pipeline completed.")
