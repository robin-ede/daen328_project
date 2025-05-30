import requests
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s"
)
logger = logging.getLogger(__name__)


def fetch_data(limit=50000, max_records=None):
    API_URL = "https://data.cityofchicago.org/resource/4ijn-s7e5.json"
    offset = 0
    all_data = []
    total_fetched = 0

    logger.info("Starting data fetch...")

    while True:
        logger.info(f"Fetching records with offset {offset} and limit {limit}...")
        params = {
            "$limit": limit,
            "$offset": offset
        }
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if not data:
            logger.info("No more data to fetch.")
            break

        all_data.extend(data)
        batch_size = len(data)
        total_fetched += batch_size
        logger.info(f"Fetched {batch_size} records. Total so far: {total_fetched}.")

        offset += limit

        if max_records and len(all_data) >= max_records:
            all_data = all_data[:max_records]
            logger.info(f"Reached max_records limit of {max_records}. Stopping fetch.")
            break

    logger.info(f"Finished fetching data. Total records fetched: {len(all_data)}")
    return pd.DataFrame(all_data)
