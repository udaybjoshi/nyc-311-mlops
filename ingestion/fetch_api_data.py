"""
Fetch and store NYC 311 data into the Bronze layer (MySQL).
Supports:
- Single-day ingestion (incremental mode).
- Multi-day historical backfill (last N days).
"""

import os
import requests
import json
import logging
from datetime import datetime, timedelta
from data.db_utils import get_mysql_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/fetch_api_data.log")
    ]
)
logger = logging.getLogger(__name__)

NYC_311_API = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"  # Example endpoint
BRONZE_TABLE = "bronze_raw_requests"


def fetch_data_for_date(date_str: str, limit: int = 1000):
    """
    Fetch 311 complaint data for a specific date from the API.
    Returns a list of raw JSON strings.
    """
    params = {
        "$limit": limit,
        "$where": f"created_date >= '{date_str}T00:00:00' AND created_date < '{date_str}T23:59:59'"
    }
    try:
        logger.info(f"Fetching NYC 311 data for {date_str}...")
        resp = requests.get(NYC_311_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"Fetched {len(data)} records for {date_str}.")
        return [json.dumps(record) for record in data]
    except Exception as e:
        logger.error(f"API fetch failed for {date_str}: {e}", exc_info=True)
        return []


def save_to_mysql(records, date_str: str):
    """
    Insert raw JSON records into the Bronze table.
    Skips duplicates using INSERT IGNORE.
    """
    if not records:
        logger.info(f"No records to save for {date_str}.")
        return

    conn = get_mysql_connection()
    cursor = conn.cursor()

    # Ensure Bronze table exists
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            raw_json JSON,
            created_at DATE,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_date_json (created_at, raw_json(255))
        )
    """)
    conn.commit()

    insert_sql = f"INSERT IGNORE INTO {BRONZE_TABLE} (raw_json, created_at) VALUES (%s, %s)"

    inserted = 0
    for record in records:
        try:
            cursor.execute(insert_sql, (record, date_str))
            inserted += 1
        except Exception as e:
            logger.error(f"Failed to insert record for {date_str}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Inserted {inserted}/{len(records)} Bronze records for {date_str}.")


def backfill_bronze(days: int):
    """
    Fetch and store NYC 311 API data for the last `days` days.
    Runs oldest → newest to maintain chronological order.
    """
    today = datetime.now().date()
    for i in range(days, 0, -1):
        target_date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        logger.info(f"Backfilling Bronze layer for {target_date}...")
        try:
            records = fetch_data_for_date(target_date)
            save_to_mysql(records, target_date)
        except Exception as e:
            logger.error(f"Backfill failed for {target_date}: {e}", exc_info=True)


