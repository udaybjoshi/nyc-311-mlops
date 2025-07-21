"""
NYC 311 API Ingestion Script with Logging

Pulls daily batches of NYC 311 service request data and stores
raw JSON records into a MySQL database (Bronze layer).
Now uses Python logging for better observability.
"""

import requests
import os
import sys
import json
import argparse
import logging
from datetime import datetime, timedelta
from data.db_utils import get_mysql_connection

# Ensure we are using the correct Python environment
expected_python = os.path.abspath(os.path.join(os.getcwd(), ".venv/bin/python"))
current_python = sys.executable

if not current_python.startswith(expected_python):
    print(f"WARNING: You are using {current_python}, not the project's .venv interpreter.")
    print(f"To fix: run `source .venv/bin/activate` from the project root.")
    sys.exit(1)

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 1000

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/ingestion.log")]
)
logger = logging.getLogger(__name__)

def build_query_params(target_date, offset):
    """Build query parameters for API requests."""
    start = f"{target_date}T00:00:00"
    end = f"{target_date}T23:59:59"
    return {
        "$where": f"created_date between '{start}' and '{end}'",
        "$limit": PAGE_SIZE,
        "$offset": offset
    }

def fetch_data_for_date(target_date):
    """Fetch all NYC 311 data for a given date via pagination."""
    logger.info(f"Fetching NYC 311 data for {target_date}...")
    all_records = []
    offset = 0
    while True:
        params = build_query_params(target_date, offset)
        r = requests.get(BASE_URL, params=params)
        if r.status_code != 200:
            logger.error(f"Error fetching data: {r.status_code}")
            break

        batch = r.json()
        if not batch:
            break

        all_records.extend(batch)
        logger.info(f"Fetched {len(batch)} records (offset {offset})")

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
    return all_records

def save_to_mysql(records, target_date):
    """Save fetched records into MySQL (Bronze layer)."""
    if not records:
        logger.warning("No records to save.")
        return

    conn = get_mysql_connection()
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO bronze_raw_requests (ingestion_date, raw_json)
        VALUES (%s, CAST(%s AS JSON))
    """
    for rec in records:
        cursor.execute(insert_sql, (target_date, json.dumps(rec)))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Saved {len(records)} records to MySQL (Bronze layer).")

def main():
    """Run the ingestion for a given date (defaults to yesterday)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Date to fetch in YYYY-MM-DD format (default: yesterday)")
    args = parser.parse_args()

    target_date = args.date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    records = fetch_data_for_date(target_date)
    save_to_mysql(records, target_date)

if __name__ == "__main__":
    main()

