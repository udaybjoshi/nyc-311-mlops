"""
Fetch NYC 311 API data into Bronze.
Supports:
- Single-day fetch
- Parallel backfill (with configurable thread count)
- Per-day ETL logging
"""

import argparse
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
from data.db_utils import get_mysql_connection

NYC_311_API = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/fetch_bronze.log")]
)
logger = logging.getLogger(__name__)


def init_tables():
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_raw_requests (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ingestion_date DATE NOT NULL,
            raw_json JSON NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            run_date DATE NOT NULL,
            records_fetched INT,
            status VARCHAR(20),
            duration_seconds DECIMAL(10,2),
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()


def fetch_data_for_date(target_date: str, limit: int = 5000):
    params = {
        "$where": f"created_date >= '{target_date}T00:00:00' AND created_date < '{target_date}T23:59:59'",
        "$limit": limit
    }
    try:
        resp = requests.get(NYC_311_API, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Fetch failed for {target_date}: {e}")
        return []


def save_to_mysql(records, ingestion_date: str):
    if not records:
        return 0
    conn = get_mysql_connection()
    cursor = conn.cursor()
    insert_sql = "INSERT INTO bronze_raw_requests (ingestion_date, raw_json) VALUES (%s, %s)"
    for record in records:
        cursor.execute(insert_sql, (ingestion_date, pd.io.json.dumps(record)))
    conn.commit()
    cursor.close()
    conn.close()
    return len(records)


def log_etl_run(date_str, count, status="SUCCESS", duration=0.0, error=None):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO etl_log (run_date, records_fetched, status, duration_seconds, error_message)
        VALUES (%s, %s, %s, %s, %s)
    """, (date_str, count, status, duration, error))
    conn.commit()
    cursor.close()
    conn.close()


def process_day(date_str):
    start = time()
    try:
        records = fetch_data_for_date(date_str)
        count = save_to_mysql(records, date_str)
        duration = time() - start
        log_etl_run(date_str, count, "SUCCESS", duration)
        logger.info(f"{date_str}: Inserted {count} rows in {duration:.2f}s")
    except Exception as e:
        duration = time() - start
        log_etl_run(date_str, 0, "FAILURE", duration, str(e))
        logger.error(f"{date_str}: Failed ({e})")


def run_backfill(days: int, parallel: int = 5):
    today = datetime.today()
    dates = [(today - timedelta(days=i+1)).strftime("%Y-%m-%d") for i in range(days)]
    logger.info(f"Starting backfill for {days} days using {parallel} threads")
    init_tables()

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = [executor.submit(process_day, d) for d in dates]
        for _ in as_completed(futures):
            pass  # Each future logs individually


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Fetch a single day (YYYY-MM-DD)")
    parser.add_argument("--backfill", type=int, help="Backfill N days")
    parser.add_argument("--parallel", type=int, default=5, help="Number of threads for backfill")
    args = parser.parse_args()

    init_tables()

    if args.backfill:
        run_backfill(args.backfill, args.parallel)
    elif args.date:
        process_day(args.date)
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        process_day(yesterday)



