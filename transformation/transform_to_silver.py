"""
Incremental Bronze → Silver Transformation with Cleaning and Standardization.

Enhancements:
- Processes only new Bronze records (incremental).
- Parses and standardizes dates.
- Cleans and uppercases text fields (borough, complaint_type).
- Handles nulls.
- Deduplicates by `bronze_id`.
- Exposed as a Prefect @task for orchestration.
"""

import pandas as pd
import json
import logging
from data.db_utils import get_mysql_connection
from prefect import task

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/transform_silver.log")]
)
logger = logging.getLogger(__name__)


def get_last_processed_id():
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_cleaned_requests (
            id INT AUTO_INCREMENT PRIMARY KEY,
            bronze_id INT UNIQUE,
            created_date DATETIME,
            complaint_type VARCHAR(255),
            borough VARCHAR(255),
            status VARCHAR(255),
            latitude DECIMAL(10,7),
            longitude DECIMAL(10,7),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    cursor.execute("SELECT MAX(bronze_id) FROM silver_cleaned_requests")
    last_id = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return last_id if last_id else 0


def fetch_new_bronze_data(last_id):
    conn = get_mysql_connection()
    query = f"SELECT id, raw_json FROM bronze_raw_requests WHERE id > {last_id}"
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} new Bronze records (id > {last_id}).")
    return df


def normalize_and_clean(df):
    if df.empty:
        return pd.DataFrame()

    records = [json.loads(r) for r in df["raw_json"]]
    norm = pd.json_normalize(records)

    # Keep only needed fields
    cols = ["created_date", "complaint_type", "borough", "status", "latitude", "longitude"]
    norm = norm[cols]

    # Cleaning & standardization
    norm["created_date"] = pd.to_datetime(norm["created_date"], errors="coerce")
    norm["complaint_type"] = norm["complaint_type"].fillna("UNKNOWN").str.upper().str.strip()
    norm["borough"] = norm["borough"].fillna("UNKNOWN").str.

