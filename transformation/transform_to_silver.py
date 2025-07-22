"""
Bronze → Silver Transformation Script (Incremental + Backfill Support)

This script processes raw 311 API data stored in the `bronze_raw_requests` table and
transforms it into a cleaned, standardized Silver table (`silver_cleaned_requests`).

Key Features:
    - **Incremental loading:** Only processes new Bronze records since last processed `bronze_id`.
    - **Optional backfill:** Supports historical processing of N past days.
    - **Data cleaning:** Parses `created_date`, standardizes borough and complaint_type fields,
      fills nulls, converts numeric fields, and removes duplicates.
    - **Error handling:** Retries database operations on failure.
    - **Prefect integration:** Exposed as a Prefect `@task` for orchestration.

Tables Used:
    - **Input:** `bronze_raw_requests` (id, ingestion_date, raw_json)
    - **Output:** `silver_cleaned_requests` (bronze_id, created_date, complaint_type, borough, etc.)

Example:
    Run incrementally (default mode):
        >>> python -m transformation.transform_to_silver

    Run backfill for last 30 days:
        >>> python -m transformation.transform_to_silver --backfill 30
"""

import pandas as pd
import json
import logging
import argparse
import time
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
    """
    Retrieve the last processed Bronze record ID from `silver_cleaned_requests`.
    Ensures the Silver table exists (creates if needed).

    Returns:
        int: The highest `bronze_id` already processed (or 0 if none).
    """
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


def fetch_new_bronze_data(last_id: int):
    """
    Fetch raw Bronze records (JSON) newer than the last processed ID.

    Args:
        last_id (int): Highest Bronze record ID already processed.

    Returns:
        pd.DataFrame: DataFrame with `id` and `raw_json` columns for new records.
    """
    conn = get_mysql_connection()
    query = f"SELECT id, raw_json FROM bronze_raw_requests WHERE id > {last_id}"
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} new Bronze records (id > {last_id}).")
    return df


def fetch_backfill_bronze_data(days: int):
    """
    Fetch raw Bronze records for the last N days (for historical backfill).

    Args:
        days (int): Number of days to pull from `bronze_raw_requests`.

    Returns:
        pd.DataFrame: DataFrame with raw JSON for historical processing.
    """
    conn = get_mysql_connection()
    query = f"""
        SELECT id, raw_json FROM bronze_raw_requests
        WHERE ingestion_date >= CURDATE() - INTERVAL {days} DAY
    """
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} Bronze records for backfill (last {days} days).")
    return df


def normalize_and_clean(df: pd.DataFrame):
    """
    Normalize and clean raw Bronze JSON into structured Silver data.

    Cleaning Steps:
        - Flatten nested JSON into tabular columns.
        - Parse `created_date` into datetime.
        - Uppercase/strip text fields (borough, complaint_type, status).
        - Convert `latitude` and `longitude` to numeric (coerce invalid to NaN).
        - Deduplicate by `bronze_id`.

    Args:
        df (pd.DataFrame): Raw Bronze records.

    Returns:
        pd.DataFrame: Cleaned Silver-ready DataFrame.
    """
    if df.empty:
        return pd.DataFrame()

    records = [json.loads(r) for r in df["raw_json"]]
    norm = pd.json_normalize(records)
    cols = ["created_date", "complaint_type", "borough", "status", "latitude", "longitude"]
    norm = norm[cols]

    # Standardization & cleaning
    norm["created_date"] = pd.to_datetime(norm["created_date"], errors="coerce")
    for col in ["complaint_type", "borough", "status"]:
        norm[col] = norm[col].fillna("UNKNOWN").str.upper().str.strip()

    norm["latitude"] = pd.to_numeric(norm["latitude"], errors="coerce")
    norm["longitude"] = pd.to_numeric(norm["longitude"], errors="coerce")

    # Deduplicate
    norm["bronze_id"] = df["id"]
    norm = norm.drop_duplicates(subset=["bronze_id"])

    logger.info(f"Cleaned and transformed {len(norm)} Silver records.")
    return norm


def write_to_silver(df: pd.DataFrame, retries: int = 3, delay: int = 5):
    """
    Insert cleaned Silver records into `silver_cleaned_requests` with retry logic.

    Args:
        df (pd.DataFrame): Cleaned Silver records.
        retries (int): Max retry attempts for DB operations.
        delay (int): Delay (seconds) between retries.
    """
    if df.empty:
        logger.info("No new Silver records to write.")
        return

    attempt = 0
    while attempt < retries:
        try:
            conn = get_mysql_connection()
            cursor = conn.cursor()
            insert_sql = """
                INSERT IGNORE INTO silver_cleaned_requests
                (bronze_id, created_date, complaint_type, borough, status, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            for _, row in df.iterrows():
                cursor.execute(insert_sql, (
                    row["bronze_id"], row["created_date"], row["complaint_type"],
                    row["borough"], row["status"], row["latitude"], row["longitude"]
                ))
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Wrote {len(df)} records to Silver table.")
            break
        except Exception as e:
            attempt += 1
            logger.error(f"Error writing to Silver (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)


@task(name="bronze-to-silver")
def bronze_to_silver_task(backfill_days: int = None):
    """
    Prefect task: Run Bronze → Silver transformation.
    
    Args:
        backfill_days (int, optional): If provided, process historical Bronze data for the past N days.
        Otherwise, runs incrementally (only new Bronze rows).
    """
    if backfill_days:
        bronze_df = fetch_backfill_bronze_data(backfill_days)
    else:
        last_id = get_last_processed_id()
        bronze_df = fetch_new_bronze_data(last_id)
    silver_df = normalize_and_clean(bronze_df)
    write_to_silver(silver_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Bronze raw data to Silver cleaned table.")
    parser.add_argument("--backfill", type=int, help="Process historical data for the past N days.")
    args = parser.parse_args()
    bronze_to_silver_task(backfill_days=args.backfill)


