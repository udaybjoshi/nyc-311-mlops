"""
Bronze → Silver Transformation (Incremental, Production-Ready).

Features:
- Processes ONLY new Bronze records since the last processed `bronze_id`.
- Cleans and normalizes:
    * Parses dates
    * Uppercases & trims borough, complaint_type, status
    * Fills nulls
    * Deduplicates by `bronze_id`
- Inserts cleaned results into `silver_cleaned_requests`.
- Includes error handling and retry logic.
- Exposed as a Prefect @task for orchestration.
"""

import pandas as pd
import json
import logging
import time
from data.db_utils import get_mysql_connection
from prefect import task

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/transform_silver.log")
    ]
)
logger = logging.getLogger(__name__)


def ensure_silver_table():
    """Ensure the Silver table exists with proper schema."""
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
    cursor.close()
    conn.close()
    logger.info("Verified Silver table exists.")


def get_last_processed_id():
    """Get the last processed Bronze ID from Silver for incremental loading."""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(bronze_id) FROM silver_cleaned_requests")
    last_id = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return last_id if last_id else 0


def fetch_new_bronze_data(last_id: int):
    """Fetch only new Bronze data since the last processed ID."""
    conn = get_mysql_connection()
    query = f"SELECT id, raw_json FROM bronze_raw_requests WHERE id > {last_id}"
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} new Bronze records (id > {last_id}).")
    return df

def fetch_backfill_bronze_data(days: int):
    """Fetch all Bronze data for the last `days` days (for backfill)."""
    conn = get_mysql_connection()
    query = f"""
        SELECT id, raw_json
        FROM bronze_raw_requests
        WHERE created_at >= CURDATE() - INTERVAL {days} DAY
        ORDER BY id ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} Bronze records for backfill ({days} days).")
    return df


def normalize_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize JSON, clean fields, and deduplicate."""
    if df.empty:
        logger.warning("No new Bronze records to process.")
        return pd.DataFrame()

    try:
        # Parse JSON
        records = [json.loads(r) for r in df["raw_json"]]
        norm = pd.json_normalize(records)

        # Keep only needed fields
        cols = ["created_date", "complaint_type", "borough", "status", "latitude", "longitude"]
        norm = norm[cols]

        # Clean and standardize
        norm["created_date"] = pd.to_datetime(norm["created_date"], errors="coerce")
        norm["complaint_type"] = norm["complaint_type"].fillna("UNKNOWN").str.upper().str.strip()
        norm["borough"] = norm["borough"].fillna("UNKNOWN").str.upper().str.strip()
        norm["status"] = norm["status"].fillna("UNKNOWN").str.upper().str.strip()
        norm["latitude"] = pd.to_numeric(norm["latitude"], errors="coerce")
        norm["longitude"] = pd.to_numeric(norm["longitude"], errors="coerce")

        # Deduplicate by Bronze ID
        norm["bronze_id"] = df["id"]
        norm = norm.drop_duplicates(subset=["bronze_id"])

        logger.info(f"Cleaned and normalized {len(norm)} Silver records.")
        return norm

    except Exception as e:
        logger.error(f"Data normalization failed: {e}", exc_info=True)
        return pd.DataFrame()  # Return empty DataFrame on failure


def write_to_silver(df: pd.DataFrame, retries: int = 3, delay: int = 5):
    """Insert cleaned Silver data into MySQL with retries."""
    if df.empty:
        logger.info("No new Silver records to insert.")
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
                    row["bronze_id"],
                    row["created_date"],
                    row["complaint_type"],
                    row["borough"],
                    row["status"],
                    row["latitude"],
                    row["longitude"]
                ))

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Inserted {len(df)} records into Silver table.")
            break  # Exit loop if successful

        except Exception as e:
            attempt += 1
            logger.error(f"Failed to insert Silver data (attempt {attempt}/{retries}): {e}", exc_info=True)
            time.sleep(delay)
            if attempt == retries:
                logger.critical("Max retries reached. Silver write failed.")


@task(name="bronze-to-silver")
def bronze_to_silver_task(backfill_days: int = 0):
    """
    Prefect task: Incremental Bronze → Silver transformation with optional backfill.
    If backfill_days > 0, loads last `backfill_days` days from Bronze.
    Otherwise, only processes new records incrementally.
    """
    ensure_silver_table()
    if backfill_days > 0:
        bronze_df = fetch_backfill_bronze_data(backfill_days)
    else:
        last_id = get_last_processed_id()
        bronze_df = fetch_new_bronze_data(last_id)

    silver_df = normalize_and_clean(bronze_df)
    write_to_silver(silver_df)


