"""
Silver → Gold Transformation (Incremental + Backfill + Retry).

- Processes ONLY new Silver records (incremental mode) OR
  reprocesses last `N` days (backfill mode).
- Derives features:
    * date, hour, day_of_week
    * slow_case label (proxy: open complaints)
- Aggregates complaint counts and slow_case_ratio.
- Inserts results into `gold_aggregated_complaints`.
- Includes error handling, retries, and Prefect task integration.
"""

import pandas as pd
import logging
import time
from data.db_utils import get_mysql_connection
from prefect import task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/transform_gold.log")
    ]
)
logger = logging.getLogger(__name__)


def ensure_gold_table():
    """Ensure the Gold table exists with proper schema."""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_aggregated_complaints (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            borough VARCHAR(255),
            complaint_type VARCHAR(255),
            hour INT,
            day_of_week VARCHAR(20),
            complaint_count INT,
            slow_case_ratio DECIMAL(5,2),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Verified Gold table exists.")


def get_last_processed_date():
    """Get the last processed date from Gold for incremental loading."""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM gold_aggregated_complaints")
    last_date = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return last_date


def fetch_new_silver_data(last_date=None):
    """Fetch only new Silver data newer than the last Gold date."""
    conn = get_mysql_connection()
    if last_date:
        query = f"""
            SELECT created_date, complaint_type, borough, status
            FROM silver_cleaned_requests
            WHERE DATE(created_date) > '{last_date}'
        """
    else:
        query = "SELECT created_date, complaint_type, borough, status FROM silver_cleaned_requests"

    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} Silver records for Gold processing.")
    return df


def fetch_backfill_silver_data(days: int):
    """Fetch all Silver data for the last `days` days (for backfill)."""
    conn = get_mysql_connection()
    query = f"""
        SELECT created_date, complaint_type, borough, status
        FROM silver_cleaned_requests
        WHERE DATE(created_date) >= CURDATE() - INTERVAL {days} DAY
        ORDER BY created_date ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} Silver records for backfill ({days} days).")
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive temporal features and aggregate complaint data."""
    if df.empty:
        logger.warning("No Silver records to transform.")
        return pd.DataFrame()

    try:
        df["date"] = pd.to_datetime(df["created_date"], errors="coerce").dt.date
        df["hour"] = pd.to_datetime(df["created_date"], errors="coerce").dt.hour
        df["day_of_week"] = pd.to_datetime(df["created_date"], errors="coerce").dt.day_name()

        df["slow_case"] = df["status"].fillna("").str.contains("OPEN").astype(int)

        df = df.dropna(subset=["date"])

        agg = (
            df.groupby(["date", "borough", "complaint_type", "hour", "day_of_week"])
            .agg(
                complaint_count=("complaint_type", "size"),
                slow_cases=("slow_case", "sum")
            )
            .reset_index()
        )

        agg["slow_case_ratio"] = (agg["slow_cases"] / agg["complaint_count"]).round(2)
        agg.drop(columns=["slow_cases"], inplace=True)

        logger.info(f"Generated {len(agg)} Gold records.")
        return agg

    except Exception as e:
        logger.error(f"Feature engineering failed: {e}", exc_info=True)
        return pd.DataFrame()


def write_to_gold(df: pd.DataFrame, retries: int = 3, delay: int = 5):
    """Insert aggregated Gold data into MySQL with retries."""
    if df.empty:
        logger.info("No Gold records to insert.")
        return

    attempt = 0
    while attempt < retries:
        try:
            conn = get_mysql_connection()
            cursor = conn.cursor()

            insert_sql = """
            INSERT INTO gold_aggregated_complaints
            (date, borough, complaint_type, hour, day_of_week, complaint_count, slow_case_ratio)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            for _, row in df.iterrows():
                cursor.execute(insert_sql, (
                    row["date"],
                    row["borough"],
                    row["complaint_type"],
                    int(row["hour"]),
                    row["day_of_week"],
                    int(row["complaint_count"]),
                    float(row["slow_case_ratio"]),
                ))

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Inserted {len(df)} Gold records.")
            break

        except Exception as e:
            attempt += 1
            logger.error(f"Insert failed (attempt {attempt}/{retries}): {e}", exc_info=True)
            time.sleep(delay)
            if attempt == retries:
                logger.critical("Max retries reached. Gold write failed.")


@task(name="silver-to-gold")
def silver_to_gold_task(backfill_days: int = 0):
    """
    Prefect task: Silver → Gold transformation.
    - Incremental mode (default) processes only new records.
    - Backfill mode (`backfill_days > 0`) reprocesses historical data.
    """
    ensure_gold_table()
    if backfill_days > 0:
        silver_df = fetch_backfill_silver_data(backfill_days)
    else:
        last_date = get_last_processed_date()
        silver_df = fetch_new_silver_data(last_date)

    gold_df = engineer_features(silver_df)
    write_to_gold(gold_df)




