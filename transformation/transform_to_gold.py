"""
Silver → Gold Transformation Script (Feature Engineering + Aggregation + Forecast Input)

This version is enhanced to support:
    - Feature engineering for Silver → Gold.
    - Return of the aggregated Gold DataFrame for downstream ML tasks (forecasting, anomaly detection).

Output:
    - Writes to `gold_aggregated_complaints` (for persistence).
    - Returns a citywide daily aggregation DataFrame (for forecast/anomaly modules).
"""

import pandas as pd
import logging
import time
from data.db_utils import get_mysql_connection
from prefect import task

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/transform_gold.log")]
)
logger = logging.getLogger(__name__)


def ensure_gold_table():
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


def fetch_silver_data():
    conn = get_mysql_connection()
    query = "SELECT created_date, complaint_type, borough, status FROM silver_cleaned_requests"
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} Silver records for Gold transformation.")
    return df


def engineer_features(df: pd.DataFrame):
    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["created_date"]).dt.date
    df["hour"] = pd.to_datetime(df["created_date"]).dt.hour
    df["day_of_week"] = pd.to_datetime(df["created_date"]).dt.day_name()
    df["slow_case"] = (df["status"].str.contains("OPEN", na=False)).astype(int)

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

    logger.info(f"Engineered {len(agg)} Gold records with features.")
    return agg


def write_to_gold(df: pd.DataFrame, retries: int = 3, delay: int = 5):
    if df.empty:
        logger.info("No Gold records to write.")
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
                    row["date"], row["borough"], row["complaint_type"],
                    int(row["hour"]), row["day_of_week"],
                    int(row["complaint_count"]), float(row["slow_case_ratio"])
                ))
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Wrote {len(df)} records to Gold table.")
            break
        except Exception as e:
            attempt += 1
            logger.error(f"Error writing to Gold (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)


@task(name="silver-to-gold")
def silver_to_gold_task():
    """
    Prefect task: Run Silver → Gold transformation and return citywide daily DataFrame.
    """
    ensure_gold_table()
    silver_df = fetch_silver_data()
    gold_df = engineer_features(silver_df)
    write_to_gold(gold_df)

    # Return daily city-level aggregation for downstream forecast
    if not gold_df.empty:
        daily_df = (
            gold_df.groupby("date")
            .agg(request_count=("complaint_count", "sum"))
            .reset_index()
            .rename(columns={"date": "request_date"})
        )
        logger.info(f"Returning {len(daily_df)} records for forecast/anomaly pipeline.")
        return daily_df

    return pd.DataFrame()





