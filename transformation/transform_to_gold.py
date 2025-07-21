"""
Silver → Gold Transformation with Feature Engineering.

Features added:
- Day of week.
- Hour of day.
- Complaint frequency (aggregated counts).
- Label: "slow_case" if open > 5 days (requires closed_date, if available).

Exposed as a Prefect @task.
"""

import pandas as pd
import logging
from data.db_utils import get_mysql_connection
from prefect import task

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
    query = """
        SELECT created_date, complaint_type, borough, status
        FROM silver_cleaned_requests
    """
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} Silver records for Gold aggregation.")
    return df


def engineer_features(df):
    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["created_date"]).dt.date
    df["hour"] = pd.to_datetime(df["created_date"]).dt.hour
    df["day_of_week"] = pd.to_datetime(df["created_date"]).dt.day_name()

    # Placeholder "slow_case" label (since we may not have closed_date)
    df["slow_case"] = (df["status"].str.contains("OPEN")).astype(int)

    # Aggregation by date, borough, complaint_type, hour, day_of_week
    agg = (
        df.groupby(["date", "borough", "complaint_type", "hour", "day_of_week"])
        .agg(
            complaint_count=("complaint_type", "size"),
            slow_cases=("slow_case", "sum")
        )
        .reset_index()
    )

    # Calculate slow case ratio
    agg["slow_case_ratio"] = (agg["slow_cases"] / agg["complaint_count"]).round(2)
    agg.drop(columns=["slow_cases"], inplace=True)

    logger.info(f"Engineered {len(agg)} Gold records with features.")
    return agg


def write_to_gold(df):
    if df.empty:
        logger.info("No Gold records to write.")
        return

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
    logger.info(f"Wrote {len(df)} Gold records to table.")


@task(name="silver-to-gold")
def silver_to_gold_task():
    ensure_gold_table()
    silver_df = fetch_silver_data()
    gold_df = engineer_features(silver_df)
    write_to_gold(gold_df)
