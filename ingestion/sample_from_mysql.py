import pandas as pd
from sqlalchemy import create_engine
import os

def get_realistic_forecast_data(start_date="2023-01-01", end_date="2024-01-01"):
    """Fetch daily request volume from gold_aggregated_complaints.

    Args:
        start_date (str): Lower bound for date filter (inclusive).
        end_date (str): Upper bound for date filter (exclusive).

    Returns:
        pd.DataFrame: DataFrame with 'request_date' and 'request_count'.
    """
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@"
        f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
    )

    query = f"""
        SELECT date AS request_date, SUM(complaint_count) AS request_count
        FROM gold_aggregated_complaints
        WHERE date >= '{start_date}' AND date < '{end_date}'
        GROUP BY date
        ORDER BY date
    """

    df = pd.read_sql(query, engine)
    df['request_date'] = pd.to_datetime(df['request_date'])
    df['request_count'] = pd.to_numeric(df['request_count'], errors='coerce').fillna(0)
    return df