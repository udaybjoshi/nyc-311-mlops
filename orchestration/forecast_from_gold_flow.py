"""
Prefect Flow: NYC 311 Forecasting from Gold Layer

This flow skips ingestion and transformation steps and directly pulls
daily service request data from the Gold table in MySQL to run a forecast.

Intended for scheduled daily or weekly runs where Gold layer data is up-to-date.

Requirements:
    - MLflow configured
    - MySQL with gold_aggregated_complaints table populated
    - Environment variables set: MYSQL_HOST, MYSQL_PORT, etc.
"""

from prefect import flow, task
from ingestion.sample_from_mysql import get_realistic_forecast_data
from ml.forecast import forecast_requests
from ml.anomaly import detect_anomalies
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


@task(name="forecast-and-detect-from-gold")
def forecast_from_gold():
    """Fetches gold data, runs forecast, and detects anomalies."""
    df = get_realistic_forecast_data("2023-01-01", "2024-01-01")

    if df.empty:
        logger.warning("No data retrieved from gold layer.")
        return

    forecast_df = forecast_requests(df, periods=7, log_run=True, use_optuna=False)

    merged = df.rename(columns={'request_date': 'ds', 'request_count': 'actual'}).merge(
        forecast_df[['ds', 'yhat']], on='ds', how='left'
    ).rename(columns={'yhat': 'predicted'})

    result_df = detect_anomalies(merged)
    logger.info("Anomalies detected:")
    logger.info(result_df[result_df['is_anomaly']].tail())

    return result_df


@flow(name="nyc311-forecast-from-gold-pipeline")
def forecast_from_gold_pipeline():
    """Main Prefect flow entry point."""
    logger.info("Launching forecasting pipeline from gold layer...")
    forecast_from_gold()
    logger.info("Pipeline completed successfully.")


if __name__ == "__main__":
    forecast_from_gold_pipeline()