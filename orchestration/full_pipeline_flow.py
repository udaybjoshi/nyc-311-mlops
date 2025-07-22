"""
Full Prefect Pipeline: NYC 311 Medallion Architecture (Bronze → Silver → Gold + ML Training)

This module defines a Prefect flow (`medallion_pipeline`) that:
    1. Ingests NYC 311 API data into the Bronze layer (raw JSON in MySQL).
    2. Cleans, deduplicates, and standardizes the data into the Silver layer.
    3. Engineers features and aggregates data into the Gold layer.
    4. Trains a RandomForest model on Gold features and logs results to MLflow.

Supports:
    - Daily scheduled ingestion (default: yesterday).
    - Manual runs for any date via `target_date` argument.
    - Can be orchestrated by Prefect Orion or Prefect Cloud.
"""

from prefect import flow, task
from datetime import datetime, timedelta
from ingestion.fetch_api_data import fetch_data_for_date, save_to_mysql
from transformation.transform_to_silver import bronze_to_silver_task
from transformation.transform_to_gold import silver_to_gold_task
from ml.train_model import main as train_model
import logging

# Configure logging for the flow
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


@task(name="train-model-task")
def train_model_task():
    """
    Train a machine learning model (RandomForest) on Gold-layer features.

    Behavior:
        - Loads Gold data from MySQL (`gold_aggregated_complaints`).
        - Preprocesses features (encoding, label creation).
        - Trains a RandomForestClassifier to predict "slow" cases.
        - Logs parameters, metrics, and the trained model to MLflow.
    """
    logger.info("Starting model training and MLflow logging...")
    train_model()
    logger.info("Model training complete.")


@flow(name="nyc311-medallion-mlflow-pipeline")
def medallion_pipeline(target_date: str = None):
    """
    Prefect flow to run the complete Medallion + ML pipeline for NYC 311 data.

    Steps:
        1. Bronze (Raw Ingestion): Fetch NYC 311 API data for `target_date`.
        2. Silver (Cleaned): Clean, standardize, and deduplicate new Bronze data.
        3. Gold (Aggregated Features): Engineer features (hour, day, ratios).
        4. ML Training: Train and log a RandomForest model with MLflow.

    Args:
        target_date (str, optional): Date to process ("YYYY-MM-DD").
            Defaults to yesterday if not provided.

    Behavior:
        - Writes raw JSON to `bronze_raw_requests`.
        - Populates `silver_cleaned_requests` with cleaned rows.
        - Populates `gold_aggregated_complaints` with engineered features.
        - Trains and logs model (parameters, metrics, artifact) in MLflow.

    Usage:
        Run manually:
            python -m orchestration.full_pipeline_flow

        Run for specific date:
            python -m orchestration.full_pipeline_flow --target_date 2025-07-19

        Or deploy as a Prefect scheduled flow.
    """
    if not target_date:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"Starting Medallion + ML pipeline for {target_date}")

    # Bronze: Ingest raw data from NYC 311 API
    records = fetch_data_for_date(target_date)
    save_to_mysql(records, target_date)

    # Silver: Clean & standardize Bronze data
    bronze_to_silver_task()

    # Gold: Feature engineering and aggregation
    silver_to_gold_task()

    # ML: Train model and log to MLflow
    train_model_task()

    logger.info(f"Pipeline completed successfully for {target_date}")


if __name__ == "__main__":
    medallion_pipeline()
