"""Prophet training path (14-day horizon) with US holidays and MLflow logging."""
# %% run cell
from __future__ import annotations
import mlflow
import pandas as pd
from prophet import Prophet
from pyspark.sql import SparkSession, functions as F
from nyc311.utils.config import load_config
from nyc311.utils.logger import get_logger
from nyc311.utils.delta_helpers import overwrite_table

log = get_logger(__name__)

def _load_series(spark: SparkSession, gold_fqtn: str) -> pd.DataFrame:
    """Load total daily series from Gold as Pandas.

    Args:
      spark: Active SparkSession.
      gold_fqtn: Fully qualified Gold table name.

    Returns:
      pd.DataFrame: Columns ['ds','y'] sorted by date.
    """
    sdf = spark.table(gold_fqtn)
    pdf = (sdf.groupBy("ds").agg(F.sum("y").alias("y")).orderBy("ds")).toPandas()
    pdf["ds"] = pd.to_datetime(pdf["ds"])
    return pdf

def train_and_log(pdf: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Train Prophet (with US holidays), log metrics/artifacts, and return forecasts.

    Args:
      pdf: Training series with columns ['ds','y'].
      horizon: Forecast horizon in days.

    Returns:
      pd.DataFrame: Forecast with ['ds','yhat','yhat_lower','yhat_upper'].
    """
    mlflow.set_experiment(None)
    with mlflow.start_run(run_name="prophet_14d_us_holidays"):
        m = Prophet(weekly_seasonality=True, yearly_seasonality=True, daily_seasonality=False)
        m.add_country_holidays(country_name="US")
        m.fit(pdf.rename(columns={"y": "y"}))
        future = m.make_future_dataframe(periods=horizon)
        fcst = m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
        if len(pdf) > 28:
            eval_df = fcst.merge(pdf, on="ds", how="left").dropna()
            tail = eval_df.tail(horizon)
            mape = (abs(tail["y"] - tail["yhat"]) / tail["y"].clip(lower=1)).mean()
            mlflow.log_metric("mape_tail", float(mape))
        mlflow.log_param("horizon_days", horizon)
        mlflow.sklearn.log_model(m, artifact_path="prophet_model")
        return fcst

def run(env: str | None = None) -> dict:
    """End-to-end training and forecast write to Delta.

    Args:
      env: Environment selector (dev|qa|prod).

    Returns:
      dict: Summary with forecast table FQTN.
    """
    cfg = load_config(env)
    spark = SparkSession.builder.getOrCreate()
    gold = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_gold}.nyc311_daily"
    out = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_gold}.nyc311_forecasts"

    series = _load_series(spark, gold)
    if series.empty:
        raise RuntimeError("No data to forecast.")
    fcst = train_and_log(series, cfg.pipelines.forecast_horizon_days)
    sdf = spark.createDataFrame(fcst)
    overwrite_table(sdf, out, partitions=["ds"])
    log.info("Forecasts written: %s", out)
    return {"forecast_table": out}
