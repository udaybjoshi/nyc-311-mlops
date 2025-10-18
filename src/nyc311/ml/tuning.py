"""Optuna tuning for Prophet with MLflow tracking and Registry promotion.

This module:
  * Loads the Gold daily time series.
  * Runs Optuna (default 20 trials) to minimize MAPE on a recent backtest window.
  * Logs each trial to MLflow, registers a model version for each run, and
    tags versions with comparable MAPE.
  * Promotes the best version to **Staging** if it improves over current Staging.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

import optuna
import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
from prophet import Prophet
from pyspark.sql import SparkSession, functions as F

from nyc311.utils.config import load_config
from nyc311.utils.logger import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class TuneConfig:
    """Configuration for the tuning process.

    Attributes:
        horizon_days: Forecast horizon used for evaluation/prediction.
        backtest_days: Days at tail of series used for MAPE evaluation.
        model_name: Registry name for the Prophet model.
    """
    horizon_days: int
    backtest_days: int = 28
    model_name: str = "nyc311_forecaster"


def _load_aggregate_series(spark: SparkSession, gold_fqtn: str) -> pd.DataFrame:
    """Load aggregated (total) daily series from the Gold table.

    Args:
        spark: Active SparkSession.
        gold_fqtn: Fully qualified Gold table name.

    Returns:
        pd.DataFrame: Columns ['ds','y'] sorted by ds ascending.
    """
    sdf = spark.table(gold_fqtn)
    pdf = (sdf.groupBy("ds").agg(F.sum("y").alias("y")).orderBy("ds")).toPandas()
    pdf["ds"] = pd.to_datetime(pdf["ds"])
    return pdf


def _evaluate_mape(pdf: pd.DataFrame, fcst: pd.DataFrame, horizon: int, backtest_days: int) -> float:
    """Compute MAPE over the smaller of `horizon` or `backtest_days`.

    Args:
        pdf: Actuals with columns ['ds','y'].
        fcst: Forecast with columns ['ds','yhat', ...].
        horizon: Target forecast horizon in days.
        backtest_days: Number of tail days to use for evaluation.

    Returns:
        float: Mean absolute percentage error.
    """
    merged = fcst.merge(pdf, on="ds", how="left").dropna()
    tail = merged.tail(min(horizon, backtest_days))
    return float((abs(tail["y"] - tail["yhat"]) / tail["y"].clip(lower=1)).mean())


def _build_model(trial: optuna.trial.Trial) -> Prophet:
    """Create a Prophet model with hyperparameters suggested by an Optuna trial.

    Args:
        trial: Optuna trial providing parameter suggestions.

    Returns:
        Prophet: Configured Prophet model with US holiday effects added.
    """
    params = {
        "growth": trial.suggest_categorical("growth", ["linear"]),
        "changepoint_prior_scale": trial.suggest_float("changepoint_prior_scale", 0.01, 0.5, log=True),
        "seasonality_prior_scale": trial.suggest_float("seasonality_prior_scale", 0.01, 10.0, log=True),
        "holidays_prior_scale": trial.suggest_float("holidays_prior_scale", 0.01, 10.0, log=True),
        "seasonality_mode": trial.suggest_categorical("seasonality_mode", ["additive", "multiplicative"]),
        "weekly_seasonality": trial.suggest_categorical("weekly_seasonality", [True, False]),
        "yearly_seasonality": trial.suggest_categorical("yearly_seasonality", [True, False]),
        "daily_seasonality": False,
    }
    m = Prophet(
        growth=params["growth"],
        changepoint_prior_scale=params["changepoint_prior_scale"],
        seasonality_prior_scale=params["seasonality_prior_scale"],
        holidays_prior_scale=params["holidays_prior_scale"],
        seasonality_mode=params["seasonality_mode"],
        weekly_seasonality=params["weekly_seasonality"],
        yearly_seasonality=params["yearly_seasonality"],
        daily_seasonality=False,
    )
    m.add_country_holidays(country_name="US")
    # Attach params for later logging.
    m._nyc311_params = params  # type: ignore[attr-defined]
    return m


def _log_and_register_model(run_id: str, model_name: str, artifact_path: str,
                            metrics: Dict[str, Any], params: Dict[str, Any]) -> str:
    """Log params/metrics to MLflow and register the model artifact.

    Args:
        run_id: MLflow run ID that contains artifacts/metrics.
        model_name: Target Registry model name.
        artifact_path: Subpath of the logged model within the run.
        metrics: Dictionary of numeric metrics to log/tag.
        params: Dictionary of parameters to log.

    Returns:
        str: Created model version string.
    """
    client = MlflowClient()
    for k, v in params.items():
        client.log_param(run_id, k, v)
    for k, v in metrics.items():
        client.log_metric(run_id, k, float(v))
    model_uri = f"runs:/{run_id}/{artifact_path}"
    mv = client.create_model_version(name=model_name, source=model_uri, run_id=run_id)
    client.set_model_version_tag(name=model_name, version=mv.version, key="mape_eval", value=str(metrics.get("mape")))
    return str(mv.version)


def tune_and_register(env: str | None = None, n_trials: int = 20) -> Dict[str, Any]:
    """Run Optuna tuning, register best model, and promote to Staging if better.

    This function runs an Optuna study with `n_trials`, logs each trial's model to
    MLflow, registers new versions in the Model Registry, and compares MAPE against
    the current Staging version to decide promotion.

    Args:
        env: Environment selector (dev|qa|prod). Defaults to ENV or 'dev'.
        n_trials: Number of Optuna trials to execute.

    Returns:
        Dict[str, Any]: Summary including best MAPE, staging version, and model name.

    Raises:
        RuntimeError: If no data is available or no versions are registered.
    """
    cfg = load_config(env)
    spark = SparkSession.builder.getOrCreate()
    gold = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_gold}.nyc311_daily"

    series = _load_aggregate_series(spark, gold)
    if series.empty:
        raise RuntimeError("No data available for tuning.")

    tcfg = TuneConfig(horizon_days=cfg.pipelines.forecast_horizon_days)
    mlflow.set_tracking_uri("databricks")
    study = optuna.create_study(direction="minimize", study_name=f"prophet_nyc311_{cfg.env}")

    def objective(trial: optuna.trial.Trial) -> float:
        """Optuna objective that returns MAPE for a trial."""
        m = _build_model(trial)
        with mlflow.start_run(run_name=f"optuna_trial_{trial.number}") as run:
            m.fit(series.rename(columns={"y": "y"}))
            future = m.make_future_dataframe(periods=tcfg.horizon_days)
            fcst = m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
            mape = _evaluate_mape(series, fcst, tcfg.horizon_days, tcfg.backtest_days)

            # Log model via pyfunc wrapper to ensure consistent inference API.
            import mlflow.pyfunc

            class ProphetWrapper(mlflow.pyfunc.PythonModel):
                """Thin wrapper to expose Prophet via mlflow.pyfunc interface."""
                def __init__(self, inner):
                    self.inner = inner
                def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
                    return self.inner.predict(model_input)

            mlflow.pyfunc.log_model(
                artifact_path="model",
                python_model=ProphetWrapper(m),
                signature=None,
                input_example=series.head(5),
            )

            metrics = {"mape": mape}
            params = getattr(m, "_nyc311_params", {})
            _log_and_register_model(run.info.run_id, tcfg.model_name, "model", metrics, params)
            return mape

    study.optimize(objective, n_trials=n_trials)

    # Choose best registered version by lowest tagged mape_eval.
    client = MlflowClient()
    versions = client.search_model_versions(f"name='{tcfg.model_name}'")
    best_version = None
    best_mape = 1e9
    for mv in versions:
        tag = mv.tags.get("mape_eval")
        if not tag:
            continue
        try:
            val = float(tag)
        except ValueError:
            continue
        if val < best_mape:
            best_mape = val
            best_version = mv

    if best_version is None:
        raise RuntimeError("No registered versions found after tuning.")

    # Compare to current Staging and promote if improved.
    def _current_stage_mape(stage: str) -> float | None:
        for mv in versions:
            if mv.current_stage == stage:
                tag = mv.tags.get("mape_eval")
                return float(tag) if tag else None
        return None

    current_staging_mape = _current_stage_mape("Staging")
    if (current_staging_mape is None) or (best_mape < current_staging_mape):
        client.transition_model_version_stage(
            name=tcfg.model_name,
            version=best_version.version,
            stage="Staging",
            archive_existing_versions=False,
        )
        client.set_model_version_tag(tcfg.model_name, best_version.version, "promoted_by", "optuna_tuner")
        log.info("Promoted model %s v%s to Staging (MAPE=%.4f)", tcfg.model_name, best_version.version, best_mape)
    else:
        log.info("Existing Staging model kept (%.4f <= %.4f).", current_staging_mape, best_mape)

    return {"study_best_mape": best_mape, "staging_version": best_version.version, "model_name": tcfg.model_name}
