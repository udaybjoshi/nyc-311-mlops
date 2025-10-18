"""Flag anomalies when actual y > yhat_upper (95% CI) for 3+ consecutive days."""

from __future__ import annotations
from pyspark.sql import SparkSession, functions as F, Window
from nyc311.utils.config import load_config
from nyc311.utils.logger import get_logger
from nyc311.utils.delta_helpers import overwrite_table

log = get_logger(__name__)

def run(env: str | None = None) -> dict:
    """Compute anomaly flags from Gold actuals vs forecast upper bound.

    A row is an anomaly if `y > yhat_upper`. We also compute a running streak
    and emit `alert=True` when the streak reaches 3+ days.

    Args:
      env: Environment selector (dev|qa|prod).

    Returns:
      dict: Summary with anomalies table FQTN.
    """
    cfg = load_config(env)
    spark = SparkSession.builder.getOrCreate()
    gold = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_gold}.nyc311_daily"
    fcst = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_gold}.nyc311_forecasts"
    out  = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_gold}.nyc311_anomalies"

    actual = (spark.table(gold).groupBy("ds").agg(F.sum("y").alias("y")))
    pred   = spark.table(fcst).select("ds", "yhat_upper")

    joined = actual.join(pred, "ds", "left")
    flagged = joined.withColumn("is_anomaly", F.col("y") > F.col("yhat_upper"))
    w = Window.orderBy("ds")
    streak = F.sum(F.when(F.col("is_anomaly"), F.lit(1)).otherwise(F.lit(0))).over(
        w.rowsBetween(Window.unboundedPreceding, 0)
    )
    outdf = flagged.withColumn("anomaly_streak", streak).withColumn("alert", F.col("anomaly_streak") >= 3)

    overwrite_table(outdf, out, partitions=["ds"])
    log.info("Anomalies written: %s", out)
    return {"anomalies_table": out}
