"""Silver → Gold daily aggregates and simple lag/MA features."""

from __future__ import annotations
from pyspark.sql import SparkSession, functions as F
from nyc311.utils.config import load_config
from nyc311.utils.logger import get_logger
from nyc311.utils.delta_helpers import overwrite_table

log = get_logger(__name__)

def run(env: str | None = None) -> dict:
    """Aggregate Silver to daily Gold features.

    Creates daily counts grouped by (ds, complaint_type, borough, agency)
    and computes `y_lag7` and `y_ma7`.

    Args:
      env: Environment selector (dev|qa|prod).

    Returns:
      dict: Summary with Gold table FQTN.
    """
    cfg = load_config(env)
    spark = SparkSession.builder.getOrCreate()
    silver = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_silver}.nyc311_clean"
    gold = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_gold}.nyc311_daily"

    df = spark.table(silver).withColumn("ds", F.to_date("created_ts_utc"))
    agg = df.groupBy("ds", "complaint_type", "borough", "agency").agg(F.count("*").alias("y"))

    from pyspark.sql.window import Window
    w = Window.partitionBy("complaint_type", "borough", "agency").orderBy("ds")
    agg = (agg
           .withColumn("y_lag7", F.lag("y", 7).over(w))
           .withColumn("y_ma7", F.avg("y").over(w.rowsBetween(-6, 0))))

    overwrite_table(agg, gold, partitions=["ds"])
    log.info("Gold daily written: %s", gold)
    return {"gold_table": gold}

