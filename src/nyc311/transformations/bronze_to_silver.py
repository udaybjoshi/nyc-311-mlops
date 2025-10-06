"""Bronze → Silver with Great Expectations and schema enforcement."""

from __future__ import annotations
from pyspark.sql import SparkSession, functions as F, types as T
from nyc311.utils.config import load_config
from nyc311.utils.logger import get_logger
from nyc311.utils.delta_helpers import add_ingestion_partition, upsert_on_key

log = get_logger(__name__)

CORE_MAP = {
    "unique_key": T.LongType(),
    "created_date": T.TimestampType(),
    "closed_date": T.TimestampType(),
    "agency": T.StringType(),
    "agency_name": T.StringType(),
    "complaint_type": T.StringType(),
    "descriptor": T.StringType(),
    "borough": T.StringType(),
    "incident_zip": T.StringType(),
    "latitude": T.DoubleType(),
    "longitude": T.DoubleType(),
    "status": T.StringType(),
}

def _select_cast(df):
    """Select and cast only the core columns defined in CORE_MAP.

    Args:
      df: Input Spark DataFrame from Bronze.

    Returns:
      DataFrame: Narrowed and type-cast DataFrame.
    """
    cols = []
    for src, t in CORE_MAP.items():
        if src in df.columns:
            cols.append(F.col(src).cast(t).alias(src))
    return df.select(*cols)

def _run_expectations(pdf):
    """Run minimal Great Expectations validations on a small sample.

    Args:
      pdf: Pandas DataFrame sample.

    Returns:
      bool: True if validation passes.

    Raises:
      AssertionError: If expectations fail.
    """
    import great_expectations as ge
    gdf = ge.from_pandas(pdf)
    gdf.expect_column_values_to_not_be_null("complaint_type")
    gdf.expect_column_values_to_be_between("latitude", -90, 90, mostly=0.99)
    gdf.expect_column_values_to_be_between("longitude", -180, 180, mostly=0.99)
    result = gdf.validate()
    if not result.success:
        raise AssertionError("Great Expectations validation failed")
    return True

def run(env: str | None = None) -> dict:
    """Transform Bronze table to Silver with schema and DQ checks.

    Steps:
      1. Load Bronze Delta.
      2. Select & cast core columns.
      3. Build UTC timestamps and flags.
      4. Run Great Expectations smoke checks on a small sample.
      5. Upsert into Silver by `unique_key`.

    Args:
      env: Environment selector (dev|qa|prod).

    Returns:
      dict: Summary with Silver table FQTN.
    """
    cfg = load_config(env)
    spark = SparkSession.builder.getOrCreate()
    bronze = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_bronze}.nyc311_raw"
    silver = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_silver}.nyc311_clean"

    df = spark.table(bronze)
    df = _select_cast(df)
    df = (df
           .withColumn("created_ts_utc", F.to_utc_timestamp("created_date", "UTC"))
           .withColumn("closed_ts_utc", F.to_utc_timestamp("closed_date", "UTC"))
           .withColumn("is_closed", F.col("closed_date").isNotNull())
           .drop("created_date", "closed_date"))

    sample_pdf = df.limit(2000).toPandas()
    _run_expectations(sample_pdf)

    df = add_ingestion_partition(df)
    upsert_on_key(spark, df, silver, key_cols=["unique_key"])
    log.info("Silver upserted: %s", silver)
    return {"silver_table": silver}
