"""Delta helpers: ACID writes, partitioning, and simple upserts."""

from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession, functions as F

def add_ingestion_partition(df: DataFrame) -> DataFrame:
    """Attach an `ingestion_date` column (UTC date) for partitioning.

    Args:
      df: Input Spark DataFrame.

    Returns:
      DataFrame: DataFrame with `ingestion_date` column added.
    """
    return df.withColumn("ingestion_date", F.to_date(F.current_timestamp()))

def overwrite_table(df: DataFrame, fqtn: str, partitions: list[str] | None = None) -> None:
    """Overwrite a Delta table with optional partitioning.

    Args:
      df: Spark DataFrame to write.
      fqtn: Fully qualified table name (catalog.schema.table).
      partitions: Optional list of partition columns.

    Returns:
      None
    """
    w = df
    if partitions:
        w = w.repartition(*[F.col(p) for p in partitions])
    (w.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(fqtn))

def append_table(df: DataFrame, fqtn: str) -> None:
    """Append rows to a Delta table.

    Args:
      df: Spark DataFrame.
      fqtn: Fully qualified table name.

    Returns:
      None
    """
    df.write.format("delta").mode("append").saveAsTable(fqtn)

def upsert_on_key(spark: SparkSession, staged: DataFrame, fqtn: str, key_cols: list[str]) -> None:
    """Upsert rows into a Delta table on specified keys.

    Args:
      spark: Active SparkSession.
      staged: Staged DataFrame to merge.
      fqtn: Fully qualified target table name.
      key_cols: Merge key columns.

    Returns:
      None
    """
    from delta.tables import DeltaTable
    if spark._jsparkSession.catalog().tableExists(fqtn):
        tgt = DeltaTable.forName(spark, fqtn)
        cond = " AND ".join([f"t.{k}=s.{k}" for k in key_cols])
        (tgt.alias("t").merge(staged.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        staged.write.format("delta").mode("overwrite").saveAsTable(fqtn)
