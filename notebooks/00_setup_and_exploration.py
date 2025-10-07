"""
Setup & Exploration.

Utility notebook to sanity-check Spark/Unity Catalog connectivity and quickly
sample Bronze/Silver/Gold tables without modifying pipeline state.
"""

# Databricks notebook source
# MAGIC %md # Setup & Exploration

# %%COMMAND ----------
from nyc311.utils.config import load_config


def main() -> None:
    """Run basic environment and table sanity checks.

    Prints the resolved catalog/schema names and displays a small sample from any
    existing tables (if they exist).
    """
    cfg = load_config()
    spark = spark  # noqa: F821

    print("Catalog:", cfg.databricks.uc_catalog)
    print("Schemas:", cfg.databricks.uc_bronze, cfg.databricks.uc_silver, cfg.databricks.uc_gold)

    def _show_if_exists(fqtn: str, n: int = 5):
        if spark._jsparkSession.catalog().tableExists(fqtn):
            print(f"Sample from {fqtn}:")
            display(spark.table(fqtn).limit(n))
        else:
            print(f"(missing) {fqtn}")

    _show_if_exists(f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_bronze}.nyc311_raw")
    _show_if_exists(f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_silver}.nyc311_clean")
    _show_if_exists(f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_gold}.nyc311_daily")


if __name__ == "__main__":
    main()
