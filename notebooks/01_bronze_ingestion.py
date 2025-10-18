"""
Bronze Ingestion (last 90d + 1d overlap).

Thin wrapper that calls the package entrypoint to fetch data from the NYC 311 API
and write/merge into the Bronze Delta table under Unity Catalog.
"""

# Databricks notebook source
# MAGIC %md # Bronze Ingestion (last 90d + 1d overlap)

# %%COMMAND ----------
import os
import json
import subprocess
import sys


def main() -> None:
    """Execute Bronze ingestion for the selected environment.

    Uses ENV (dev|qa|prod) from the environment; defaults to 'dev'.
    Prints the JSON summary returned by the entrypoint.
    """
    env = os.getenv("ENV", "dev")
    out = subprocess.check_output(
        [sys.executable, "-m", "nyc311.entrypoints.bronze_ingest", "--env", env]
    )
    print(json.loads(out.decode()))


if __name__ == "__main__":
    main()


