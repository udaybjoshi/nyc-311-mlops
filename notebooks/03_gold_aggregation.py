"""
Gold Daily Aggregates.

Thin wrapper that builds daily counts and simple lag/MA features from Silver,
writing a Delta table partitioned by date.
"""

# Databricks notebook source
# MAGIC %md # Gold Daily Aggregates

# %%COMMAND ----------
import os
import json
import subprocess
import sys


def main() -> None:
    """Execute Silver→Gold aggregation for the selected environment.

    Uses ENV (dev|qa|prod); defaults to 'dev'.
    Prints the JSON summary returned by the entrypoint.
    """
    env = os.getenv("ENV", "dev")
    out = subprocess.check_output(
        [sys.executable, "-m", "nyc311.entrypoints.gold_aggregate", "--env", env]
    )
    print(json.loads(out.decode()))


if __name__ == "__main__":
    main()
