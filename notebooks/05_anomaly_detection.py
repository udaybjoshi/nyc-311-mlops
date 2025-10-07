"""
Anomaly Detection (upper bound breaches).

Thin wrapper that flags anomalies where actual y exceeds the forecast upper bound,
and emits multi-day streak alerts.
"""

# Databricks notebook source
# MAGIC %md # Anomaly Detection (upper bound breaches)

# %%COMMAND ----------
import os
import json
import subprocess
import sys


def main() -> None:
    """Run anomaly detection over Gold actuals vs forecasts.

    Uses ENV (dev|qa|prod); defaults to 'dev'.
    Prints the JSON summary returned by the entrypoint.
    """
    env = os.getenv("ENV", "dev")
    out = subprocess.check_output(
        [sys.executable, "-m", "nyc311.entrypoints.anomaly_detect", "--env", env]
    )
    print(json.loads(out.decode()))


if __name__ == "__main__":
    main()
