"""
Forecast (Production Registry Model).

Thin wrapper that always loads `models:/nyc311_forecaster/Production` via mlflow.pyfunc
and writes the next 14 days of forecasts into the Gold schema.
"""

# Databricks notebook source
# MAGIC %md # Forecast (Production Registry Model)
# MAGIC Uses **models:/nyc311_forecaster/Production** for the next 14 days.

# %%COMMAND ----------
import os
import json
import subprocess
import sys


def main() -> None:
    """Run batch inference using the Production model.

    Uses ENV (dev|qa|prod); defaults to 'dev'.
    Prints the JSON summary returned by the entrypoint.
    """
    env = os.getenv("ENV", "dev")
    out = subprocess.check_output(
        [
            sys.executable,
            "-m",
            "nyc311.entrypoints.forecast_infer",
            "--env",
            env,
            "--model-name",
            "nyc311_forecaster",
        ]
    )
    print(json.loads(out.decode()))


if __name__ == "__main__":
    main()
