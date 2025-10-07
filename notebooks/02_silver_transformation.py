"""
Silver Transformation.

Thin wrapper that standardizes Bronze -> Silver with schema enforcement
and Great Expectations smoke checks.
"""

# Databricks notebook source
# MAGIC %md # Silver Transformation

# %%COMMAND ----------
import os
import json
import subprocess
import sys


def main() -> None:
    """Execute Bronze→Silver transformation for the selected environment.

    Uses ENV (dev|qa|prod); defaults to 'dev'.
    Prints the JSON summary returned by the entrypoint.
    """
    env = os.getenv("ENV", "dev")
    out = subprocess.check_output(
        [sys.executable, "-m", "nyc311.entrypoints.silver_transform", "--env", env]
    )
    print(json.loads(out.decode()))


if __name__ == "__main__":
    main()
