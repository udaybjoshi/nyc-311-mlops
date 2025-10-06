# Databricks notebook source
# MAGIC %md # Bronze Ingestion (last 90d + 1d overlap)

# %%COMMAND ----------
import os, json, subprocess, sys
env = os.getenv("ENV", "dev")
print(f"ENV={env}")
out = subprocess.check_output([sys.executable, "-m", "nyc311.entrypoints.bronze_ingest", "--env", env])
print(json.loads(out.decode()))