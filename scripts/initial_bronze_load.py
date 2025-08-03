# Create a one-liner script to trigger the initial 2-year bronze load

script_content = """
Script: Initial 2-year Bronze Layer Load

This script runs the NYC 311 API ingestion for the past 730 days
and loads the data into the `bronze_raw_requests` table using
parallel threading for speed.

Usage:
    python scripts/initial_bronze_load.py
"""

import subprocess

# Run the fetch script with 730-day backfill
subprocess.run(["python", "ingestion/fetch_api_data.py", "--backfill", "730", "--parallel", "5"], check=True)

