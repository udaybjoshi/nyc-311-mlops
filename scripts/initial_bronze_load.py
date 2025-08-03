"""
Script: Initial 2-year Bronze Layer Load

This script runs the NYC 311 API ingestion for the past 730 days
and loads the data into the `bronze_raw_requests` table using
parallel threading for speed.
"""

import subprocess
import os

# Paths
this_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(this_dir, ".."))
script_path = os.path.join(project_root, "ingestion", "fetch_api_data.py")

# Inherit and override environment
env = os.environ.copy()
env["PYTHONPATH"] = project_root

# Run the script with PYTHONPATH set correctly
subprocess.run(
    ["python", script_path, "--backfill", "730", "--parallel", "5"],
    cwd=project_root,
    env=env,
    check=True
)

