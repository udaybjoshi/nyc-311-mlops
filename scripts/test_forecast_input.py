"""
Test script: Fetch realistic forecast data from MySQL.

This script pulls historical NYC 311 request volumes from the
gold_aggregated_complaints table using the get_realistic_forecast_data utility.
"""

from ingestion.sample_from_mysql import get_realistic_forecast_data

df = get_realistic_forecast_data("2023-01-01", "2024-01-01")
print(df.head())
print(f"Retrieved {len(df)} rows.")