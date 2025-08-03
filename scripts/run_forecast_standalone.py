"""
Script: Run a Prophet forecast outside of Prefect pipeline.

This is useful for experimenting with model tuning or generating
forecast output for a single dataset manually.
"""

from ingestion.sample_from_mysql import get_realistic_forecast_data
from ml.forecast import forecast_requests

df = get_realistic_forecast_data("2023-01-01", "2024-01-01")
forecast_df = forecast_requests(df, periods=7, log_run=True, use_optuna=False)

print(forecast_df.tail())