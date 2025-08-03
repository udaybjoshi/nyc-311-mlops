"""
Script: Plot the forecast output using Plotly.

This assumes forecast_requests() was run and the output is stored in a CSV.
"""

import pandas as pd
import plotly.express as px

df = pd.read_csv("output/test_forecast.csv")

fig = px.line(df, x='ds', y=['yhat', 'yhat_lower', 'yhat_upper'], title="Forecasted NYC 311 Request Volume")
fig.show()