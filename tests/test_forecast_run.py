import pandas as pd
import pytest
from notebooks.forecast import forecast_requests

def test_forecast_requests_basic():
    """Test forecast_requests with synthetic input.

    Ensures that the function:
    - Accepts a DataFrame with 'request_date' and 'request_count'
    - Returns a DataFrame with Prophet forecast output
    - Produces the expected columns and row count

    Raises:
        AssertionError: If the output does not match expected format or length.
    """
    df = pd.DataFrame({
        'request_date': pd.date_range(start="2023-01-01", periods=60, freq='D'),
        'request_count': [100 + (i % 7) * 10 for i in range(60)]
    })

    forecast_df = forecast_requests(df, periods=7, log_run=False, use_optuna=False)

    assert isinstance(forecast_df, pd.DataFrame)
    assert all(col in forecast_df.columns for col in ['ds', 'yhat', 'yhat_lower', 'yhat_upper'])
    assert len(forecast_df) >= 67  # 60 input + 7 forecast days
