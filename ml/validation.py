import pandas as pd

def validate_forecast_input(df: pd.DataFrame):
    """Validates and prepares input data for Prophet forecasting.

    Ensures the DataFrame contains 'ds' and 'y' columns with appropriate types,
    and removes invalid or missing entries.

    Args:
        df (pd.DataFrame): DataFrame with 'ds' and 'y' columns.

    Returns:
        pd.DataFrame: Cleaned and validated DataFrame ready for Prophet.

    Raises:
        ValueError: If required columns are missing or types are invalid.
    """
    required_cols = {'ds', 'y'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"DataFrame must contain columns {required_cols}, but got {df.columns.tolist()}")

    if not pd.api.types.is_datetime64_any_dtype(df['ds']):
        try:
            df['ds'] = pd.to_datetime(df['ds'])
        except Exception as e:
            raise ValueError(f"'ds' column could not be converted to datetime: {e}")

    if not pd.api.types.is_numeric_dtype(df['y']):
        try:
            df['y'] = pd.to_numeric(df['y'], errors='coerce')
        except Exception as e:
            raise ValueError(f"'y' column could not be converted to numeric: {e}")

    df = df.dropna(subset=['ds', 'y'])

    if (df['y'] < 0).any():
        print("Warning: Negative values found in 'y' (request_count). Prophet may not behave well.")

    return df.reset_index(drop=True)