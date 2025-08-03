import pandas as pd

def detect_anomalies(df, threshold=1.25):
    """Detects anomalies where actual request counts exceed a multiple of the forecast.

    Args:
        df (pd.DataFrame): A DataFrame with at least 'actual' and 'predicted' columns.
        threshold (float, optional): Multiplier to flag anomalies. Defaults to 1.25.

    Returns:
        pd.DataFrame: Original DataFrame with an added 'is_anomaly' boolean column.
    """
    df['is_anomaly'] = df['actual'] > df['predicted'] * threshold
    return df