from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import pandas as pd
import mlflow
import optuna
import matplotlib.pyplot as plt
from ml.validation import validate_forecast_input

def forecast_requests(df, periods=7, log_run=True, use_optuna=True, n_trials=30):
    """Train a Prophet model to forecast NYC 311 request volumes.

    Args:
        df (pd.DataFrame): A DataFrame with columns ['request_date', 'request_count'].
        periods (int, optional): Number of days into the future to forecast. Defaults to 7.
        log_run (bool, optional): If True, logs the run and artifacts to MLflow. Defaults to True.
        use_optuna (bool, optional): If True, runs Optuna to tune Prophet hyperparameters. Defaults to True.
        n_trials (int, optional): Number of Optuna trials. Defaults to 30.

    Returns:
        pd.DataFrame: Forecasted DataFrame with columns ['ds', 'yhat', 'yhat_lower', 'yhat_upper'].
    """
    df = df.rename(columns={'request_date': 'ds', 'request_count': 'y'})
    df = validate_forecast_input(df)

    if use_optuna:
        best_params = tune_prophet(df, n_trials=n_trials)
        model = Prophet(**best_params)
    else:
        model = Prophet()

    model.add_country_holidays(country_name='US')
    model.fit(df)

    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    if log_run:
        with mlflow.start_run():
            mlflow.log_param("forecast_period", periods)
            if use_optuna:
                for k, v in best_params.items():
                    mlflow.log_param(k, v)
            fig = model.plot(forecast)
            plt.title("NYC 311 Forecast")
            fig_path = "forecast_plot.png"
            fig.savefig(fig_path)
            mlflow.log_artifact(fig_path)

    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

def tune_prophet(df, n_trials=30):
    """Run Optuna hyperparameter tuning for a Prophet model.

    Args:
        df (pd.DataFrame): DataFrame with Prophet-formatted columns ['ds', 'y'].
        n_trials (int, optional): Number of optimization trials. Defaults to 30.

    Returns:
        dict: Dictionary of best hyperparameters found by Optuna.
    """
    def objective(trial):
        params = {
            'changepoint_prior_scale': trial.suggest_float('changepoint_prior_scale', 0.001, 0.5, log=True),
            'seasonality_prior_scale': trial.suggest_float('seasonality_prior_scale', 0.01, 10.0, log=True),
            'holidays_prior_scale': trial.suggest_float('holidays_prior_scale', 0.01, 10.0, log=True),
            'seasonality_mode': trial.suggest_categorical('seasonality_mode', ['additive', 'multiplicative']),
            'changepoint_range': trial.suggest_float('changepoint_range', 0.8, 0.95)
        }
        model = Prophet(**params)
        model.add_country_holidays(country_name='US')
        model.fit(df)
        df_cv = cross_validation(model, initial='365 days', period='30 days', horizon='30 days', parallel="threads")
        df_p = performance_metrics(df_cv, rolling_window=1)
        return df_p['mape'].mean()

    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=n_trials)
    return study.best_trial.params