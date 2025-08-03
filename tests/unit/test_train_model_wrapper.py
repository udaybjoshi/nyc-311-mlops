import pytest
from unittest.mock import patch
from orchestration.full_pipeline_flow import train_model_task

@patch("orchestration.full_pipeline_flow.train_model")
def test_train_model_task_calls_train_model(mock_train):
    train_model_task()
    mock_train.assert_called_once()