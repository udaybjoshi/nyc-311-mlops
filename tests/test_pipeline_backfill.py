# tests/test_pipeline_backfill.py

import pytest
from unittest.mock import patch

# Import the flow but alias it to full_pipeline to keep test names consistent
from orchestration.full_pipeline_flow import medallion_pipeline as full_pipeline

@patch("orchestration.full_pipeline_flow.train_model_task")
@patch("orchestration.full_pipeline_flow.silver_to_gold_task")
@patch("orchestration.full_pipeline_flow.bronze_to_silver_task")
@patch("orchestration.full_pipeline_flow.save_to_mysql")
@patch("orchestration.full_pipeline_flow.fetch_data_for_date")
def test_pipeline_mocked(
    mock_fetch,
    mock_save,
    mock_silver,
    mock_gold,
    mock_train
):
    """Test that the pipeline flow runs without executing real tasks."""
    # Arrange: set mock return values
    mock_fetch.return_value = [{"mock": "record"}]
    
    # Act: run the flow
    full_pipeline(target_date="2025-08-01")
    
    # Assert: all mocks were called
    mock_fetch.assert_called_once_with("2025-08-01")
    mock_save.assert_called_once()
    mock_silver.assert_called_once()
    mock_gold.assert_called_once()
    mock_train.assert_called_once()

