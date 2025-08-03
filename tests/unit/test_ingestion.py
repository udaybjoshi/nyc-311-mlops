mport pytest
from unittest.mock import patch, MagicMock
from ingestion.fetch_api_data import fetch_data_for_date, save_to_mysql

def test_fetch_data_returns_list():
    result = fetch_data_for_date("2025-08-01")
    assert isinstance(result, list)

@patch("ingestion.fetch_api_data.create_engine")
def test_save_to_mysql(mock_engine):
    mock_conn = MagicMock()
    mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
    mock_records = [{"field1": "value1", "field2": "value2"}]
    save_to_mysql(mock_records, "2025-08-01")
    mock_engine.assert_called_once()