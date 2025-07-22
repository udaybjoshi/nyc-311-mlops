import pytest
from ingestion.fetch_api_data import fetch_data_for_date, save_to_mysql
from data.db_utils import get_mysql_connection

@pytest.mark.parametrize("days", [7])
def test_bronze_backfill_loads_expected_rows(days):
    """Verify that Bronze backfill loads data for the last `days` days."""
    # Trigger backfill
    fetch_data_for_date(backfill=True, days=days)

    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM bronze_raw_requests WHERE ingestion_date >= CURDATE() - INTERVAL %s DAY", (days,))
    row_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    assert row_count > 0, f"Expected Bronze table to have rows for last {days} days"
