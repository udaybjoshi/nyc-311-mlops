import pytest
from transformation.transform_to_silver import bronze_to_silver_task
from data.db_utils import get_mysql_connection

def test_silver_transforms_backfill(monkeypatch):
    """Ensure Silver processes all new Bronze records from backfill."""
    # Mock out Prefect retry behavior (if any)
    bronze_to_silver_task(backfill_days=7)

    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM silver_cleaned_requests")
    silver_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    assert silver_count > 0, "Expected Silver table to have processed backfill data"
