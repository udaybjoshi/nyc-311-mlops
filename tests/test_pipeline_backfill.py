import pytest
from orchestration.full_pipeline_flow import full_pipeline
from data.db_utils import get_mysql_connection

def test_full_pipeline_backfill_end_to_end():
    """Run the full ETL pipeline with backfill and verify all layers have data."""
    full_pipeline(backfill_days=7)

    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM bronze_raw_requests")
    bronze = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM silver_cleaned_requests")
    silver = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM gold_aggregated_complaints")
    gold = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    assert bronze > 0, "Bronze is empty after backfill"
    assert silver > 0, "Silver is empty after backfill"
    assert gold > 0, "Gold is empty after backfill"
