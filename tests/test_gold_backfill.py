import pytest
from transformation.transform_to_gold import silver_to_gold_task
from data.db_utils import get_mysql_connection

def test_gold_aggregation_from_backfill():
    """Ensure Gold aggregates correctly after a backfill run."""
    silver_to_gold_task()

    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM gold_aggregated_complaints")
    gold_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    assert gold_count > 0, "Expected Gold table to have aggregated results after backfill"
