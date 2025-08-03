# tests/integration/test_db_writes.py

import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

def test_gold_layer_table_has_data():
    connection = pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM gold_aggregated_complaints")
    count = cursor.fetchone()[0]
    connection.close()
    assert count > 0, "No data found in gold_aggregated_complaints table"
