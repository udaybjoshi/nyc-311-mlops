"""
NYC 311 API Ingestion Script

This script pulls daily batches of service request data from the NYC 311
Open Data API and stores the raw JSON responses in a MySQL database.
It forms the Bronze layer of the Medallion architecture for this project.

Usage:
    python ingestion/fetch_api_data.py --date YYYY-MM-DD

If no date is provided, it defaults to yesterday.
Environment variables for MySQL connection:
    MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD
"""

import requests
import json
import argparse
from datetime import datetime, timedelta
from data.db_utils import get_mysql_connection

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 1000

def build_query_params(target_date, offset):
    """
    Build query parameters for the NYC 311 API request.

    Args:
        target_date (str): The date to fetch data for in 'YYYY-MM-DD' format.
        offset (int): The pagination offset for fetching results in batches.

    Returns:
        dict: Dictionary of query parameters including date filter, limit, and offset.
    """
    start = f"{target_date}T00:00:00"
    end = f"{target_date}T23:59:59"
    return {
        "$where": f"created_date between '{start}' and '{end}'",
        "$limit": PAGE_SIZE,
        "$offset": offset
    }

def fetch_data_for_date(target_date):
    """
    Fetch all NYC 311 service request records for a given date.

    This function paginates through the API, retrieving up to 1000 records per
    page until no more records remain.

    Args:
        target_date (str): The date to fetch data for in 'YYYY-MM-DD' format.

    Returns:
        list: List of JSON objects representing 311 service requests.
    """
    print(f"Fetching NYC 311 data for {target_date}...")
    all_records = []
    offset = 0
    while True:
        params = build_query_params(target_date, offset)
        r = requests.get(BASE_URL, params=params)
        if r.status_code != 200:
            print(f"Error fetching data: {r.status_code}")
            break

        batch = r.json()
        if not batch:
            break

        all_records.extend(batch)
        print(f"Fetched {len(batch)} records (offset {offset})")

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
    return all_records

def save_to_mysql(records, target_date):
    """
    Insert raw NYC 311 API records into the Bronze layer MySQL table.

    Args:
        records (list): List of JSON objects to insert.
        target_date (str): The date associated with the ingestion (partition key).

    Returns:
        None
    """
    if not records:
        print("No records to save.")
        return

    conn = get_mysql_connection()
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO bronze_raw_requests (ingestion_date, raw_json)
        VALUES (%s, CAST(%s AS JSON))
    """

    for rec in records:
        cursor.execute(insert_sql, (target_date, json.dumps(rec)))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Saved {len(records)} records to MySQL (Bronze layer).")

def main():
    """
    CLI entry point for the ingestion script.

    Parses the optional --date argument, fetches NYC 311 API data for the date,
    and saves the results into the Bronze layer MySQL table.

    Usage:
        python ingestion/fetch_api_data.py --date YYYY-MM-DD
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Date to fetch in YYYY-MM-DD format (default: yesterday)")
    args = parser.parse_args()

    target_date = args.date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    records = fetch_data_for_date(target_date)
    save_to_mysql(records, target_date)

if __name__ == "__main__":
    main()
