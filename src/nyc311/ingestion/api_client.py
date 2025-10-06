"""NYC 311 ingestion for last N days with 1-day overlap to ensure completeness."""

from __future__ import annotations
import os, time
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Dict, Any

import requests
import pandas as pd
from dateutil import parser

from nyc311.utils.logger import get_logger
from nyc311.utils.config import load_config

log = get_logger(__name__)
API_BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

def _window(sample_days: int, overlap_days: int) -> tuple[str, str]:
    """Compute the ingestion time window in UTC ISO format.

    Args:
      sample_days: Number of trailing days to include.
      overlap_days: Extra overlap days to avoid late-arriving data gaps.

    Returns:
      tuple[str, str]: (start_iso, end_iso) inclusive start, exclusive end.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = (now - timedelta(days=sample_days + overlap_days)).replace(hour=0, minute=0, second=0)
    return start.isoformat(), now.isoformat()

def _fetch_page(where: str, limit: int, offset: int, app_token: str | None) -> list[dict[str, Any]]:
    """Fetch a single page from NYC Open Data.

    Args:
      where: SoQL WHERE clause.
      limit: Page size.
      offset: Offset for pagination.
      app_token: Optional NYC app token.

    Returns:
      list[dict]: Page of JSON records.

    Raises:
      HTTPError: If request fails.
    """
    headers = {"X-App-Token": app_token} if app_token else {}
    params = {"$select": "*", "$where": where, "$order": "created_date ASC", "$limit": limit, "$offset": offset}
    r = requests.get(API_BASE, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def _yield_records(start_iso: str, end_iso: str, page_size: int, app_token: str | None) -> Iterator[list[dict[str, Any]]]:
    """Yield paginated result pages for the window.

    Args:
      start_iso: Start of window (UTC ISO).
      end_iso: End of window (UTC ISO).
      page_size: Page size per request.
      app_token: Optional app token.

    Yields:
      list[dict]: A page of records.
    """
    where = f"created_date >= '{start_iso}' AND created_date < '{end_iso}'"
    offset = 0
    while True:
        rows = _fetch_page(where, page_size, offset, app_token)
        if not rows:
            break
        yield rows
        offset += page_size
        time.sleep(0.2)

def ingest_to_bronze(env: str | None = None) -> dict:
    """Ingest last N days of NYC 311 data into the Bronze Delta table.

    This function:
      * Loads environment config.
      * Pages through the NYC Open Data API over the computed window.
      * Normalizes minimal fields.
      * Writes/merges into UC Bronze (Delta), keyed on `unique_key` if present.

    Args:
      env: Environment selector (dev|qa|prod). Defaults to ENV or 'dev'.

    Returns:
      dict: Summary including bronze table name, record count, and window.
    """
    cfg = load_config(env)
    start_iso, end_iso = _window(cfg.pipelines.sample_days, cfg.pipelines.overlap_days)
    log.info("Ingestion window [start,end): %s → %s", start_iso, end_iso)

    frames: list[pd.DataFrame] = []
    for page in _yield_records(start_iso, end_iso, 50000, os.getenv("NYC_311_APP_TOKEN")):
        df = pd.DataFrame(page)
        if df.empty:
            continue
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        if "created_date" in df.columns:
            ts = pd.to_datetime(df["created_date"], utc=True, errors="coerce")
            df["received_year"] = ts.dt.year
            df["received_month"] = ts.dt.month
        frames.append(df)

    total = sum(len(f) for f in frames) if frames else 0
    spark = __import__("pyspark.sql").sql.SparkSession.builder.getOrCreate()
    bronze_fqtn = f"{cfg.databricks.uc_catalog}.{cfg.databricks.uc_bronze}.nyc311_raw"

    if frames:
        sdf = spark.createDataFrame(pd.concat(frames, ignore_index=True))
        from nyc311.utils.delta_helpers import add_ingestion_partition, upsert_on_key, append_table
        sdf = add_ingestion_partition(sdf)
        key_cols = ["unique_key"] if "unique_key" in sdf.columns else []
        if key_cols:
            upsert_on_key(spark, sdf, bronze_fqtn, key_cols)
        else:
            append_table(sdf, bronze_fqtn)

    return {"bronze_table": bronze_fqtn, "records": total, "start": start_iso, "end": end_iso}
