# Optional: install dependencies on non-Databricks env
# %pip install pyspark delta-spark requests backoff pyyaml python-dotenv
# %% Import Libraries
import os
from datetime import date, timedelta
from pyspark.sql import functions as F

# %% ---- Unity Catalog & Paths ----
CATALOG = os.getenv("UC_CATALOG", "nyc_311")
BRONZE = os.getenv("UC_BRONZE", "bronze")
SILVER = os.getenv("UC_SILVER", "silver")
GOLD   = os.getenv("UC_GOLD", "gold")

RAW_PATH = os.getenv("RAW_PATH", "s3://<your-bucket>/nyc_311/raw")
BRONZE_CHECKPOINT = os.getenv("BRONZE_CHECKPOINT", "s3://<your-bucket>/nyc_311/bronze/_chk")

BRONZE_TABLE = f"{CATALOG}.{BRONZE}.bronze_311"
SILVER_TABLE = f"{CATALOG}.{SILVER}.silver_311"


# %% Write mode config: only overwrite the partition we write each day
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# %% ---- NYC Open Data API ----
API_BASE  = os.getenv("NYC_API_BASE", "https://data.cityofnewyork.us/resource")
DATASET   = os.getenv("NYC_DATASET_ID", "erm2-nwe9")
APP_TOKEN = os.getenv("NYC_OPENDATA_TOKEN")  # Databricks secret in production
PAGE_SIZE = int(os.getenv("NYC_PAGE_SIZE", "50000"))

# %% Helpers: Paged NYC Open Data Fetch -> Spark DataFrame. 
import backoff, requests
from typing import List, Dict

@backoff.on_exception(backoff.expo, (requests.HTTPError, requests.Timeout), max_tries=5)
def _fetch_page(d_start: datetime, d_end: datetime, offset: int) -> List[Dict]:
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$select": "*",
        "$where": f"created_date >= '{d_start.isoformat()}' AND created_date < '{d_end.isoformat()}'",
        "$order": "created_date ASC",
    }
    r = requests.get(f"{API_BASE}/{DATASET}.json", params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_day_as_spark_df(day: datetime):
    # Fetch one day's worth of records as a Spark DataFrame (paged).
    end = day + timedelta(days=1)
    all_rows: List[Dict] = []
    offset = 0
    while True:
        batch = _fetch_page(day, end, offset)
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    if not all_rows:
        return spark.createDataFrame([], "dummy STRING")  # empty DF
    # Let Spark infer schema from JSON objects
    return spark.createDataFrame(all_rows)

# %% Initial Backfill - Last 1 year -> Raw Parquet (partitioned by p_date)

# Choose the window (default: last 365 days ending yesterday)
DAYS = int(os.getenv("BACKFILL_DAYS", "365"))
today = datetime.utcnow().date()
start_date = today - timedelta(days=DAYS)
end_day    = today - timedelta(days=1)

print(f"Backfilling from {start_day} to {end_day} into {RAW_PATH} (Parquet, partitioned by p_date)")

d = start_day
total = 0
while d <= end_day:
    df = fetch_day_as_spark_df(datetime(d.year, d.month, d.day))
    if df.columns:
        # Normalize partition column
        if "created_date" in df.columns:
            df = df.withColumn("p_date", F.to_date("created_date"))
        else:
            # fallback strictly to the loop date if created_date missing
            df = df.withColumn("p_date", F.lit(d.isoformat()))
        # Write only this partition (dynamic overwrite mode ensures single-part overwrite)
        (df.write.mode("overwrite")
           .format("parquet")
           .partitionBy("p_date")
           .save(RAW_PATH))
        total += df.count()
    d += timedelta(days=1)

print(f"Backfill complete. Rows written: {total}")

# %% Incremental Daily Load -> Raw Parquet (append/partition overwrite)

# By default, load **yesterday**. You can override with LOAD_DATE env as YYYY-MM-DD.
load_date_str = os.getenv("LOAD_DATE")
if load_date_str:
    load_day = datetime.fromisoformat(load_date_str).date()
else:
    load_day = (datetime.utcnow() - timedelta(days=1)).date()

print(f"Incremental load for day: {load_day}")

df = fetch_day_as_spark_df(datetime(load_day.year, load_day.month, load_day.day))
if df.columns:
    if "created_date" in df.columns:
        df = df.withColumn("p_date", F.to_date("created_date"))
    else:
        df = df.withColumn("p_date", F.lit(load_day.isoformat()))
    (df.write.mode("overwrite")
       .format("parquet")
       .partitionBy("p_date")
       .save(RAW_PATH))
    print(f"Wrote partition p_date={load_day}")
else:
    print("No rows for the day; nothing written.")

# %% Auto Loader (Parquet) -> Bronze Delta

# Switch Auto Loader to **parquet**
(spark.readStream.format("cloudFiles")
 .option("cloudFiles.format","parquet")
 .option("cloudFiles.schemaLocation", f"{BRONZE_CHECKPOINT}/schema")
 .load(RAW_PATH)                           # reads all p_date partitions
 .writeStream
 .option("checkpointLocation", f"{BRONZE_CHECKPOINT}/stream")
 .trigger(availableNow=True)
 .toTable(BRONZE_TABLE))

# %% Silver Transform - Clean, Normalize, Deduplicate

b = spark.table(BRONZE_TABLE)

from pyspark.sql import functions as F, Window as W
s = (b
     .withColumn("created_ts", F.to_timestamp("created_date"))
     .withColumn("closed_ts", F.to_timestamp("closed_date"))
     .withColumn("complaint_type", F.trim(F.lower(F.col("complaint_type"))))
     .withColumn("borough", F.trim(F.upper(F.col("borough"))))
     .filter(F.col("unique_key").isNotNull()))

win = W.partitionBy("unique_key").orderBy(F.col("closed_ts").desc_nulls_last(), F.col("created_ts").desc_nulls_last())
s = (s.withColumn("_rn", F.row_number().over(win))
      .filter(F.col("_rn")==1)
      .drop("_rn"))

(s.write.mode("overwrite").saveAsTable(SILVER_TABLE))

display(s.limit(20))
