import os
import time
import uuid
import json
import requests
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pyspark.sql.types import *
from pyspark.sql import functions as F
import argparse

# Load environment variables from .env file
load_dotenv()

# --------------------------------------
# Spark setup
# --------------------------------------
spark = SparkSession.builder.appName("Ingest_Bronze_Measurements").getOrCreate()

# --------------------------------------
# Country code input
# --------------------------------------
parser = argparse.ArgumentParser(description="Ingest OpenAQ measurements for sensors filtered by country.")
parser.add_argument("--country", "--country_code", dest="country_code", help="ISO country code (e.g. PT, ES)")
args, _ = parser.parse_known_args()
COUNTRY_CODE = (args.country_code or os.getenv("COUNTRY_CODE") or "PT").upper()

# --------------------------------------
# Set database & table names
# --------------------------------------
DATABASE = os.getenv("DATABASE", "airq")
BRONZE_TABLE_MEASUREMENTS = f"{DATABASE}.bronze_measurements_batches"
DIM_TABLE_SENSORS = f"{DATABASE}.dim_sensors"

# --------------------------------------
# Set values for API calls
# --------------------------------------
OPENAQ_API_BASE_URL = os.getenv("OPENAQ_API_V3_BASE_URL", "https://api.openaq.org/v3")
PAGE_LIMIT = 1000  # API pagination size
HEADERS = {'x-api-key': os.getenv("OPENAQ_API_KEY", "")}

# --------------------------------------
# Set values for rate limiting
# --------------------------------------
RATE_LIMIT_PER_MIN = 60
CONCURRENT_WORKERS = 1
SAFETY_RATIO = 0.99

# --------------------------------------
# Set values for ingestion window
# --------------------------------------
MIN_LAG_HOURS = 2
FIRST_RUN_HOURS = 4
MAX_CHUNK_HOURS = 8

# --------------------------------------
# Create database & bronze table if missing
# --------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# Define bronze table schema
SCHEMA_BRONZE_BRONZE_TABLE_MEASUREMENTS = StructType([
    StructField("batch_id", StringType(), True),
    StructField("sensor_id", IntegerType(), True),
    StructField("parameter_id", StringType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("country_code", StringType(), True),
    StructField("ingestion_datetime", StringType(), True),
    StructField("ingestion_window_datetime_from", StringType(), True),
    StructField("ingestion_window_datetime_to", StringType(), True),
    StructField("rows_fetched", IntegerType(), True),
    StructField("api_payload", StringType(), True)
])

if not spark.catalog.tableExists(BRONZE_TABLE_MEASUREMENTS):
    spark.createDataFrame([], SCHEMA_BRONZE_BRONZE_TABLE_MEASUREMENTS).write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE_MEASUREMENTS)
    print(f"✅ Created empty Delta table: {BRONZE_TABLE_MEASUREMENTS}")

# --------------------------------------
# Function: Fetch last ingestion window for a country
# --------------------------------------
def get_last_ingestion_window(country_code):
    df = (spark.table(BRONZE_TABLE_MEASUREMENTS)
              .filter(F.col("country_code") == country_code)
              .orderBy(F.col("ingestion_window_datetime_to").desc())
              .limit(1))
    if df.count() == 0:
        return None, None
    row = df.collect()[0]
    last_date_from = row["ingestion_window_datetime_from"]
    last_date_to = row["ingestion_window_datetime_to"]
    return (datetime.fromisoformat(last_date_from) if last_date_from else None,
            datetime.fromisoformat(last_date_to) if last_date_to else None)

# --------------------------------------
# Function: Compute ingestion window
# --------------------------------------
def compute_ingestion_window(last_date_to_iso: str | None,
                             now_utc: datetime,
                             min_lag_hours: int,
                             first_run_hours: int,
                             max_chunk_hours: int):
    """
    Returns (window_from, window_to) respecting API constraint: window_to <= now - min_lag_hours.
    last_date_to_iso: ISO string or None
    """
    end_cap = now_utc - timedelta(hours=min_lag_hours)

    if last_date_to_iso is None:
        window_to = end_cap
        window_from = end_cap - timedelta(hours=first_run_hours)
        return window_from, window_to

    last_date_to = datetime.fromisoformat(last_date_to_iso)
    if last_date_to >= end_cap:
        return None, None  # up to date

    gap_hours = (end_cap - last_date_to).total_seconds() / 3600.0
    chunk_hours = min(gap_hours, max_chunk_hours)

    window_from = last_date_to
    window_to = last_date_to + timedelta(hours=chunk_hours)
    if window_to > end_cap:
        window_to = end_cap
    return window_from, window_to

# --------------------------------------
# Define ingestion window
# --------------------------------------
now_utc = datetime.now(timezone.utc)
last_from, last_to = get_last_ingestion_window(COUNTRY_CODE)  # your existing function
window = compute_ingestion_window(last_to.isoformat() if last_to else None, now_utc, MIN_LAG_HOURS, FIRST_RUN_HOURS, MAX_CHUNK_HOURS)
if window == (None, None):
    print("No ingestion needed (already up to date).")
    exit(0)
else:
    date_from, date_to = window
    print(f"Ingestion window: {date_from.isoformat()} -> {date_to.isoformat()}")

# --------------------------------------
# Function: Compute sleep interval to avoid rate limiting while optimizing sleep time between requests
# --------------------------------------
def compute_sleep_interval(
    rate_limit_per_min,
    concurrent_workers,
    safety_ratio,
    avg_request_time_sec
):
    """
    Compute seconds to sleep after an API call to stay below the rate limit.
    rate_limit_per_min: allowed requests per minute (e.g. 60)
    concurrent_workers: parallel processes hitting the same key
    safety_ratio: use a fraction of the limit (0.9 = 90%)
    avg_request_time_sec: elapsed HTTP time (r.elapsed.total_seconds())
    """
    effective_limit = rate_limit_per_min * safety_ratio
    per_worker_limit = effective_limit / max(concurrent_workers, 1)
    target_interval = 60.0 / per_worker_limit          # desired start-to-start spacing
    used_time = avg_request_time_sec or 0.0             # time already spent in the request
    return max(target_interval - used_time, 0.0)

# --------------------------------------
# Function: Fetch measurements
# --------------------------------------
def fetch_measurements(sensor_id, date_from, date_to):
    """Fetches all measurements for a given sensor within a time window."""
    page = 1
    results = []
    end_formatted = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_formatted = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        url = f"{OPENAQ_API_BASE_URL}/sensors/{sensor_id}/measurements"
        headers = HEADERS
        params = {
            "limit": PAGE_LIMIT,
            "page": page,
            "datetime_from": start_formatted,
            "datetime_to": end_formatted
        }

        r = requests.get(url, headers=headers, params=params)
        if r.status_code != 200:
            print(f"⚠️ Failed for sensor {sensor_id}: {r.status_code}")
            break
        else:
            elapsed = r.elapsed.total_seconds()
            print(f"⏱️ Fetched page {page} for sensor {sensor_id} in {elapsed:.2f}s")

        # Dynamic sleep before next page (or next sensor)
        sleep_secs = compute_sleep_interval(
            rate_limit_per_min=RATE_LIMIT_PER_MIN,
            concurrent_workers=CONCURRENT_WORKERS,
            safety_ratio=SAFETY_RATIO,
            avg_request_time_sec=elapsed
        )
        if sleep_secs > 0:
            print (f"💤 Sleeping for {sleep_secs:.2f}s to respect rate limits...")
            time.sleep(sleep_secs)

        payload = r.json()
        data = payload.get("results", [])
        if not data:
            break

        results.extend(data)
        if len(data) < PAGE_LIMIT:
            break  # no more pages

        page += 1

    return results

# --------------------------------------
# Function: Fetch sensors for a specific country
# --------------------------------------
def fetch_sensors(COUNTRY_CODE):
    """Fetches sensors from the dimension table for a specific country."""
    sensors_df = spark.table(DIM_TABLE_SENSORS).select("sensor_id", "location_id", "parameter_id")
    locations_df = spark.table(f"{DATABASE}.dim_locations").select("location_id", "country_code")
    
    # Use inner if every sensor must have a valid location
    joined = sensors_df.join(locations_df, on="location_id", how="inner") \
                       .filter(F.col("country_code") == COUNTRY_CODE)
    
    total = joined.count()
    return joined.toLocalIterator(), total

# --------------------------------------
# Helper: Format seconds as HH:MM:SS.ss
# --------------------------------------
def fmt_secs(s):
    m, sec = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{int(h):02d}:{int(m):02d}:{sec:05.2f}"

# --------------------------------------
# Step 1: Fetch sensors for the specified country
# --------------------------------------
sensor_iter, total_sensors = fetch_sensors(COUNTRY_CODE)

# --------------------------------------
# Step 2: Ingest measurements for each sensor into bronze table
# --------------------------------------
batch_id = str(uuid.uuid4())
ingestion_datetime = datetime.now(timezone.utc)

step2_start = time.time()
total_rows = 0
for idx, sensor_row in enumerate(sensor_iter, start=1):
    sensor_id = sensor_row.sensor_id
    location_id = sensor_row.location_id
    parameter_id = sensor_row.parameter_id

    print(f"\n🔍 Fetching data for sensor {sensor_id} ({idx} of {total_sensors})")
    measurements = fetch_measurements(sensor_id, date_from, date_to)

    if not measurements:
        continue

    rows_fetched = len(measurements)
    print(f"➡️ Ingesting {rows_fetched} measurements for sensor {sensor_id} (location {location_id}, parameter {parameter_id})")

    # Create single row with all measurements as JSON array
    row = (
        batch_id,
        sensor_id,
        parameter_id,
        location_id,
        COUNTRY_CODE,
        ingestion_datetime.isoformat(),
        date_from.isoformat(),
        date_to.isoformat(),
        rows_fetched,
        json.dumps(measurements)  # All measurements as JSON array
    )
    
    df = spark.createDataFrame([row], SCHEMA_BRONZE_BRONZE_TABLE_MEASUREMENTS)
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE_MEASUREMENTS)
    total_rows += 1

step2_elapsed = time.time() - step2_start
print(f"✅ Ingestion complete — {total_rows} sensor batches written to {BRONZE_TABLE_MEASUREMENTS}")
print(f"⏱️ Ingest step total duration: {fmt_secs(step2_elapsed)} ({step2_elapsed:.2f}s)")
