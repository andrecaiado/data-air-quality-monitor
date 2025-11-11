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
from databricks.connect import DatabricksSession

# Load environment variables from .env file
load_dotenv()

# --------------------------------------
# Spark setup
# --------------------------------------
spark = DatabricksSession.builder.getOrCreate()

# --------------------------------------
# Arguments input
# --------------------------------------
parser = argparse.ArgumentParser(description="Ingest OpenAQ measurements for sensors filtered by country, with optional sharding.")
parser.add_argument("--country", "--country_code", dest="country_code", help="ISO country code (e.g. PT, ES)")
parser.add_argument("--shard-index", type=int, help="Zero-based shard index")
parser.add_argument("--shard-count", type=int, help="Total number of shards")
args, _ = parser.parse_known_args()
COUNTRY_CODE = (args.country_code or os.getenv("COUNTRY_CODE") or "PT").upper()
SHARD_INDEX = args.shard_index
SHARD_COUNT = args.shard_count
if (SHARD_INDEX is None) != (SHARD_COUNT is None):
    raise SystemExit("Provide both --shard-index and --shard-count or neither.")
if SHARD_COUNT is not None and (SHARD_INDEX < 0 or SHARD_INDEX >= SHARD_COUNT):
    raise SystemExit("Invalid shard index.")

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
HOURS_BACK = 4  # Fetch last 4 hours of data
PAGE_LIMIT = 1000  # API pagination size
HEADERS = {'x-api-key': os.getenv("OPENAQ_API_KEY", "")}

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
    StructField("ingestion_time", StringType(), True),
    StructField("date_from", StringType(), True),
    StructField("date_to", StringType(), True),
    StructField("rows_fetched", IntegerType(), True),
    StructField("api_payload", StringType(), True)
])

if not spark.catalog.tableExists(BRONZE_TABLE_MEASUREMENTS):
    spark.createDataFrame([], SCHEMA_BRONZE_BRONZE_TABLE_MEASUREMENTS).write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE_MEASUREMENTS)
    print(f"✅ Created empty Delta table: {BRONZE_TABLE_MEASUREMENTS}")

# --------------------------------------
# Function: Fetch last ingestion date_to
# --------------------------------------
def get_last_ingestion_date_to(country_code):
    """Fetches the last ingestion date_to from the bronze table for a specific country."""
    row = (spark.table(BRONZE_TABLE_MEASUREMENTS)
                 .filter(F.col("country_code") == country_code)
                 .agg(F.max("date_to").alias("last_date_to"))
                 .collect()[0])
    val = row["last_date_to"]

    return datetime.fromisoformat(val) if val else None

# --------------------------------------
# Define ingestion window
# --------------------------------------
date_to = datetime.now(timezone.utc)
last_ingestion_date_to = get_last_ingestion_date_to(COUNTRY_CODE)
date_from = last_ingestion_date_to if last_ingestion_date_to is not None else date_to - timedelta(hours=HOURS_BACK)

print(f"📅 Fetching data from {date_from.isoformat()} to {date_to.isoformat()}")

# --------------------------------------
# Function: Fetch measurements
# --------------------------------------
def fetch_measurements(sensor_id, start, end):
    """Fetches all measurements for a given sensor within a time window."""
    page = 1
    results = []
    end_formatted = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_formatted = start.strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        url = f"{OPENAQ_API_BASE_URL}/sensors/{sensor_id}/measurements"
        headers = HEADERS
        params = {
            "limit": PAGE_LIMIT,
            "page": page,
            "datetime_from": start_formatted,
            "datetime_to": end_formatted
        }

        time.sleep(1.05)  # # added delay to be nice to the API and avoid rate limits

        r = requests.get(url, headers=headers, params=params)
        if r.status_code != 200:
            print(f"⚠️ Failed for sensor {sensor_id}: {r.status_code}")
            break
        else:
            elapsed = r.elapsed.total_seconds()
            print(f"⏱️ Fetched page {page} for sensor {sensor_id} in {elapsed:.2f}s")

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
    
    if SHARD_COUNT is not None:
        joined = joined.filter(F.col("sensor_id") % F.lit(SHARD_COUNT) == F.lit(SHARD_INDEX))
        print(f"🔀 Shard {SHARD_INDEX}/{SHARD_COUNT} active.")

    total = joined.count()
    return joined.toLocalIterator(), total

# --------------------------------------
# Step 1: Fetch sensors for the specified country
# --------------------------------------
sensor_iter, total_sensors = fetch_sensors(COUNTRY_CODE)

# --------------------------------------
# Step 2: Ingest measurements for each sensor into bronze table
# --------------------------------------
batch_id = str(uuid.uuid4())
ingestion_time = datetime.now(timezone.utc)

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
        ingestion_time.isoformat(),
        date_from.isoformat(),
        date_to.isoformat(),
        rows_fetched,
        json.dumps(measurements)  # All measurements as JSON array
    )
    
    df = spark.createDataFrame([row], SCHEMA_BRONZE_BRONZE_TABLE_MEASUREMENTS)
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE_MEASUREMENTS)
    total_rows += 1

print(f"✅ Ingestion complete — {total_rows} sensor batches written to {BRONZE_TABLE_MEASUREMENTS}")

# Stop Spark session to avoid Python 3.13 threading cleanup warnings
spark.stop()